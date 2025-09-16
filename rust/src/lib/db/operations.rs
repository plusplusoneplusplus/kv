use std::sync::Arc;
use rocksdb::{TransactionDB, TransactionOptions, WriteOptions};
use tracing::error;
use super::super::mce::{VersionedKey, encode_mce};
use super::types::{GetResult, OpResult, TOMBSTONE_MARKER};

/// Basic CRUD operations for the database
pub struct Operations;

impl Operations {
    pub fn get(
        db: &Arc<TransactionDB>,
        current_version: &Arc<std::sync::atomic::AtomicU64>,
        key: &[u8]
    ) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

        let read_version = current_version.load(std::sync::atomic::Ordering::SeqCst);

        // Get MCE prefix for the logical key
        let mce_prefix = encode_mce(key);

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&Default::default(), &txn_opts);

        let mut latest_version: Option<u64> = None;
        let mut latest_value: Option<Vec<u8>> = None;

        // Iterate over all keys with this MCE prefix
        let iter = txn.prefix_iterator(&mce_prefix);
        for result in iter {
            match result {
                Ok((encoded_key, value)) => {
                    // Try to decode the versioned key
                    if let Ok(versioned_key) = VersionedKey::decode(&encoded_key) {
                        // Verify exact key match (MCE prefix matching should guarantee this)
                        if versioned_key.original_key == key && versioned_key.version <= read_version {
                            // Check if this is the latest version we've seen
                            if latest_version.is_none() || versioned_key.version > latest_version.unwrap() {
                                latest_version = Some(versioned_key.version);
                                latest_value = Some(value.to_vec());
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to iterate versioned keys: {}", e);
                    return Err(format!("failed to iterate keys: {}", e));
                }
            }
        }

        match latest_value {
            Some(value) => {
                // Check if the value is a tombstone (deletion marker)
                if value == TOMBSTONE_MARKER {
                    Ok(GetResult {
                        value: Vec::new(),
                        found: false, // Treat tombstones as "not found"
                    })
                } else {
                    Ok(GetResult {
                        value,
                        found: true,
                    })
                }
            }
            None => Ok(GetResult {
                value: Vec::new(),
                found: false,
            }),
        }
    }

    pub fn put(
        db: &Arc<TransactionDB>,
        current_version: &Arc<std::sync::atomic::AtomicU64>,
        key: &[u8],
        value: &[u8]
    ) -> OpResult {
        if key.is_empty() {
            return OpResult {
                success: false,
                error: "key cannot be empty".to_string(),
                error_code: Some("INVALID_KEY".to_string())
            };
        }

        // Assign version automatically
        let version = current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Create versioned key using MCE encoding
        let versioned_key = VersionedKey::new_u64(key.to_vec(), version);
        let encoded_key = versioned_key.encode();

        // Create a write transaction
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&WriteOptions::default(), &txn_opts);

        match txn.put(&encoded_key, value) {
            Ok(()) => {
                match txn.commit() {
                    Ok(()) => OpResult::success(),
                    Err(e) => {
                        error!("Failed to commit versioned put: {}", e);
                        OpResult {
                            success: false,
                            error: format!("failed to commit: {}", e),
                            error_code: Some("COMMIT_FAILED".to_string())
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to put versioned value: {}", e);
                OpResult {
                    success: false,
                    error: format!("failed to put value: {}", e),
                    error_code: Some("PUT_FAILED".to_string())
                }
            }
        }
    }

    pub fn delete(
        db: &Arc<TransactionDB>,
        current_version: &Arc<std::sync::atomic::AtomicU64>,
        key: &[u8]
    ) -> OpResult {
        if key.is_empty() {
            return OpResult {
                success: false,
                error: "key cannot be empty".to_string(),
                error_code: Some("INVALID_KEY".to_string())
            };
        }

        // Use a special tombstone value to mark deletion
        const TOMBSTONE_VALUE: &[u8] = TOMBSTONE_MARKER;

        // Assign version automatically
        let version = current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        // Create versioned key using MCE encoding
        let versioned_key = VersionedKey::new_u64(key.to_vec(), version);
        let encoded_key = versioned_key.encode();

        // Create a write transaction
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&WriteOptions::default(), &txn_opts);

        match txn.put(&encoded_key, TOMBSTONE_VALUE) {
            Ok(()) => {
                match txn.commit() {
                    Ok(()) => OpResult::success(),
                    Err(e) => {
                        error!("Failed to commit versioned delete: {}", e);
                        OpResult {
                            success: false,
                            error: format!("failed to commit: {}", e),
                            error_code: Some("COMMIT_FAILED".to_string())
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to delete versioned value: {}", e);
                OpResult {
                    success: false,
                    error: format!("failed to delete value: {}", e),
                    error_code: Some("DELETE_FAILED".to_string())
                }
            }
        }
    }

    pub fn list_keys(
        db: &Arc<TransactionDB>,
        current_version: &Arc<std::sync::atomic::AtomicU64>,
        prefix: &[u8],
        limit: u32
    ) -> Result<Vec<Vec<u8>>, String> {
        let read_version = current_version.load(std::sync::atomic::Ordering::SeqCst);
        let mut keys = Vec::new();
        let mut seen_keys = std::collections::HashSet::new();

        // Get MCE prefix for search
        let mce_prefix = encode_mce(prefix);

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&Default::default(), &txn_opts);

        // Iterate through all versioned keys with this prefix
        let iter = txn.prefix_iterator(&mce_prefix);
        for result in iter {
            match result {
                Ok((encoded_key, value)) => {
                    if let Ok(versioned_key) = VersionedKey::decode(&encoded_key) {
                        // Check if key starts with the requested prefix and is within read version
                        if versioned_key.original_key.starts_with(prefix) && versioned_key.version <= read_version {
                            // Only include if we haven't seen this logical key yet
                            if !seen_keys.contains(&versioned_key.original_key) {
                                // Skip tombstones
                                if value.as_ref() != TOMBSTONE_MARKER {
                                    keys.push(versioned_key.original_key.clone());
                                    seen_keys.insert(versioned_key.original_key);

                                    if keys.len() >= limit as usize {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to iterate keys: {}", e);
                    return Err(format!("failed to iterate keys: {}", e));
                }
            }
        }

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};
    use crate::lib::config::Config;
    use crate::TransactionalKvDatabase;

    fn setup_test_db(db_name: &str) -> (TempDir, TransactionalKvDatabase) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join(db_name);
        let config = Config::default();
        let db = TransactionalKvDatabase::new(db_path.to_str().unwrap(), &config, &[]).unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_mvcc_basic_operations() {
        let (_temp_dir, db) = setup_test_db("mvcc_basic");

        // Test basic put and get with MVCC backend
        let key = b"user:alice";
        let value1 = b"alice_v1";
        let value2 = b"alice_v2";

        // Put first value
        let result = db.put(key, value1);
        assert!(result.success, "First put should succeed");

        // Put second value (will get a newer version)
        let result = db.put(key, value2);
        assert!(result.success, "Second put should succeed");

        // Get should return the latest value
        let result = db.get(key).unwrap();
        assert!(result.found, "Should find value");
        assert_eq!(result.value, value2, "Should get latest value");
    }

    #[test]
    fn test_mvcc_delete_operations() {
        let (_temp_dir, db) = setup_test_db("mvcc_delete");

        let key = b"test_key";
        let value = b"test_value";

        // Put a value
        let result = db.put(key, value);
        assert!(result.success, "Put should succeed");

        // Verify it exists
        let result = db.get(key).unwrap();
        assert!(result.found, "Should find value");
        assert_eq!(result.value, value, "Should get correct value");

        // Delete the key
        let result = db.delete(key);
        assert!(result.success, "Delete should succeed");

        // Verify it's gone
        let result = db.get(key).unwrap();
        assert!(!result.found, "Should not find deleted value");
    }

    #[test]
    fn test_mvcc_key_isolation() {
        let (_temp_dir, db) = setup_test_db("mvcc_isolation");

        // Test that different keys don't interfere with each other
        db.put(b"user", b"admin");
        db.put(b"user:", b"separator");
        db.put(b"user:alice", b"alice_data");
        db.put(b"user:alice:profile", b"profile_data");
        db.put(b"users", b"users_table");

        // Each key should be completely isolated
        assert_eq!(db.get(b"user").unwrap().value, b"admin");
        assert_eq!(db.get(b"user:").unwrap().value, b"separator");
        assert_eq!(db.get(b"user:alice").unwrap().value, b"alice_data");
        assert_eq!(db.get(b"user:alice:profile").unwrap().value, b"profile_data");
        assert_eq!(db.get(b"users").unwrap().value, b"users_table");
    }
}