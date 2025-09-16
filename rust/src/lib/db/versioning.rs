use std::sync::Arc;
use rocksdb::{TransactionDB, TransactionOptions};
use tracing::error;
use super::super::mce::{VersionedKey, Version, encode_mce};
use super::types::{GetResult, TOMBSTONE_MARKER};

/// Versioning operations for MVCC support
pub struct Versioning;

impl Versioning {
    /// Get the current read version for snapshot isolation
    pub fn get_read_version(current_version: &Arc<std::sync::atomic::AtomicU64>) -> u64 {
        current_version.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Snapshot read at specific version (used by client for consistent reads)
    pub fn snapshot_read(
        db: &Arc<TransactionDB>,
        key: &[u8],
        read_version: u64,
        _column_family: Option<&str>
    ) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

        // Get MCE prefix for the logical key
        let mce_prefix = encode_mce(key);

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&Default::default(), &txn_opts);

        let mut latest_version: Option<Version> = None;
        let mut latest_value: Option<Vec<u8>> = None;

        // Iterate over all keys with this MCE prefix
        let iter = txn.prefix_iterator(&mce_prefix);
        for result in iter {
            match result {
                Ok((encoded_key, value)) => {
                    // Try to decode the versioned key
                    if let Ok(versioned_key) = VersionedKey::decode(&encoded_key) {
                        // Verify exact key match and version constraint
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
                    error!("Failed to iterate versioned keys for snapshot read: {}", e);
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

    /// Check if a key has been modified since a given version (placeholder for conflict detection)
    pub fn has_key_been_modified_since(_key: &[u8], _since_version: u64) -> bool {
        // TODO: Implement proper conflict detection by checking if any versions
        // of the key exist that are greater than since_version
        // In a real system, this would check key-specific version metadata
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};
    use crate::lib::config::Config;
    use crate::TransactionalKvDatabase;
    use super::super::types::{AtomicOperation, AtomicCommitRequest};

    fn setup_test_db(db_name: &str) -> (TempDir, TransactionalKvDatabase) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join(db_name);
        let config = Config::default();
        let db = TransactionalKvDatabase::new(db_path.to_str().unwrap(), &config, &[]).unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_mvcc_integration_with_get_read_version() {
        let (_temp_dir, db) = setup_test_db("test_mvcc_integration_db");

        // Simulate client workflow: commit a versionstamped key, then read it back
        let vs_op = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: b"client_test_\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            value: Some(b"test_value".to_vec()),
            column_family: None,
        };

        let initial_read_version = db.get_read_version();
        let commit_request = AtomicCommitRequest {
            read_version: initial_read_version,
            operations: vec![vs_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };

        // Perform atomic commit
        let commit_result = db.atomic_commit(commit_request);
        assert!(commit_result.success, "Atomic commit should succeed: {}", commit_result.error);
        assert_eq!(commit_result.generated_keys.len(), 1, "Should have one generated key");

        let generated_key = &commit_result.generated_keys[0];

        // Simulate what the client does: get a NEW read version after commit
        let post_commit_read_version = db.get_read_version();
        println!("Initial read version: {}", initial_read_version);
        println!("Commit version: {:?}", commit_result.committed_version);
        println!("Post-commit read version: {}", post_commit_read_version);

        // Ensure read version has advanced
        assert!(post_commit_read_version > initial_read_version,
            "Read version should advance after commit. Initial: {}, Post: {}",
            initial_read_version, post_commit_read_version);

        // Now try to read the data back using get() (which uses current read version)
        let get_result = db.get(generated_key);
        assert!(get_result.is_ok(), "Get should succeed: {:?}", get_result);
        let get_result = get_result.unwrap();

        println!("Get result found: {}", get_result.found);
        if get_result.found {
            println!("Get result value: {:?}", get_result.value);
        }

        assert!(get_result.found, "Generated key should be found after commit");
        assert_eq!(get_result.value, b"test_value", "Value should match what was stored");
    }
}