use std::sync::Arc;
use rocksdb::{TransactionDB, TransactionOptions, WriteOptions};
use tracing::error;
use super::super::mce::{VersionedKey, encode_mce};
use super::types::{GetResult, OpResult};

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
                if value == b"__DELETED__" {
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
        const TOMBSTONE_VALUE: &[u8] = b"__DELETED__";

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
                                if value.as_ref() != b"__DELETED__" {
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