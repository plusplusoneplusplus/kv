use std::sync::Arc;
use std::collections::{HashMap, HashSet};
use rocksdb::{TransactionDB, TransactionOptions};
use tracing::error;
use super::super::mce::{VersionedKey, encode_mce};
use super::types::{GetRangeResult, KeyValue};
use super::utils;

/// Range operations for the database
pub struct RangeOperations;

impl RangeOperations {
    /// Helper function to check if a range operation has more results
    pub fn has_more_results(
        db: &Arc<TransactionDB>,
        current_version: &Arc<std::sync::atomic::AtomicU64>,
        begin_key: &[u8],
        end_key: &[u8],
        begin_offset: i32,
        end_offset: i32,
        last_returned_key: Option<&[u8]>,
        limit: usize
    ) -> bool {
        // For now, return false - this is a placeholder for the full implementation
        // The actual implementation would check if there are more keys beyond the current result set
        false
    }

    /// Calculate the effective start key for range operations
    pub fn calculate_effective_begin_key(begin_key: &[u8], begin_offset: i32) -> Vec<u8> {
        utils::calculate_offset_key(begin_key, begin_offset)
    }

    /// Calculate the effective end key for range operations
    pub fn calculate_effective_end_key(end_key: &[u8], end_offset: i32) -> Vec<u8> {
        utils::calculate_offset_key(end_key, end_offset)
    }

    /// Get the latest version of a key for MVCC operations
    pub fn get_latest_version_for_key(
        versioned_entries: &HashMap<Vec<u8>, (u64, Vec<u8>)>,
        key: &[u8],
        read_version: u64
    ) -> Option<(u64, Vec<u8>)> {
        if let Some((version, value)) = versioned_entries.get(key) {
            if *version <= read_version {
                Some((*version, value.clone()))
            } else {
                None
            }
        } else {
            None
        }
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
    fn test_range_operations() {
        let (_temp_dir, db) = setup_test_db("test_range_db");

        // Insert some test data
        let put_result = db.put(b"key001", b"value1");
        assert!(put_result.success);
        let put_result = db.put(b"key002", b"value2");
        assert!(put_result.success);
        let put_result = db.put(b"key003", b"value3");
        assert!(put_result.success);
        let put_result = db.put(b"other_key", b"other_value");
        assert!(put_result.success);

        // Test get_range with full parameters (FoundationDB-style)
        let range_result = db.get_range(b"key", b"key\xFF", 0, true, 0, false, Some(10));
        assert!(range_result.success);
        assert_eq!(range_result.key_values.len(), 3);

        // Test get_range with specific bounds
        let range_result = db.get_range(b"key001", b"key003", 0, true, 0, false, Some(10));
        assert!(range_result.success);
        assert_eq!(range_result.key_values.len(), 2); // key001 and key002, but not key003 (exclusive end)

        // Test snapshot get_range
        let read_version = db.get_read_version();
        let snapshot_range_result = db.snapshot_get_range(b"key", b"key\xFF", 0, true, 0, false, read_version, Some(10));
        assert!(snapshot_range_result.success);
        assert_eq!(snapshot_range_result.key_values.len(), 3);
    }

    #[test]
    fn test_offset_based_range_queries() {
        let (_temp_dir, db) = setup_test_db("test_offset_range_db");

        // Insert test data in specific order
        let test_keys = [
            b"key_00001".to_vec(),
            b"key_00005".to_vec(),
            b"key_00010".to_vec(),
            b"key_00015".to_vec(),
            b"key_00020".to_vec(),
            b"key_00025".to_vec(),
            b"key_00030".to_vec(),
        ];

        for (i, key) in test_keys.iter().enumerate() {
            let value = format!("value_{}", i).into_bytes();
            let put_result = db.put(key, &value);
            assert!(put_result.success, "Failed to insert key: {:?}", key);
        }

        // Test 1: Basic offset range query with inclusive bounds
        let begin_key = b"key_00005";
        let end_key = b"key_00020";
        let result = db.get_range(begin_key, end_key, 0, true, 0, true, Some(10));

        assert!(result.success, "Range query should succeed: {}", result.error);
        assert_eq!(result.key_values.len(), 4, "Should return 4 keys: key_00005, key_00010, key_00015, key_00020");
        assert_eq!(result.key_values[0].key, b"key_00005");
        assert_eq!(result.key_values[1].key, b"key_00010");
        assert_eq!(result.key_values[2].key, b"key_00015");
        assert_eq!(result.key_values[3].key, b"key_00020");

        // Test 2: Exclusive begin bound
        let result = db.get_range(begin_key, end_key, 0, false, 0, true, Some(10));
        assert!(result.success);
        assert_eq!(result.key_values.len(), 3, "Should exclude begin key");
        assert_eq!(result.key_values[0].key, b"key_00010");
        assert_eq!(result.key_values[1].key, b"key_00015");
        assert_eq!(result.key_values[2].key, b"key_00020");

        // Test 3: Exclusive end bound
        let result = db.get_range(begin_key, end_key, 0, true, 0, false, Some(10));
        assert!(result.success);
        assert_eq!(result.key_values.len(), 3, "Should exclude end key");
        assert_eq!(result.key_values[0].key, b"key_00005");
        assert_eq!(result.key_values[1].key, b"key_00010");
        assert_eq!(result.key_values[2].key, b"key_00015");

        // Test 4: Both exclusive bounds
        let result = db.get_range(begin_key, end_key, 0, false, 0, false, Some(10));
        assert!(result.success);
        assert_eq!(result.key_values.len(), 2, "Should exclude both bounds");
        assert_eq!(result.key_values[0].key, b"key_00010");
        assert_eq!(result.key_values[1].key, b"key_00015");
    }
}