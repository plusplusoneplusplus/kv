use std::sync::{Arc, RwLock};
use rocksdb::{TransactionDB, WriteOptions, TransactionOptions};
use super::types::{AtomicCommitRequest, AtomicCommitResult, FaultInjectionConfig};
use super::utils;

/// Transaction management operations
pub struct Transactions;

impl Transactions {
    /// Check for fault injection before starting transaction
    pub fn check_fault_injection(
        fault_injection: &Arc<RwLock<Option<FaultInjectionConfig>>>,
        operation: &str
    ) -> Option<AtomicCommitResult> {
        if let Some(error_code) = utils::should_inject_fault(&fault_injection.read().unwrap(), operation) {
            let error_msg = match error_code.as_str() {
                "TIMEOUT" => "Operation timeout",
                "CONFLICT" => "Transaction conflict",
                _ => "Fault injected",
            };
            return Some(AtomicCommitResult {
                success: false,
                error: error_msg.to_string(),
                error_code: Some(error_code),
                committed_version: None,
                generated_keys: Vec::new(),
                generated_values: Vec::new(),
            });
        }
        None
    }

    /// Check for conflicts in read keys
    pub fn check_conflicts(
        request: &AtomicCommitRequest,
        current_version: u64
    ) -> Option<AtomicCommitResult> {
        if request.read_version < current_version {
            // In a real implementation, we'd check if specific read keys were modified
            // For now, we'll do a simplified version check
            for read_key in &request.read_conflict_keys {
                // Simplified conflict check - in reality this would check key-specific versions
                if super::versioning::Versioning::has_key_been_modified_since(read_key, request.read_version) {
                    return Some(AtomicCommitResult {
                        success: false,
                        error: format!("Conflict detected on key: {:?}", read_key),
                        error_code: Some("CONFLICT".to_string()),
                        committed_version: None,
                        generated_keys: Vec::new(),
                        generated_values: Vec::new(),
                    });
                }
            }
        }
        None
    }

    /// Create a new RocksDB transaction with appropriate options
    pub fn create_transaction(db: &Arc<TransactionDB>) -> rocksdb::Transaction<'_, TransactionDB> {
        let write_opts = WriteOptions::default();
        let mut txn_opts = TransactionOptions::default();
        txn_opts.set_snapshot(true);
        db.transaction_opt(&write_opts, &txn_opts)
    }

    /// Create success result for completed transaction
    pub fn create_success_result(
        commit_version: u64,
        generated_keys: Vec<Vec<u8>>,
        generated_values: Vec<Vec<u8>>
    ) -> AtomicCommitResult {
        AtomicCommitResult {
            success: true,
            error: String::new(),
            error_code: None,
            committed_version: Some(commit_version),
            generated_keys,
            generated_values,
        }
    }

    /// Create error result for failed transaction
    pub fn create_error_result(error_msg: &str, error_code: Option<&str>) -> AtomicCommitResult {
        AtomicCommitResult {
            success: false,
            error: error_msg.to_string(),
            error_code: error_code.map(|c| c.to_string()),
            committed_version: None,
            generated_keys: Vec::new(),
            generated_values: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};
    use crate::lib::config::Config;
    use crate::TransactionalKvDatabase;
    use crate::lib::db::types::{AtomicOperation, AtomicCommitRequest};

    fn setup_test_db(db_name: &str) -> (TempDir, TransactionalKvDatabase) {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join(db_name);
        let config = Config::default();
        let db = TransactionalKvDatabase::new(db_path.to_str().unwrap(), &config, &[]).unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_foundationdb_style_transactions() {
        let (_temp_dir, db) = setup_test_db("test_db");

        // Test get read version
        let read_version = db.get_read_version();
        assert!(read_version > 0);

        // Test snapshot read on non-existent key
        let result = db.snapshot_read(b"test_key", read_version, None);
        assert!(result.is_ok());
        assert!(!result.unwrap().found);

        // Test atomic commit with write operation
        let operations = vec![AtomicOperation {
            op_type: "set".to_string(),
            key: b"test_key".to_vec(),
            value: Some(b"test_value".to_vec()),
            column_family: None,
        }];

        let commit_request = AtomicCommitRequest {
            read_version,
            operations,
            read_conflict_keys: vec![],
            timeout_seconds: 60,
        };

        let commit_result = db.atomic_commit(commit_request);
        assert!(commit_result.success);
        assert!(commit_result.committed_version.is_some());

        // Test snapshot read on existing key
        let new_read_version = db.get_read_version();
        let result = db.snapshot_read(b"test_key", new_read_version, None);
        assert!(result.is_ok());
        let get_result = result.unwrap();
        assert!(get_result.found);
        assert_eq!(get_result.value, b"test_value".to_vec());
    }
}