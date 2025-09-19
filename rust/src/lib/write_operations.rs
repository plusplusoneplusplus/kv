use std::sync::Arc;
use tracing::{debug, warn};

use crate::lib::db::{AtomicCommitRequest, AtomicCommitResult, OpResult, AtomicOperation, FaultInjectionConfig};
use crate::lib::db_trait::KvDatabase;
use crate::lib::operations::{KvOperation, OperationResult, DatabaseOperation};

/// Write operations that modify database state and require consensus in a distributed system.
/// These operations must go through the leader and consensus mechanism in a multi-node setup.
pub struct KvWriteOperations {
    database: Arc<dyn KvDatabase>,
    verbose: bool,
}

impl KvWriteOperations {
    pub fn new(database: Arc<dyn KvDatabase>, verbose: bool) -> Self {
        Self { database, verbose }
    }

    /// Helper to convert async trait calls to sync by blocking on them
    fn run_async<F, R>(&self, future: F) -> R
    where
        F: std::future::Future<Output = R>,
    {
        // Try to use the current tokio runtime if available
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            // If we're already in a tokio runtime, try block_in_place first
            if std::thread::current().name().map_or(false, |name| name.contains("tokio")) {
                tokio::task::block_in_place(|| handle.block_on(future))
            } else {
                // If not in a tokio thread, just use the handle directly
                handle.block_on(future)
            }
        } else {
            // If we're not in a tokio context, create a new runtime
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(future)
        }
    }

    /// Execute a write operation
    pub fn execute(&self, operation: KvOperation) -> Result<OperationResult, String> {
        if operation.is_read_only() {
            return Err("Operation is not a write operation".to_string());
        }

        match operation {
            KvOperation::Set { key, value, column_family } => {
                if self.verbose {
                    debug!("Put operation: key={:?}, value_len={}, column_family={:?}", key, value.len(), column_family);
                }

                let result = self.run_async(self.database.put(&key, &value, column_family.as_deref()));

                if self.verbose {
                    debug!("Put result: success={}, error_code={:?}", result.success, result.error_code);
                    if !result.error.is_empty() {
                        warn!("Put error: {}", result.error);
                    }
                }

                Ok(OperationResult::OpResult(result))
            }

            KvOperation::Delete { key, column_family } => {
                if self.verbose {
                    debug!("Delete operation: key={:?}, column_family={:?}", key, column_family);
                }

                let result = self.run_async(self.database.delete(&key, column_family.as_deref()));

                if self.verbose {
                    debug!("Delete result: success={}, error_code={:?}", result.success, result.error_code);
                    if !result.error.is_empty() {
                        warn!("Delete error: {}", result.error);
                    }
                }

                Ok(OperationResult::OpResult(result))
            }

            KvOperation::AtomicCommit { request } => {
                if self.verbose {
                    debug!("Atomic commit: read_version={}, operations_count={}, read_conflict_keys_count={}, timeout={}s",
                           request.read_version, request.operations.len(), request.read_conflict_keys.len(), request.timeout_seconds);
                }

                let result = self.run_async(self.database.atomic_commit(request));

                if self.verbose {
                    debug!("Atomic commit result: success={}, error_code={:?}, committed_version={:?}",
                           result.success, result.error_code, result.committed_version);
                    if !result.error.is_empty() {
                        warn!("Atomic commit error: {}", result.error);
                    }
                }

                Ok(OperationResult::AtomicCommitResult(result))
            }

            KvOperation::SetVersionstampedKey { key_prefix, value, column_family } => {
                if self.verbose {
                    debug!("Set versionstamped key: key_prefix={:?}, value_len={}, column_family={:?}",
                           key_prefix, value.len(), column_family);
                }

                // Get current read version for the transaction
                let read_version = self.run_async(self.database.get_read_version());

                // Create a single versionstamped operation
                let versionstamp_operation = AtomicOperation {
                    op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
                    key: key_prefix,
                    value: Some(value),
                    column_family,
                };

                // Create atomic commit request with this single operation
                let atomic_request = AtomicCommitRequest {
                    read_version,
                    operations: vec![versionstamp_operation],
                    read_conflict_keys: vec![], // No read conflicts for single versionstamp operation
                    timeout_seconds: 60, // Default timeout
                };

                let result = self.run_async(self.database.atomic_commit(atomic_request));

                if self.verbose {
                    debug!("Versionstamped key result: success={}, generated_keys_count={}, committed_version={:?}",
                           result.success, result.generated_keys.len(), result.committed_version);
                    if !result.error.is_empty() {
                        warn!("Versionstamped key error: {}", result.error);
                    }
                }

                Ok(OperationResult::AtomicCommitResult(result))
            }

            KvOperation::SetVersionstampedValue { key, value_prefix, column_family } => {
                if self.verbose {
                    debug!("Set versionstamped value: key={:?}, value_prefix_len={}, column_family={:?}",
                           key, value_prefix.len(), column_family);
                }

                // Get current read version for the transaction
                let read_version = self.run_async(self.database.get_read_version());

                // Create a single versionstamped value operation
                let versionstamp_operation = AtomicOperation {
                    op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
                    key,
                    value: Some(value_prefix),
                    column_family,
                };

                // Create atomic commit request with this single operation
                let atomic_request = AtomicCommitRequest {
                    read_version,
                    operations: vec![versionstamp_operation],
                    read_conflict_keys: vec![], // No read conflicts for single versionstamp operation
                    timeout_seconds: 60, // Default timeout
                };

                let result = self.run_async(self.database.atomic_commit(atomic_request));

                if self.verbose {
                    debug!("Versionstamped value result: success={}, generated_values_count={}, committed_version={:?}",
                           result.success, result.generated_values.len(), result.committed_version);
                    if !result.error.is_empty() {
                        warn!("Versionstamped value error: {}", result.error);
                    }
                }

                Ok(OperationResult::AtomicCommitResult(result))
            }

            KvOperation::SetFaultInjection { config } => {
                if self.verbose {
                    debug!("Setting fault injection: config={:?}", config);
                }

                let result = self.run_async(self.database.set_fault_injection(config));

                if self.verbose {
                    debug!("Fault injection result: success={}", result.success);
                    if !result.error.is_empty() {
                        warn!("Fault injection error: {}", result.error);
                    }
                }

                Ok(OperationResult::OpResult(result))
            }

            _ => Err("Invalid write operation".to_string()),
        }
    }

    /// Individual operation methods for backward compatibility

    pub fn put(&self, key: &[u8], value: &[u8], column_family: Option<&str>) -> OpResult {
        let operation = KvOperation::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            column_family: column_family.map(|s| s.to_string()),
        };

        match self.execute(operation).expect("Put should not fail") {
            OperationResult::OpResult(result) => result,
            _ => panic!("Unexpected result type for Put"),
        }
    }

    pub fn delete(&self, key: &[u8], column_family: Option<&str>) -> OpResult {
        let operation = KvOperation::Delete {
            key: key.to_vec(),
            column_family: column_family.map(|s| s.to_string()),
        };

        match self.execute(operation).expect("Delete should not fail") {
            OperationResult::OpResult(result) => result,
            _ => panic!("Unexpected result type for Delete"),
        }
    }

    pub fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
        let operation = KvOperation::AtomicCommit { request };

        match self.execute(operation).expect("AtomicCommit should not fail") {
            OperationResult::AtomicCommitResult(result) => result,
            _ => panic!("Unexpected result type for AtomicCommit"),
        }
    }

    pub fn set_versionstamped_key(
        &self,
        key_prefix: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>,
    ) -> AtomicCommitResult {
        let operation = KvOperation::SetVersionstampedKey {
            key_prefix,
            value,
            column_family,
        };

        match self.execute(operation).expect("SetVersionstampedKey should not fail") {
            OperationResult::AtomicCommitResult(result) => result,
            _ => panic!("Unexpected result type for SetVersionstampedKey"),
        }
    }

    pub fn set_versionstamped_value(
        &self,
        key: Vec<u8>,
        value_prefix: Vec<u8>,
        column_family: Option<String>,
    ) -> AtomicCommitResult {
        let operation = KvOperation::SetVersionstampedValue {
            key,
            value_prefix,
            column_family,
        };

        match self.execute(operation).expect("SetVersionstampedValue should not fail") {
            OperationResult::AtomicCommitResult(result) => result,
            _ => panic!("Unexpected result type for SetVersionstampedValue"),
        }
    }

    pub fn set_fault_injection(&self, config: Option<FaultInjectionConfig>) -> OpResult {
        let operation = KvOperation::SetFaultInjection { config };

        match self.execute(operation).expect("SetFaultInjection should not fail") {
            OperationResult::OpResult(result) => result,
            _ => panic!("Unexpected result type for SetFaultInjection"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::operations::DatabaseOperation;

    #[test]
    fn test_write_operation_validation() {
        // All operations accepted by KvWriteOperations should be write operations
        let write_ops = vec![
            KvOperation::Set { key: b"key".to_vec(), value: b"value".to_vec(), column_family: None },
            KvOperation::Delete { key: b"key".to_vec(), column_family: None },
            KvOperation::SetFaultInjection { config: None },
        ];

        for op in write_ops {
            assert!(!op.is_read_only(), "Operation should be write operation: {:?}", op);
        }
    }

    #[test]
    fn test_read_operation_rejection() {
        use std::collections::HashMap;
        use std::sync::Mutex;
        use async_trait::async_trait;
        use crate::lib::db::GetResult;

        // Mock database for testing
        #[derive(Debug)]
        struct MockKvDatabase {
            data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
        }

        #[async_trait]
        impl crate::lib::db_trait::KvDatabase for MockKvDatabase {
            async fn get(&self, key: &[u8], _column_family: Option<&str>) -> Result<GetResult, String> {
                let data = self.data.lock().unwrap();
                match data.get(key) {
                    Some(value) => Ok(GetResult { value: value.clone(), found: true }),
                    None => Ok(GetResult { value: Vec::new(), found: false }),
                }
            }

            async fn put(&self, key: &[u8], value: &[u8], _column_family: Option<&str>) -> OpResult {
                let mut data = self.data.lock().unwrap();
                data.insert(key.to_vec(), value.to_vec());
                OpResult { success: true, error: String::new(), error_code: None }
            }

            async fn delete(&self, _key: &[u8], _column_family: Option<&str>) -> OpResult {
                OpResult { success: true, error: String::new(), error_code: None }
            }

            async fn list_keys(&self, _prefix: &[u8], _limit: u32, _column_family: Option<&str>) -> Result<Vec<Vec<u8>>, String> {
                Ok(vec![])
            }

            async fn get_range(
                &self, _begin_key: &[u8], _end_key: &[u8], _begin_offset: i32, _begin_or_equal: bool,
                _end_offset: i32, _end_or_equal: bool, _limit: Option<i32>, _column_family: Option<&str>,
            ) -> Result<crate::lib::db::GetRangeResult, String> {
                Ok(crate::lib::db::GetRangeResult { key_values: vec![], success: true, error: String::new(), has_more: false })
            }

            async fn atomic_commit(&self, _request: AtomicCommitRequest) -> AtomicCommitResult {
                AtomicCommitResult {
                    success: true, error: String::new(), error_code: None,
                    committed_version: Some(1), generated_keys: vec![], generated_values: vec![],
                }
            }

            async fn get_read_version(&self) -> u64 { 1 }

            async fn snapshot_read(&self, key: &[u8], _read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
                self.get(key, column_family).await
            }

            async fn snapshot_get_range(
                &self, begin_key: &[u8], end_key: &[u8], begin_offset: i32, begin_or_equal: bool,
                end_offset: i32, end_or_equal: bool, _read_version: u64, limit: Option<i32>, column_family: Option<&str>,
            ) -> Result<crate::lib::db::GetRangeResult, String> {
                self.get_range(begin_key, end_key, begin_offset, begin_or_equal, end_offset, end_or_equal, limit, column_family).await
            }

            async fn set_fault_injection(&self, _config: Option<FaultInjectionConfig>) -> OpResult {
                OpResult { success: true, error: String::new(), error_code: None }
            }
        }

        let mock_db = Arc::new(MockKvDatabase { data: Mutex::new(HashMap::new()) }) as Arc<dyn crate::lib::db_trait::KvDatabase>;
        let write_ops = KvWriteOperations::new(mock_db, false);

        // Try to execute a read operation - should fail
        let read_operation = KvOperation::Get {
            key: b"key".to_vec(),
            column_family: None,
        };

        let result = write_ops.execute(read_operation);
        assert!(result.is_err(), "Read operation should be rejected by write operations");
        assert_eq!(result.unwrap_err(), "Operation is not a write operation");
    }
}