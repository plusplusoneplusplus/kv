use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use tokio::sync::{RwLock, oneshot};

use kv_storage_api::KvDatabase;
use crate::lib::operations::{KvOperation, OperationResult};
use super::errors::{RoutingError, RoutingResult};

/// State Machine Executor that applies consensus decisions to the local database.
/// This component is responsible for:
/// - Tracking the sequence of applied operations to maintain consistency
/// - Applying operations to the local database in the correct order
/// - Managing pending operation responses for synchronous request handling
/// - Separating local execution from consensus application
pub struct KvStoreExecutor {
    /// The underlying database where operations are applied
    database: Arc<dyn KvDatabase>,

    /// Sequence number of the last applied operation
    /// This ensures operations are applied in the correct order
    applied_sequence: Arc<AtomicU64>,

    /// Pending responses for operations waiting for consensus
    /// Maps sequence number to response sender
    pending_responses: Arc<RwLock<HashMap<u64, oneshot::Sender<OperationResult>>>>,
}

impl KvStoreExecutor {
    /// Create a new State Machine Executor
    pub fn new(database: Arc<dyn KvDatabase>) -> Self {
        Self {
            database,
            applied_sequence: Arc::new(AtomicU64::new(0)),
            pending_responses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the current applied sequence number
    pub fn get_applied_sequence(&self) -> u64 {
        self.applied_sequence.load(Ordering::SeqCst)
    }

    /// Get access to the underlying database
    pub fn database(&self) -> &Arc<dyn KvDatabase> {
        &self.database
    }

    /// Check if an operation with the given sequence has been applied
    pub fn is_applied(&self, sequence: u64) -> bool {
        self.get_applied_sequence() >= sequence
    }

    /// Apply an operation to the local database as part of the state machine
    /// This method should only be called by the consensus system when an operation
    /// has been agreed upon by the cluster.
    pub async fn apply_operation(&self, sequence: u64, operation: KvOperation) -> RoutingResult<OperationResult> {
        // Ensure operations are applied in order
        let expected_sequence = self.get_applied_sequence() + 1;
        if sequence != expected_sequence {
            return Err(RoutingError::SequenceError(format!(
                "Expected sequence {}, got {}",
                expected_sequence, sequence
            )));
        }

        // Apply the operation to the database
        let result = self.execute_on_database(operation).await?;

        // Update the applied sequence atomically
        self.applied_sequence.store(sequence, Ordering::SeqCst);

        // Notify any pending request waiting for this operation
        if let Some(sender) = self.pending_responses.write().await.remove(&sequence) {
            // Ignore if the receiver has been dropped
            let _ = sender.send(result.clone());
        }

        Ok(result)
    }

    /// Execute an operation directly on the database
    /// This is similar to the routing manager's execute_locally but focused on state machine execution
    pub async fn execute_on_database(&self, operation: KvOperation) -> RoutingResult<OperationResult> {
        match operation {
            KvOperation::Get { key, column_family } => {
                let result = self.database.get(&key, column_family.as_deref()).await
                    .map_err(|e| RoutingError::DatabaseError(e))?;
                Ok(OperationResult::GetResult(Ok(result)))
            }

            KvOperation::GetRange {
                begin_key,
                end_key,
                begin_offset,
                begin_or_equal,
                end_offset,
                end_or_equal,
                limit,
                column_family,
            } => {
                let result = self.database.get_range(
                    &begin_key,
                    &end_key,
                    begin_offset,
                    begin_or_equal,
                    end_offset,
                    end_or_equal,
                    limit,
                    column_family.as_deref(),
                ).await
                .map_err(|e| RoutingError::DatabaseError(e))?;
                Ok(OperationResult::GetRangeResult(result))
            }

            KvOperation::SnapshotRead { key, read_version, column_family } => {
                let result = self.database.snapshot_read(&key, read_version, column_family.as_deref()).await
                    .map_err(|e| RoutingError::DatabaseError(e))?;
                Ok(OperationResult::GetResult(Ok(result)))
            }

            KvOperation::SnapshotGetRange {
                begin_key,
                end_key,
                begin_offset,
                begin_or_equal,
                end_offset,
                end_or_equal,
                read_version,
                limit,
                column_family,
            } => {
                let result = self.database.snapshot_get_range(
                    &begin_key,
                    &end_key,
                    begin_offset,
                    begin_or_equal,
                    end_offset,
                    end_or_equal,
                    read_version,
                    limit,
                    column_family.as_deref(),
                ).await
                .map_err(|e| RoutingError::DatabaseError(e))?;
                Ok(OperationResult::GetRangeResult(result))
            }

            KvOperation::GetReadVersion => {
                let version = self.database.get_read_version().await;
                Ok(OperationResult::ReadVersion(version))
            }

            KvOperation::Ping { message, timestamp } => {
                let server_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);

                let response_message = message.unwrap_or_else(|| b"pong".to_vec());
                let client_timestamp = timestamp.unwrap_or(0);

                Ok(OperationResult::PingResult {
                    message: response_message,
                    client_timestamp,
                    server_timestamp,
                })
            }

            KvOperation::Set { key, value, column_family } => {
                let result = self.database.put(&key, &value, column_family.as_deref()).await;
                Ok(OperationResult::OpResult(result))
            }

            KvOperation::Delete { key, column_family } => {
                let result = self.database.delete(&key, column_family.as_deref()).await;
                Ok(OperationResult::OpResult(result))
            }

            KvOperation::AtomicCommit { request } => {
                let result = self.database.atomic_commit(request).await;
                Ok(OperationResult::AtomicCommitResult(result))
            }

            KvOperation::SetVersionstampedKey { key_prefix, value, column_family } => {
                // Generate versionstamped key
                let timestamp = self.database.get_read_version().await;
                let mut full_key = key_prefix;

                if full_key.len() >= 10 {
                    let len = full_key.len();
                    let last_10 = &full_key[len-10..];
                    if last_10.iter().all(|&b| b == 0) {
                        full_key.truncate(len - 10);
                        full_key.extend_from_slice(&timestamp.to_be_bytes());
                        full_key.extend_from_slice(&[0u8, 0u8]);
                    }
                }

                let put_result = self.database.put(&full_key, &value, column_family.as_deref()).await;

                let atomic_result = kv_storage_api::AtomicCommitResult {
                    success: put_result.success,
                    error: put_result.error,
                    error_code: put_result.error_code,
                    committed_version: Some(timestamp),
                    generated_keys: if put_result.success { vec![full_key] } else { vec![] },
                    generated_values: vec![],
                };
                Ok(OperationResult::AtomicCommitResult(atomic_result))
            }

            KvOperation::SetVersionstampedValue { key, value_prefix, column_family } => {
                // Generate versionstamped value
                let timestamp = self.database.get_read_version().await;
                let mut full_value = value_prefix;

                if full_value.len() >= 10 {
                    let len = full_value.len();
                    let last_10 = &full_value[len-10..];
                    if last_10.iter().all(|&b| b == 0) {
                        full_value.truncate(len - 10);
                        full_value.extend_from_slice(&timestamp.to_be_bytes());
                        full_value.extend_from_slice(&[0u8, 0u8]);
                    }
                }

                let put_result = self.database.put(&key, &full_value, column_family.as_deref()).await;

                let atomic_result = kv_storage_api::AtomicCommitResult {
                    success: put_result.success,
                    error: put_result.error,
                    error_code: put_result.error_code,
                    committed_version: Some(timestamp),
                    generated_keys: vec![],
                    generated_values: if put_result.success { vec![full_value] } else { vec![] },
                };
                Ok(OperationResult::AtomicCommitResult(atomic_result))
            }

            KvOperation::SetFaultInjection { config } => {
                let result = self.database.set_fault_injection(config).await;
                Ok(OperationResult::OpResult(result))
            }

            // Diagnostic operations - these are handled at the routing manager level
            // and should not reach the executor, but we need to handle them to avoid compile errors
            KvOperation::GetClusterHealth
            | KvOperation::GetDatabaseStats { .. }
            | KvOperation::GetNodeInfo { .. } => {
                Err(RoutingError::DatabaseError("Diagnostic operations should be handled at routing level".to_string()))
            }
        }
    }

    /// Register a pending response for an operation that will be processed through consensus
    /// Returns a receiver that will get the result once the operation is applied
    pub async fn register_pending_operation(&self, sequence: u64) -> oneshot::Receiver<OperationResult> {
        let (sender, receiver) = oneshot::channel();
        self.pending_responses.write().await.insert(sequence, sender);
        receiver
    }

    /// Check if there are any pending operations waiting for responses
    pub async fn has_pending_operations(&self) -> bool {
        !self.pending_responses.read().await.is_empty()
    }

    /// Get the count of pending operations
    pub async fn pending_operations_count(&self) -> usize {
        self.pending_responses.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use kv_storage_api::{GetResult, OpResult, GetRangeResult, AtomicCommitRequest, AtomicCommitResult, FaultInjectionConfig};
    use std::sync::Mutex;

    #[derive(Debug)]
    struct MockDatabase {
        data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    }

    impl MockDatabase {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
            }
        }
    }

    #[async_trait]
    impl KvDatabase for MockDatabase {
        async fn get(&self, key: &[u8], _column_family: Option<&str>) -> Result<GetResult, String> {
            let data = self.data.lock().unwrap();
            match data.get(key) {
                Some(value) => Ok(GetResult {
                    value: value.clone(),
                    found: true,
                }),
                None => Ok(GetResult {
                    value: Vec::new(),
                    found: false,
                }),
            }
        }

        async fn put(&self, key: &[u8], value: &[u8], _column_family: Option<&str>) -> OpResult {
            let mut data = self.data.lock().unwrap();
            data.insert(key.to_vec(), value.to_vec());
            OpResult {
                success: true,
                error: String::new(),
                error_code: None,
            }
        }

        async fn delete(&self, key: &[u8], _column_family: Option<&str>) -> OpResult {
            let mut data = self.data.lock().unwrap();
            data.remove(key);
            OpResult {
                success: true,
                error: String::new(),
                error_code: None,
            }
        }

        async fn list_keys(&self, _prefix: &[u8], _limit: u32, _column_family: Option<&str>) -> Result<Vec<Vec<u8>>, String> {
            Ok(vec![])
        }

        async fn get_range(
            &self,
            _begin_key: &[u8],
            _end_key: &[u8],
            _begin_offset: i32,
            _begin_or_equal: bool,
            _end_offset: i32,
            _end_or_equal: bool,
            _limit: Option<i32>,
            _column_family: Option<&str>,
        ) -> Result<GetRangeResult, String> {
            Ok(GetRangeResult {
                key_values: vec![],
                success: true,
                error: String::new(),
                has_more: false,
            })
        }

        async fn atomic_commit(&self, _request: AtomicCommitRequest) -> AtomicCommitResult {
            AtomicCommitResult {
                success: true,
                error: String::new(),
                error_code: None,
                committed_version: Some(1),
                generated_keys: Vec::new(),
                generated_values: Vec::new(),
            }
        }

        async fn get_read_version(&self) -> u64 {
            1
        }

        async fn snapshot_read(&self, key: &[u8], _read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
            self.get(key, column_family).await
        }

        async fn snapshot_get_range(
            &self,
            begin_key: &[u8],
            end_key: &[u8],
            begin_offset: i32,
            begin_or_equal: bool,
            end_offset: i32,
            end_or_equal: bool,
            _read_version: u64,
            limit: Option<i32>,
            column_family: Option<&str>,
        ) -> Result<GetRangeResult, String> {
            self.get_range(begin_key, end_key, begin_offset, begin_or_equal, end_offset, end_or_equal, limit, column_family).await
        }

        async fn set_fault_injection(&self, _config: Option<FaultInjectionConfig>) -> OpResult {
            OpResult {
                success: true,
                error: String::new(),
                error_code: None,
            }
        }

        async fn get_database_statistics(&self) -> Result<std::collections::HashMap<String, u64>, String> {
            let data = self.data.lock().unwrap();
            let mut stats = std::collections::HashMap::new();

            // Mock database statistics
            stats.insert("total_keys".to_string(), data.len() as u64);
            stats.insert("total_size_bytes".to_string(), (data.len() as u64) * 50); // Estimate 50 bytes per key
            stats.insert("active_transactions".to_string(), 0);
            stats.insert("committed_transactions".to_string(), 0);
            stats.insert("aborted_transactions".to_string(), 0);
            stats.insert("read_operations".to_string(), 0);
            stats.insert("write_operations".to_string(), 0);
            stats.insert("cache_hit_rate_percent".to_string(), 90);
            stats.insert("compaction_pending_bytes".to_string(), 0);
            stats.insert("average_response_time_ms".to_string(), 1);

            Ok(stats)
        }
    }

    #[tokio::test]
    async fn test_executor_creation() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let executor = KvStoreExecutor::new(mock_db);

        assert_eq!(executor.get_applied_sequence(), 0);
        assert!(!executor.is_applied(1));
        assert!(executor.is_applied(0));
    }

    #[tokio::test]
    async fn test_applied_sequence_tracking() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let executor = KvStoreExecutor::new(mock_db);

        // Initially no operations applied
        assert_eq!(executor.get_applied_sequence(), 0);

        // Test sequence checking
        assert!(executor.is_applied(0));  // 0 is considered "applied" (initial state)
        assert!(!executor.is_applied(1)); // 1 is not applied yet
        assert!(!executor.is_applied(5)); // 5 is not applied yet
    }

    #[tokio::test]
    async fn test_apply_operation_sequence_tracking() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let executor = KvStoreExecutor::new(mock_db);

        // Test applying operations in sequence
        let op1 = KvOperation::Set {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            column_family: None,
        };

        let result = executor.apply_operation(1, op1).await;
        assert!(result.is_ok());
        assert_eq!(executor.get_applied_sequence(), 1);
        assert!(executor.is_applied(1));
        assert!(!executor.is_applied(2));

        // Apply second operation
        let op2 = KvOperation::Set {
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
            column_family: None,
        };

        let result = executor.apply_operation(2, op2).await;
        assert!(result.is_ok());
        assert_eq!(executor.get_applied_sequence(), 2);
        assert!(executor.is_applied(2));
        assert!(!executor.is_applied(3));
    }

    #[tokio::test]
    async fn test_apply_operation_out_of_sequence_error() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let executor = KvStoreExecutor::new(mock_db);

        // Try to apply operation 3 before 1 and 2
        let op = KvOperation::Set {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            column_family: None,
        };

        let result = executor.apply_operation(3, op).await;
        assert!(result.is_err());

        if let Err(RoutingError::SequenceError(msg)) = result {
            assert!(msg.contains("Expected sequence 1, got 3"));
        } else {
            panic!("Expected SequenceError, got {:?}", result);
        }

        // Sequence should remain unchanged
        assert_eq!(executor.get_applied_sequence(), 0);
    }

    #[tokio::test]
    async fn test_pending_operation_management() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let executor = KvStoreExecutor::new(mock_db);

        // Initially no pending operations
        assert!(!executor.has_pending_operations().await);
        assert_eq!(executor.pending_operations_count().await, 0);

        // Register a pending operation
        let receiver = executor.register_pending_operation(1).await;
        assert!(executor.has_pending_operations().await);
        assert_eq!(executor.pending_operations_count().await, 1);

        // Apply the operation, which should notify the pending receiver
        let op = KvOperation::Set {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            column_family: None,
        };

        let apply_result = executor.apply_operation(1, op).await;
        assert!(apply_result.is_ok());

        // The pending operation should have been removed
        assert!(!executor.has_pending_operations().await);
        assert_eq!(executor.pending_operations_count().await, 0);

        // The receiver should have gotten the result
        let received_result = receiver.await;
        assert!(received_result.is_ok());

        match received_result.unwrap() {
            OperationResult::OpResult(op_result) => {
                assert!(op_result.success);
            }
            _ => panic!("Expected OpResult"),
        }
    }

    #[tokio::test]
    async fn test_get_operation_through_executor() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let executor = KvStoreExecutor::new(mock_db);

        // First, set a value through the executor
        let set_op = KvOperation::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            column_family: None,
        };

        let result = executor.apply_operation(1, set_op).await;
        assert!(result.is_ok());

        // Now get the value
        let get_op = KvOperation::Get {
            key: b"test_key".to_vec(),
            column_family: None,
        };

        let result = executor.apply_operation(2, get_op).await;
        assert!(result.is_ok());

        match result.unwrap() {
            OperationResult::GetResult(Ok(get_result)) => {
                assert!(get_result.found);
                assert_eq!(get_result.value, b"test_value");
            }
            _ => panic!("Expected successful GetResult"),
        }
    }
}