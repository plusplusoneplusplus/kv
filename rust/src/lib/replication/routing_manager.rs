use std::sync::Arc;

use crate::lib::db_trait::KvDatabase;
use crate::lib::config::DeploymentMode;
use crate::lib::operations::{KvOperation, DatabaseOperation, OperationType, OperationResult};
use super::errors::{RoutingError, RoutingResult};

/// Central routing manager that decides how to handle operations based on deployment mode
/// and operation type. In Phase 1, this simply forwards all operations to the local database.
/// Future phases will add consensus-based routing for write operations.
pub struct RoutingManager {
    database: Arc<dyn KvDatabase>,
    deployment_mode: DeploymentMode,
    #[allow(dead_code)]
    node_id: Option<u32>,
}

impl RoutingManager {
    /// Create a new routing manager
    pub fn new(
        database: Arc<dyn KvDatabase>,
        deployment_mode: DeploymentMode,
        node_id: Option<u32>,
    ) -> Self {
        Self {
            database,
            deployment_mode,
            node_id,
        }
    }

    /// Route an operation based on its type and current deployment mode
    pub async fn route_operation(&self, operation: KvOperation) -> RoutingResult<OperationResult> {
        match self.deployment_mode {
            DeploymentMode::Standalone => {
                // In standalone mode, execute everything locally
                self.execute_locally(operation).await
            }
            DeploymentMode::Replicated => {
                // In Phase 1 of replicated mode, still execute locally
                // TODO: In future phases, route writes through consensus
                match operation.operation_type() {
                    OperationType::Read => {
                        // Read operations can be served locally
                        self.execute_locally(operation).await
                    }
                    OperationType::Write => {
                        // Write operations - for now execute locally
                        // TODO: Route through consensus in future phases
                        self.execute_locally(operation).await
                    }
                }
            }
        }
    }

    /// Execute an operation directly on the local database
    async fn execute_locally(&self, operation: KvOperation) -> RoutingResult<OperationResult> {
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
                // For versionstamped operations, we need to generate the key
                // The key_prefix should contain placeholder bytes (usually null bytes) that we replace
                // with the versionstamp. In FoundationDB, this is typically 10 bytes.
                let timestamp = self.database.get_read_version().await;
                let mut full_key = key_prefix;

                // Find the placeholder bytes (usually trailing null bytes) and replace them
                // This is a simplified implementation - in reality, FoundationDB has specific rules
                // about where the versionstamp goes. For this test, we'll replace the last 10 bytes
                // if they exist and are null bytes.
                if full_key.len() >= 10 {
                    let len = full_key.len();
                    let last_10 = &full_key[len-10..];
                    if last_10.iter().all(|&b| b == 0) {
                        // Replace the last 10 bytes with timestamp (8 bytes) + 2-byte transaction order
                        full_key.truncate(len - 10);
                        full_key.extend_from_slice(&timestamp.to_be_bytes()); // 8 bytes
                        full_key.extend_from_slice(&[0u8, 0u8]); // 2 bytes for transaction order
                    }
                }

                let put_result = self.database.put(&full_key, &value, column_family.as_deref()).await;

                // Return AtomicCommitResult with generated key
                let atomic_result = crate::lib::db::AtomicCommitResult {
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
                // For versionstamped values, we need to generate the value
                // Similar to keys, we replace placeholder bytes with the versionstamp
                let timestamp = self.database.get_read_version().await;
                let mut full_value = value_prefix;

                // Find the placeholder bytes (usually trailing null bytes) and replace them
                if full_value.len() >= 10 {
                    let len = full_value.len();
                    let last_10 = &full_value[len-10..];
                    if last_10.iter().all(|&b| b == 0) {
                        // Replace the last 10 bytes with timestamp (8 bytes) + 2-byte transaction order
                        full_value.truncate(len - 10);
                        full_value.extend_from_slice(&timestamp.to_be_bytes()); // 8 bytes
                        full_value.extend_from_slice(&[0u8, 0u8]); // 2 bytes for transaction order
                    }
                }

                let put_result = self.database.put(&key, &full_value, column_family.as_deref()).await;

                // Return AtomicCommitResult with generated value
                let atomic_result = crate::lib::db::AtomicCommitResult {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::lib::config::DeploymentMode;
    use crate::lib::db::{GetResult, OpResult, GetRangeResult, AtomicCommitRequest, AtomicCommitResult, FaultInjectionConfig};
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct MockDatabase {
        data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
        version_counter: std::sync::atomic::AtomicU64,
    }

    impl MockDatabase {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
                version_counter: std::sync::atomic::AtomicU64::new(1),
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

        async fn list_keys(&self, prefix: &[u8], limit: u32, _column_family: Option<&str>) -> Result<Vec<Vec<u8>>, String> {
            let data = self.data.lock().unwrap();
            let mut matching_keys: Vec<Vec<u8>> = data
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect();
            matching_keys.sort();
            matching_keys.truncate(limit as usize);
            Ok(matching_keys)
        }

        async fn get_range(
            &self,
            begin_key: &[u8],
            end_key: &[u8],
            _begin_offset: i32,
            begin_or_equal: bool,
            _end_offset: i32,
            end_or_equal: bool,
            limit: Option<i32>,
            _column_family: Option<&str>,
        ) -> Result<GetRangeResult, String> {
            let data = self.data.lock().unwrap();
            let mut key_values: Vec<crate::lib::db::KeyValue> = data
                .iter()
                .filter(|(key, _)| {
                    let include_start = if begin_or_equal {
                        key.as_slice() >= begin_key
                    } else {
                        key.as_slice() > begin_key
                    };
                    let include_end = if end_or_equal {
                        key.as_slice() <= end_key
                    } else {
                        key.as_slice() < end_key
                    };
                    include_start && include_end
                })
                .map(|(k, v)| crate::lib::db::KeyValue {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect();

            key_values.sort_by(|a, b| a.key.cmp(&b.key));

            if let Some(limit) = limit {
                if limit > 0 {
                    key_values.truncate(limit as usize);
                }
            }

            Ok(GetRangeResult {
                key_values,
                success: true,
                error: String::new(),
                has_more: false,
            })
        }

        async fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
            let mut data = self.data.lock().unwrap();

            for operation in request.operations {
                match operation.op_type.as_str() {
                    "PUT" => {
                        if let Some(value) = operation.value {
                            data.insert(operation.key, value);
                        }
                    }
                    "DELETE" => {
                        data.remove(&operation.key);
                    }
                    _ => {}
                }
            }

            let committed_version = self.version_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

            AtomicCommitResult {
                success: true,
                error: String::new(),
                error_code: None,
                committed_version: Some(committed_version),
                generated_keys: Vec::new(),
                generated_values: Vec::new(),
            }
        }

        async fn get_read_version(&self) -> u64 {
            self.version_counter.load(std::sync::atomic::Ordering::SeqCst)
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
    }

    #[tokio::test]
    async fn test_routing_manager_standalone_mode() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let routing_manager = RoutingManager::new(mock_db, DeploymentMode::Standalone, None);

        // Test GET operation
        let get_op = KvOperation::Get {
            key: b"test_key".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(get_op).await;
        assert!(result.is_ok());

        // Test SET operation
        let set_op = KvOperation::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(set_op).await;
        assert!(result.is_ok());

        // Verify the value was set
        let get_op = KvOperation::Get {
            key: b"test_key".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(get_op).await;
        assert!(result.is_ok());

        if let Ok(OperationResult::GetResult(Ok(get_result))) = result {
            assert!(get_result.found);
            assert_eq!(get_result.value, b"test_value");
        } else {
            panic!("Expected successful get result");
        }
    }

    #[tokio::test]
    async fn test_routing_manager_replicated_mode() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let routing_manager = RoutingManager::new(mock_db, DeploymentMode::Replicated, Some(1));

        // Test that replicated mode works the same as standalone for now
        let set_op = KvOperation::Set {
            key: b"replicated_key".to_vec(),
            value: b"replicated_value".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(set_op).await;
        assert!(result.is_ok());

        let get_op = KvOperation::Get {
            key: b"replicated_key".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(get_op).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_routing_manager_ping_operation() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let routing_manager = RoutingManager::new(mock_db, DeploymentMode::Standalone, None);

        let ping_op = KvOperation::Ping {
            message: Some(b"hello".to_vec()),
            timestamp: Some(12345),
        };

        let result = routing_manager.route_operation(ping_op).await;
        assert!(result.is_ok());

        if let Ok(OperationResult::PingResult { message, client_timestamp, server_timestamp }) = result {
            assert_eq!(message, b"hello");
            assert_eq!(client_timestamp, 12345);
            assert!(server_timestamp > 0);
        } else {
            panic!("Expected successful ping result");
        }
    }
}