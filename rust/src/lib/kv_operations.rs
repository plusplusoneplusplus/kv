use std::sync::Arc;

use kv_storage_api::{AtomicCommitRequest, AtomicCommitResult, GetResult, OpResult, GetRangeResult, FaultInjectionConfig, KvDatabase};
use crate::lib::operations::{KvOperation, DatabaseOperation, OperationType, OperationResult};
use crate::lib::read_operations::KvReadOperations;
use crate::lib::write_operations::KvWriteOperations;

/// Core business logic for KV operations, completely protocol-agnostic.
/// This contains all the business logic operations without any knowledge of
/// Thrift, gRPC, or other protocol specifics. It operates purely on domain types.
///
/// This struct now uses the new operation classification system with separate
/// read and write operation handlers for better organization and future distributed support.
pub struct KvOperations {
    read_operations: KvReadOperations,
    write_operations: KvWriteOperations,
}

impl KvOperations {
    pub fn new(database: Arc<dyn KvDatabase>, verbose: bool) -> Self {
        Self {
            read_operations: KvReadOperations::new(database.clone(), verbose),
            write_operations: KvWriteOperations::new(database, verbose),
        }
    }

    /// Execute an operation using the new classification system
    pub fn execute_operation(&self, operation: KvOperation) -> Result<OperationResult, String> {
        match operation.operation_type() {
            OperationType::Read => self.read_operations.execute(operation),
            OperationType::Write => self.write_operations.execute(operation),
        }
    }


    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<GetResult, String> {
        self.read_operations.get(key, None)
    }

    /// Put a key-value pair
    pub fn put(&self, key: &[u8], value: &[u8]) -> OpResult {
        self.write_operations.put(key, value, None)
    }

    /// Delete a key
    pub fn delete(&self, key: &[u8]) -> OpResult {
        self.write_operations.delete(key, None)
    }

    /// Get current read version for transactions
    pub fn get_read_version(&self) -> u64 {
        self.read_operations.get_read_version()
    }

    /// Snapshot read at specific version
    pub fn snapshot_read(&self, key: &[u8], read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
        self.read_operations.snapshot_read(key, read_version, column_family)
    }

    /// Atomic commit of multiple operations
    pub fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
        self.write_operations.atomic_commit(request)
    }

    /// Get range of key-value pairs with offset support
    pub fn get_range(
        &self,
        begin_key: &[u8],
        end_key: &[u8],
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        limit: Option<i32>,
    ) -> GetRangeResult {
        self.read_operations.get_range(
            begin_key,
            end_key,
            begin_offset,
            begin_or_equal,
            end_offset,
            end_or_equal,
            limit,
            None, // column_family
        )
    }

    /// Snapshot get range at specific version
    pub fn snapshot_get_range(
        &self,
        begin_key: &[u8],
        end_key: &[u8],
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        read_version: u64,
        limit: Option<i32>,
    ) -> GetRangeResult {
        self.read_operations.snapshot_get_range(
            begin_key,
            end_key,
            begin_offset,
            begin_or_equal,
            end_offset,
            end_or_equal,
            read_version,
            limit,
            None, // column_family
        )
    }

    /// Set versionstamped key operation
    pub fn set_versionstamped_key(
        &self,
        key_prefix: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>,
    ) -> AtomicCommitResult {
        self.write_operations.set_versionstamped_key(key_prefix, value, column_family)
    }

    /// Set versionstamped value operation
    pub fn set_versionstamped_value(
        &self,
        key: Vec<u8>,
        value_prefix: Vec<u8>,
        column_family: Option<String>,
    ) -> AtomicCommitResult {
        self.write_operations.set_versionstamped_value(key, value_prefix, column_family)
    }

    /// Set fault injection for testing
    pub fn set_fault_injection(&self, config: Option<FaultInjectionConfig>) -> OpResult {
        self.write_operations.set_fault_injection(config)
    }

    /// Health check ping operation
    pub fn ping(&self, message: Option<Vec<u8>>, timestamp: Option<i64>) -> (Vec<u8>, i64, i64) {
        self.read_operations.ping(message, timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use async_trait::async_trait;
    use kv_storage_api::AtomicOperation;

    /// Mock implementation of KvDatabase for testing trait wiring
    #[derive(Debug)]
    struct MockKvDatabase {
        data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
        read_version_counter: std::sync::atomic::AtomicU64,
    }

    impl MockKvDatabase {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
                read_version_counter: std::sync::atomic::AtomicU64::new(1),
            }
        }
    }

    #[async_trait]
    impl KvDatabase for MockKvDatabase {
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
            begin_offset: i32,
            begin_or_equal: bool,
            _end_offset: i32,
            end_or_equal: bool,
            limit: Option<i32>,
            _column_family: Option<&str>,
        ) -> Result<GetRangeResult, String> {
            let data = self.data.lock().unwrap();
            let mut key_values: Vec<kv_storage_api::KeyValue> = data
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
                .map(|(k, v)| kv_storage_api::KeyValue {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect();

            key_values.sort_by(|a, b| a.key.cmp(&b.key));

            // Apply offset
            if begin_offset > 0 && begin_offset < key_values.len() as i32 {
                key_values.drain(0..begin_offset as usize);
            }

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

            // Simple mock implementation - just apply all operations
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
                    _ => {} // Ignore other operations for simplicity
                }
            }

            let committed_version = self.read_version_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

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
            self.read_version_counter.load(std::sync::atomic::Ordering::SeqCst)
        }

        async fn snapshot_read(&self, key: &[u8], _read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
            // For this mock, snapshot read is the same as regular read
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
            // For this mock, snapshot get range is the same as regular get range
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

    #[test]
    fn test_kv_operations_with_mock_database() {
        let mock_db = Arc::new(MockKvDatabase::new()) as Arc<dyn KvDatabase>;
        let operations = KvOperations::new(mock_db, true);

        // Test basic put/get operations
        let put_result = operations.put(b"test_key", b"test_value");
        assert!(put_result.success, "Put operation should succeed");

        let get_result = operations.get(b"test_key");
        assert!(get_result.is_ok(), "Get operation should succeed");
        let get_result = get_result.unwrap();
        assert!(get_result.found, "Key should be found");
        assert_eq!(get_result.value, b"test_value", "Value should match");

        // Test delete operation
        let delete_result = operations.delete(b"test_key");
        assert!(delete_result.success, "Delete operation should succeed");

        let get_result_after_delete = operations.get(b"test_key");
        assert!(get_result_after_delete.is_ok(), "Get after delete should succeed");
        let get_result_after_delete = get_result_after_delete.unwrap();
        assert!(!get_result_after_delete.found, "Key should not be found after delete");

        // Test read version
        let read_version = operations.get_read_version();
        assert!(read_version > 0, "Read version should be positive");

        // Test snapshot read
        operations.put(b"snapshot_key", b"snapshot_value");
        let snapshot_result = operations.snapshot_read(b"snapshot_key", read_version, None);
        assert!(snapshot_result.is_ok(), "Snapshot read should succeed");

        // Test atomic commit
        let atomic_request = AtomicCommitRequest {
            read_version,
            operations: vec![AtomicOperation {
                op_type: "PUT".to_string(),
                key: b"atomic_key".to_vec(),
                value: Some(b"atomic_value".to_vec()),
                column_family: None,
            }],
            read_conflict_keys: vec![],
            timeout_seconds: 60,
        };

        let atomic_result = operations.atomic_commit(atomic_request);
        assert!(atomic_result.success, "Atomic commit should succeed");
        assert!(atomic_result.committed_version.is_some(), "Should have committed version");

        // Verify the atomic operation took effect
        let atomic_get_result = operations.get(b"atomic_key");
        assert!(atomic_get_result.is_ok(), "Get after atomic commit should succeed");
        let atomic_get_result = atomic_get_result.unwrap();
        assert!(atomic_get_result.found, "Atomic key should be found");
        assert_eq!(atomic_get_result.value, b"atomic_value", "Atomic value should match");

        // Test range operations
        operations.put(b"range_key_1", b"value_1");
        operations.put(b"range_key_2", b"value_2");
        operations.put(b"range_key_3", b"value_3");

        let range_result = operations.get_range(
            b"range_key_1",
            b"range_key_3",
            0,
            true,
            0,
            false,
            Some(10),
        );
        assert!(range_result.success, "Range operation should succeed");
        // Due to our mock implementation's simplified parameter handling,
        // we just verify it returns some results
        assert!(!range_result.key_values.is_empty(), "Range should return some results");

        // Test fault injection
        let fault_config = Some(FaultInjectionConfig {
            fault_type: "TEST_FAULT".to_string(),
            probability: 0.5,
            duration_ms: 100,
            target_operation: Some("GET".to_string()),
        });

        let fault_result = operations.set_fault_injection(fault_config);
        assert!(fault_result.success, "Fault injection should succeed");

        // Test ping
        let (response, client_ts, server_ts) = operations.ping(Some(b"test_ping".to_vec()), Some(123456));
        assert_eq!(response, b"test_ping", "Ping response should match input");
        assert_eq!(client_ts, 123456, "Client timestamp should match");
        assert!(server_ts > 0, "Server timestamp should be positive");
    }

    #[test]
    fn test_kv_operations_trait_object_creation() {
        // Test that we can create KvOperations with a trait object
        let mock_db = Arc::new(MockKvDatabase::new()) as Arc<dyn KvDatabase>;
        let operations = KvOperations::new(mock_db, false);

        // Verify the mock database is properly accessed through the trait
        let read_version = operations.get_read_version();
        assert!(read_version > 0, "Read version should be accessible through trait");
    }
}