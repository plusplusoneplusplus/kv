use std::sync::Arc;
use tracing::{debug, warn};

use crate::lib::db::{AtomicCommitRequest, AtomicCommitResult, GetResult, OpResult, GetRangeResult, FaultInjectionConfig, AtomicOperation};
use crate::lib::db_trait::KvDatabase;

/// Core business logic for KV operations, completely protocol-agnostic.
/// This contains all the business logic operations without any knowledge of
/// Thrift, gRPC, or other protocol specifics. It operates purely on domain types.
pub struct KvOperations {
    database: Arc<dyn KvDatabase>,
    verbose: bool,
}

impl KvOperations {
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

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<GetResult, String> {
        if self.verbose {
            debug!("Get operation: key={:?}", key);
        }

        let result = self.run_async(self.database.get(key, None));

        if self.verbose {
            match &result {
                Ok(get_result) => debug!("Get result: found={}, value_len={}", get_result.found, get_result.value.len()),
                Err(e) => warn!("Get error: {}", e),
            }
        }

        result
    }

    /// Put a key-value pair
    pub fn put(&self, key: &[u8], value: &[u8]) -> OpResult {
        if self.verbose {
            debug!("Put operation: key={:?}, value_len={}", key, value.len());
        }

        let result = self.run_async(self.database.put(key, value, None));

        if self.verbose {
            debug!("Put result: success={}, error_code={:?}", result.success, result.error_code);
            if !result.error.is_empty() {
                warn!("Put error: {}", result.error);
            }
        }

        result
    }

    /// Delete a key
    pub fn delete(&self, key: &[u8]) -> OpResult {
        if self.verbose {
            debug!("Delete operation: key={:?}", key);
        }

        let result = self.run_async(self.database.delete(key, None));

        if self.verbose {
            debug!("Delete result: success={}, error_code={:?}", result.success, result.error_code);
            if !result.error.is_empty() {
                warn!("Delete error: {}", result.error);
            }
        }

        result
    }

    /// Get current read version for transactions
    pub fn get_read_version(&self) -> u64 {
        if self.verbose {
            debug!("Getting read version");
        }

        let version = self.run_async(self.database.get_read_version());

        if self.verbose {
            debug!("Read version retrieved: {}", version);
        }

        version
    }

    /// Snapshot read at specific version
    pub fn snapshot_read(&self, key: &[u8], read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
        if self.verbose {
            debug!("Snapshot read: key={:?}, read_version={}, column_family={:?}", key, read_version, column_family);
        }

        let result = self.run_async(self.database.snapshot_read(key, read_version, column_family));

        if self.verbose {
            match &result {
                Ok(get_result) => debug!("Snapshot read result: found={}, value_len={}", get_result.found, get_result.value.len()),
                Err(e) => warn!("Snapshot read error: {}", e),
            }
        }

        result
    }

    /// Atomic commit of multiple operations
    pub fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
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

        result
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
        if self.verbose {
            debug!("Get range: begin_key={:?}, end_key={:?}, begin_offset={}, begin_or_equal={}, end_offset={}, end_or_equal={}, limit={:?}",
                   begin_key, end_key, begin_offset, begin_or_equal, end_offset, end_or_equal, limit);
        }

        // Use trait interface directly since signatures now match
        let result = match self.run_async(self.database.get_range(
            begin_key,
            end_key,
            begin_offset,
            begin_or_equal,
            end_offset,
            end_or_equal,
            limit,
            None, // column_family
        )) {
            Ok(get_range_result) => get_range_result,
            Err(error) => GetRangeResult {
                key_values: Vec::new(),
                success: false,
                error,
                has_more: false,
            },
        };

        if self.verbose {
            debug!("Get range result: success={}, key_values_count={}, error={}",
                   result.success, result.key_values.len(), result.error);
        }

        result
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
        if self.verbose {
            debug!("Snapshot get range: begin_key={:?}, end_key={:?}, begin_offset={}, begin_or_equal={}, end_offset={}, end_or_equal={}, read_version={}, limit={:?}",
                   begin_key, end_key, begin_offset, begin_or_equal, end_offset, end_or_equal, read_version, limit);
        }

        // Use trait interface directly since signatures now match
        let result = match self.run_async(self.database.snapshot_get_range(
            begin_key,
            end_key,
            begin_offset,
            begin_or_equal,
            end_offset,
            end_or_equal,
            read_version,
            limit,
            None, // column_family
        )) {
            Ok(get_range_result) => get_range_result,
            Err(error) => GetRangeResult {
                key_values: Vec::new(),
                success: false,
                error,
                has_more: false,
            },
        };

        if self.verbose {
            debug!("Snapshot get range result: success={}, key_values_count={}, error={}",
                   result.success, result.key_values.len(), result.error);
        }

        result
    }

    /// Set versionstamped key operation
    pub fn set_versionstamped_key(
        &self,
        key_prefix: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>,
    ) -> AtomicCommitResult {
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

        result
    }

    /// Set versionstamped value operation
    pub fn set_versionstamped_value(
        &self,
        key: Vec<u8>,
        value_prefix: Vec<u8>,
        column_family: Option<String>,
    ) -> AtomicCommitResult {
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

        result
    }

    /// Set fault injection for testing
    pub fn set_fault_injection(&self, config: Option<FaultInjectionConfig>) -> OpResult {
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

        result
    }

    /// Health check ping operation
    pub fn ping(&self, message: Option<Vec<u8>>, timestamp: Option<i64>) -> (Vec<u8>, i64, i64) {
        let server_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let response_message = message.unwrap_or_else(|| "pong".to_string().into_bytes());
        let client_timestamp = timestamp.unwrap_or(server_timestamp);

        if self.verbose {
            debug!("Ping: message={:?}, client_timestamp={}, server_timestamp={}",
                   response_message, client_timestamp, server_timestamp);
        }

        (response_message, client_timestamp, server_timestamp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use async_trait::async_trait;

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