use std::sync::Arc;
use tracing::{debug, warn};

use crate::lib::db::{GetResult, GetRangeResult};
use crate::lib::db_trait::KvDatabase;
use crate::lib::operations::{KvOperation, OperationResult, DatabaseOperation};

/// Read-only operations that can be served by any node in a distributed system.
/// These operations don't modify the database state and can be safely executed
/// without consensus in a multi-node setup.
pub struct KvReadOperations {
    database: Arc<dyn KvDatabase>,
    verbose: bool,
}

impl KvReadOperations {
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

    /// Execute a read operation
    pub fn execute(&self, operation: KvOperation) -> Result<OperationResult, String> {
        if !operation.is_read_only() {
            return Err("Operation is not a read operation".to_string());
        }

        match operation {
            KvOperation::Get { key, column_family } => {
                if self.verbose {
                    debug!("Get operation: key={:?}, column_family={:?}", key, column_family);
                }

                let result = self.run_async(self.database.get(&key, column_family.as_deref()));

                if self.verbose {
                    match &result {
                        Ok(get_result) => debug!("Get result: found={}, value_len={}", get_result.found, get_result.value.len()),
                        Err(e) => warn!("Get error: {}", e),
                    }
                }

                Ok(OperationResult::GetResult(result))
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
                if self.verbose {
                    debug!("Get range: begin_key={:?}, end_key={:?}, begin_offset={}, begin_or_equal={}, end_offset={}, end_or_equal={}, limit={:?}, column_family={:?}",
                           begin_key, end_key, begin_offset, begin_or_equal, end_offset, end_or_equal, limit, column_family);
                }

                let result = match self.run_async(self.database.get_range(
                    &begin_key,
                    &end_key,
                    begin_offset,
                    begin_or_equal,
                    end_offset,
                    end_or_equal,
                    limit,
                    column_family.as_deref(),
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

                Ok(OperationResult::GetRangeResult(result))
            }

            KvOperation::SnapshotRead { key, read_version, column_family } => {
                if self.verbose {
                    debug!("Snapshot read: key={:?}, read_version={}, column_family={:?}", key, read_version, column_family);
                }

                let result = self.run_async(self.database.snapshot_read(&key, read_version, column_family.as_deref()));

                if self.verbose {
                    match &result {
                        Ok(get_result) => debug!("Snapshot read result: found={}, value_len={}", get_result.found, get_result.value.len()),
                        Err(e) => warn!("Snapshot read error: {}", e),
                    }
                }

                Ok(OperationResult::GetResult(result))
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
                if self.verbose {
                    debug!("Snapshot get range: begin_key={:?}, end_key={:?}, begin_offset={}, begin_or_equal={}, end_offset={}, end_or_equal={}, read_version={}, limit={:?}, column_family={:?}",
                           begin_key, end_key, begin_offset, begin_or_equal, end_offset, end_or_equal, read_version, limit, column_family);
                }

                let result = match self.run_async(self.database.snapshot_get_range(
                    &begin_key,
                    &end_key,
                    begin_offset,
                    begin_or_equal,
                    end_offset,
                    end_or_equal,
                    read_version,
                    limit,
                    column_family.as_deref(),
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

                Ok(OperationResult::GetRangeResult(result))
            }

            KvOperation::GetReadVersion => {
                if self.verbose {
                    debug!("Getting read version");
                }

                let version = self.run_async(self.database.get_read_version());

                if self.verbose {
                    debug!("Read version retrieved: {}", version);
                }

                Ok(OperationResult::ReadVersion(version))
            }

            KvOperation::Ping { message, timestamp } => {
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

                Ok(OperationResult::PingResult {
                    message: response_message,
                    client_timestamp,
                    server_timestamp,
                })
            }

            _ => Err("Invalid read operation".to_string()),
        }
    }

    /// Individual operation methods for backward compatibility

    pub fn get(&self, key: &[u8], column_family: Option<&str>) -> Result<GetResult, String> {
        let operation = KvOperation::Get {
            key: key.to_vec(),
            column_family: column_family.map(|s| s.to_string()),
        };

        match self.execute(operation)? {
            OperationResult::GetResult(result) => result,
            _ => Err("Unexpected result type".to_string()),
        }
    }

    pub fn get_read_version(&self) -> u64 {
        let operation = KvOperation::GetReadVersion;

        match self.execute(operation).expect("GetReadVersion should not fail") {
            OperationResult::ReadVersion(version) => version,
            _ => panic!("Unexpected result type for GetReadVersion"),
        }
    }

    pub fn snapshot_read(&self, key: &[u8], read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
        let operation = KvOperation::SnapshotRead {
            key: key.to_vec(),
            read_version,
            column_family: column_family.map(|s| s.to_string()),
        };

        match self.execute(operation)? {
            OperationResult::GetResult(result) => result,
            _ => Err("Unexpected result type".to_string()),
        }
    }

    pub fn get_range(
        &self,
        begin_key: &[u8],
        end_key: &[u8],
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        limit: Option<i32>,
        column_family: Option<&str>,
    ) -> GetRangeResult {
        let operation = KvOperation::GetRange {
            begin_key: begin_key.to_vec(),
            end_key: end_key.to_vec(),
            begin_offset,
            begin_or_equal,
            end_offset,
            end_or_equal,
            limit,
            column_family: column_family.map(|s| s.to_string()),
        };

        match self.execute(operation).expect("GetRange should not fail") {
            OperationResult::GetRangeResult(result) => result,
            _ => panic!("Unexpected result type for GetRange"),
        }
    }

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
        column_family: Option<&str>,
    ) -> GetRangeResult {
        let operation = KvOperation::SnapshotGetRange {
            begin_key: begin_key.to_vec(),
            end_key: end_key.to_vec(),
            begin_offset,
            begin_or_equal,
            end_offset,
            end_or_equal,
            read_version,
            limit,
            column_family: column_family.map(|s| s.to_string()),
        };

        match self.execute(operation).expect("SnapshotGetRange should not fail") {
            OperationResult::GetRangeResult(result) => result,
            _ => panic!("Unexpected result type for SnapshotGetRange"),
        }
    }

    pub fn ping(&self, message: Option<Vec<u8>>, timestamp: Option<i64>) -> (Vec<u8>, i64, i64) {
        let operation = KvOperation::Ping { message, timestamp };

        match self.execute(operation).expect("Ping should not fail") {
            OperationResult::PingResult { message, client_timestamp, server_timestamp } => {
                (message, client_timestamp, server_timestamp)
            }
            _ => panic!("Unexpected result type for Ping"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lib::operations::DatabaseOperation;

    #[test]
    fn test_read_operation_validation() {
        // All operations accepted by KvReadOperations should be read-only
        let read_ops = vec![
            KvOperation::Get { key: b"key".to_vec(), column_family: None },
            KvOperation::GetReadVersion,
            KvOperation::Ping { message: None, timestamp: None },
        ];

        for op in read_ops {
            assert!(op.is_read_only(), "Operation should be read-only: {:?}", op);
        }
    }

    #[test]
    fn test_write_operation_rejection() {
        use std::collections::HashMap;
        use std::sync::Mutex;
        use async_trait::async_trait;
        use crate::lib::db::OpResult;

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

            async fn put(&self, _key: &[u8], _value: &[u8], _column_family: Option<&str>) -> OpResult {
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
            ) -> Result<GetRangeResult, String> {
                Ok(GetRangeResult { key_values: vec![], success: true, error: String::new(), has_more: false })
            }

            async fn atomic_commit(&self, _request: crate::lib::db::AtomicCommitRequest) -> crate::lib::db::AtomicCommitResult {
                crate::lib::db::AtomicCommitResult {
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
            ) -> Result<GetRangeResult, String> {
                self.get_range(begin_key, end_key, begin_offset, begin_or_equal, end_offset, end_or_equal, limit, column_family).await
            }

            async fn set_fault_injection(&self, _config: Option<crate::lib::db::FaultInjectionConfig>) -> OpResult {
                OpResult { success: true, error: String::new(), error_code: None }
            }
        }

        let mock_db = Arc::new(MockKvDatabase { data: Mutex::new(HashMap::new()) }) as Arc<dyn crate::lib::db_trait::KvDatabase>;
        let read_ops = KvReadOperations::new(mock_db, false);

        // Try to execute a write operation - should fail
        let write_operation = KvOperation::Set {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
            column_family: None,
        };

        let result = read_ops.execute(write_operation);
        assert!(result.is_err(), "Write operation should be rejected by read operations");
        assert_eq!(result.unwrap_err(), "Operation is not a read operation");
    }
}