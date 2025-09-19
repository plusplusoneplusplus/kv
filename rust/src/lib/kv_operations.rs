use std::sync::Arc;
use tracing::{debug, warn};

use crate::lib::db::{TransactionalKvDatabase, AtomicCommitRequest, AtomicCommitResult, GetResult, OpResult, GetRangeResult, FaultInjectionConfig, AtomicOperation};

/// Core business logic for KV operations, completely protocol-agnostic.
/// This contains all the business logic operations without any knowledge of
/// Thrift, gRPC, or other protocol specifics. It operates purely on domain types.
pub struct KvOperations {
    database: Arc<TransactionalKvDatabase>,
    verbose: bool,
}

impl KvOperations {
    pub fn new(database: Arc<TransactionalKvDatabase>, verbose: bool) -> Self {
        Self { database, verbose }
    }

    /// Get a value by key
    pub fn get(&self, key: &[u8]) -> Result<GetResult, String> {
        if self.verbose {
            debug!("Get operation: key={:?}", key);
        }

        let result = self.database.get(key);

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

        let result = self.database.put(key, value);

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

        let result = self.database.delete(key);

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

        let version = self.database.get_read_version();

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

        let result = self.database.snapshot_read(key, read_version, column_family);

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

        let result = self.database.atomic_commit(request);

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

        let result = self.database.get_range(
            begin_key,
            end_key,
            begin_offset,
            begin_or_equal,
            end_offset,
            end_or_equal,
            limit,
        );

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

        let result = self.database.snapshot_get_range(
            begin_key,
            end_key,
            begin_offset,
            begin_or_equal,
            end_offset,
            end_or_equal,
            read_version,
            limit,
        );

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
        let read_version = self.database.get_read_version();

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

        let result = self.database.atomic_commit(atomic_request);

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
        let read_version = self.database.get_read_version();

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

        let result = self.database.atomic_commit(atomic_request);

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

        let result = self.database.set_fault_injection(config);

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