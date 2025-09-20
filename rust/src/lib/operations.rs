use serde::{Deserialize, Serialize};
use kv_storage_api::{AtomicCommitRequest, FaultInjectionConfig};

/// Trait for database operations to classify them by type and routing requirements
pub trait DatabaseOperation {
    /// Returns true if this operation only reads data and doesn't modify it
    fn is_read_only(&self) -> bool;

    /// Returns true if this operation requires consensus in a distributed system
    fn requires_consensus(&self) -> bool {
        !self.is_read_only()
    }

    /// Returns the type classification of this operation
    fn operation_type(&self) -> OperationType;
}

/// Classification of database operations for routing and consensus
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationType {
    /// Read operations - can be served by any node, don't require consensus
    Read,
    /// Write operations - must go through leader and consensus in distributed mode
    Write,
}

/// Unified enum representing all possible KV store operations
/// This separates read and write operations for proper routing in distributed systems
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvOperation {
    // Read operations - can be served by any node
    /// Get a value by key
    Get {
        key: Vec<u8>,
        column_family: Option<String>,
    },
    /// Get range of key-value pairs with offset support
    GetRange {
        begin_key: Vec<u8>,
        end_key: Vec<u8>,
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        limit: Option<i32>,
        column_family: Option<String>,
    },
    /// Snapshot read at specific version
    SnapshotRead {
        key: Vec<u8>,
        read_version: u64,
        column_family: Option<String>,
    },
    /// Snapshot get range at specific version
    SnapshotGetRange {
        begin_key: Vec<u8>,
        end_key: Vec<u8>,
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        read_version: u64,
        limit: Option<i32>,
        column_family: Option<String>,
    },
    /// Get current read version for transactions
    GetReadVersion,
    /// Health check ping operation
    Ping {
        message: Option<Vec<u8>>,
        timestamp: Option<i64>,
    },

    // Write operations - must go through leader and consensus
    /// Set a key-value pair
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>,
    },
    /// Delete a key
    Delete {
        key: Vec<u8>,
        column_family: Option<String>,
    },
    /// Atomic commit of multiple operations
    AtomicCommit {
        request: AtomicCommitRequest,
    },
    /// Set versionstamped key operation
    SetVersionstampedKey {
        key_prefix: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>,
    },
    /// Set versionstamped value operation
    SetVersionstampedValue {
        key: Vec<u8>,
        value_prefix: Vec<u8>,
        column_family: Option<String>,
    },
    /// Set fault injection for testing
    SetFaultInjection {
        config: Option<FaultInjectionConfig>,
    },
}

impl DatabaseOperation for KvOperation {
    fn is_read_only(&self) -> bool {
        matches!(self,
            KvOperation::Get { .. } |
            KvOperation::GetRange { .. } |
            KvOperation::SnapshotRead { .. } |
            KvOperation::SnapshotGetRange { .. } |
            KvOperation::GetReadVersion |
            KvOperation::Ping { .. }
        )
    }

    fn operation_type(&self) -> OperationType {
        if self.is_read_only() {
            OperationType::Read
        } else {
            OperationType::Write
        }
    }
}

/// Result types for operation execution
#[derive(Debug, Clone)]
pub enum OperationResult {
    GetResult(Result<kv_storage_api::GetResult, String>),
    GetRangeResult(kv_storage_api::GetRangeResult),
    OpResult(kv_storage_api::OpResult),
    AtomicCommitResult(kv_storage_api::AtomicCommitResult),
    ReadVersion(u64),
    PingResult {
        message: Vec<u8>,
        client_timestamp: i64,
        server_timestamp: i64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_operations_classification() {
        let read_ops = vec![
            KvOperation::Get { key: b"key".to_vec(), column_family: None },
            KvOperation::GetRange {
                begin_key: b"a".to_vec(),
                end_key: b"z".to_vec(),
                begin_offset: 0,
                begin_or_equal: true,
                end_offset: 0,
                end_or_equal: false,
                limit: None,
                column_family: None,
            },
            KvOperation::SnapshotRead { key: b"key".to_vec(), read_version: 123, column_family: None },
            KvOperation::GetReadVersion,
            KvOperation::Ping { message: None, timestamp: None },
        ];

        for op in read_ops {
            assert!(op.is_read_only(), "Operation {:?} should be read-only", op);
            assert_eq!(op.operation_type(), OperationType::Read);
            assert!(!op.requires_consensus(), "Read operations should not require consensus");
        }
    }

    #[test]
    fn test_write_operations_classification() {
        let write_ops = vec![
            KvOperation::Set { key: b"key".to_vec(), value: b"value".to_vec(), column_family: None },
            KvOperation::Delete { key: b"key".to_vec(), column_family: None },
            KvOperation::AtomicCommit {
                request: AtomicCommitRequest {
                    read_version: 123,
                    operations: vec![],
                    read_conflict_keys: vec![],
                    timeout_seconds: 60,
                }
            },
            KvOperation::SetVersionstampedKey {
                key_prefix: b"prefix".to_vec(),
                value: b"value".to_vec(),
                column_family: None,
            },
            KvOperation::SetVersionstampedValue {
                key: b"key".to_vec(),
                value_prefix: b"prefix".to_vec(),
                column_family: None,
            },
            KvOperation::SetFaultInjection { config: None },
        ];

        for op in write_ops {
            assert!(!op.is_read_only(), "Operation {:?} should not be read-only", op);
            assert_eq!(op.operation_type(), OperationType::Write);
            assert!(op.requires_consensus(), "Write operations should require consensus");
        }
    }

    #[test]
    fn test_operation_serialization() {
        let op = KvOperation::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            column_family: Some("test_cf".to_string()),
        };

        // Test that operations can be serialized and deserialized for network transmission
        let serialized = bincode::serialize(&op).expect("Should serialize");
        let deserialized: KvOperation = bincode::deserialize(&serialized).expect("Should deserialize");

        match (&op, &deserialized) {
            (KvOperation::Set { key: k1, value: v1, column_family: cf1 },
             KvOperation::Set { key: k2, value: v2, column_family: cf2 }) => {
                assert_eq!(k1, k2);
                assert_eq!(v1, v2);
                assert_eq!(cf1, cf2);
            }
            _ => panic!("Deserialized operation doesn't match original"),
        }
    }
}