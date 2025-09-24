use serde::{Deserialize, Serialize};
use kv_storage_api::{GetResult, GetRangeResult, OpResult, AtomicCommitRequest, AtomicCommitResult, FaultInjectionConfig};
use crate::generated::kvstore::{ClusterHealth, DatabaseStats, NodeStatus};

/// Types of operations supported by the KV store
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OperationType {
    Read,
    Write,
}

/// Trait for operations that can be performed on the database
pub trait DatabaseOperation {
    fn is_read_only(&self) -> bool;
    fn requires_consensus(&self) -> bool {
        !self.is_read_only()
    }
    fn operation_type(&self) -> OperationType {
        if self.is_read_only() {
            OperationType::Read
        } else {
            OperationType::Write
        }
    }
    /// Indicates whether this operation should be routed through consensus layer
    /// or handled locally at the routing manager level
    fn should_route_to_consensus(&self) -> bool {
        true // Default: most operations go through consensus
    }
}

/// All operations that can be performed on the distributed KV store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvOperation {
    // Read operations - can be served by any node
    Get {
        key: Vec<u8>,
        column_family: Option<String>,
    },
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
    SnapshotRead {
        key: Vec<u8>,
        read_version: u64,
        column_family: Option<String>,
    },
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
    GetReadVersion,
    Ping {
        message: Option<Vec<u8>>,
        timestamp: Option<i64>,
    },

    // Write operations - must go through leader and consensus
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>,
    },
    Delete {
        key: Vec<u8>,
        column_family: Option<String>,
    },
    AtomicCommit {
        request: AtomicCommitRequest,
    },
    SetVersionstampedKey {
        key_prefix: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>,
    },
    SetVersionstampedValue {
        key: Vec<u8>,
        value_prefix: Vec<u8>,
        column_family: Option<String>,
    },
    SetFaultInjection {
        config: Option<FaultInjectionConfig>,
    },

    // Diagnostic operations for cluster management
    GetClusterHealth,
    GetDatabaseStats {
        include_detailed: bool,
    },
    GetNodeInfo {
        node_id: Option<u32>,
    },
}

impl DatabaseOperation for KvOperation {
    fn is_read_only(&self) -> bool {
        match self {
            KvOperation::Get { .. }
            | KvOperation::GetRange { .. }
            | KvOperation::SnapshotRead { .. }
            | KvOperation::SnapshotGetRange { .. }
            | KvOperation::GetReadVersion
            | KvOperation::Ping { .. }
            | KvOperation::GetClusterHealth
            | KvOperation::GetDatabaseStats { .. }
            | KvOperation::GetNodeInfo { .. } => true,

            KvOperation::Set { .. }
            | KvOperation::Delete { .. }
            | KvOperation::AtomicCommit { .. }
            | KvOperation::SetVersionstampedKey { .. }
            | KvOperation::SetVersionstampedValue { .. }
            | KvOperation::SetFaultInjection { .. } => false,
        }
    }

    fn should_route_to_consensus(&self) -> bool {
        match self {
            // Diagnostic operations are handled locally at routing manager level
            KvOperation::GetClusterHealth
            | KvOperation::GetDatabaseStats { .. }
            | KvOperation::GetNodeInfo { .. } => false,

            // All other operations go through consensus layer
            _ => true,
        }
    }
}

/// Results returned by operations
/// Note: Diagnostic operation results (ClusterHealth, DatabaseStats, NodeStatus)
/// are not serializable as they come from generated code without Serialize derives.
/// These operations should bypass consensus entirely.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationResult {
    GetResult(Result<GetResult, String>),
    GetRangeResult(GetRangeResult),
    OpResult(OpResult),
    AtomicCommitResult(AtomicCommitResult),
    ReadVersion(u64),
    PingResult {
        message: Vec<u8>,
        client_timestamp: i64,
        server_timestamp: i64,
    },

    // Diagnostic operation results - these should never go through consensus
    // and are handled separately to avoid serialization issues
    #[serde(skip)]
    ClusterHealthResult(ClusterHealth),
    #[serde(skip)]
    DatabaseStatsResult(DatabaseStats),
    #[serde(skip)]
    NodeInfoResult(NodeStatus),
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_classification() {
        let get_op = KvOperation::Get {
            key: b"test".to_vec(),
            column_family: None,
        };
        assert!(get_op.is_read_only());
        assert_eq!(get_op.operation_type(), OperationType::Read);
        assert!(!get_op.requires_consensus());

        let set_op = KvOperation::Set {
            key: b"test".to_vec(),
            value: b"value".to_vec(),
            column_family: None,
        };
        assert!(!set_op.is_read_only());
        assert_eq!(set_op.operation_type(), OperationType::Write);
        assert!(set_op.requires_consensus());
    }

    #[test]
    fn test_ping_operation() {
        let ping_op = KvOperation::Ping {
            message: Some(b"hello".to_vec()),
            timestamp: Some(12345),
        };
        assert!(ping_op.is_read_only());
        assert_eq!(ping_op.operation_type(), OperationType::Read);
    }

    #[test]
    fn test_atomic_commit_operation() {
        let atomic_op = KvOperation::AtomicCommit {
            request: AtomicCommitRequest {
                read_version: 1,
                operations: vec![],
                read_conflict_keys: vec![],
                timeout_seconds: 60,
            },
        };
        assert!(!atomic_op.is_read_only());
        assert_eq!(atomic_op.operation_type(), OperationType::Write);
        assert!(atomic_op.requires_consensus());
    }

    #[test]
    fn test_versionstamped_operations() {
        let key_op = KvOperation::SetVersionstampedKey {
            key_prefix: b"test".to_vec(),
            value: b"value".to_vec(),
            column_family: None,
        };
        assert!(!key_op.is_read_only());
        assert!(key_op.requires_consensus());

        let value_op = KvOperation::SetVersionstampedValue {
            key: b"test".to_vec(),
            value_prefix: b"value".to_vec(),
            column_family: None,
        };
        assert!(!value_op.is_read_only());
        assert!(value_op.requires_consensus());
    }
}