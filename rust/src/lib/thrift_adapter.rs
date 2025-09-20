use std::sync::Arc;

use kv_storage_api::{AtomicOperation, FaultInjectionConfig};
use crate::lib::operations::{KvOperation, OperationResult};
use crate::lib::replication::RoutingManager;
use crate::generated::kvstore::*;

/// Thrift-specific adapter that implements the TransactionalKVSyncHandler trait.
/// This handles all Thrift protocol conversions and delegates operations to the RoutingManager.
/// It acts as a pure adapter layer that converts Thrift requests to KvOperations and
/// routes them through the distributed routing system.
pub struct ThriftKvAdapter {
    routing_manager: Arc<RoutingManager>,
}

impl ThriftKvAdapter {
    pub fn new(routing_manager: Arc<RoutingManager>) -> Self {
        Self {
            routing_manager,
        }
    }

    /// Utility method to execute async operations synchronously for Thrift handlers
    fn execute_operation(&self, operation: KvOperation) -> crate::lib::replication::RoutingResult<OperationResult> {
        // Try to use the current runtime, otherwise create a new one for testing
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.block_on(async {
                    self.routing_manager.route_operation(operation).await
                })
            }
            Err(_) => {
                // No current runtime, create a new one (for testing scenarios)
                let rt = tokio::runtime::Runtime::new().unwrap();
                rt.block_on(async {
                    self.routing_manager.route_operation(operation).await
                })
            }
        }
    }
}

impl TransactionalKVSyncHandler for ThriftKvAdapter {
    // FoundationDB-style client-side transaction methods

    fn handle_get_read_version(&self, _req: GetReadVersionRequest) -> thrift::Result<GetReadVersionResponse> {
        let operation = KvOperation::GetReadVersion;
        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::ReadVersion(version)) => Ok(GetReadVersionResponse::new(
                version as i64,
                true,
                None
            )),
            Err(e) => Ok(GetReadVersionResponse::new(
                0,
                false,
                Some(e.to_string())
            )),
            _ => Ok(GetReadVersionResponse::new(
                0,
                false,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    fn handle_snapshot_read(&self, req: SnapshotReadRequest) -> thrift::Result<SnapshotReadResponse> {
        let operation = KvOperation::SnapshotRead {
            key: req.key,
            read_version: req.read_version as u64,
            column_family: req.column_family,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::GetResult(Ok(get_result))) => Ok(SnapshotReadResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Ok(OperationResult::GetResult(Err(e))) => Ok(SnapshotReadResponse::new(
                Vec::new(),
                false,
                Some(e)
            )),
            Err(e) => Ok(SnapshotReadResponse::new(
                Vec::new(),
                false,
                Some(e.to_string())
            )),
            _ => Ok(SnapshotReadResponse::new(
                Vec::new(),
                false,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    fn handle_atomic_commit(&self, req: crate::generated::kvstore::AtomicCommitRequest) -> thrift::Result<AtomicCommitResponse> {
        // Convert Thrift operations to internal format
        let operations: Vec<AtomicOperation> = req.operations.into_iter()
            .map(|op| AtomicOperation {
                op_type: op.type_,
                key: op.key,
                value: op.value,
                column_family: op.column_family,
            })
            .collect();

        let atomic_request = kv_storage_api::AtomicCommitRequest {
            read_version: req.read_version as u64,
            operations,
            read_conflict_keys: req.read_conflict_keys,
            timeout_seconds: req.timeout_seconds.map(|t| t as u64).unwrap_or(60),
        };

        let operation = KvOperation::AtomicCommit {
            request: atomic_request,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::AtomicCommitResult(commit_result)) => Ok(AtomicCommitResponse::new(
                commit_result.success,
                if commit_result.error.is_empty() { None } else { Some(commit_result.error) },
                commit_result.error_code,
                commit_result.committed_version.map(|v| v as i64)
            )),
            Err(e) => Ok(AtomicCommitResponse::new(
                false,
                Some(e.to_string()),
                None,
                None
            )),
            _ => Ok(AtomicCommitResponse::new(
                false,
                Some("Unexpected result type".to_string()),
                None,
                None
            )),
        }
    }

    // Non-transactional operations for backward compatibility

    fn handle_get(&self, req: GetRequest) -> thrift::Result<GetResponse> {
        let operation = KvOperation::Get {
            key: req.key,
            column_family: None,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::GetResult(Ok(get_result))) => Ok(GetResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Ok(OperationResult::GetResult(Err(e))) => Ok(GetResponse::new(
                Vec::new(),
                false,
                Some(e)
            )),
            Err(e) => Ok(GetResponse::new(
                Vec::new(),
                false,
                Some(e.to_string())
            )),
            _ => Ok(GetResponse::new(
                Vec::new(),
                false,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    fn handle_set_key(&self, req: SetRequest) -> thrift::Result<SetResponse> {
        let operation = KvOperation::Set {
            key: req.key,
            value: req.value,
            column_family: None,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::OpResult(op_result)) => Ok(SetResponse::new(
                op_result.success,
                if op_result.error.is_empty() { None } else { Some(op_result.error) },
                op_result.error_code
            )),
            Err(e) => Ok(SetResponse::new(
                false,
                Some(e.to_string()),
                None
            )),
            _ => Ok(SetResponse::new(
                false,
                Some("Unexpected result type".to_string()),
                None
            )),
        }
    }

    fn handle_delete_key(&self, req: DeleteRequest) -> thrift::Result<DeleteResponse> {
        let operation = KvOperation::Delete {
            key: req.key,
            column_family: None,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::OpResult(op_result)) => Ok(DeleteResponse::new(
                op_result.success,
                if op_result.error.is_empty() { None } else { Some(op_result.error) },
                op_result.error_code
            )),
            Err(e) => Ok(DeleteResponse::new(
                false,
                Some(e.to_string()),
                None
            )),
            _ => Ok(DeleteResponse::new(
                false,
                Some("Unexpected result type".to_string()),
                None
            )),
        }
    }

    // Range operations

    fn handle_get_range(&self, req: GetRangeRequest) -> thrift::Result<GetRangeResponse> {

        // Apply FoundationDB-style defaults for missing keys
        let begin_key = req.begin_key.as_deref().unwrap_or(b"").to_vec(); // Empty string = beginning of keyspace
        let end_key = req.end_key.as_deref().unwrap_or(&[0xFF]).to_vec(); // Single 0xFF = end of keyspace

        let operation = KvOperation::GetRange {
            begin_key,
            end_key,
            begin_offset: req.begin_offset.unwrap_or(0),
            begin_or_equal: req.begin_or_equal.unwrap_or(true),
            end_offset: req.end_offset.unwrap_or(0),
            end_or_equal: req.end_or_equal.unwrap_or(false),
            limit: req.limit,
            column_family: None,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::GetRangeResult(range_result)) => {
                if range_result.success {
                    let key_values: Vec<KeyValue> = range_result.key_values.into_iter()
                        .map(|kv| KeyValue::new(kv.key, kv.value))
                        .collect();

                    Ok(GetRangeResponse::new(
                        key_values,
                        true,
                        None,
                        range_result.has_more
                    ))
                } else {
                    Ok(GetRangeResponse::new(
                        Vec::new(),
                        false,
                        Some(range_result.error),
                        false
                    ))
                }
            }
            Err(e) => Ok(GetRangeResponse::new(
                Vec::new(),
                false,
                Some(e.to_string()),
                false
            )),
            _ => Ok(GetRangeResponse::new(
                Vec::new(),
                false,
                Some("Unexpected result type".to_string()),
                false
            )),
        }
    }

    // Backward compatibility snapshot operations

    fn handle_snapshot_get(&self, req: SnapshotGetRequest) -> thrift::Result<SnapshotGetResponse> {
        // This is the same as handle_snapshot_read, just different Thrift method name
        let operation = KvOperation::SnapshotRead {
            key: req.key,
            read_version: req.read_version as u64,
            column_family: req.column_family,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::GetResult(Ok(get_result))) => Ok(SnapshotGetResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Ok(OperationResult::GetResult(Err(e))) => Ok(SnapshotGetResponse::new(
                Vec::new(),
                false,
                Some(e)
            )),
            Err(e) => Ok(SnapshotGetResponse::new(
                Vec::new(),
                false,
                Some(e.to_string())
            )),
            _ => Ok(SnapshotGetResponse::new(
                Vec::new(),
                false,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    fn handle_snapshot_get_range(&self, req: SnapshotGetRangeRequest) -> thrift::Result<SnapshotGetRangeResponse> {

        // Apply FoundationDB-style defaults for missing keys
        let begin_key = req.begin_key.as_deref().unwrap_or(b"").to_vec(); // Empty string = beginning of keyspace
        let end_key = req.end_key.as_deref().unwrap_or(&[0xFF]).to_vec(); // Single 0xFF = end of keyspace

        let operation = KvOperation::SnapshotGetRange {
            begin_key,
            end_key,
            begin_offset: req.begin_offset.unwrap_or(0),
            begin_or_equal: req.begin_or_equal.unwrap_or(true),
            end_offset: req.end_offset.unwrap_or(0),
            end_or_equal: req.end_or_equal.unwrap_or(false),
            read_version: req.read_version as u64,
            limit: req.limit,
            column_family: None,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::GetRangeResult(range_result)) => {
                if range_result.success {
                    let key_values: Vec<KeyValue> = range_result.key_values.into_iter()
                        .map(|kv| KeyValue::new(kv.key, kv.value))
                        .collect();

                    Ok(SnapshotGetRangeResponse::new(
                        key_values,
                        true,
                        None,
                        range_result.has_more
                    ))
                } else {
                    Ok(SnapshotGetRangeResponse::new(
                        Vec::new(),
                        false,
                        Some(range_result.error),
                        false
                    ))
                }
            }
            Err(e) => Ok(SnapshotGetRangeResponse::new(
                Vec::new(),
                false,
                Some(e.to_string()),
                false
            )),
            _ => Ok(SnapshotGetRangeResponse::new(
                Vec::new(),
                false,
                Some("Unexpected result type".to_string()),
                false
            )),
        }
    }

    // Conflict detection stubs - handled client-side in FoundationDB model

    fn handle_add_read_conflict(&self, _req: AddReadConflictRequest) -> thrift::Result<AddReadConflictResponse> {
        Ok(AddReadConflictResponse::new(
            false,
            Some("Conflict detection handled client-side in FoundationDB model".to_string())
        ))
    }

    fn handle_add_read_conflict_range(&self, _req: AddReadConflictRangeRequest) -> thrift::Result<AddReadConflictRangeResponse> {
        Ok(AddReadConflictRangeResponse::new(
            false,
            Some("Conflict detection handled client-side in FoundationDB model".to_string())
        ))
    }

    // Version management stubs - handled client-side in FoundationDB model

    fn handle_set_read_version(&self, _req: SetReadVersionRequest) -> thrift::Result<SetReadVersionResponse> {
        Ok(SetReadVersionResponse::new(
            false,
            Some("Read version managed client-side in FoundationDB model".to_string())
        ))
    }

    fn handle_get_committed_version(&self, _req: GetCommittedVersionRequest) -> thrift::Result<GetCommittedVersionResponse> {
        Ok(GetCommittedVersionResponse::new(
            0,
            false,
            Some("Committed version managed client-side in FoundationDB model".to_string())
        ))
    }

    // Versionstamped operation implementation

    fn handle_set_versionstamped_key(&self, req: SetVersionstampedKeyRequest) -> thrift::Result<SetVersionstampedKeyResponse> {
        let operation = KvOperation::SetVersionstampedKey {
            key_prefix: req.key_prefix,
            value: req.value,
            column_family: req.column_family,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::AtomicCommitResult(commit_result)) => {
                if commit_result.success && !commit_result.generated_keys.is_empty() {
                    Ok(SetVersionstampedKeyResponse::new(
                        commit_result.generated_keys[0].clone(),
                        true,
                        None
                    ))
                } else {
                    Ok(SetVersionstampedKeyResponse::new(
                        Vec::new(),
                        false,
                        if commit_result.error.is_empty() {
                            Some("Failed to generate versionstamped key".to_string())
                        } else {
                            Some(commit_result.error)
                        }
                    ))
                }
            }
            Err(e) => Ok(SetVersionstampedKeyResponse::new(
                Vec::new(),
                false,
                Some(e.to_string())
            )),
            _ => Ok(SetVersionstampedKeyResponse::new(
                Vec::new(),
                false,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    fn handle_set_versionstamped_value(&self, req: SetVersionstampedValueRequest) -> thrift::Result<SetVersionstampedValueResponse> {
        let operation = KvOperation::SetVersionstampedValue {
            key: req.key,
            value_prefix: req.value_prefix,
            column_family: req.column_family,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::AtomicCommitResult(commit_result)) => {
                if commit_result.success && !commit_result.generated_values.is_empty() {
                    Ok(SetVersionstampedValueResponse::new(
                        commit_result.generated_values[0].clone(),
                        true,
                        None
                    ))
                } else {
                    Ok(SetVersionstampedValueResponse::new(
                        Vec::new(),
                        false,
                        if commit_result.error.is_empty() {
                            Some("Failed to generate versionstamped value".to_string())
                        } else {
                            Some(commit_result.error)
                        }
                    ))
                }
            }
            Err(e) => Ok(SetVersionstampedValueResponse::new(
                Vec::new(),
                false,
                Some(e.to_string())
            )),
            _ => Ok(SetVersionstampedValueResponse::new(
                Vec::new(),
                false,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    // Fault injection for testing

    fn handle_set_fault_injection(&self, req: FaultInjectionRequest) -> thrift::Result<FaultInjectionResponse> {

        let config = if req.probability.map(|p| p.into_inner()).unwrap_or(0.0) > 0.0 {
            Some(FaultInjectionConfig {
                fault_type: req.fault_type,
                probability: req.probability.map(|p| p.into_inner()).unwrap_or(0.0),
                duration_ms: req.duration_ms.unwrap_or(0),
                target_operation: req.target_operation,
            })
        } else {
            None // Disable fault injection
        };

        let operation = KvOperation::SetFaultInjection { config };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::OpResult(op_result)) => Ok(FaultInjectionResponse::new(
                op_result.success,
                if op_result.error.is_empty() { None } else { Some(op_result.error) }
            )),
            Err(e) => Ok(FaultInjectionResponse::new(
                false,
                Some(e.to_string())
            )),
            _ => Ok(FaultInjectionResponse::new(
                false,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    // Diagnostic API endpoints for cluster management

    fn handle_get_cluster_health(&self, req: GetClusterHealthRequest) -> thrift::Result<GetClusterHealthResponse> {
        // TODO: Implement authentication check
        if let Some(_auth_token) = req.auth_token {
            // For now, accept any non-empty token
        }

        let operation = KvOperation::GetClusterHealth;
        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::ClusterHealthResult(cluster_health)) => {
                Ok(GetClusterHealthResponse::new(
                    true,
                    Some(cluster_health),
                    None
                ))
            }
            Err(e) => Ok(GetClusterHealthResponse::new(
                false,
                None,
                Some(e.to_string())
            )),
            _ => Ok(GetClusterHealthResponse::new(
                false,
                None,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    fn handle_get_database_stats(&self, req: GetDatabaseStatsRequest) -> thrift::Result<GetDatabaseStatsResponse> {
        // TODO: Implement authentication check
        if let Some(_auth_token) = req.auth_token {
            // For now, accept any non-empty token
        }

        let include_detailed = req.include_detailed_stats.unwrap_or(false);
        let operation = KvOperation::GetDatabaseStats { include_detailed };
        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::DatabaseStatsResult(stats)) => {
                Ok(GetDatabaseStatsResponse::new(
                    true,
                    Some(stats),
                    None
                ))
            }
            Err(e) => Ok(GetDatabaseStatsResponse::new(
                false,
                None,
                Some(e.to_string())
            )),
            _ => Ok(GetDatabaseStatsResponse::new(
                false,
                None,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    fn handle_get_node_info(&self, req: GetNodeInfoRequest) -> thrift::Result<GetNodeInfoResponse> {
        // TODO: Implement authentication check
        if let Some(_auth_token) = req.auth_token {
            // For now, accept any non-empty token
        }

        let operation = KvOperation::GetNodeInfo {
            node_id: req.node_id.map(|id| id as u32),
        };
        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::NodeInfoResult(node_info)) => {
                Ok(GetNodeInfoResponse::new(
                    true,
                    Some(node_info),
                    None
                ))
            }
            Err(e) => Ok(GetNodeInfoResponse::new(
                false,
                None,
                Some(e.to_string())
            )),
            _ => Ok(GetNodeInfoResponse::new(
                false,
                None,
                Some("Unexpected result type".to_string())
            )),
        }
    }

    // Health check

    fn handle_ping(&self, req: PingRequest) -> thrift::Result<PingResponse> {
        let operation = KvOperation::Ping {
            message: req.message,
            timestamp: req.timestamp,
        };

        let result = self.execute_operation(operation);

        match result {
            Ok(OperationResult::PingResult { message, client_timestamp, server_timestamp }) => {
                Ok(PingResponse::new(message, client_timestamp, server_timestamp))
            }
            Err(e) => {
                // For ping, we'll still return a response even on error
                let server_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                Ok(PingResponse::new(
                    format!("ERROR: {}", e).into_bytes(),
                    req.timestamp.unwrap_or(0),
                    server_timestamp
                ))
            }
            _ => {
                let server_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);
                Ok(PingResponse::new(
                    b"ERROR: Unexpected result type".to_vec(),
                    req.timestamp.unwrap_or(0),
                    server_timestamp
                ))
            }
        }
    }
}