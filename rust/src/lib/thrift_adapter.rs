use std::sync::Arc;

use crate::lib::db::{TransactionalKvDatabase, AtomicOperation, FaultInjectionConfig};
use crate::lib::kv_operations::KvOperations;
use crate::generated::kvstore::*;

/// Thrift-specific adapter that implements the TransactionalKVSyncHandler trait.
/// This handles all Thrift protocol conversions and delegates business logic to KvOperations.
/// It acts as a pure adapter layer without any business logic of its own.
pub struct ThriftKvAdapter {
    operations: KvOperations,
}

impl ThriftKvAdapter {
    pub fn new(database: Arc<TransactionalKvDatabase>, verbose: bool) -> Self {
        Self {
            operations: KvOperations::new(database, verbose),
        }
    }
}

impl TransactionalKVSyncHandler for ThriftKvAdapter {
    // FoundationDB-style client-side transaction methods

    fn handle_get_read_version(&self, _req: GetReadVersionRequest) -> thrift::Result<GetReadVersionResponse> {
        let read_version = self.operations.get_read_version();

        Ok(GetReadVersionResponse::new(
            read_version as i64,
            true,
            None
        ))
    }

    fn handle_snapshot_read(&self, req: SnapshotReadRequest) -> thrift::Result<SnapshotReadResponse> {
        let result = self.operations.snapshot_read(&req.key, req.read_version as u64, req.column_family.as_deref());

        match result {
            Ok(get_result) => Ok(SnapshotReadResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Err(e) => Ok(SnapshotReadResponse::new(
                Vec::new(),
                false,
                Some(e)
            ))
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

        let atomic_request = crate::lib::db::AtomicCommitRequest {
            read_version: req.read_version as u64,
            operations,
            read_conflict_keys: req.read_conflict_keys,
            timeout_seconds: req.timeout_seconds.map(|t| t as u64).unwrap_or(60),
        };

        let result = self.operations.atomic_commit(atomic_request);

        Ok(AtomicCommitResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code,
            result.committed_version.map(|v| v as i64)
        ))
    }

    // Non-transactional operations for backward compatibility

    fn handle_get(&self, req: GetRequest) -> thrift::Result<GetResponse> {
        let result = self.operations.get(&req.key);

        match result {
            Ok(get_result) => Ok(GetResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Err(e) => Ok(GetResponse::new(
                Vec::new(),
                false,
                Some(e)
            )),
        }
    }

    fn handle_set_key(&self, req: SetRequest) -> thrift::Result<SetResponse> {
        let result = self.operations.put(&req.key, &req.value);

        Ok(SetResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    fn handle_delete_key(&self, req: DeleteRequest) -> thrift::Result<DeleteResponse> {
        let result = self.operations.delete(&req.key);

        Ok(DeleteResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    // Range operations

    fn handle_get_range(&self, req: GetRangeRequest) -> thrift::Result<GetRangeResponse> {
        // Apply FoundationDB-style defaults for missing keys
        let begin_key = req.begin_key.as_deref().unwrap_or(b""); // Empty string = beginning of keyspace
        let end_key = req.end_key.as_deref().unwrap_or(&[0xFF]); // Single 0xFF = end of keyspace

        let result = self.operations.get_range(
            begin_key,
            end_key,
            req.begin_offset.unwrap_or(0),
            req.begin_or_equal.unwrap_or(true),
            req.end_offset.unwrap_or(0),
            req.end_or_equal.unwrap_or(false),
            req.limit
        );

        if result.success {
            let key_values: Vec<KeyValue> = result.key_values.into_iter()
                .map(|kv| KeyValue::new(kv.key, kv.value))
                .collect();

            Ok(GetRangeResponse::new(
                key_values,
                true,
                None,
                result.has_more
            ))
        } else {
            Ok(GetRangeResponse::new(
                Vec::new(),
                false,
                Some(result.error),
                false
            ))
        }
    }

    // Backward compatibility snapshot operations

    fn handle_snapshot_get(&self, req: SnapshotGetRequest) -> thrift::Result<SnapshotGetResponse> {
        let result = self.operations.snapshot_read(&req.key, req.read_version as u64, req.column_family.as_deref());

        match result {
            Ok(get_result) => Ok(SnapshotGetResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Err(e) => Ok(SnapshotGetResponse::new(
                Vec::new(),
                false,
                Some(e)
            )),
        }
    }

    fn handle_snapshot_get_range(&self, req: SnapshotGetRangeRequest) -> thrift::Result<SnapshotGetRangeResponse> {
        // Apply FoundationDB-style defaults for missing keys
        let begin_key = req.begin_key.as_deref().unwrap_or(b""); // Empty string = beginning of keyspace
        let end_key = req.end_key.as_deref().unwrap_or(&[0xFF]); // Single 0xFF = end of keyspace

        let result = self.operations.snapshot_get_range(
            begin_key,
            end_key,
            req.begin_offset.unwrap_or(0),
            req.begin_or_equal.unwrap_or(true),
            req.end_offset.unwrap_or(0),
            req.end_or_equal.unwrap_or(false),
            req.read_version as u64,
            req.limit
        );

        if result.success {
            let key_values: Vec<KeyValue> = result.key_values.into_iter()
                .map(|kv| KeyValue::new(kv.key, kv.value))
                .collect();

            Ok(SnapshotGetRangeResponse::new(
                key_values,
                true,
                None,
                result.has_more
            ))
        } else {
            Ok(SnapshotGetRangeResponse::new(
                Vec::new(),
                false,
                Some(result.error),
                false
            ))
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
        let result = self.operations.set_versionstamped_key(req.key_prefix, req.value, req.column_family);

        if result.success && !result.generated_keys.is_empty() {
            Ok(SetVersionstampedKeyResponse::new(
                result.generated_keys[0].clone(),
                true,
                None
            ))
        } else {
            Ok(SetVersionstampedKeyResponse::new(
                Vec::new(),
                false,
                if result.error.is_empty() {
                    Some("Failed to generate versionstamped key".to_string())
                } else {
                    Some(result.error)
                }
            ))
        }
    }

    fn handle_set_versionstamped_value(&self, req: SetVersionstampedValueRequest) -> thrift::Result<SetVersionstampedValueResponse> {
        let result = self.operations.set_versionstamped_value(req.key, req.value_prefix, req.column_family);

        if result.success && !result.generated_values.is_empty() {
            Ok(SetVersionstampedValueResponse::new(
                result.generated_values[0].clone(),
                true,
                None
            ))
        } else {
            Ok(SetVersionstampedValueResponse::new(
                Vec::new(),
                false,
                if result.error.is_empty() {
                    Some("Failed to generate versionstamped value".to_string())
                } else {
                    Some(result.error)
                }
            ))
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

        let result = self.operations.set_fault_injection(config);

        Ok(FaultInjectionResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) }
        ))
    }

    // Health check

    fn handle_ping(&self, req: PingRequest) -> thrift::Result<PingResponse> {
        let (message, timestamp, server_timestamp) = self.operations.ping(req.message, req.timestamp);
        Ok(PingResponse::new(message, timestamp, server_timestamp))
    }
}