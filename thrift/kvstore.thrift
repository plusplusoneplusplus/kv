namespace rs kvstore

// Core non-transactional operations (for backward compatibility)
struct GetRequest {
    1: required binary key,
    2: optional string column_family
}

struct GetResponse {
    1: required binary value,
    2: required bool found,
    3: optional string error
}

struct SetRequest {
    1: required binary key,
    2: required binary value,
    3: optional string column_family
}

struct SetResponse {
    1: required bool success,
    2: optional string error,
    3: optional string error_code
}

struct DeleteRequest {
    1: required binary key,
    2: optional string column_family
}

struct DeleteResponse {
    1: required bool success,
    2: optional string error,
    3: optional string error_code
}

// Range operations (simplified)
struct GetRangeRequest {
    1: required binary start_key,
    2: optional binary end_key,
    3: optional i32 limit = 1000,
    4: optional string column_family
}

struct KeyValue {
    1: required binary key,
    2: required binary value
}

struct GetRangeResponse {
    1: required list<KeyValue> key_values,
    2: required bool success,
    3: optional string error
}

// Legacy snapshot operations (for backward compatibility)
struct SnapshotGetRequest {
    1: required binary key,
    2: required i64 read_version,
    3: optional string column_family
}

struct SnapshotGetResponse {
    1: required binary value,
    2: required bool found,
    3: optional string error
}

struct SnapshotGetRangeRequest {
    1: required binary start_key,
    2: optional binary end_key,
    3: required i64 read_version,
    4: optional i32 limit = 1000,
    5: optional string column_family
}

struct SnapshotGetRangeResponse {
    1: required list<KeyValue> key_values,
    2: required bool success,
    3: optional string error
}

// Legacy stub structures (for backward compatibility - return errors)
struct AddReadConflictRequest {
    1: required binary key,
    2: optional string column_family
}

struct AddReadConflictResponse {
    1: required bool success,
    2: optional string error
}

struct AddReadConflictRangeRequest {
    1: required binary start_key,
    2: required binary end_key,
    3: optional string column_family
}

struct AddReadConflictRangeResponse {
    1: required bool success,
    2: optional string error
}

struct SetReadVersionRequest {
    1: required i64 version
}

struct SetReadVersionResponse {
    1: required bool success,
    2: optional string error
}

struct GetCommittedVersionRequest {
    // Empty request
}

struct GetCommittedVersionResponse {
    1: required i64 version,
    2: required bool success,
    3: optional string error
}

struct SetVersionstampedKeyRequest {
    1: required binary key_prefix,
    2: required binary value,
    3: optional string column_family
}

struct SetVersionstampedKeyResponse {
    1: required binary generated_key,
    2: required bool success,
    3: optional string error
}

struct SetVersionstampedValueRequest {
    1: required binary key,
    2: required binary value_prefix,
    3: optional string column_family
}

struct SetVersionstampedValueResponse {
    1: required binary generated_value,
    2: required bool success,
    3: optional string error
}

// Fault injection for testing
struct FaultInjectionRequest {
    1: required string fault_type,  // "timeout", "conflict", "corruption", "network"
    2: optional double probability = 0.0,  // 0.0 to 1.0
    3: optional i32 duration_ms = 0,
    4: optional string target_operation  // "get", "set", "commit", etc.
}

struct FaultInjectionResponse {
    1: required bool success,
    2: optional string error
}

// Client-side atomic operations (FoundationDB style)
struct Operation {
    1: required string type,  // "set", "delete" 
    2: required binary key,
    3: optional binary value,  // Only for "set" operations
    4: optional string column_family
}

struct AtomicCommitRequest {
    1: required i64 read_version,  // Client's read version for conflict detection
    2: required list<Operation> operations,  // All buffered operations
    3: required list<binary> read_conflict_keys,  // Keys read during transaction
    4: optional i64 timeout_seconds = 60
}

struct AtomicCommitResponse {
    1: required bool success,
    2: optional string error,
    3: optional string error_code,  // "CONFLICT", "TIMEOUT", etc.
    4: optional i64 committed_version  // Version assigned to this commit
}

struct GetReadVersionRequest {
    // Empty - just requests current read version
}

struct GetReadVersionResponse {
    1: required i64 read_version,
    2: required bool success,
    3: optional string error
}

struct SnapshotReadRequest {
    1: required binary key,
    2: required i64 read_version,
    3: optional string column_family
}

struct SnapshotReadResponse {
    1: required binary value,
    2: required bool found,
    3: optional string error
}

// Health check (preserved from original)
struct PingRequest {
    1: optional string message,
    2: optional i64 timestamp
}

struct PingResponse {
    1: required string message,
    2: required i64 timestamp,
    3: required i64 server_timestamp
}

// The FoundationDB-style transactional key-value store service
service TransactionalKV {
    // FoundationDB-style client-side transactions
    GetReadVersionResponse getReadVersion(1: GetReadVersionRequest request),
    SnapshotReadResponse snapshotRead(1: SnapshotReadRequest request),  
    AtomicCommitResponse atomicCommit(1: AtomicCommitRequest request),
    
    // Non-transactional operations for backward compatibility
    GetResponse get(1: GetRequest request),
    SetResponse setKey(1: SetRequest request),
    DeleteResponse deleteKey(1: DeleteRequest request),
    
    // Range operations (simplified)
    GetRangeResponse getRange(1: GetRangeRequest request),
    
    // Legacy snapshot operations (for backward compatibility)
    SnapshotGetResponse snapshotGet(1: SnapshotGetRequest request),
    SnapshotGetRangeResponse snapshotGetRange(1: SnapshotGetRangeRequest request),
    
    // Legacy conflict detection and version management (stubs that return errors)
    AddReadConflictResponse addReadConflict(1: AddReadConflictRequest request),
    AddReadConflictRangeResponse addReadConflictRange(1: AddReadConflictRangeRequest request),
    SetReadVersionResponse setReadVersion(1: SetReadVersionRequest request),
    GetCommittedVersionResponse getCommittedVersion(1: GetCommittedVersionRequest request),
    
    // Legacy versionstamped operations (stubs that return errors)
    SetVersionstampedKeyResponse setVersionstampedKey(1: SetVersionstampedKeyRequest request),
    SetVersionstampedValueResponse setVersionstampedValue(1: SetVersionstampedValueRequest request),
    
    // Fault injection for testing
    FaultInjectionResponse setFaultInjection(1: FaultInjectionRequest request),
    
    // Health check
    PingResponse ping(1: PingRequest request)
}