namespace rs kvstore

// Transaction lifecycle structures
struct BeginTransactionRequest {
    1: optional list<string> column_families,
    2: optional i64 timeout_seconds = 60
}

struct BeginTransactionResponse {
    1: required string transaction_id,
    2: required bool success,
    3: optional string error
}

struct CommitTransactionRequest {
    1: required string transaction_id
}

struct CommitTransactionResponse {
    1: required bool success,
    2: optional string error
}

struct AbortTransactionRequest {
    1: required string transaction_id
}

struct AbortTransactionResponse {
    1: required bool success,
    2: optional string error
}

// Core transactional operations
struct GetRequest {
    1: required string transaction_id,
    2: required string key,
    3: optional string column_family
}

struct GetResponse {
    1: required string value,
    2: required bool found,
    3: optional string error
}

struct SetRequest {
    1: required string transaction_id,
    2: required string key,
    3: required string value,
    4: optional string column_family
}

struct SetResponse {
    1: required bool success,
    2: optional string error
}

struct DeleteRequest {
    1: required string transaction_id,
    2: required string key,
    3: optional string column_family
}

struct DeleteResponse {
    1: required bool success,
    2: optional string error
}

// Range operations
struct GetRangeRequest {
    1: required string transaction_id,
    2: required string start_key,
    3: optional string end_key,
    4: optional i32 limit = 1000,
    5: optional string column_family
}

struct KeyValue {
    1: required string key,
    2: required string value
}

struct GetRangeResponse {
    1: required list<KeyValue> key_values,
    2: required bool success,
    3: optional string error
}

// Snapshot operations
struct SnapshotGetRequest {
    1: required string transaction_id,
    2: required string key,
    3: required i64 read_version,
    4: optional string column_family
}

struct SnapshotGetResponse {
    1: required string value,
    2: required bool found,
    3: optional string error
}

struct SnapshotGetRangeRequest {
    1: required string transaction_id,
    2: required string start_key,
    3: optional string end_key,
    4: required i64 read_version,
    5: optional i32 limit = 1000,
    6: optional string column_family
}

struct SnapshotGetRangeResponse {
    1: required list<KeyValue> key_values,
    2: required bool success,
    3: optional string error
}

// Conflict detection
struct AddReadConflictRequest {
    1: required string transaction_id,
    2: required string key,
    3: optional string column_family
}

struct AddReadConflictResponse {
    1: required bool success,
    2: optional string error
}

struct AddReadConflictRangeRequest {
    1: required string transaction_id,
    2: required string start_key,
    3: required string end_key,
    4: optional string column_family
}

struct AddReadConflictRangeResponse {
    1: required bool success,
    2: optional string error
}

// Version management
struct SetReadVersionRequest {
    1: required string transaction_id,
    2: required i64 version
}

struct SetReadVersionResponse {
    1: required bool success,
    2: optional string error
}

struct GetCommittedVersionRequest {
    1: required string transaction_id
}

struct GetCommittedVersionResponse {
    1: required i64 version,
    2: required bool success,
    3: optional string error
}

// Versionstamped operations
struct SetVersionstampedKeyRequest {
    1: required string transaction_id,
    2: required string key_prefix,
    3: required string value,
    4: optional string column_family
}

struct SetVersionstampedKeyResponse {
    1: required string generated_key,
    2: required bool success,
    3: optional string error
}

struct SetVersionstampedValueRequest {
    1: required string transaction_id,
    2: required string key,
    3: required string value_prefix,
    4: optional string column_family
}

struct SetVersionstampedValueResponse {
    1: required string generated_value,
    2: required bool success,
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

// The transactional key-value store service definition
service TransactionalKV {
    // Transaction lifecycle
    BeginTransactionResponse beginTransaction(1: BeginTransactionRequest request),
    CommitTransactionResponse commitTransaction(1: CommitTransactionRequest request),
    AbortTransactionResponse abortTransaction(1: AbortTransactionRequest request),
    
    // Core transactional operations
    GetResponse get(1: GetRequest request),
    SetResponse setKey(1: SetRequest request),
    DeleteResponse delete_key(1: DeleteRequest request),
    
    // Range operations
    GetRangeResponse getRange(1: GetRangeRequest request),
    
    // Snapshot operations
    SnapshotGetResponse snapshotGet(1: SnapshotGetRequest request),
    SnapshotGetRangeResponse snapshotGetRange(1: SnapshotGetRangeRequest request),
    
    // Conflict detection
    AddReadConflictResponse addReadConflict(1: AddReadConflictRequest request),
    AddReadConflictRangeResponse addReadConflictRange(1: AddReadConflictRangeRequest request),
    
    // Version management
    SetReadVersionResponse setReadVersion(1: SetReadVersionRequest request),
    GetCommittedVersionResponse getCommittedVersion(1: GetCommittedVersionRequest request),
    
    // Versionstamped operations
    SetVersionstampedKeyResponse setVersionstampedKey(1: SetVersionstampedKeyRequest request),
    SetVersionstampedValueResponse setVersionstampedValue(1: SetVersionstampedValueRequest request),
    
    // Health check
    PingResponse ping(1: PingRequest request)
}