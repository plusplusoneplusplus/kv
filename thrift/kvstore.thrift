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

// Range operations (FoundationDB-aligned)
struct GetRangeRequest {
    1: optional binary begin_key,      // Default: empty string (beginning of keyspace)
    2: optional binary end_key,        // Default: single 0xFF byte (end of keyspace) 
    3: optional i32 begin_offset = 0,
    4: optional bool begin_or_equal = true,
    5: optional i32 end_offset = 0,
    6: optional bool end_or_equal = false,
    7: optional i32 limit = 1000,
    8: optional string column_family
}

struct KeyValue {
    1: required binary key,
    2: required binary value
}

struct GetRangeResponse {
    1: required list<KeyValue> key_values,
    2: required bool success,
    3: optional string error,
    4: required bool has_more = false
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
    1: optional binary begin_key,      // Default: empty string (beginning of keyspace)
    2: optional binary end_key,        // Default: single 0xFF byte (end of keyspace)
    3: optional i32 begin_offset = 0,
    4: optional bool begin_or_equal = true,
    5: optional i32 end_offset = 0,
    6: optional bool end_or_equal = false,
    7: required i64 read_version,
    8: optional i32 limit = 1000,
    9: optional string column_family
}

struct SnapshotGetRangeResponse {
    1: required list<KeyValue> key_values,
    2: required bool success,
    3: optional string error,
    4: required bool has_more = false
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
    1: optional binary message,
    2: optional i64 timestamp
}

struct PingResponse {
    1: required binary message,
    2: required i64 timestamp,
    3: required i64 server_timestamp
}

// Diagnostic API structures for cluster management

// Node status information
struct NodeStatus {
    1: required i32 node_id,
    2: required string endpoint,
    3: required string status,  // "healthy", "degraded", "unreachable", "unknown"
    4: required bool is_leader,
    5: required i64 last_seen_timestamp,
    6: required i64 term,
    7: optional i64 uptime_seconds
}

// Cluster health information
struct ClusterHealth {
    1: required list<NodeStatus> nodes,
    2: required i32 healthy_nodes_count,
    3: required i32 total_nodes_count,
    4: optional NodeStatus leader,
    5: required string cluster_status,  // "healthy", "degraded", "unhealthy"
    6: required i64 current_term
}

// Database statistics
struct DatabaseStats {
    1: required i64 total_keys,
    2: required i64 total_size_bytes,
    3: required i64 write_operations_count,
    4: required i64 read_operations_count,
    5: required double average_response_time_ms,
    6: required i64 active_transactions,
    7: required i64 committed_transactions,
    8: required i64 aborted_transactions,
    9: optional i64 cache_hit_rate_percent,
    10: optional i64 compaction_pending_bytes
}


// Diagnostic requests and responses

struct GetClusterHealthRequest {
    1: optional string auth_token
}

struct GetClusterHealthResponse {
    1: required bool success,
    2: optional ClusterHealth cluster_health,
    3: optional string error
}

struct GetDatabaseStatsRequest {
    1: optional string auth_token,
    2: optional bool include_detailed_stats = false
}

struct GetDatabaseStatsResponse {
    1: required bool success,
    2: optional DatabaseStats database_stats,
    3: optional string error
}


struct GetNodeInfoRequest {
    1: optional string auth_token,
    2: optional i32 node_id  // If not provided, returns info for current node
}

struct GetNodeInfoResponse {
    1: required bool success,
    2: optional NodeStatus node_info,
    3: optional string error
}

// Consensus-related structures (Raft protocol)

struct LogEntry {
    1: required i64 term,
    2: required i64 index,
    3: required binary data,  // Serialized operation
    4: required string entry_type  // "operation", "config_change", "noop"
}

struct AppendEntriesRequest {
    1: required i64 term,
    2: required i32 leader_id,
    3: required i64 prev_log_index,
    4: required i64 prev_log_term,
    5: required list<LogEntry> entries,
    6: required i64 leader_commit
}

struct AppendEntriesResponse {
    1: required i64 term,
    2: required bool success,
    3: optional i64 last_log_index,  // For leader to update next_index
    4: optional string error
}

struct RequestVoteRequest {
    1: required i64 term,
    2: required i32 candidate_id,
    3: required i64 last_log_index,
    4: required i64 last_log_term
}

struct RequestVoteResponse {
    1: required i64 term,
    2: required bool vote_granted,
    3: optional string error
}

struct InstallSnapshotRequest {
    1: required i64 term,
    2: required i32 leader_id,
    3: required i64 last_included_index,
    4: required i64 last_included_term,
    5: required i64 offset,
    6: required binary data,
    7: required bool done
}

struct InstallSnapshotResponse {
    1: required i64 term,
    2: required bool success,
    3: optional string error
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
    PingResponse ping(1: PingRequest request),
    
    // Diagnostic API endpoints for cluster management
    GetClusterHealthResponse getClusterHealth(1: GetClusterHealthRequest request),
    GetDatabaseStatsResponse getDatabaseStats(1: GetDatabaseStatsRequest request),
    GetNodeInfoResponse getNodeInfo(1: GetNodeInfoRequest request)
}

// Consensus service for Raft protocol communication between nodes
service ConsensusService {
    AppendEntriesResponse appendEntries(1: AppendEntriesRequest request),
    RequestVoteResponse requestVote(1: RequestVoteRequest request),
    InstallSnapshotResponse installSnapshot(1: InstallSnapshotRequest request)
}