namespace rs kvstore

// Request structure for Get operation
struct GetRequest {
    1: required string key
}

// Response structure for Get operation
struct GetResponse {
    1: required string value
    2: required bool found
}

// Request structure for Put operation
struct PutRequest {
    1: required string key
    2: required string value
}

// Response structure for Put operation
struct PutResponse {
    1: required bool success
    2: optional string error
}

// Request structure for Delete operation
struct DeleteRequest {
    1: required string key
}

// Response structure for Delete operation
struct DeleteResponse {
    1: required bool success
    2: optional string error
}

// Request structure for ListKeys operation
struct ListKeysRequest {
    1: optional string prefix
    2: optional i32 limit
}

// Response structure for ListKeys operation
struct ListKeysResponse {
    1: required list<string> keys
}

// Request structure for Ping operation
struct PingRequest {
    1: optional string message
    2: optional i64 timestamp
}

// Response structure for Ping operation
struct PingResponse {
    1: required string message
    2: required i64 timestamp
    3: required i64 server_timestamp
}

// The key-value store service definition
service KVStore {
    // Get a value by key
    GetResponse get(1: GetRequest request)
    
    // Put a key-value pair
    PutResponse put(1: PutRequest request)
    
    // Delete a key
    DeleteResponse delete_key(1: DeleteRequest request)
    
    // List all keys with optional prefix
    ListKeysResponse list_keys(1: ListKeysRequest request)
    
    // Ping for health check and latency testing
    PingResponse ping(1: PingRequest request)
}