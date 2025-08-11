# Rust Thrift Server Transaction Support Design

## Overview

This document outlines the design changes required to add **session-based transaction support** to the Rust Thrift server in the KV store project, enabling it to serve as an adapter compatible with the 3FS metadata system's transactional KV interface.

The current server already uses RocksDB TransactionDB which provides full ACID transaction capabilities. However, it creates and commits a new transaction for each RPC operation. This design extends it to support **multi-operation transactions** that span multiple RPC calls, leveraging RocksDB's existing transaction engine while adding the session management and protocol adaptation layers needed for 3FS integration.

## Current Architecture Analysis

### Existing Implementation

The current Rust Thrift server (`rust/src/thrift_main.rs`) provides:

1. **Single-operation interface**: Each RPC handles one atomic operation (get, put, delete, list_keys, ping)
2. **RocksDB TransactionDB backend**: Uses RocksDB's transaction capabilities but only for individual operations
3. **Concurrent processing**: Read operations are concurrent with semaphore control, write operations are serialized
4. **Sync Thrift interface**: Uses synchronous Thrift handlers with Tokio runtime for async database operations

### Current Limitations for 3FS Integration

1. **No transaction session management**: Each RPC creates and commits its own RocksDB transaction immediately - no multi-operation transactions
2. **No session state tracking**: Cannot maintain transaction state across multiple RPC calls
3. **No version management**: No read version setting or commit version tracking for 3FS compatibility
4. **Protocol mismatch**: Current Thrift interface doesn't support 3FS transaction semantics
5. **No coroutine support**: 3FS interface expects `CoTryTask<T>` return types

**Note**: The underlying RocksDB TransactionDB already provides ACID properties, conflict detection, and isolation. We need to add session management and protocol adaptation layers.

## Target 3FS Compatibility Requirements

Based on the 3FS `ITransaction.h` interface, the transactional KV adapter must support:

### Transaction Interface
- **Read Operations**: `snapshotGet`, `get`, `snapshotGetRange`, `getRange`
- **Write Operations**: `set`, `clear`, `setVersionstampedKey`, `setVersionstampedValue`
- **Conflict Management**: `addReadConflict`, `addReadConflictRange`
- **Transaction Control**: `commit`, `cancel`, `reset`
- **Version Management**: `setReadVersion`, `getCommittedVersion`

### ACID Properties (Provided by RocksDB TransactionDB)
- **Atomicity**: All operations in a transaction succeed or fail together *(handled by RocksDB)*
- **Consistency**: Maintain data integrity constraints *(handled by RocksDB)*
- **Isolation**: Pessimistic concurrency control with conflict detection *(handled by RocksDB)*
- **Durability**: Committed transactions survive failures *(handled by RocksDB)*

### Coroutine Support
- All operations return `CoTryTask<T>`
- Support async/await patterns
- Proper error propagation

## Design Approach

**Core Principle**: Leverage RocksDB TransactionDB's proven ACID transaction engine while adding session management and protocol adaptation layers.

**What RocksDB TransactionDB Already Provides**:
- Full ACID transaction semantics
- Pessimistic locking and conflict detection
- Snapshot isolation
- Deadlock prevention
- Transaction lifecycle (begin/commit/rollback)

**What We Need to Add**:
- Session management to keep transactions alive across multiple RPC calls
- Transaction ID mapping to RocksDB Transaction objects
- Protocol adaptation between Thrift RPC and 3FS interfaces
- Session cleanup and timeout handling

### Phase 1: Protocol Extension

#### 1.1 Thrift Interface Extension

Extend `thrift/kvstore.thrift` to support transaction operations:

```thrift
// Transaction identifier
typedef string TransactionId

// Transaction begin request/response
struct BeginTransactionRequest {
    1: optional i64 read_version
}

struct BeginTransactionResponse {
    1: required TransactionId transaction_id
    2: optional string error
}

// Transactional operation requests include transaction_id
struct TxnGetRequest {
    1: required TransactionId transaction_id
    2: required string key
    3: optional bool snapshot_read = false
}

struct TxnGetResponse {
    1: required string value
    2: required bool found
    3: optional string error
}

struct TxnSetRequest {
    1: required TransactionId transaction_id
    2: required string key
    3: required string value
}

struct TxnSetResponse {
    1: required bool success
    2: optional string error
}

// Range operations
struct KeySelector {
    1: required string key
    2: required bool inclusive
}

struct TxnGetRangeRequest {
    1: required TransactionId transaction_id
    2: required KeySelector begin
    3: required KeySelector end
    4: required i32 limit
    5: optional bool snapshot_read = false
}

struct KeyValue {
    1: required string key
    2: required string value
}

struct TxnGetRangeResponse {
    1: required list<KeyValue> kvs
    2: required bool has_more
    3: optional string error
}

// Conflict tracking
struct AddReadConflictRequest {
    1: required TransactionId transaction_id
    2: required string key
}

struct AddReadConflictRangeRequest {
    1: required TransactionId transaction_id
    2: required string begin_key
    3: required string end_key
}

struct ConflictResponse {
    1: required bool success
    2: optional string error
}

// Versionstamp operations
struct TxnSetVersionstampedRequest {
    1: required TransactionId transaction_id
    2: required string key
    3: required string value
    4: required i32 offset
    5: required bool key_versioned // true for key, false for value
}

// Transaction control
struct CommitTransactionRequest {
    1: required TransactionId transaction_id
}

struct CommitTransactionResponse {
    1: required bool success
    2: required i64 committed_version
    3: optional string error
}

struct CancelTransactionRequest {
    1: required TransactionId transaction_id
}

struct TransactionControlResponse {
    1: required bool success
    2: optional string error
}

// Extended service interface
service KVStore {
    // Existing single-operation methods (for backward compatibility)
    GetResponse get(1: GetRequest request)
    PutResponse put(1: PutRequest request)
    DeleteResponse delete_key(1: DeleteRequest request)
    ListKeysResponse list_keys(1: ListKeysRequest request)
    PingResponse ping(1: PingRequest request)
    
    // New transaction methods
    BeginTransactionResponse begin_transaction(1: BeginTransactionRequest request)
    TxnGetResponse txn_get(1: TxnGetRequest request)
    TxnGetRangeResponse txn_get_range(1: TxnGetRangeRequest request)
    TxnSetResponse txn_set(1: TxnSetRequest request)
    TxnSetResponse txn_clear(1: TxnSetRequest request)  // reuse TxnSetRequest, ignore value
    TxnSetResponse txn_set_versionstamped(1: TxnSetVersionstampedRequest request)
    ConflictResponse add_read_conflict(1: AddReadConflictRequest request)
    ConflictResponse add_read_conflict_range(1: AddReadConflictRangeRequest request)
    CommitTransactionResponse commit_transaction(1: CommitTransactionRequest request)
    TransactionControlResponse cancel_transaction(1: CancelTransactionRequest request)
    TransactionControlResponse reset_transaction(1: CancelTransactionRequest request)
}
```

#### 1.2 Transaction Session Management

Implement **session management layer** that maps transaction IDs to RocksDB Transaction objects:

```rust
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use rocksdb::{Transaction, TransactionOptions, TransactionDB};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TransactionId(String);

impl TransactionId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }
}

// Session state - NOT transaction logic (RocksDB handles that)
#[derive(Debug)]
pub struct TransactionSession {
    pub transaction: Transaction<'static, TransactionDB>,  // RocksDB transaction object
    pub read_version: Option<i64>,                         // 3FS compatibility metadata
    pub created_at: std::time::Instant,                   // Session lifecycle management
}

// Session manager - maps IDs to RocksDB transactions
pub struct TransactionManager {
    active_sessions: Arc<RwLock<HashMap<TransactionId, TransactionSession>>>,
    db: Arc<TransactionDB>,  // RocksDB handles all transaction logic
}

impl TransactionManager {
    pub async fn begin_transaction(&self, read_version: Option<i64>) -> Result<TransactionId, String> {
        let txn_id = TransactionId::new();
        
        // Let RocksDB create and manage the transaction
        let transaction = self.db.transaction_opt(&Default::default(), &TransactionOptions::default());
        
        // We just manage the session mapping
        let session = TransactionSession {
            transaction,
            read_version,
            created_at: std::time::Instant::now(),
        };
        
        let mut sessions = self.active_sessions.write().await;
        sessions.insert(txn_id.clone(), session);
        
        Ok(txn_id)
    }
    
    pub async fn get_session(&self, txn_id: &TransactionId) -> Result<&TransactionSession, String> {
        let sessions = self.active_sessions.read().await;
        sessions.get(txn_id)
            .ok_or_else(|| format!("Transaction session {} not found", txn_id.0))
    }
    
    pub async fn commit_transaction(&self, txn_id: &TransactionId) -> Result<i64, String> {
        let mut sessions = self.active_sessions.write().await;
        
        if let Some(session) = sessions.remove(txn_id) {
            // RocksDB handles all the commit logic (ACID, conflict detection, etc.)
            session.transaction.commit()
                .map_err(|e| format!("Commit failed: {}", e))?;
            
            // Return commit version for 3FS compatibility
            let committed_version = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as i64;
            
            Ok(committed_version)
        } else {
            Err(format!("Transaction session {} not found", txn_id.0))
        }
    }
    
    pub async fn cancel_transaction(&self, txn_id: &TransactionId) -> Result<(), String> {
        let mut sessions = self.active_sessions.write().await;
        
        if let Some(session) = sessions.remove(txn_id) {
            // RocksDB handles rollback logic
            session.transaction.rollback()
                .map_err(|e| format!("Rollback failed: {}", e))?;
            Ok(())
        } else {
            Err(format!("Transaction session {} not found", txn_id.0))
        }
    }
    
    // Session cleanup - remove expired sessions
    pub async fn cleanup_expired_sessions(&self) {
        let mut sessions = self.active_sessions.write().await;
        let now = std::time::Instant::now();
        let timeout = Duration::from_secs(300); // 5 minute timeout
        
        sessions.retain(|txn_id, session| {
            if now.duration_since(session.created_at) > timeout {
                // Let RocksDB clean up the transaction
                let _ = session.transaction.rollback();
                tracing::warn!("Cleaned up expired transaction session: {}", txn_id.0);
                false
            } else {
                true
            }
        });
    }
}
```

### Phase 2: Transaction Operations Implementation

#### 2.1 Transactional Handler Implementation

```rust
struct KvStoreTransactionalHandler {
    transaction_manager: Arc<TransactionManager>,
    runtime_handle: Handle,
}

impl KVStoreSyncHandler for KvStoreTransactionalHandler {
    // Existing single-operation methods remain for backward compatibility
    fn handle_get(&self, req: GetRequest) -> thrift::Result<GetResponse> {
        // Existing implementation
    }
    
    // New transactional methods
    fn handle_begin_transaction(&self, req: BeginTransactionRequest) -> thrift::Result<BeginTransactionResponse> {
        let result = self.runtime_handle.block_on(
            self.transaction_manager.begin_transaction(req.read_version)
        );
        
        match result {
            Ok(txn_id) => Ok(BeginTransactionResponse {
                transaction_id: txn_id.0,
                error: None,
            }),
            Err(e) => Ok(BeginTransactionResponse {
                transaction_id: String::new(),
                error: Some(e),
            }),
        }
    }
    
    fn handle_txn_get(&self, req: TxnGetRequest) -> thrift::Result<TxnGetResponse> {
        let txn_id = TransactionId(req.transaction_id);
        
        let result = self.runtime_handle.block_on(async {
            let session = self.transaction_manager.get_session(&txn_id).await?;
            
            // Use RocksDB transaction to perform the get operation
            // RocksDB handles conflict detection, isolation, etc.
            let value_opt = session.transaction.get(&req.key)
                .map_err(|e| format!("Get failed: {}", e))?;
            
            match value_opt {
                Some(value_bytes) => Ok(TxnGetResponse {
                    value: String::from_utf8_lossy(&value_bytes).to_string(),
                    found: true,
                    error: None,
                }),
                None => Ok(TxnGetResponse {
                    value: String::new(),
                    found: false,
                    error: None,
                }),
            }
        });
        
        match result {
            Ok(response) => Ok(response),
            Err(e) => Ok(TxnGetResponse {
                value: String::new(),
                found: false,
                error: Some(e),
            }),
        }
    }
    
    fn handle_txn_set(&self, req: TxnSetRequest) -> thrift::Result<TxnSetResponse> {
        let txn_id = TransactionId(req.transaction_id);
        
        let result = self.runtime_handle.block_on(async {
            let session = self.transaction_manager.get_session(&txn_id).await?;
            
            // Use RocksDB transaction to perform the set operation
            // RocksDB handles conflict detection, locking, etc.
            session.transaction.put(&req.key, &req.value)
                .map_err(|e| format!("Set failed: {}", e))?;
            
            Ok(())
        });
        
        match result {
            Ok(_) => Ok(TxnSetResponse {
                success: true,
                error: None,
            }),
            Err(e) => Ok(TxnSetResponse {
                success: false,
                error: Some(e),
            }),
        }
    }
    
    fn handle_commit_transaction(&self, req: CommitTransactionRequest) -> thrift::Result<CommitTransactionResponse> {
        let txn_id = TransactionId(req.transaction_id);
        
        // Delegate to session manager, which delegates to RocksDB for actual commit
        let result = self.runtime_handle.block_on(
            self.transaction_manager.commit_transaction(&txn_id)
        );
        
        match result {
            Ok(committed_version) => Ok(CommitTransactionResponse {
                success: true,
                committed_version,
                error: None,
            }),
            Err(e) => Ok(CommitTransactionResponse {
                success: false,
                committed_version: 0,
                error: Some(e),
            }),
        }
    }
}
```

### Phase 3: 3FS Adapter Implementation

#### 3.1 3FS Adapter Implementation (Protocol Translation)

Create a **protocol adapter** that translates between 3FS interfaces and Thrift RPC calls:

```rust
// src/fdb_compat/mod.rs - Protocol adapter, NOT transaction implementation
use async_trait::async_trait;
use std::sync::Arc;

pub type CoTryTask<T> = Result<T, TransactionError>;

#[derive(Debug, Clone)]
pub enum TransactionError {
    Conflict,         // RocksDB conflict detection triggered
    MaybeCommitted,   // Network error during commit
    Throttled,        // RocksDB throttling
    TooOld,          // Read version too old
    NetworkError(String),
    Failed(String),
}

// 3FS interface - we provide protocol translation
#[async_trait]
pub trait IKVEngine: Send + Sync {
    async fn create_readonly_transaction(&self) -> Box<dyn IReadOnlyTransaction>;
    async fn create_readwrite_transaction(&self) -> Box<dyn IReadWriteTransaction>;
}

// Protocol adapter - translates 3FS calls to Thrift RPC
pub struct RustThriftKVEngine {
    thrift_client: Arc<ThriftClient>, // Connection pool to Rust Thrift servers
}

impl RustThriftKVEngine {
    pub fn new(endpoints: Vec<String>) -> Self {
        // Initialize Thrift client connection pool
        let thrift_client = Arc::new(ThriftClient::new(endpoints));
        Self { thrift_client }
    }
}

#[async_trait]
impl IKVEngine for RustThriftKVEngine {
    async fn create_readonly_transaction(&self) -> Box<dyn IReadOnlyTransaction> {
        // Make Thrift RPC to begin transaction on server
        let txn_id = self.thrift_client.begin_transaction(None).await
            .expect("Failed to begin transaction");
        Box::new(RustThriftTransaction::new(self.thrift_client.clone(), txn_id))
    }
    
    async fn create_readwrite_transaction(&self) -> Box<dyn IReadWriteTransaction> {
        let txn_id = self.thrift_client.begin_transaction(None).await
            .expect("Failed to begin transaction");
        Box::new(RustThriftTransaction::new(self.thrift_client.clone(), txn_id))
    }
}

// Protocol translator - converts 3FS method calls to Thrift RPCs
pub struct RustThriftTransaction {
    client: Arc<ThriftClient>,
    transaction_id: TransactionId,
    read_version: Option<i64>,
    committed_version: Option<i64>,
}

#[async_trait]
impl IReadWriteTransaction for RustThriftTransaction {
    // Translate 3FS set() call to Thrift RPC
    async fn set(&mut self, key: &str, value: &str) -> CoTryTask<()> {
        let request = TxnSetRequest {
            transaction_id: self.transaction_id.0.clone(),
            key: key.to_string(),
            value: value.to_string(),
        };
        
        // Make Thrift RPC - server uses RocksDB transaction for actual work
        let response = self.client.txn_set(request).await
            .map_err(|e| TransactionError::NetworkError(e.to_string()))?;
            
        if response.success {
            Ok(())
        } else {
            // Map server errors to 3FS error types
            Err(TransactionError::Failed(response.error.unwrap_or_default()))
        }
    }
    
    // Translate 3FS commit() call to Thrift RPC
    async fn commit(&mut self) -> CoTryTask<()> {
        let request = CommitTransactionRequest {
            transaction_id: self.transaction_id.0.clone(),
        };
        
        // Make Thrift RPC - server uses RocksDB transaction.commit()
        let response = self.client.commit_transaction(request).await
            .map_err(|e| TransactionError::NetworkError(e.to_string()))?;
            
        if response.success {
            self.committed_version = Some(response.committed_version);
            Ok(())
        } else {
            // Map RocksDB errors from server to 3FS error types
            let error = response.error.unwrap_or_default();
            if error.contains("conflict") {
                Err(TransactionError::Conflict)  // RocksDB detected conflict
            } else {
                Err(TransactionError::Failed(error))
            }
        }
    }
    
    // Other methods translate 3FS calls to corresponding Thrift RPCs...
}
```

**Key Point**: This adapter is **pure protocol translation**. The actual transaction logic (ACID, conflicts, etc.) happens in RocksDB on the server side.

#### 3.2 Integration with HybridKvEngine

Add the Rust Thrift adapter to 3FS's HybridKvEngine:

```cpp
// In 3FS codebase: src/fdb/HybridKvEngineConfig.h
struct RustThriftKVConfig : public ConfigBase<RustThriftKVConfig> {
    CONFIG_ITEM(endpoints, std::vector<std::string>{"127.0.0.1:9090"});
    CONFIG_ITEM(timeout_ms, 10000);
    CONFIG_ITEM(max_connections, 10);
    CONFIG_ITEM(retry_attempts, 3);
};

struct HybridKvEngineConfig : public ConfigBase<HybridKvEngineConfig> {
    CONFIG_ITEM(use_memkv, false);
    CONFIG_OBJ(fdb, kv::fdb::FDBConfig);
    CONFIG_OBJ(rust_thrift, RustThriftKVConfig);
    CONFIG_ITEM(engine_type, std::string("fdb")); // "fdb", "mem", "rust_thrift"
};
```

### Phase 4: Error Handling and Monitoring

#### 4.1 Error Code Mapping

Map RocksDB errors from server responses to 3FS TransactionCode:

```rust
impl From<rocksdb::Error> for TransactionError {
    fn from(error: rocksdb::Error) -> Self {
        // This happens on the server side - map RocksDB errors to response errors
        match error.kind() {
            rocksdb::ErrorKind::Busy => TransactionError::Conflict,
            rocksdb::ErrorKind::TryAgain => TransactionError::Throttled,
            rocksdb::ErrorKind::TimedOut => TransactionError::NetworkError("Timeout".to_string()),
            _ => TransactionError::Failed(error.to_string()),
        }
    }
}

// Client-side: Map Thrift response errors to 3FS errors
impl RustThriftTransaction {
    fn map_server_error(error_msg: &str) -> TransactionError {
        if error_msg.contains("conflict") {
            TransactionError::Conflict
        } else if error_msg.contains("timeout") {
            TransactionError::NetworkError(error_msg.to_string())
        } else {
            TransactionError::Failed(error_msg.to_string())
        }
    }
}
```

#### 4.2 Monitoring Integration

Add metrics following 3FS patterns:

```rust
use tracing::{info, error, warn};

pub struct Metrics {
    pub total_transactions: Arc<AtomicU64>,
    pub failed_transactions: Arc<AtomicU64>,
    pub conflict_count: Arc<AtomicU64>,
    pub commit_latency: Arc<RwLock<Vec<Duration>>>,
}

impl TransactionManager {
    async fn commit_transaction_with_metrics(&self, txn_id: &TransactionId) -> Result<i64, String> {
        let start = std::time::Instant::now();
        
        let result = self.commit_transaction(txn_id).await;
        
        let duration = start.elapsed();
        
        match &result {
            Ok(version) => {
                self.metrics.total_transactions.fetch_add(1, Ordering::Relaxed);
                info!("Transaction session {} committed successfully with version {}, duration: {:?}", 
                      txn_id.0, version, duration);
            }
            Err(e) => {
                self.metrics.failed_transactions.fetch_add(1, Ordering::Relaxed);
                if e.contains("conflict") {
                    self.metrics.conflict_count.fetch_add(1, Ordering::Relaxed);
                    info!("Transaction session {} failed due to RocksDB conflict detection", txn_id.0);
                } else {
                    error!("Transaction session {} commit failed: {}, duration: {:?}", 
                           txn_id.0, e, duration);
                }
            }
        }
        
        result
    }
}
```

## Implementation Timeline

### Phase 1 (Week 1-2): Protocol and Session Management
- Extend Thrift interface with transaction operations
- Implement **session management layer** that maps transaction IDs to RocksDB Transaction objects
- Add transaction session lifecycle management (begin/commit/cancel)
- Basic unit tests for session management (RocksDB transaction logic already tested)

### Phase 2 (Week 3-4): Transaction Operations
- Implement transactional RPC handlers that delegate to RocksDB transactions
- Add range query support with KeySelector
- Add versionstamp operation support (if needed for 3FS compatibility)
- Integration tests with RocksDB (leveraging existing transaction capabilities)

### Phase 3 (Week 5-6): 3FS Protocol Adapter Layer
- Create **protocol translation layer** that implements 3FS ITransaction interface
- Implement Thrift client pool for connecting to transaction servers
- Add error mapping between RocksDB errors and 3FS error types
- Integration with 3FS HybridKvEngine
- End-to-end testing with 3FS metadata operations

### Phase 4 (Week 7-8): Production Readiness
- Add comprehensive monitoring and metrics
- Implement connection pooling and failover for Thrift client
- Session cleanup and timeout handling
- Load testing and production validation
- Documentation and operational procedures

**Key Insight**: Since RocksDB TransactionDB already handles ACID properties, most effort goes into session management and protocol adaptation rather than implementing transaction logic.

## Testing Strategy

### Unit Tests
- Session lifecycle management (begin/commit/cancel/cleanup)
- Transaction ID mapping correctness
- Error handling and edge cases in RPC layer
- Protocol translation correctness

### Integration Tests
- Multi-operation transactions across RPC calls
- Concurrent transaction session handling
- 3FS metadata operation compatibility
- Failover and recovery scenarios
- RocksDB transaction behavior (conflict detection, isolation, etc.)

### Performance Tests
- Transaction session throughput benchmarks
- RPC latency measurements under load
- Memory usage and connection pooling efficiency
- Comparison with FoundationDB adapter performance
- RocksDB transaction performance (baseline testing)

## Configuration

### Server Configuration
```toml
[transaction_sessions]
max_concurrent_sessions = 1000
session_timeout_seconds = 300
cleanup_interval_seconds = 60

[thrift]
bind_address = "0.0.0.0:9090"
max_connections = 100

[rocksdb]
# Existing RocksDB TransactionDB configuration
# Transaction behavior is handled by RocksDB
```

### 3FS Integration Configuration
```toml
[kv_engine]
engine_type = "rust_thrift"

[kv_engine.rust_thrift]
endpoints = ["127.0.0.1:9090", "127.0.0.1:9091"]
timeout_ms = 10000
max_connections = 10
retry_attempts = 3
```

## Operational Considerations

### Deployment
- Deploy Rust Thrift servers with load balancing
- Configure health checks and monitoring
- Set up log aggregation and alerting

### Monitoring
- Transaction session success/failure rates
- RocksDB conflict detection rates (from server responses)
- Connection pool utilization (Thrift clients)
- Session cleanup and timeout rates
- RocksDB performance metrics (on server side)

### Maintenance
- Session cleanup procedures (not transaction cleanup - RocksDB handles that)
- Database backup and recovery (RocksDB standard procedures)
- Rolling upgrades and protocol compatibility

## Summary

This design provides a comprehensive roadmap for adding **session-based transaction support** to the current Rust Thrift server by:

1. **Leveraging RocksDB TransactionDB**: Use proven ACID transaction engine for all transaction logic
2. **Adding Session Management**: Map transaction IDs to RocksDB Transaction objects across RPC calls
3. **Protocol Adaptation**: Translate between 3FS interfaces and Thrift RPC protocols
4. **Minimal Transaction Logic**: Focus on session management and protocol translation, not reimplementing database transactions

The result is a production-ready transactional KV service that can serve as an alternative to FoundationDB in 3FS while building on RocksDB's proven transaction capabilities rather than reimplementing them.