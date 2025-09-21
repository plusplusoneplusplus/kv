# RSML Consensus Integration Plan

## Overview

This document outlines the plan to integrate RSML (Replicated State Machine Library) with the KV store to replace the current MockConsensusEngine with a production-ready Multi-Paxos consensus implementation.

## Background Research

### RSML Architecture Analysis

RSML provides a complete Multi-Paxos consensus framework with:

- **ConsensusReplica**: Central orchestrator managing all consensus components
- **ViewManager**: Handles view-based leadership and 1a/1b message processing
- **Proposer**: Manages proposal creation and 2a message broadcasting
- **Acceptor**: Handles promise storage and value acceptance
- **Learner**: Manages commit detection and execution ordering
- **NetworkManager**: Unified networking layer with transport abstraction

### Key Discovery: Storage is Provided

**Important**: RSML handles its own consensus persistence through:
- Storage traits (`AcceptorStorage`, `StorageInterface`) defining interfaces
- Built-in implementations:
  - `InMemoryStorage` for testing/non-persistent scenarios
  - `WALStorage` for production durability
- `DefaultAcceptorLog<S>` wrapper that works with any storage backend

**No need to implement custom RocksDB storage adapters for consensus metadata.**

### Current KV Store Integration Points

The existing system uses:
- `MockConsensusEngine` implementing `ConsensusEngine` trait
- `KvStateMachine` implementing basic `StateMachine` trait
- `KvStoreExecutor` for applying operations to RocksDB
- `ConsensusKvDatabase` wrapper coordinating consensus and execution

## Integration Plan

### 1. Create consensus-rsml Crate

**Location**: `rust/crates/consensus-rsml/`

**Dependencies**:
```toml
[dependencies]
rsml = { path = "../../../third_party/RSML" }
consensus-api = { path = "../consensus-api" }
kv-storage-api = { path = "../kv-storage-api" }
tokio = { version = "1.0", features = ["full"] }
serde = { version = "1.0", features = ["derive"] }
bincode = "1.3"
tracing = "0.1"
```

**Module Structure**:
```
src/
├── lib.rs              # Public API and re-exports
├── engine.rs           # RsmlConsensusEngine implementation
├── execution.rs        # KvExecutionNotifier bridge
├── config.rs           # Configuration management
└── error.rs            # Error types and conversions
```

### 2. Core Components

#### A. RsmlConsensusEngine

Implement the `ConsensusEngine` trait from consensus-api:

```rust
pub struct RsmlConsensusEngine {
    consensus_replica: Arc<Mutex<ConsensusReplica>>,
    replica_id: ReplicaId,
    current_term: Arc<AtomicU64>,
    last_applied_index: Arc<AtomicU64>,
    execution_notifier: Arc<Mutex<KvExecutionNotifier>>,
}

#[async_trait]
impl ConsensusEngine for RsmlConsensusEngine {
    async fn propose(&self, data: Vec<u8>) -> ConsensusResult<ProposeResponse>;
    async fn start(&mut self) -> ConsensusResult<()>;
    async fn stop(&mut self) -> ConsensusResult<()>;
    fn is_leader(&self) -> bool;
    // ... other methods
}
```

**Key Responsibilities**:
- Wrap RSML's ConsensusReplica lifecycle
- Map `propose()` to RSML's `ExecuteRequestAsync`
- Track leadership status through ViewManager
- Handle cluster membership operations

#### B. KvExecutionNotifier (Critical Integration Point)

Bridge between RSML consensus and KV store execution:

```rust
pub struct KvExecutionNotifier {
    executor: Arc<KvStoreExecutor>,
    applied_sequence: Arc<AtomicU64>,
}

#[async_trait]
impl ExecutionNotifier for KvExecutionNotifier {
    async fn notify_commit(&mut self, sequence: u64, value: CommittedValue) -> LearnerResult<()> {
        // 1. Deserialize KvOperation from CommittedValue
        let operation: KvOperation = bincode::deserialize(&value.data)?;

        // 2. Apply through existing KvStoreExecutor
        let result = self.executor.apply_operation(sequence, operation).await?;

        // 3. Update applied sequence tracking
        self.applied_sequence.store(sequence, Ordering::SeqCst);

        Ok(())
    }

    fn next_expected_sequence(&self) -> u64 {
        self.applied_sequence.load(Ordering::SeqCst) + 1
    }

    // ... other methods
}
```

#### C. Configuration Management

```rust
pub struct KvRsmlConfig {
    pub replica_id: u64,
    pub cluster_config: ClusterConfig,
    pub storage_config: StorageConfig,
    pub network_config: NetworkConfig,
    pub performance_params: PerformanceParameters,
}

pub enum StorageConfig {
    InMemory,                    // For testing
    WriteAheadLog { path: PathBuf }, // For production
}

pub struct ClusterConfig {
    pub replicas: HashMap<u64, ReplicaInfo>,
}

pub struct ReplicaInfo {
    pub host: String,
    pub consensus_port: u16,
    pub client_port: u16,
}
```

### 3. Integration Architecture

```
Client Request → KV Store API
       ↓
ConsensusKvDatabase
       ↓
RsmlConsensusEngine (implements ConsensusEngine)
       ↓
RSML ConsensusReplica
       ├── Storage: InMemoryStorage or WALStorage (RSML-provided)
       ├── NetworkManager → TCP/InMemory Transport
       └── Learner → KvExecutionNotifier
                          ↓
                    KvStoreExecutor → RocksDB (KV data only)
```

**Data Flow**:
1. Client submits KV operation via `propose()`
2. RsmlConsensusEngine serializes operation and calls RSML's `ExecuteRequestAsync`
3. RSML handles Multi-Paxos protocol (1a/1b/2a/2b messages)
4. Upon consensus, Learner calls `KvExecutionNotifier::notify_commit`
5. Notifier deserializes operation and applies via `KvStoreExecutor`
6. Results flow back through the consensus response

### 4. Implementation Steps

#### Step 1: Create Crate Structure
- Set up Cargo.toml with proper dependencies
- Create module structure with stub implementations
- Add basic error types and configuration structs

#### Step 2: Implement RsmlConsensusEngine
- Initialize RSML ConsensusReplica with appropriate storage:
  - Testing: `DefaultAcceptorLog::new(InMemoryStorage::new())`
  - Production: `DefaultAcceptorLog::new(WALStorage::new(path)?)`
- Implement ConsensusEngine trait methods
- Handle RSML component lifecycle (initialize, start, stop)
- Map node IDs between consensus-api (String) and RSML (u64)

#### Step 3: Build KV Execution Bridge
- Implement ExecutionNotifier trait for KvExecutionNotifier
- Handle operation serialization/deserialization (KvOperation ↔ Vec<u8>)
- Integrate with existing KvStoreExecutor
- Implement proper sequence tracking and error handling

#### Step 4: Network Configuration
- Use RSML's NetworkManager with appropriate transport:
  - InMemoryTransport for testing (shared message bus)
  - TcpTransport for production (real network connections)
- Configure message routing and component registration
- Handle connection management and fault tolerance

#### Step 5: Testing Infrastructure
- Unit tests for each component using InMemoryTransport
- Integration tests following RSML's basic_consensus_test.rs pattern
- Multi-replica coordination tests with shared message bus
- Fault injection and recovery testing
- Performance benchmarking against MockConsensusEngine

### 5. Configuration Examples

#### Testing Configuration
```toml
[consensus]
replica_id = 0
storage_type = "memory"
transport_type = "memory"

[consensus.cluster]
size = 3

[consensus.replicas]
0 = { host = "127.0.0.1", consensus_port = 8000, client_port = 9000 }
1 = { host = "127.0.0.1", consensus_port = 8001, client_port = 9001 }
2 = { host = "127.0.0.1", consensus_port = 8002, client_port = 9002 }
```

#### Production Configuration
```toml
[consensus]
replica_id = 0
storage_type = "wal"
transport_type = "tcp"

[consensus.storage]
wal_path = "./data/consensus/wal"
# KV data continues using existing RocksDB path
kv_data_path = "./data/rocksdb"

[consensus.network]
bind_address = "0.0.0.0:8000"
connect_timeout_ms = 5000
request_timeout_ms = 30000

[consensus.performance]
enable_message_batching = true
message_batch_size = 100
max_concurrent_requests = 1000

[consensus.cluster]
# Production cluster members
0 = { host = "node1.cluster.local", consensus_port = 8000, client_port = 9000 }
1 = { host = "node2.cluster.local", consensus_port = 8000, client_port = 9000 }
2 = { host = "node3.cluster.local", consensus_port = 8000, client_port = 9000 }
```

### 6. Key Integration Points

#### Operation Serialization
```rust
// Serialize KvOperation for consensus
let proposal_data = bincode::serialize(&kv_operation)?;

// Deserialize in ExecutionNotifier
let kv_operation: KvOperation = bincode::deserialize(&committed_value.data)?;
```

#### Node ID Mapping
```rust
// consensus-api uses String, RSML uses u64
pub fn string_to_replica_id(node_id: &str) -> Result<ReplicaId, ParseError> {
    let id: u64 = node_id.parse()?;
    Ok(ReplicaId::new(id))
}

pub fn replica_id_to_string(replica_id: ReplicaId) -> String {
    replica_id.as_u64().to_string()
}
```

#### Error Translation
```rust
impl From<RSMLError> for ConsensusError {
    fn from(error: RSMLError) -> Self {
        match error {
            RSMLError::NetworkError(e) => ConsensusError::NetworkError { message: e.to_string() },
            RSMLError::ViewError(e) => ConsensusError::LeadershipError { message: e.to_string() },
            // ... other mappings
        }
    }
}
```

### 7. Migration Strategy

#### Phase 1: Parallel Implementation
- Create consensus-rsml crate alongside existing MockConsensusEngine
- Add feature flag "rsml-consensus" to switch implementations
- Maintain backward compatibility with existing APIs

#### Phase 2: Testing and Validation
- Comprehensive testing in isolated environments
- Performance benchmarking against mock implementation
- Integration testing with real workloads
- Fault tolerance and recovery validation

#### Phase 3: Gradual Rollout
- Feature flag rollout with monitoring
- A/B testing between implementations
- Performance monitoring and optimization
- Documentation and operational guides

#### Migration Code Example
```rust
#[cfg(feature = "rsml-consensus")]
use consensus_rsml::RsmlConsensusEngine as DefaultConsensusEngine;

#[cfg(not(feature = "rsml-consensus"))]
use consensus_mock::MockConsensusEngine as DefaultConsensusEngine;

impl ConsensusKvDatabase {
    pub fn new(node_id: NodeId, database: Arc<dyn KvDatabase>) -> Self {
        let executor = Arc::new(KvStoreExecutor::new(database));

        #[cfg(feature = "rsml-consensus")]
        let consensus_engine = {
            let config = KvRsmlConfig::from_env()?;
            RsmlConsensusEngine::new(node_id, config, executor.clone())?
        };

        #[cfg(not(feature = "rsml-consensus"))]
        let consensus_engine = {
            let kv_state_machine = Box::new(KvStateMachine::new(executor.clone()));
            MockConsensusEngine::new(node_id, kv_state_machine)
        };

        // ... rest of implementation
    }
}
```

## Expected Benefits

### Consensus Correctness
- Production-ready Multi-Paxos implementation with formal guarantees
- Proper leader election and view management
- Byzantine fault tolerance with quorum-based decisions
- Exactly-once operation execution semantics

### Performance Improvements
- Optimized consensus protocol with batching support
- Efficient network communication with message compression
- Reduced latency through view-based leadership
- Concurrent request processing with proper ordering

### Operational Excellence
- Comprehensive logging and monitoring integration
- Graceful failure handling and recovery
- Dynamic cluster membership management
- Performance tuning and optimization capabilities

## Risks and Mitigations

### Integration Complexity
- **Risk**: Complex integration between RSML and existing KV store
- **Mitigation**: Incremental implementation with comprehensive testing at each step

### Performance Regression
- **Risk**: RSML might be slower than mock implementation
- **Mitigation**: Extensive benchmarking and performance optimization before rollout

### Operational Complexity
- **Risk**: More complex deployment and debugging
- **Mitigation**: Thorough documentation, monitoring, and operator training

### Migration Issues
- **Risk**: Data consistency issues during migration
- **Mitigation**: Feature flags, parallel testing, and gradual rollout strategy

## Success Criteria

1. **Functional**: All existing KV operations work correctly with RSML
2. **Performance**: Latency within 10% of mock implementation under normal load
3. **Reliability**: Handles node failures gracefully with proper consensus
4. **Scalability**: Supports cluster sizes from 3-7 nodes efficiently
5. **Operational**: Clear monitoring, logging, and debugging capabilities

## Timeline Estimate

- **Week 1-2**: Crate setup and basic RsmlConsensusEngine implementation
- **Week 3-4**: KvExecutionNotifier and integration testing
- **Week 5-6**: Network configuration and multi-replica testing
- **Week 7-8**: Performance optimization and comprehensive testing
- **Week 9-10**: Documentation, migration tooling, and production readiness

## Conclusion

This integration plan leverages RSML's production-ready consensus implementation while maintaining the existing KV store architecture. By focusing on the execution bridge and configuration management, we can achieve robust distributed consensus without reimplementing storage or networking components.