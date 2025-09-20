# Multi-Node Architecture Design for Thrift Server

## Executive Summary

This document outlines the design for converting the existing single-node Thrift server into a distributed, multi-node system with replication. The design follows a **refactor-first approach** to minimize risk, separates read and write operations for optimal performance, and abstracts the replication layer to support different consensus algorithms while preserving the existing codebase structure.

## Design Principles

1. **Refactor First**: Minimize changes to existing working code before adding distributed features
2. **Read/Write Separation**: Reads can be served by any node; writes must go through consensus
3. **Operation-Based Replication**: Individual operations are replicated rather than wrapping the entire database
4. **Backward Compatibility**: Existing clients continue to work without changes
5. **Flexible Consensus**: Support multiple consensus algorithms (Raft, Paxos) through abstraction

## Architecture Overview

### Core Components

1. **Operation Classification**: Categorize operations as read-only or write operations
2. **Routing Manager**: Routes operations based on type and consistency requirements
3. **Consensus Integration**: Abstract consensus layer supporting multiple algorithms
4. **State Machine Executor**: Applies consensus decisions to local database
5. **Enhanced Thrift Adapter**: Server-side routing with read/write separation logic
6. **Client**: Client-side routing with leader discovery and read load balancing

### System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Applications                      │
│           (Load balances reads, routes writes to leader)    │
└────────────────────────────┬────────────────────────────────┘
          Reads: Any Node    │ Writes: Leader Only
     ┌─────────────── ───────┼────────────────────────┐
     │                       │                        │
┌────▼─────┐          ┌──────▼────┐            ┌─────▼────┐
│  Node 0  │          │  Node 1   │            │  Node 2  │
│ (Leader) │◄─────────┤ (Follower)│◄───────────┤(Follower)│
├──────────┤Consensus ├───────────┤  Consensus ├──────────┤
│Enhanced  │Messages  │ Enhanced  │  Messages  │ Enhanced │
│ Thrift   │          │  Thrift   │            │ Thrift   │
│ Adapter  │          │ Adapter   │            │ Adapter  │
├──────────┤          ├───────────┤            ├──────────┤
│ Routing  │          │ Routing   │            │ Routing  │
│ Manager  │          │ Manager   │            │ Manager  │
├──────────┤          ├───────────┤            ├──────────┤
│ RocksDB  │          │ RocksDB   │            │ RocksDB  │
└──────────┘          └───────────┘            └──────────┘

Read Flow:  Client ──→ Any Node ──→ Local DB
Write Flow: Client ──→ Leader ──→ Consensus ──→ All Nodes ──→ Local DB
```

## Detailed Design

### 1. Operation Classification

Define operations by their characteristics to enable proper routing:

```rust
// rust/src/replication/operations.rs
pub trait DatabaseOperation {
    fn is_read_only(&self) -> bool;
    fn requires_consensus(&self) -> bool {
        !self.is_read_only()
    }
    fn operation_type(&self) -> OperationType;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvOperation {
    // Read operations - can be served by any node
    Get {
        key: Vec<u8>,
        column_family: Option<String>
    },
    GetRange {
        begin_key: Vec<u8>,
        end_key: Vec<u8>,
        limit: Option<i32>,
        column_family: Option<String>
    },
    SnapshotRead {
        key: Vec<u8>,
        read_version: u64,
        column_family: Option<String>
    },
    GetReadVersion,

    // Write operations - must go through leader and consensus
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>
    },
    Delete {
        key: Vec<u8>,
        column_family: Option<String>
    },
    AtomicCommit {
        request: AtomicCommitRequest
    },
}

impl DatabaseOperation for KvOperation {
    fn is_read_only(&self) -> bool {
        matches!(self,
            KvOperation::Get { .. } |
            KvOperation::GetRange { .. } |
            KvOperation::SnapshotRead { .. } |
            KvOperation::GetReadVersion
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

#[derive(Debug, Clone)]
pub enum OperationType {
    Read,
    Write,
}
```

### 2. Routing Manager with Read/Write Separation

Central component that routes operations based on their type and consistency requirements:

```rust
// rust/src/replication/routing_manager.rs
pub struct RoutingManager {
    local_db: Arc<TransactionalKvDatabase>,
    consensus_manager: Arc<dyn ConsensusManager>,
    read_consistency: ReadConsistencyLevel,
    node_id: u32,
    leader_info: Arc<RwLock<Option<NodeInfo>>>,
}

#[derive(Debug, Clone)]
pub enum ReadConsistencyLevel {
    /// Read from local node immediately (fastest, eventual consistency)
    Local,
    /// Read from leader only (slower, strong consistency)
    Leader,
    /// Read with confirmation from leader (balanced consistency/performance)
    ReadIndex,
}

impl RoutingManager {
    pub async fn execute_operation(&self, operation: KvOperation)
        -> Result<OperationResult, RoutingError> {

        if operation.is_read_only() {
            self.handle_read_operation(operation).await
        } else {
            self.handle_write_operation(operation).await
        }
    }

    async fn handle_read_operation(&self, operation: KvOperation)
        -> Result<OperationResult, RoutingError> {

        match self.read_consistency {
            ReadConsistencyLevel::Local => {
                // Serve directly from local database
                self.execute_on_local_db(operation).await
            }

            ReadConsistencyLevel::Leader => {
                if self.consensus_manager.is_leader().await {
                    // We're the leader, serve locally
                    self.execute_on_local_db(operation).await
                } else {
                    // Return error - client should retry on leader
                    Err(RoutingError::NotLeader {
                        leader: self.get_current_leader().await,
                    })
                }
            }

            ReadConsistencyLevel::ReadIndex => {
                // Confirm we're up-to-date before serving
                let read_index = self.consensus_manager.get_read_index().await?;
                self.wait_for_applied_index(read_index).await?;
                self.execute_on_local_db(operation).await
            }
        }
    }

    async fn handle_write_operation(&self, operation: KvOperation)
        -> Result<OperationResult, RoutingError> {

        if !self.consensus_manager.is_leader().await {
            return Err(RoutingError::NotLeader {
                leader: self.get_current_leader().await,
            });
        }

        // Serialize operation for consensus
        let operation_data = bincode::serialize(&operation)?;

        // Submit to consensus for replication
        let consensus_result = self.consensus_manager
            .propose_operation(operation_data)
            .await?;

        Ok(OperationResult::from(consensus_result))
    }

    async fn execute_on_local_db(&self, operation: KvOperation)
        -> Result<OperationResult, RoutingError> {

        match operation {
            KvOperation::Get { key, column_family } => {
                let result = self.local_db.get(&key, column_family.as_deref()).await?;
                Ok(OperationResult::GetResult(result))
            }
            KvOperation::GetRange { begin_key, end_key, limit, column_family } => {
                let result = self.local_db.get_range(
                    &begin_key, &end_key, 0, true, 0, false, limit
                ).await?;
                Ok(OperationResult::GetRangeResult(result))
            }
            KvOperation::Set { key, value, column_family } => {
                let result = self.local_db.put(&key, &value, column_family.as_deref()).await;
                Ok(OperationResult::OpResult(result))
            }
            // ... handle other operation types
        }
    }
}
```

### 3. Thrift Adapter Integration

The existing `ThriftKvAdapter` will be modified to use the `RoutingManager` instead of calling the database directly. The key changes are:

- Replace `database.get()` calls with `routing_manager.execute_operation(KvOperation::Get{...})`
- Replace `database.put()` calls with `routing_manager.execute_operation(KvOperation::Set{...})`
- Add error handling for `RoutingError::NotLeader` cases, returning appropriate Thrift error responses
- Convert between Thrift request/response types and internal `KvOperation` enums

This maintains full backward compatibility while adding distributed routing capabilities.

### 4. State Machine Executor

Applies consensus decisions to the local database:

```rust
// rust/src/consensus/executor.rs
pub struct KvStoreExecutor {
    database: Arc<TransactionalKvDatabase>,
    applied_sequence: Arc<AtomicU64>,
    pending_responses: Arc<RwLock<HashMap<u64, oneshot::Sender<OperationResult>>>>,
}

impl KvStoreExecutor {
    pub fn new(database: Arc<TransactionalKvDatabase>) -> Self {
        Self {
            database,
            applied_sequence: Arc::new(AtomicU64::new(0)),
            pending_responses: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Apply operation directly to local database (bypassing consensus)
    pub async fn apply_operation(&self, operation: KvOperation) -> Result<OperationResult, ExecutorError> {
        match operation {
            KvOperation::Set { key, value, column_family } => {
                let result = self.database.put(&key, &value, column_family.as_deref()).await;
                Ok(OperationResult::OpResult(result))
            }
            KvOperation::Delete { key, column_family } => {
                let result = self.database.delete(&key, column_family.as_deref()).await;
                Ok(OperationResult::OpResult(result))
            }
            KvOperation::AtomicCommit { request } => {
                let result = self.database.atomic_commit(request).await;
                Ok(OperationResult::AtomicCommitResult(result))
            }
            // Read operations should not come through consensus
            _ => Err(ExecutorError::InvalidOperationForConsensus),
        }
    }
}

// For consensus integration (Raft/Paxos)
#[async_trait]
impl StateMachine for KvStoreExecutor {
    async fn apply(&mut self, sequence: u64, data: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        // Deserialize the operation
        let operation: KvOperation = bincode::deserialize(data)?;

        // Apply to local database
        let result = self.apply_operation(operation).await?;

        // Update applied sequence
        self.applied_sequence.store(sequence, Ordering::SeqCst);

        // Serialize result for response
        let response = bincode::serialize(&result)?;
        Ok(response)
    }
}
```

### 5. Multi-Node Server Configuration

Enhanced configuration supporting different deployment modes and read preferences:

```toml
# bin/replicated_config.toml
[deployment]
mode = "replicated"
instance_id = 0  # Node ID (0, 1, or 2)
replica_endpoints = [
    "localhost:9090",  # Node 0
    "localhost:9091",  # Node 1
    "localhost:9092"   # Node 2
]

[consensus]
algorithm = "raft"  # or "paxos"
election_timeout_ms = 5000
heartbeat_interval_ms = 1000
max_batch_size = 100
max_outstanding_proposals = 1000

[reads]
consistency_level = "local"  # local, leader, read_index
allow_stale_reads = true
max_staleness_ms = 1000

[database]
base_path = "./data/replicated"

[rocksdb]
# Standard RocksDB configuration
write_buffer_size_mb = 64
max_write_buffer_number = 4
block_cache_size_mb = 512
```

### 6. Multi-Node Client with Load Balancing

Client that automatically discovers leaders and load balances read operations:

```rust
// rust/src/client/multi_node_client.rs
pub struct MultiNodeClient {
    nodes: Vec<NodeClient>,
    current_leader: Arc<RwLock<Option<usize>>>,
    read_strategy: ReadStrategy,
    connection_pool: Arc<ConnectionPool>,
}

#[derive(Debug, Clone)]
pub enum ReadStrategy {
    /// Round-robin reads across all nodes
    RoundRobin,
    /// Always read from leader for strong consistency
    LeaderOnly,
    /// Read from nearest/fastest node
    Nearest,
    /// Sticky reads (same node for client session)
    Sticky { node_index: usize },
}

impl MultiNodeClient {
    pub async fn new(endpoints: Vec<String>, read_strategy: ReadStrategy) -> Result<Self, ClientError> {
        let nodes = futures::future::try_join_all(
            endpoints.iter().map(|ep| NodeClient::connect(ep))
        ).await?;

        let mut client = Self {
            nodes,
            current_leader: Arc::new(RwLock::new(None)),
            read_strategy,
            connection_pool: Arc::new(ConnectionPool::new()),
        };

        // Discover initial leader
        client.discover_leader().await?;

        Ok(client)
    }

    pub async fn get(&self, key: &[u8]) -> Result<GetResult, ClientError> {
        match self.read_strategy {
            ReadStrategy::RoundRobin => {
                let node_idx = self.next_read_node().await;
                self.execute_read_with_fallback(node_idx, |client| client.get(key)).await
            }

            ReadStrategy::LeaderOnly => {
                let leader_idx = self.get_leader().await?;
                self.nodes[leader_idx].get(key).await
            }

            ReadStrategy::Nearest => {
                let nearest_idx = self.find_nearest_node().await;
                self.execute_read_with_fallback(nearest_idx, |client| client.get(key)).await
            }

            ReadStrategy::Sticky { node_index } => {
                self.execute_read_with_fallback(node_index, |client| client.get(key)).await
            }
        }
    }

    pub async fn set(&self, key: &[u8], value: &[u8]) -> Result<OpResult, ClientError> {
        // Writes always go to leader
        let leader_idx = self.get_leader().await?;

        match self.nodes[leader_idx].set(key, value).await {
            Ok(result) => Ok(result),
            Err(ClientError::NotLeader) => {
                // Leader changed, rediscover and retry once
                self.discover_leader().await?;
                let new_leader_idx = self.get_leader().await?;
                self.nodes[new_leader_idx].set(key, value).await
            }
            Err(e) => Err(e),
        }
    }

    async fn execute_read_with_fallback<T, F, Fut>(&self,
        preferred_node: usize,
        operation: F
    ) -> Result<T, ClientError>
    where
        F: Fn(&NodeClient) -> Fut + Clone,
        Fut: Future<Output = Result<T, ClientError>>,
    {
        // Try preferred node first
        match operation(&self.nodes[preferred_node]).await {
            Ok(result) => Ok(result),
            Err(ClientError::NodeUnavailable) => {
                // Try leader as fallback
                let leader_idx = self.get_leader().await?;
                if leader_idx != preferred_node {
                    operation(&self.nodes[leader_idx]).await
                } else {
                    Err(ClientError::AllNodesUnavailable)
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn discover_leader(&self) -> Result<(), ClientError> {
        for (idx, node) in self.nodes.iter().enumerate() {
            if let Ok(true) = node.is_leader().await {
                *self.current_leader.write().await = Some(idx);
                return Ok(());
            }
        }
        Err(ClientError::NoLeaderFound)
    }
}
```

## Implementation Status

### Phase 0: Code Preparation and Refactoring ✅ **COMPLETED**

**Goal**: Minimize risk by refactoring existing code without adding distributed features.

1. ✅ **Extract Operation Types** (`rust/src/lib/operations.rs`)
   - ✅ Implemented `DatabaseOperation` trait with `is_read_only()` and `requires_consensus()` methods
   - ✅ Complete `KvOperation` enum with all read and write operations properly categorized
   - ✅ Full serialization support with Serde and bincode
   - ✅ Comprehensive test coverage for operation classification

2. ✅ **Create Database Abstraction** (`rust/src/lib/database_factory.rs`)
   - ✅ Enhanced database factory pattern supporting both standalone and replicated modes
   - ✅ Consistent `KvDatabase` trait interface used throughout the system
   - ✅ `OperationResult` types defined for network serialization
   - ✅ Separate database paths for different replica instances

3. ✅ **Refactor Thrift Adapter** ✅ **COMPLETED**
   - ✅ Clean `ThriftKvAdapter` structure exists in `rust/src/lib/thrift_adapter.rs`
   - ✅ Separation of concerns between protocol handling and business logic
   - ✅ Routing logic fully implemented - all operations go through `RoutingManager`
   - ✅ Operation dispatcher for distributed routing implemented

4. ✅ **Add Configuration Structure** (`rust/src/lib/config.rs`)
   - ✅ Complete deployment mode configuration with `DeploymentMode::Replicated`
   - ✅ Node identity support with `instance_id` and `replica_endpoints`
   - ✅ Backward compatibility maintained for existing single-node config
   - ✅ Integration with database factory pattern

### Phase 1: Basic Routing Infrastructure ✅ **COMPLETED**

**Goal**: Add routing layer without consensus - operations still execute locally.

5. ✅ **Implement Routing Manager** ✅ **COMPLETED**
   - ✅ Complete `RoutingManager` implementation in `rust/src/lib/replication/routing_manager.rs`
   - ✅ Full read/write classification at routing level using `DatabaseOperation` trait
   - ✅ Placeholder for consensus integration prepared for Phase 2

6. ✅ **Create Enhanced Thrift Adapter** ✅ **COMPLETED**
   - ✅ Full routing manager integration in Thrift adapter (`rust/src/lib/thrift_adapter.rs`)
   - ✅ No direct database calls - all operations routed through `RoutingManager`
   - ✅ Distributed error handling patterns implemented

7. ✅ **Add Local Testing** ✅ **COMPLETED**
   - ✅ Comprehensive routing-specific tests implemented (`routing_manager.rs:402-538`)
   - ✅ Regression testing for both standalone and replicated modes

### Phase 2: Multi-Node Foundation ❌ **NOT STARTED**

**Goal**: Add multi-node structure without consensus - each node operates independently.

8. ❌ **Create Multi-Node Server**
   - ❌ No multi-node server executable implemented
   - ❌ Servers still run as independent single-node instances
   - ❌ No inter-node awareness or communication infrastructure

9. ❌ **Implement Basic Client Routing**
   - ❌ Client connects to single endpoint only (`rust/src/client/client.rs`)
   - ❌ No leader election or discovery mechanisms
   - ❌ No read load balancing across multiple nodes

10. ❌ **Add State Machine Executor**
    - ❌ No state machine executor implementation found
    - ❌ No operation logging for consensus preparation
    - ❌ No separation between local execution and consensus application

### Phase 3: Consensus Integration ❌ **NOT STARTED**

**Goal**: Add actual consensus algorithm and operation replication.

11. ❌ **Integrate Consensus Library**
    - ❌ No consensus library dependencies in `Cargo.toml`
    - ❌ No consensus manager abstraction or implementation
    - ❌ Only placeholder comments referencing "RSML consensus"

12. ❌ **Enable Write Replication**
    - ❌ Write operations still execute locally on each node
    - ❌ No consensus-based operation replication
    - ❌ No distributed transaction coordination

13. ❌ **Add Leader Election**
    - ❌ No leader election algorithm implemented
    - ❌ No leader discovery in client
    - ❌ No leader change handling mechanisms

### Phase 4: Client Enhancement ❌ **NOT STARTED**

**Goal**: Add sophisticated client features for production use.

14. ❌ **Enhanced Client Features**
    - ❌ Single consistency level (direct database access)
    - ❌ No connection pooling for multiple nodes
    - ❌ Basic error handling, no retry policies or circuit breakers

15. ❌ **Client Load Balancing**
    - ❌ No read strategy implementations
    - ❌ No latency-based routing capabilities
    - ❌ No session affinity support

### Phase 5: Testing and Validation ❌ **NOT STARTED**

**Goal**: Comprehensive testing of distributed system behavior.

16. ❌ **Integration Testing**
    - ⚠️ `TestCluster` trait exists but no multi-node implementation
    - ❌ No leader election test scenarios
    - ❌ No network partition simulation

17. ❌ **Failure Testing**
    - ❌ No node failure recovery testing
    - ❌ No split-brain prevention tests
    - ❌ No data consistency verification across nodes

18. ❌ **Performance Testing**
    - ❌ No distributed performance benchmarks
    - ❌ No consensus latency analysis
    - ❌ No read scaling verification

## Current Implementation Summary

### What's Working ✅
- **Single-node KV store** with full transactional support via Thrift and gRPC
- **Complete operation classification system** with proper read/write separation
- **Configuration framework** ready for multi-node deployment
- **Database abstraction layer** supporting both standalone and replicated modes
- **Solid foundation** for distributed system implementation

### What's Missing ❌
- **Consensus Integration**: No Raft/Paxos implementation or RSML integration
- **Multi-node Client**: Client only connects to single node
- **Distributed Testing**: No multi-node test infrastructure
- **State Machine**: No consensus-based operation application

### Next Priority Tasks
1. **Choose and Integrate Consensus Library**
   - Add Raft library dependency (e.g., `raft-rs` or `openraft`)
   - Implement consensus manager abstraction
   - Create state machine executor

2. **Add Basic Multi-node Infrastructure**
   - Multi-node server executable
   - Node discovery and health checking
   - Basic client-side load balancing

3. **Implement Multi-node Client**
   - Client with leader discovery and failover
   - Read load balancing across replicas
   - Connection pooling and retry logic

The codebase has excellent foundations for distributed implementation, with the hardest design decisions already made and core abstractions in place. The remaining work is primarily about connecting these pieces with consensus and networking logic.

## Configuration Examples

### Single Node (Backward Compatible)
```toml
[deployment]
mode = "standalone"

[database]
base_path = "./data/standalone"
```

### Three-Node Cluster - Node 0
```toml
[deployment]
mode = "replicated"
instance_id = 0
replica_endpoints = ["localhost:9090", "localhost:9091", "localhost:9092"]

[reads]
consistency_level = "local"
```

### Three-Node Cluster - Node 1
```toml
[deployment]
mode = "replicated"
instance_id = 1
replica_endpoints = ["localhost:9090", "localhost:9091", "localhost:9092"]

[reads]
consistency_level = "read_index"  # More consistent reads on this node
```

## Benefits of This Design

1. **Refactor-First Safety**: Minimal risk by changing working code incrementally
2. **Read Performance**: Read operations scale with number of nodes
3. **Write Consistency**: All writes go through consensus for strong consistency
4. **Flexible Consistency**: Different consistency levels for different use cases
5. **Backward Compatibility**: Existing single-node deployments continue working
6. **Client Transparency**: Existing clients work without changes
7. **Operational Simplicity**: Clear separation between read and write behavior
8. **Future Extensibility**: Abstract consensus layer supports different algorithms

## Testing Strategy

### Unit Tests
- Operation classification and routing logic
- Routing manager with different consistency levels
- State machine executor operation application
- Client load balancing strategies

### Integration Tests
- Three-node cluster setup and teardown
- Leader election and failover scenarios
- Read scaling across multiple nodes
- Write consistency verification
- Network partition recovery

### Performance Tests
- Read throughput scaling with node count
- Write latency with consensus overhead
- Client connection pooling efficiency
- Different read strategy performance comparison

This design provides a clear path from the current single-node system to a fully distributed multi-node system while minimizing risk through incremental refactoring and maintaining backward compatibility.