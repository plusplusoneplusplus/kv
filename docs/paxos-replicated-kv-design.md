# Paxos-Replicated Transactional KV Store Design

## Overview

This document describes the design for integrating the RSML (Replicated State Machine Library) Paxos consensus implementation with the existing transactional key-value store, creating a highly available and consistent distributed database.

## Architecture

### System Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    Single Process Architecture                  │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐                                            │
│  │  Thrift Server  │ ──────┐                                    │
│  │   (Port 9091)   │       │                                    │
│  └─────────────────┘       │                                    │
│                            │                                    │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │              ReplicatedKvStore                             │ │
│  │  ┌───────────────────────────────────────────────────────┐ │ │
│  │  │                PrimaryRouter                          │ │ │
│  │  │  - Detects current primary replica                    │ │ │
│  │  │  - Routes Thrift requests to primary                  │ │ │
│  │  └───────────────────────────────────────────────────────┘ │ │
│  │                                                            │ │
│  │  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐           │ │
│  │  │ Replica 1   │ │ Replica 2   │ │ Replica 3   │           │ │
│  │  │ (Primary)   │ │ (Secondary) │ │ (Secondary) │           │ │
│  │  │             │ │             │ │             │           │ │
│  │  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │           │ │
│  │  │ │ RSML    │ │ │ │ RSML    │ │ │ │ RSML    │ │           │ │
│  │  │ │Consensus│ │ │ │Consensus│ │ │ │Consensus│ │           │ │
│  │  │ │Replica  │ │ │ │Replica  │ │ │ │Replica  │ │           │ │
│  │  │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │           │ │
│  │  │      │      │ │      │      │ │      │      │           │ │
│  │  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │           │ │
│  │  │ │KvStore  │ │ │ │KvStore  │ │ │ │KvStore  │ │           │ │
│  │  │ │Executor │ │ │ │Executor │ │ │ │Executor │ │           │ │
│  │  │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │           │ │
│  │  │      │      │ │      │      │ │      │      │           │ │
│  │  │ ┌─────────┐ │ │ ┌─────────┐ │ │ ┌─────────┐ │           │ │
│  │  │ │RocksDB  │ │ │ │RocksDB  │ │ │ │RocksDB  │ │           │ │
│  │  │ │Instance │ │ │ │Instance │ │ │ │Instance │ │           │ │
│  │  │ └─────────┘ │ │ └─────────┘ │ │ └─────────┘ │           │ │
│  │  └─────────────┘ │ └───────────┘ │ └───────────┘           │ │
│  │         │        │        │      │        │                │ │
│  └────────────────────────────────────────────────────────────┘ │
│            │                 │               │                  │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │                     TCP Network                            │ │
│  │  Port 6001 ◄────────┼─────────────────┼────────► Port 6003 │ │
│  │     ▲               │                 │               ▲    │ │
│  │     └───────────────┼─────────────────┼───────────────┘    │ │
│  │                     ▼                 ▼                    │ │
│  │                Port 6002                                   │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### Key Design Principles

1. **Single Process, Multiple Virtual Replicas**: Three RSML consensus replicas run as virtual nodes within a single process
2. **Primary-Secondary Architecture**: One replica serves as primary (handles Thrift requests), others are secondaries
3. **Paxos Consensus**: All state changes go through Paxos consensus before being applied to local storage
4. **Separate Storage**: Each replica maintains its own RocksDB instance for isolation
5. **TCP Networking**: Replicas communicate via TCP on different ports (6001, 6002, 6003)

## Component Specifications

### 1. ReplicatedKvStore

The main orchestrator that manages the three-replica cluster.

**Responsibilities:**
- Initialize and manage three RSML ConsensusReplica instances
- Set up TCP networking between replicas
- Coordinate startup and shutdown
- Provide interface for client operations
- Handle primary election and failover

**Key Methods:**
```rust
impl ReplicatedKvStore {
    // Initialize the replicated store with 3 replicas
    async fn new(config: ReplicatedConfig) -> Result<Self, ReplicationError>

    // Start all replicas and wait for initial primary election
    async fn start(&mut self) -> Result<(), ReplicationError>

    // Submit a transactional operation for consensus
    async fn execute_operation(&self, operation: KvOperation) -> Result<OpResult, ReplicationError>

    // Get current primary replica ID
    fn get_primary_replica(&self) -> Option<ReplicaId>

    // Check cluster health
    fn is_healthy(&self) -> bool

    // Graceful shutdown
    async fn shutdown(&mut self) -> Result<(), ReplicationError>
}
```

### 2. KvStoreExecutor

Implements RSML's `ExecutionNotifier` trait to execute committed operations on the local RocksDB instance.

**Responsibilities:**
- Receive committed operations from RSML learner
- Execute operations on TransactionalKvDatabase
- Maintain exactly-once execution semantics
- Handle operation ordering and conflict resolution

**Key Implementation:**
```rust
#[async_trait]
impl ExecutionNotifier for KvStoreExecutor {
    // Execute a single committed operation
    async fn notify_commit(&mut self, sequence: u64, value: CommittedValue) -> LearnerResult<()>

    // Execute a batch of committed operations
    async fn notify_commit_batch(&mut self, commits: Vec<(u64, CommittedValue)>) -> LearnerResult<()>

    // Get next expected sequence number
    fn next_expected_sequence(&self) -> u64

    // Check if ready for more commits
    fn is_ready(&self) -> bool
}
```

### 3. PrimaryRouter

Routes client requests to the current primary replica.

**Responsibilities:**
- Track current primary replica using RSML ViewManager
- Route Thrift requests to primary
- Handle primary failover scenarios
- Reject requests when no primary is available

**Key Methods:**
```rust
impl PrimaryRouter {
    // Get current primary replica
    fn get_primary(&self) -> Option<ReplicaId>

    // Execute operation on primary replica
    async fn route_to_primary(&self, operation: KvOperation) -> Result<OpResult, RoutingError>

    // Check if specific replica is currently primary
    fn is_primary(&self, replica_id: ReplicaId) -> bool
}
```

### 4. ReplicatedConfig

Configuration structure for the replicated system.

**Configuration Elements:**
```rust
pub struct ReplicatedConfig {
    // Replica configuration
    pub replica_ids: Vec<ReplicaId>,
    pub base_tcp_port: u16,

    // Database configuration
    pub db_base_path: PathBuf,
    pub db_config: DbConfig,

    // Network configuration
    pub network_timeouts: NetworkTimeouts,
    pub retry_config: RetryConfig,

    // Consensus configuration
    pub consensus_config: ConsensusConfig,

    // Thrift server configuration
    pub thrift_port: u16,
    pub thrift_config: ThriftConfig,
}
```

## Operation Flow

### 1. Startup Sequence

1. **Configuration Loading**: Load ReplicatedConfig from file or environment
2. **Database Initialization**: Create separate RocksDB instances for each replica
3. **Network Setup**: Initialize TCP transport for each replica on ports 6001-6003
4. **RSML Initialization**: Create ConsensusReplica instances with appropriate configs
5. **Replica Startup**: Start each RSML replica and wait for network connectivity
6. **Primary Election**: Wait for initial leader election to complete
7. **Thrift Server**: Start Thrift server and connect to PrimaryRouter
8. **Health Check**: Verify cluster is healthy and ready for requests

### 2. Request Processing

1. **Client Request**: Thrift client sends request to port 9091
2. **Primary Routing**: PrimaryRouter determines current primary replica
3. **Operation Conversion**: Convert Thrift request to KvOperation
4. **Consensus Proposal**: Primary replica submits operation to Paxos
5. **Consensus Agreement**: All replicas participate in Paxos consensus
6. **Committed Execution**: Each replica executes operation via KvStoreExecutor
7. **Response**: Primary sends response back to Thrift client

### 3. Failover Handling

1. **Primary Failure Detection**: RSML ViewManager detects primary failure
2. **View Change**: New view change initiated with different primary
3. **Election Process**: Paxos leader election selects new primary
4. **Router Update**: PrimaryRouter updates to new primary
5. **Request Resumption**: New requests routed to new primary
6. **Consistency**: All replicas remain consistent through WAL and Paxos

## Data Models

### 1. KvOperation

Represents operations that can be replicated via Paxos:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvOperation {
    // Basic operations
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },

    // Range operations
    GetRange {
        start_key: Vec<u8>,
        end_key: Option<Vec<u8>>,
        limit: Option<usize>
    },

    // Transactional operations
    BeginTransaction { read_version: Option<u64>, timeout: Option<u64> },
    CommitTransaction { tx_id: TransactionId, operations: Vec<AtomicOperation> },
    AbortTransaction { tx_id: TransactionId },

    // Snapshot operations
    GetReadVersion,
    SnapshotRead { key: Vec<u8>, read_version: u64 },
}
```

### 2. OperationResult

Results returned from executed operations:

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationResult {
    GetResult { value: Option<Vec<u8>> },
    SetResult { success: bool },
    DeleteResult { success: bool },
    RangeResult { key_values: Vec<KeyValue>, has_more: bool },
    TransactionResult { tx_id: Option<TransactionId> },
    CommitResult { success: bool, committed_version: Option<u64> },
    ReadVersionResult { version: u64 },
    Error { message: String, code: Option<String> },
}
```

## Storage Architecture

### 1. Database Isolation

Each replica maintains separate RocksDB instances:
- **Replica 1**: `./data/replica_1/`
- **Replica 2**: `./data/replica_2/`
- **Replica 3**: `./data/replica_3/`

### 2. WAL Integration

RSML provides Write-Ahead Logging:
- **WAL Files**: `./wal/replica_{id}.wal`
- **Durability**: Operations persisted before acknowledgment
- **Recovery**: Replicas recover from WAL on restart

### 3. Transaction Isolation

Each replica's TransactionalKvDatabase provides:
- **ACID Transactions**: Full transactional semantics
- **Snapshot Isolation**: Consistent point-in-time reads
- **Conflict Detection**: Read-write conflict detection
- **Atomic Commits**: All-or-nothing transaction commits

## Error Handling

### 1. Network Errors

- **Connection Failures**: Automatic retry with exponential backoff
- **Timeout Handling**: Configurable timeouts for operations
- **Partition Tolerance**: Continue operating with majority quorum

### 2. Consensus Errors

- **No Primary**: Reject requests until primary elected
- **Consensus Timeout**: Return timeout error to client
- **View Changes**: Handle ongoing view changes gracefully

### 3. Storage Errors

- **RocksDB Errors**: Map to appropriate client errors
- **Transaction Conflicts**: Return conflict errors to client
- **Disk Space**: Handle storage capacity issues

## Performance Considerations

### 1. Throughput Optimization

- **Request Batching**: Batch operations in single Paxos proposal
- **Parallel Execution**: Execute non-conflicting operations in parallel
- **Network Pipelining**: Pipeline consensus messages
- **Connection Pooling**: Reuse TCP connections between replicas

### 2. Latency Optimization

- **Local Reads**: Read from local replica for snapshot reads
- **Fast Path**: Optimize common case with single-RTT consensus
- **PreVote**: Use PreVote to reduce unnecessary view changes
- **Write Coalescing**: Combine multiple operations

### 3. Memory Management

- **Connection Limits**: Limit concurrent connections
- **Buffer Management**: Efficient memory usage for large operations
- **Cache Optimization**: Configure RocksDB caches appropriately
- **WAL Retention**: Clean up old WAL files periodically

## Configuration

### 1. Environment Variables

```bash
# Replica configuration
REPLICATED_REPLICA_COUNT=3
REPLICATED_BASE_TCP_PORT=6001

# Database configuration
REPLICATED_DB_BASE_PATH=./data
REPLICATED_DB_CONFIG_PATH=./config/db_config.toml

# Network configuration
REPLICATED_NETWORK_TIMEOUT=10s
REPLICATED_CONNECTION_TIMEOUT=5s

# Thrift server
THRIFT_PORT=9091
THRIFT_BIND_ADDRESS=0.0.0.0
```

### 2. Configuration File

```toml
[replicated]
replica_count = 3
base_tcp_port = 6001
db_base_path = "./data"

[consensus]
request_timeout = "10s"
view_change_timeout = "5s"
max_batch_size = 100

[network]
connection_timeout = "5s"
retry_attempts = 3
retry_backoff = "100ms"

[thrift]
port = 9091
bind_address = "0.0.0.0"
max_connections = 1000
```

## Testing Strategy

### 1. Unit Tests

- **Component Isolation**: Test each component independently
- **Mock Dependencies**: Use mocks for RSML and RocksDB
- **Error Scenarios**: Test error handling paths
- **Configuration**: Test configuration loading and validation

### 2. Integration Tests

- **End-to-End**: Full request processing through Thrift to storage
- **Failover**: Test primary failover scenarios
- **Consensus**: Verify Paxos consensus correctness
- **Consistency**: Ensure all replicas stay consistent

### 3. Performance Tests

- **Throughput**: Measure operations per second
- **Latency**: Measure request latency distribution
- **Scalability**: Test with increasing load
- **Stress**: Test under resource constraints

### 4. Chaos Tests

- **Network Partitions**: Test split-brain scenarios
- **Process Failures**: Kill and restart replicas
- **Disk Failures**: Simulate storage failures
- **Clock Skew**: Test with time synchronization issues

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] Create replicated module structure
- [ ] Implement basic ReplicatedKvStore
- [ ] Add TCP networking setup
- [ ] Basic primary detection

### Phase 2: RSML Integration
- [ ] Implement KvStoreExecutor
- [ ] Integrate with RSML ConsensusReplica
- [ ] Add WAL persistence
- [ ] Basic operation execution

### Phase 3: Thrift Integration
- [ ] Implement PrimaryRouter
- [ ] Connect Thrift server to replicated store
- [ ] Add operation conversion
- [ ] Error handling and responses

### Phase 4: Advanced Features
- [ ] Transaction support
- [ ] Snapshot reads
- [ ] Range operations
- [ ] Performance optimizations

### Phase 5: Production Readiness
- [ ] Comprehensive testing
- [ ] Configuration management
- [ ] Monitoring and metrics
- [ ] Documentation

## Future Enhancements

### 1. Multi-Node Deployment

Extend from single-process to true distributed deployment:
- **Physical Distribution**: Deploy replicas on different machines
- **Service Discovery**: Automatic replica discovery
- **Load Balancing**: Distribute clients across multiple primaries
- **Geographic Distribution**: Cross-datacenter replication

### 2. Dynamic Reconfiguration

Support changing cluster membership:
- **Adding Replicas**: Safely add new replicas to cluster
- **Removing Replicas**: Remove replicas without data loss
- **Configuration Updates**: Update consensus parameters dynamically
- **Rolling Upgrades**: Upgrade replicas without downtime

### 3. Advanced Features

Additional functionality for production use:
- **Read Replicas**: Non-voting replicas for read scaling
- **Compression**: Data compression for storage efficiency
- **Encryption**: Data encryption at rest and in transit
- **Backup/Restore**: Automated backup and point-in-time recovery

### 4. Monitoring and Observability

Production monitoring capabilities:
- **Metrics**: Prometheus metrics for throughput, latency, errors
- **Tracing**: Distributed tracing for request debugging
- **Health Checks**: Comprehensive health monitoring
- **Alerting**: Alert on consensus failures and performance issues