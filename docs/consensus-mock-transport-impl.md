# Consensus-Mock Network Transport Implementation Plan

## Overview
This document outlines the plan to add inter-node network transport capabilities to the consensus-mock implementation, enabling real distributed consensus among the 3-node cluster without implementing a full Raft protocol.

## Current State Analysis

### Existing Limitations
The current consensus-mock implementation has the following limitations:
- **In-process only**: Uses `ConsensusMessageBus` with broadcast channels that only work within a single process
- **No network communication**: All nodes must share the same message bus instance
- **No real distribution**: Cannot achieve consensus between separate processes/machines
- **Simulated replication**: Messages are "published" to a bus but not actually sent over network

### Existing Strengths to Preserve
- Clean consensus API implementation
- Good testing infrastructure with message bus for debugging
- Simple leader/follower model
- Log replication logic already present
- State machine abstraction working well

## Design Goals

1. **Minimal invasiveness**: Keep changes focused on transport layer
2. **Reuse existing infrastructure**: Leverage Thrift protocol already in use
3. **Maintain simplicity**: Avoid complex consensus algorithms
4. **Enable real distribution**: Allow consensus across network boundaries
5. **Preserve testability**: Keep in-memory mode for unit tests

## Architecture Design

### High-Level Architecture
```
┌─────────────────┐     Thrift RPC      ┌─────────────────┐
│   Node 0        │◄───────────────────►│   Node 1        │
│   (Leader)      │                      │   (Follower)    │
│                 │                      │                 │
│ MockConsensus   │                      │ MockConsensus   │
│ + Transport     │                      │ + Transport     │
└─────────────────┘                      └─────────────────┘
         ▲                                        ▲
         │              Thrift RPC                │
         └───────────────────────────────────────┘
                            │
                   ┌─────────────────┐
                   │   Node 2        │
                   │   (Follower)    │
                   │                 │
                   │ MockConsensus   │
                   │ + Transport     │
                   └─────────────────┘
```

### Component Design

#### 1. Thrift Protocol Extensions
Add consensus RPCs to `kvstore.thrift`:

```thrift
// Consensus messages
struct ConsensusLogEntry {
    1: required i64 index,
    2: required i64 term,
    3: required binary data,
    4: required i64 timestamp
}

struct AppendEntryRequest {
    1: required string leader_id,
    2: required ConsensusLogEntry entry,
    3: required i64 prev_log_index,
    4: required i64 prev_log_term,
    5: required i64 leader_term
}

struct AppendEntryResponse {
    1: required string node_id,
    2: required bool success,
    3: required i64 index,
    4: optional string error
}

struct CommitNotificationRequest {
    1: required string leader_id,
    2: required i64 commit_index,
    3: required i64 leader_term
}

struct CommitNotificationResponse {
    1: required bool success
}

// Add to TransactionalKV service
service TransactionalKV {
    // ... existing methods ...

    // Consensus RPCs
    AppendEntryResponse appendEntry(1: AppendEntryRequest request)
    CommitNotificationResponse commitNotification(1: CommitNotificationRequest request)
}
```

#### 2. NetworkTransport Module
Create `rust/crates/consensus-mock/src/network_transport.rs`:

```rust
pub struct NetworkTransport {
    node_id: String,
    node_endpoints: HashMap<String, String>,  // node_id -> "host:port"
    client_pool: HashMap<String, Arc<ThriftClient>>,
    message_bus: Option<Arc<ConsensusMessageBus>>, // For debugging
}

impl NetworkTransport {
    pub async fn new(
        node_id: String,
        endpoints: HashMap<String, String>
    ) -> Result<Self> {
        // Initialize Thrift clients to all other nodes
    }

    pub async fn send_append_entry(
        &self,
        target_node: &str,
        entry: LogEntry,
        leader_id: String,
        prev_log_index: Index,
        prev_log_term: Term
    ) -> Result<AppendEntryResponse> {
        // Send via Thrift RPC
    }

    pub async fn send_commit_notification(
        &self,
        target_node: &str,
        commit_index: Index,
        leader_id: String,
        leader_term: Term
    ) -> Result<()> {
        // Send via Thrift RPC
    }
}
```

#### 3. Modified MockConsensusEngine
Update `rust/crates/consensus-mock/src/mock_node.rs`:

```rust
pub struct MockConsensusEngine {
    // ... existing fields ...

    // Add network transport
    network_transport: Option<Arc<NetworkTransport>>,

    // Keep message bus for local testing/debugging
    message_bus: Option<Arc<ConsensusMessageBus>>,
}

impl MockConsensusEngine {
    pub fn with_network_transport(
        node_id: NodeId,
        state_machine: Box<dyn StateMachine>,
        network_transport: Arc<NetworkTransport>,
    ) -> Self {
        // Initialize with network transport
    }

    async fn replicate_to_followers(&self, entry: LogEntry) {
        if let Some(transport) = &self.network_transport {
            // Use network transport
            for follower_id in self.get_follower_ids() {
                tokio::spawn(async move {
                    transport.send_append_entry(...).await;
                });
            }
        } else if let Some(bus) = &self.message_bus {
            // Fall back to in-memory bus for tests
            bus.publish(...);
        }
    }
}
```

#### 4. Consensus RPC Handlers
Add to `rust/src/servers/thrift_server.rs`:

```rust
impl TransactionalKVSyncHandler for ThriftKvAdapter {
    // ... existing methods ...

    fn handle_append_entry(&self, req: AppendEntryRequest) -> thrift::Result<AppendEntryResponse> {
        // Forward to consensus engine
        let consensus = self.routing_manager.get_consensus_engine();
        consensus.handle_append_entry(req).await
    }

    fn handle_commit_notification(&self, req: CommitNotificationRequest) -> thrift::Result<CommitNotificationResponse> {
        // Forward to consensus engine
        let consensus = self.routing_manager.get_consensus_engine();
        consensus.handle_commit_notification(req).await
    }
}
```

#### 5. Simple Leader Election
Implement timeout-based leader selection:

```rust
pub struct LeaderElection {
    node_id: u32,
    cluster_size: u32,
    current_leader: AtomicU32,
    last_heartbeat: AtomicU64,
    election_timeout_ms: u64,
}

impl LeaderElection {
    pub fn new(node_id: u32, cluster_size: u32) -> Self {
        Self {
            node_id,
            cluster_size,
            current_leader: AtomicU32::new(0), // Node 0 starts as leader
            last_heartbeat: AtomicU64::new(current_time_ms()),
            election_timeout_ms: 5000, // 5 second timeout
        }
    }

    pub async fn check_leadership(&self) -> bool {
        let current_time = current_time_ms();
        let last_hb = self.last_heartbeat.load(Ordering::Relaxed);

        if current_time - last_hb > self.election_timeout_ms {
            // Timeout - elect next leader
            let old_leader = self.current_leader.load(Ordering::Relaxed);
            let new_leader = (old_leader + 1) % self.cluster_size;

            if new_leader == self.node_id {
                // We are the new leader
                self.current_leader.store(new_leader, Ordering::Relaxed);
                return true;
            }
        }

        self.current_leader.load(Ordering::Relaxed) == self.node_id
    }

    pub fn record_heartbeat(&self) {
        self.last_heartbeat.store(current_time_ms(), Ordering::Relaxed);
    }
}
```

#### 6. KvStateMachine Implementation
Create `rust/src/lib/replication/kv_state_machine.rs`:

```rust
pub struct KvStateMachine {
    database: Arc<dyn KvDatabase>,
}

impl StateMachine for KvStateMachine {
    fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
        // Deserialize KvOperation from entry.data
        let operation: KvOperation = bincode::deserialize(&entry.data)?;

        // Execute operation on database
        let result = self.database.execute_operation(operation).await?;

        // Serialize result
        Ok(bincode::serialize(&result)?)
    }

    fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
        // Create database snapshot
        self.database.create_snapshot()
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
        // Restore database from snapshot
        self.database.restore_snapshot(snapshot)
    }
}
```

#### 7. RoutingManager Integration
Update `rust/src/lib/replication/routing_manager.rs`:

```rust
pub struct RoutingManager {
    // ... existing fields ...
    consensus_engine: Option<Arc<MockConsensusEngine>>,
}

impl RoutingManager {
    pub fn new_with_consensus(
        database: Arc<dyn KvDatabase>,
        deployment_mode: DeploymentMode,
        node_id: u32,
        cluster_endpoints: Vec<String>,
    ) -> Self {
        let state_machine = Box::new(KvStateMachine::new(database.clone()));

        // Create network transport
        let transport = NetworkTransport::new(
            node_id.to_string(),
            create_endpoint_map(cluster_endpoints),
        );

        // Create consensus engine with transport
        let consensus = MockConsensusEngine::with_network_transport(
            node_id.to_string(),
            state_machine,
            Arc::new(transport),
        );

        Self {
            database,
            deployment_mode,
            node_id: Some(node_id),
            consensus_engine: Some(Arc::new(consensus)),
        }
    }

    pub async fn route_operation(&self, operation: KvOperation) -> RoutingResult<OperationResult> {
        if operation.is_read_only() {
            // Reads go directly to local database
            self.execute_locally(operation).await
        } else {
            // Writes go through consensus
            if let Some(consensus) = &self.consensus_engine {
                if consensus.is_leader() {
                    // We are leader - propose through consensus
                    let data = bincode::serialize(&operation)?;
                    let response = consensus.propose(data).await?;

                    if response.success {
                        // Wait for commit
                        let result_data = consensus.wait_for_commit(response.index.unwrap()).await?;
                        Ok(bincode::deserialize(&result_data)?)
                    } else {
                        Err(RoutingError::NotLeader)
                    }
                } else {
                    // Forward to leader
                    self.forward_to_leader(operation).await
                }
            } else {
                // No consensus - execute locally
                self.execute_locally(operation).await
            }
        }
    }
}
```

## Implementation Phases

### Phase 1: Protocol and Transport (2 days)
1. Add Thrift consensus message definitions
2. Generate Thrift code
3. Implement NetworkTransport module
4. Add connection pooling and retry logic

### Phase 2: Consensus Integration (2 days)
1. Modify MockConsensusEngine to use NetworkTransport
2. Add consensus RPC handlers to thrift_server
3. Implement incoming message processing
4. Keep backward compatibility with in-memory mode

### Phase 3: Leader Election (1 day)
1. Implement simple timeout-based leader election
2. Add heartbeat mechanism
3. Handle leader changes
4. Test failover scenarios

### Phase 4: State Machine and Routing (2 days)
1. Implement KvStateMachine wrapper
2. Update RoutingManager to use consensus
3. Add leader forwarding logic
4. Handle operation serialization

### Phase 5: Testing and Validation (2 days)
1. Create integration tests for 3-node consensus
2. Test leader failure and election
3. Test network partition scenarios
4. Verify consistency guarantees

## Testing Strategy

### Unit Tests
- NetworkTransport connection management
- Leader election timeout logic
- Message serialization/deserialization
- State machine operations

### Integration Tests
- 3-node cluster consensus
- Leader failure and re-election
- Network partition handling
- Consistency under concurrent writes

### Performance Tests
- Latency of consensus operations
- Throughput with replication
- Network overhead measurement

## Configuration Changes

### Cluster Configuration
```toml
[deployment]
mode = "replicated"
instance_id = 0
replica_endpoints = ["localhost:9090", "localhost:9091", "localhost:9092"]

[consensus]
type = "mock"
election_timeout_ms = 5000
heartbeat_interval_ms = 1000
```

## Rollback Plan

The implementation maintains backward compatibility:
- In-memory message bus still works for testing
- Can disable consensus with configuration
- Existing single-node deployments unaffected
- Can revert to local execution if consensus fails

## Success Criteria

1. **Functional**: 3 nodes achieve consensus on write operations
2. **Reliable**: Leader election works on node failure
3. **Consistent**: All nodes have same committed state
4. **Performant**: <100ms latency for consensus operations
5. **Testable**: Both network and in-memory modes work

## Future Extensions

- Add Raft-style term-based voting
- Implement log compaction and snapshots
- Add membership change support
- Optimize batch replication
- Add metrics and monitoring

## Dependencies

- Existing Thrift infrastructure
- Tokio async runtime
- Network connectivity between nodes
- No external consensus libraries needed

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|---------|------------|
| Network failures | Consensus stalls | Add timeout and retry logic |
| Split brain | Data inconsistency | Simple leader election prevents this |
| Performance overhead | Slow writes | Batch operations, optimize serialization |
| Complex debugging | Hard to troubleshoot | Keep message bus for logging |

## Conclusion

This implementation plan provides a practical path to add real network-based consensus to the mock implementation without the complexity of a full Raft protocol. It leverages existing infrastructure, maintains simplicity, and provides a foundation for future enhancements.