# Mock Consensus Transport Implementation

This document describes how the mock consensus layer works in the KV store implementation, focusing on node identification, leadership assignment, and the transport layer.

## Overview

The mock consensus implementation provides a simplified consensus algorithm for testing and development purposes. It uses deterministic leadership assignment and straightforward node identification.

## Node Identification

### Node ID Conversion Flow

```
start_cluster.sh: --node-id 0, 1, 2, ...
        ↓
shard_node.rs: node_id.to_string()
        ↓
MockConsensusEngine: "0", "1", "2", ...
        ↓
Leadership: "0"=Leader, others=Followers
```

### Implementation Details

**Command Line Parsing** (`rust/src/servers/shard_node.rs:44-46`):
```rust
#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    node_id: Option<u32>,  // Numeric ID from --node-id
}
```

**String Conversion** (`rust/src/servers/shard_node.rs:273, 285, 293`):
```rust
// Convert for transport layer
let transport = ThriftTransport::with_endpoints(
    node_id.to_string(),  // 0 → "0", 1 → "1", etc.
    endpoint_map,
).await;

// Convert for consensus engine
MockConsensusEngine::with_network_transport(
    node_id.to_string(),  // Pass string ID to consensus
    state_machine,
    Arc::new(transport),
)
```

## Leadership Assignment

### Static Leadership Model

The mock consensus uses a **hardcoded leadership assignment** approach:

**Primary Assignment** (`rust/src/servers/shard_node.rs:282-296`):
- **Node ID 0**: Always becomes **Leader**
- **Node ID 1+**: Always become **Followers**

```rust
let consensus_engine = if node_id == 0 {
    // Node 0 starts as leader
    MockConsensusEngine::with_network_transport(
        node_id.to_string(),  // "0"
        state_machine,
        Arc::new(transport),
    )
} else {
    // Other nodes start as followers
    MockConsensusEngine::follower_with_network_transport(
        node_id.to_string(),  // "1", "2", "3", ...
        state_machine,
        Arc::new(transport),
    )
};
```

### Election Logic

**Election Rules** (`rust/crates/consensus-mock/src/mock_node.rs:184-185`):
```rust
// Hard-coded election logic: node wins if its ID starts with "leader"
let wins_election = self.node_id.starts_with("leader");
```

**Note**: This election logic is only used for explicit election triggers (`trigger_leader_election()`), not for initial startup leadership assignment.

## Cluster Configuration

### Script Integration

**start_cluster.sh assigns numeric node IDs**:
```bash
# Start each node with numeric node ID
nohup "$PROJECT_ROOT/rust/target/debug/thrift-server" \
    --node-id $node \          # 0, 1, 2, etc.
    --port $port \
    --config "$CONFIG_FILE" \
    > "$LOG_FILE" 2>&1 &
```

### Example 3-Node Cluster

| Script Assignment | Server Processing | Consensus Role |
|-------------------|-------------------|----------------|
| `--node-id 0` | String ID "0" | **Leader** (port 9090) |
| `--node-id 1` | String ID "1" | **Follower** (port 9091) |
| `--node-id 2` | String ID "2" | **Follower** (port 9092) |

## Mock Consensus Engine Components

### Key Traits and Structures

**ConsensusEngine trait** (`rust/crates/consensus-api/src/traits.rs`):
- `is_leader()`: Returns current leadership status
- `node_id()`: Returns string node identifier
- `propose()`: Submits operations for consensus

**MockConsensusEngine** (`rust/crates/consensus-mock/src/mock_node.rs`):
- `new()`: Creates node with default leader behavior
- `new_follower()`: Creates explicit follower node
- `with_network_transport()`: Leader with network transport
- `follower_with_network_transport()`: Follower with network transport

### Network Transport

Network communication uses the generated Thrift protocol:
- Client: `GeneratedThriftTransport` (rocksdb_server::lib::consensus_transport)
  - Uses generated `ConsensusServiceSyncClient` to send real RPCs
  - Manages endpoint map (node_id → host:port)
- Server: `ConsensusThriftServer` (rocksdb_server::lib::consensus_thrift)
  - Wraps `ConsensusServiceSyncProcessor` and adapts to `ConsensusServiceHandler`
  - Blocks on IO and processes actual AppendEntries/RequestVote/Snapshot

## Testing Patterns

### Constructor Usage

```rust
// Leader node
let leader = MockConsensusEngine::new("0".to_string(), state_machine);

// Follower nodes
let follower = MockConsensusEngine::new_follower("1".to_string(), state_machine);

// With network transport
let leader = MockConsensusEngine::with_network_transport(
    "0".to_string(),
    state_machine,
    Arc::new(transport)
);
```

### Election Testing

```rust
// Trigger election
let (won_election, new_term) = consensus.trigger_leader_election().await;

// Check results
assert_eq!(won_election, false); // Non-"leader" prefixed IDs lose
assert_eq!(consensus.is_leader(), false);
```

## Key Files

- **Server Entry Point**: `rust/src/servers/shard_node.rs`
- **Mock Consensus**: `rust/crates/consensus-mock/src/mock_node.rs`
- **Consensus API**: `rust/crates/consensus-api/src/traits.rs`
- **Start Script**: `scripts/start_cluster.sh`
- **Network Transport**: `rust/crates/consensus-mock/src/transport.rs`

## Limitations

1. Deterministic leadership on startup (node 0 leader) remains
2. No dynamic elections unless explicitly triggered in tests
3. Simplified request/response logic in the adapter for non-AppendEntries calls
4. Still a mock consensus; not production-grade

This implementation is designed for testing and development, providing predictable behavior without the complexity of production consensus algorithms like Raft or PBFT.
