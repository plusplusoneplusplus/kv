# consensus-rsml

Rust wrapper around the RSML (Replicated State Machine Library) consensus engine, implementing the `consensus-api` trait for integration with the KV store.

## Overview

This crate provides `RsmlConsensusEngine`, a Paxos-based consensus implementation supporting multi-node replication with automatic leader election. It requires a minimum cluster size of 3 nodes for quorum. Nodes start with `initial_can_become_primary=false` and the slot monitor determines eligibility, triggering leader election after a randomized timeout (5-8 seconds).

## Transport Options

- **InMemory**: For single-node testing only. Multi-node InMemory clusters cannot perform leader election because each node creates its own isolated message bus. For multi-node testing, use TCP transport.
- **TCP** (feature `tcp`): Production-ready TCP transport with connection pooling and automatic reconnection. Required for multi-node leader election and replication testing.

## Key Components

- **RsmlConsensusEngine**: Main consensus engine implementing `ConsensusEngine` trait
- **ViewManager**: Handles leader election with randomized timeouts
- **SlotMonitor**: Tracks committed slots and manages primary eligibility
- **KvStoreExecutor**: Adapter for applying consensus decisions to the KV store state machine

## Testing

```bash
# Run integration tests
cargo test --package consensus-rsml --features rsml

# Run TCP replication test (requires tcp feature)
cargo test test_rsml_tcp_leader_follower_replication --features "rsml,tcp"
```

Tests create 3-node clusters and verify leader election, replication, and state consistency. TCP tests explicitly wait 10 seconds for leader election to complete before proposing operations.

## Known Limitations

RSML doesn't automatically wire Acceptor → Learner notifications, so TCP tests use `deliver_committed_value_for_testing()` as a workaround to manually deliver committed values to learners.
