# Consensus Mock Decoupling and Plugin Architecture Plan

This document proposes a clean decoupling of the mock consensus integration from the Thrift and shard server layers, and introduces a small set of pluggable abstractions to support multiple consensus backends and network transports without entangling application code.

## Problem Statement

Today, the server crate directly references mock consensus implementation details and the Thrift consensus service:

- `DefaultConsensusFactory` constructs `consensus_mock::MockConsensusEngine`, builds a Thrift transport, and starts a Thrift consensus server inline.
- `ConsensusThriftServer` and its adapter depend on `consensus_mock::ConsensusServiceHandler`, including type conversions to mock-specific types.
- Consensus endpoint selection uses a hardcoded mapping (`replica_port - 2000`).
- The handler downcasts `Box<dyn ConsensusEngine>` to `MockConsensusEngine` to append entries/apply commits.

This tight coupling makes it hard to:

- Swap implementations (e.g., RSML, in-memory) or transports (gRPC) without touching shard server code.
- Configure endpoints cleanly (instead of relying on port arithmetic).
- Test with in-memory transports vs. Thrift without invasive changes.

## Current Integration Overview

- Factory and setup
  - `rust/src/lib/consensus_factory.rs` builds `ConsensusKvDatabase`, chooses single vs multi-node, and in multi-node starts a consensus Thrift server.
- Network client path
  - `rust/src/lib/consensus_transport.rs` implements `consensus_mock::NetworkTransport` using generated Thrift `ConsensusService` client.
- Network server path
  - `rust/src/lib/consensus_thrift.rs` hosts a Thrift server. `ConsensusServiceAdapter` forwards to `consensus_mock::ConsensusServiceHandler` and uses mock-specific types.
- Shard server
  - `rust/src/servers/shard_server.rs` builds `RoutingManager` around `ConsensusKvDatabase` and serves the KV Thrift API.

## Goals

- Abstract consensus engine creation and network binding behind small, generic traits.
- Keep shard server unaware of the concrete consensus implementation and its wire protocol.
- Prefer explicit configuration for consensus endpoints; allow fallback to port delta as a compatibility shim.
- Remove downcasts in handlers by introducing well-defined follower-side RPC entry points (optional but desirable).

## Proposed Abstractions

Introduce a new module `consensus_plugin` (server crate) exposing two traits.

1) ConsensusNetwork

```rust
pub trait ConsensusNetwork: Send + Sync {
    fn build_transport(
        &self,
        node_id: consensus_api::NodeId,
        endpoints: std::collections::HashMap<consensus_api::NodeId, String>,
    ) -> Box<dyn consensus_mock::NetworkTransport>;

    fn spawn_server(
        &self,
        port: u16,
        node_id: u32,
        engine: std::sync::Arc<parking_lot::RwLock<Box<dyn consensus_api::ConsensusEngine>>>,
    ) -> Result<std::thread::JoinHandle<()>, Box<dyn std::error::Error>>;
}
```

2) ConsensusPlugin

```rust
pub trait ConsensusPlugin: Send + Sync {
    fn network(&self) -> &dyn ConsensusNetwork;

    fn build_engine(
        &self,
        node_id: consensus_api::NodeId,
        state_machine: Box<dyn consensus_api::StateMachine>,
        transport: Box<dyn consensus_mock::NetworkTransport>,
        role: ConsensusNodeRole,
    ) -> Box<dyn consensus_api::ConsensusEngine>;
}

pub enum ConsensusNodeRole { Leader, Follower }
```

Reference implementations:

- ThriftConsensusNetwork
  - `build_transport` wraps current `GeneratedThriftTransport`.
  - `spawn_server` wraps current `ConsensusThriftServer`.

- MockConsensusPlugin
  - `build_engine` calls `MockConsensusEngine::{with,follower_with}_network_transport`.

Optional follower RPC separation (future-ready):

- Define a small `FollowerRpc` trait in `consensus-api` (or a separate crate) with `handle_append_entries` and friends. Implement in each engine.
- `ConsensusServiceAdapter` depends on `dyn FollowerRpc` instead of mock-specific handler and avoids downcasts.

## Factory and Server Changes

- `DefaultConsensusFactory` takes a `Box<dyn ConsensusPlugin>` (with a default `MockConsensusPlugin` instance if none is supplied):
  - Multi-node flow:
    - Use `plugin.network().build_transport(...)` to create a transport.
    - Use `plugin.build_engine(...)` to construct the engine for leader/follower role.
    - Use `plugin.network().spawn_server(...)` to host consensus RPC on this node.
  - Single-node flow can still call an in-process engine path (mock), or use the plugin with an in-memory transport.

- `shard_server.rs` endpoint selection:
  - Prefer `cluster_config.consensus.as_ref().and_then(|c| c.endpoints.clone())` when present.
  - Fallback to `deployment.replica_endpoints` + a configurable `consensus_port_delta` (default `-2000`) if no explicit endpoints are configured.

## Refactor Plan (Incremental)

1. Introduce traits and module
   - Add `rust/src/lib/consensus_plugin.rs` with `ConsensusNetwork`, `ConsensusPlugin`, and `ConsensusNodeRole`.

2. Implement `ThriftConsensusNetwork`
   - Adapt current `GeneratedThriftTransport` and `ConsensusThriftServer` to implement `ConsensusNetwork`.
   - No behavioral change; just a wrapper.

3. Implement `MockConsensusPlugin`
   - Bridge `MockConsensusEngine` creation for leader/follower with the `ThriftConsensusNetwork` transport.

4. Refactor `DefaultConsensusFactory`
   - Replace direct imports of `consensus_mock`/`consensus_thrift` with `dyn ConsensusPlugin` calls.
   - Maintain existing return type `ConsensusSetup` and keep `ConsensusServerHandle` for compatibility.

5. Update `shard_server.rs` configuration flow
   - When multi-node, compute consensus endpoints via config first. Only apply the `port - 2000` fallback if no explicit endpoints are provided.

6. Tests and compatibility
   - Keep all existing tests green; default plugin remains mock+Thrift so behavior doesn’t change.
   - Add a couple of unit tests for the new traits to ensure network/server binder glue compiles and can be mocked in tests.

## Configuration Changes

- Extend `Config::ConsensusConfig` usage:
  - Respect `consensus.endpoints: Option<Vec<String>>` for consensus RPC addresses.
  - Optionally add `consensus.rpc_port_delta: Option<i32>`; default to `-2000` for backward compatibility.

Behavioral priority when multi-node:

1. Use `consensus.endpoints` if set.
2. Else compute from `deployment.replica_endpoints` and `rpc_port_delta`.

## Acceptance Criteria

- Server compiles and runs with no behavior change by default.
- Shard server no longer imports `consensus_mock` or `consensus_thrift` directly; it only sees `DefaultConsensusFactory` and config.
- `DefaultConsensusFactory` constructs consensus via plugin interfaces.
- Consensus Thrift server/client logic is isolated under a `ThriftConsensusNetwork` implementation.
- Port arithmetic is no longer hardwired; it is a fallback only.
- All existing tests pass; add small coverage for the new plugin scaffolding.

## Risks and Mitigations

- Risk: API creep in traits. Mitigation: keep traits minimal and aligned with current needs (transport builder + server binder + engine factory).
- Risk: Test flakiness around server threads. Mitigation: maintain current threading model; keep join handles, and retain small `sleep` buffers in integration tests.
- Risk: Over-abstracting mock specifics. Mitigation: optional `FollowerRpc` trait helps avoid downcasts without forcing full generality today.

## Future Extensions

- Additional networks: gRPC, QUIC.
- Additional engines: RSML integration behind `ConsensusPlugin`.
- In-memory transport for unit/integration tests without sockets.
- Feature flags to choose plugin at compile time; or runtime selection via config.

## Work Breakdown (Code Pointers)

- New
  - `rust/src/lib/consensus_plugin.rs`: traits + enums.
  - `rust/src/lib/consensus_network_thrift.rs`: ThriftConsensusNetwork implementation.
  - `rust/src/lib/consensus_plugin_mock.rs`: MockConsensusPlugin.

- Modified
  - `rust/src/lib/consensus_factory.rs`: replace direct mock/thrift usage with plugin calls.
  - `rust/src/servers/shard_server.rs`: prefer `consensus.endpoints` and pass to factory.
  - `rust/src/lib/consensus_thrift.rs`, `rust/src/lib/consensus_transport.rs`: may be moved behind network impl module; initial step can keep paths but re-export via the network module.

## Test Plan

- Re-run existing consensus replication tests: `rust/tests/consensus_replication_tests.rs`.
- Smoke test shard server with Thrift benchmark client.
- Add unit tests for plugin wiring (e.g., a dummy `ConsensusNetwork` that records calls, ensuring factory uses it).

## Backward Compatibility

- Default behavior and ports remain unchanged unless `consensus.endpoints` is provided.
- The `port - 2000` mapping remains available as a fallback to avoid breaking existing scripts/configs.

---

Appendix A: Rationale for Removing Downcasts

The current `ConsensusServiceHandler` downcasts `dyn ConsensusEngine` to `MockConsensusEngine` to carry out follower log/commit actions. Introducing a follower-side RPC trait allows handlers to remain generic and reduces the risk of runtime errors if/when a different engine is plugged in.

