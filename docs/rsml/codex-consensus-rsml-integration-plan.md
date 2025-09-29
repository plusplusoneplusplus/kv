# Codex RSML Consensus Integration Plan

## Overview

Goal: Introduce a production-grade RSML-backed consensus engine (consensus-rsml) into our unified Rust workspace, replacing or complementing the mock consensus. The integration must preserve our ConsensusEngine trait, work with the existing KvStoreExecutor and KV state machine, enable in-process testing with in-memory transport, and support multi-process TCP-backed e2e tests mirroring RSML’s basic integration patterns.

Key constraints from repo rules:
- Never build in release mode during this work.
- Fix all Rust build warnings in new crates and touched modules.
- Avoid over-simplified fallbacks; if the design doesn’t fit, pause and request guidance.

## What We Learned From RSML Integrated Test

Reference: third_party/RSML/tests/integrated/basic_consensus_test.rs

- Launch model: Spawns 3 replica processes (cargo run --bin consensus_replica) and a test client (cargo run --bin test_client) using a JSON cluster config.
- Endpoints: Each replica has a consensus TCP port and a client TCP port; the client probes primaries and issues requests.
- Health/primary: Client probes for primary via a GetPrimaryStatus message; simple test replica treats ID 0 as primary.
- Request path: Client sends ExecuteRequest to primary, which returns only after commit (i.e., leader replies post-commit per the test’s comment).

Takeaway for us:
- Provide an in-process path for unit/integration tests (no processes) using InMemoryTransport.
- Provide an optional e2e harness using RSML’s TCP transport and/or RSML’s test binaries to validate multi-process behavior.
- Match the “only respond after commit” behavior (our engine propose() can block until commit for correctness and simplicity of mapping).

## Crate(s) To Add

1) consensus-rsml (primary integration crate)
- Path: rust/crates/consensus-rsml/
- Purpose: Implement consensus_api::ConsensusEngine backed by RSML’s ConsensusReplica.
- Modules:
  - engine.rs: RsmlConsensusEngine (ConsensusEngine impl)
  - execution.rs: KvExecutionNotifier (RSML ExecutionNotifier -> KvStoreExecutor)
  - config.rs: Bridge config (replica IDs, transports, WAL paths)
  - id.rs: NodeId(String) <-> RSML ReplicaId(u64) mapping utilities
  - error.rs: Error conversions (RSML -> consensus_api)
  - lib.rs: Re-exports, feature flags, crate wiring

2) (Optional) consensus-rsml-tests (only if we want an isolated test harness)
- Path: rust/crates/consensus-rsml-tests/
- Purpose: In-process and cross-process integration tests without binding into main crates; uses consensus-rsml as a dev-dependency.
- Tests live under rust/tests/ by default; only add this crate if isolation is useful.

## API Bridging And Semantics

Our trait: consensus_api::ConsensusEngine
- propose(data: Vec<u8>) -> ProposeResponse
- start/stop
- wait_for_commit(index)
- identity, term, last_applied_index, cluster membership

RSML primitives we map to:
- ConsensusReplica::start(), shutdown(), get_status()
- ConsensusReplica::execute_request_async(ClientRequest) -> oneshot::Receiver<ExecutionResult>
- Proposer::ExecutionResult { success, result_data, sequence_number, committed_view }
- View/leadership via ViewManager snapshot and status

Mapping decisions:
- Index: RSML sequence_number (u64)
- Term: RSML committed_view.number (u64)
- Leader: map ReplicaId(u64) -> NodeId(String)
- Propose behavior: For correctness and simple mapping, propose() will block until commit and then return success=true with index=sequence_number. This guarantees wait_for_commit(index) can be satisfied immediately or via a small cache.
  - Rationale: RSML’s execute_request_async returns a completion channel with sequence number only upon commit. Returning an index before commit would require tapping into internal sequencing paths and adds complexity/tight coupling.

ConsensusKvDatabase implications:
- With RSML, execution is driven by the Learner through our KvExecutionNotifier. Therefore, ConsensusKvDatabase must not re-apply ops after wait_for_commit. Plan to guard the old flow behind a feature flag:
  - Feature rsml-consensus: ConsensusKvDatabase uses RsmlConsensusEngine.propose() (blocking until commit), then returns OperationResult produced by the notifier. wait_for_commit becomes a no-op for the caller path.
  - Non-rsml: Keep current mock implementation behavior (propose -> wait_for_commit -> local apply).

## Shard Server + Database Integration

Integration points and behavior changes:
- `rust/src/lib/kv_state_machine.rs`
  - Under feature `rsml-consensus`, change `execute_write_operation` to:
    - `propose(op)` → get `index` → `wait_for_commit(index)` → deserialize `OperationResult` and return it.
    - Do not re-apply the operation locally; RSML Learner applies via `KvExecutionNotifier`.
  - Non-RSML path remains unchanged (mock engine: `wait_for_commit` returns the committed operation then local apply).
- `rust/src/servers/shard_server.rs`
  - Add engine selection: if config selects RSML, instantiate `RsmlConsensusEngine` and skip `ConsensusThriftServer` (only used by mock path).
  - Support both RSML in-memory transport (tests/dev, single-process) and TCP transport (multi-process) based on config.
- `rust/src/lib/config.rs`
  - Extend `ConsensusConfig` with `engine: String` (values: `"mock"`, `"rsml"`) and optional RSML transport fields (e.g., `transport = { type = "memory" | "tcp", bind = "host:port" }`).
  - Provide cluster endpoint mapping for RSML TCP (`[consensus.cluster.endpoints]`).
  - Preserve backward compatibility by defaulting to mock when unspecified.

## Execution Bridge (KvExecutionNotifier)

Responsibilities:
- Implement rsml::learner::notification::ExecutionNotifier.
- On notify_commit(sequence, CommittedValue):
  - Deserialize KvOperation from CommittedValue (bincode)
  - Apply via KvStoreExecutor::apply_operation(sequence, operation)
  - Publish result back into a small results cache keyed by sequence_number for the engine.wait_for_commit()
  - Maintain applied_sequence for next_expected_sequence()

Notes:
- Exactly-once semantics: The Executor layer enforces idempotent application using the provided sequence number.
- If batching is enabled by RSML, notify_commit_batch must iterate and apply in sequence order and fill the cache accordingly.
- Result shape: Produce OperationResult bytes (or agreed result struct) into ExecutionResult.result_data. Our propose() will convert this to the consensus_api ProposeResponse as needed.

## Network And Storage Configuration

Transport options:
- InMemoryTransport with create_shared_message_bus() for in-process tests.
- TcpTransport (feature tcp_transport in RSML) for cross-process clusters. Map bind address + peer addresses from config.

Storage options:
- InMemory/WAL for acceptor persistence (via DefaultAcceptorLog<InMemory|WALStorage>). Use WAL in real deployments; keep in-memory for fast tests.

Replica IDs and Node IDs:
- RSML uses ReplicaId(u64). Our NodeId is String.
- Enforce numeric NodeId strings or provide an explicit mapping table in config.rs.
- Provide helpers: string_to_replica_id(), replica_id_to_string().

## Engine Lifecycle And Methods

start()
- Build RSML Configuration (acceptor set, proposers, learners) based on our cluster config.
- Create NetworkManager with chosen transport.
- Construct ConsensusReplica via new_with_wal(...) or new(...) depending on storage.
- Inject KvExecutionNotifier into new_with_wal_and_execution_notifier for correct commit application.
- Call ConsensusReplica.start().

propose(data)
- Build ClientRequest { client_id, request_id, payload=data }.
- client_id: stable per process, request_id: monotonic u64 (engine-internal).
- Await execute_request_async -> ExecutionResult.
- Translate to ProposeResponse { success, index=sequence_number, term=committed_view.number, leader_id }.
- Store result_data in an internal cache keyed by sequence_number for wait_for_commit.

wait_for_commit(index)
- Return result_data from the cache; if missing, wait briefly (should be present because propose blocks until commit). Provide a bounded wait to avoid deadlock if the propose came from another client instance.

is_leader/current_term/node_id/last_applied_index
- Use ConsensusReplica::get_status() and our KvExecutionNotifier’s applied_sequence.

Cluster membership (add/remove nodes)
- Phase 1: Defer dynamic reconfiguration; expose NotImplemented errors mapped cleanly. RSML reconfiguration APIs are not surfaced yet; revisit after base integration is stable.

## Tests And Validation

In-process tests (fast path)
- Build 3 replicas with shared in-memory message bus (create_shared_message_bus()).
- Start 3 RsmlConsensusEngine instances with unique replica_ids.
- Drive a write via the elected leader; assert commit on all.
- Verify KvStoreExecutor applied_sequence increments and results match.

Cross-process TCP e2e (optional but recommended)
- Mirror third_party/RSML/tests/integrated/basic_consensus_test.rs behaviors:
  - Spawn 3 replica processes (either our server embedding RSML or RSML’s own consensus_replica bin for early validation).
  - Add a small test client that probes primary and sends requests over TCP using RSML’s message formats (or reuse RSML’s test_client binary as a black-box verification).
  - Validate health, primary discovery, single request, and sequence test flows.

Feature-gated integration test
- Under rust/tests/rsml_integration/, add tests that only compile/run with feature rsml-consensus (to avoid coupling with mock engine runs).

Warnings discipline
- Add #![deny(warnings)] to new crates during development; fix all warnings.

## File/Module Plan

consensus-rsml/Cargo.toml
- Depend on rsml (path = ../../../third_party/RSML), consensus-api, kv-storage-api, tokio, serde, bincode, tracing.
- Optional features: tcp (enables RSML tcp_transport), wal (enables WAL path), test-utils (helpers for in-process clusters).

consensus-rsml/src/lib.rs
- pub mod engine; pub mod execution; pub mod config; pub mod id; pub mod error;
- Feature gates and re-exports.

consensus-rsml/src/engine.rs
- struct RsmlConsensusEngine { replica_id, inner: Arc<Mutex<ConsensusReplica>>, notifier: Arc<KvExecutionNotifier>, result_cache: Arc<DashMap<u64, Vec<u8>>> , ... }
- impl ConsensusEngine for RsmlConsensusEngine { propose/start/stop/wait_for_commit/... }

consensus-rsml/src/execution.rs
- struct KvExecutionNotifier { executor: Arc<KvStoreExecutor>, applied: AtomicU64, result_tx: Option<Sender<(u64, Vec<u8>)>> }
- impl ExecutionNotifier for KvExecutionNotifier { notify_commit/notify_commit_batch/next_expected_sequence/... }

consensus-rsml/src/config.rs
- Bridge types: ClusterConfig, ReplicaInfo, StorageConfig, NetworkConfig; conversion to RSML’s ConsensusReplicaConfig + TcpConfig.

consensus-rsml/src/id.rs
- String <-> ReplicaId mapping. Enforce numeric-only NodeIds in first iteration.

consensus-rsml/src/error.rs
- Map RSML RSMLError/ReplicaError/NetworkError to consensus_api::ConsensusError.

Core crate changes (feature-gated wiring):
- `rust/src/lib/kv_state_machine.rs`
  - Add RSML branch that deserializes `OperationResult` from `wait_for_commit(index)` and returns it without re-applying ops.
- `rust/src/servers/shard_server.rs`
  - Engine selection and RSML transport setup; skip `ConsensusThriftServer` when RSML is active.
- `rust/src/lib/config.rs`
  - Add `engine` field and RSML transport/endpoint config.

rust/tests/rsml_integration/
- in_memory_cluster.rs: 3-node in-process test (election, propose, commit, apply verification).
- tcp_cluster_smoke.rs (optional): spawns external RSML binaries based on JSON config as smoke test.

Workspace and features:
- `rust/Cargo.toml`
  - Add feature `rsml-consensus` that pulls in `consensus-rsml` and gates the RSML paths.
  - Keep `consensus-rsml` excluded from workspace by default to avoid CI dependency on the submodule; include locally when available.

## Configuration Surfaces

Minimal TOML example (testing, in-memory):

[consensus]
engine = "rsml"
replica_id = "1"
storage = { type = "memory" }
transport = { type = "memory" }

[consensus.cluster]
acceptors = ["1", "2", "3"]
proposers = ["1"]
learners = ["1"]

TCP example (dev multi-process):

[consensus]
engine = "rsml"
replica_id = "1"
storage = { type = "wal", wal_path = "./data/consensus/wal-1" }
transport = { type = "tcp", bind = "127.0.0.1:8000" }

[consensus.cluster.endpoints]
"1" = { host = "127.0.0.1", consensus_port = 8000 }
"2" = { host = "127.0.0.1", consensus_port = 8001 }
"3" = { host = "127.0.0.1", consensus_port = 8002 }

## Migration Strategy

Phase 1: Parallel engine
- Add consensus-rsml behind feature rsml-consensus.
- Wire ConsensusKvDatabase::new() to instantiate RsmlConsensusEngine under that feature; otherwise keep MockConsensusEngine.

Phase 2: Adjust apply path
- Under rsml-consensus, ConsensusKvDatabase should stop applying after wait_for_commit (results come from notifier). Return OperationResult from RSML’s ExecutionResult.

Phase 3: Expand tests and stabilize
- In-process 3-node tests, failure injection, timeouts.
- Optional cross-process smoke test against RSML binaries.
- Benchmark vs. mock engine; iterate on buffer/batch settings.

## Milestones

- M1: Scaffold `consensus-rsml` crate, single-node in-memory RSML engine compiles with no warnings; basic propose/commit test passes.
- M2: Feature-gated wiring in `kv_state_machine.rs`, `shard_server.rs`, and config; 3-node in-process RSML test passes.
- M3: RSML TCP networking and a multi-process smoke test; shard server selection works with config.
- M4: Observability (leader/view/sequence), robustness (timeouts/retries), and parity hardening with mock path.

## Open Questions / Follow-ups

- Dynamic reconfiguration (add/remove nodes): out of scope initially; evaluate RSML support later.
- Term/reporting: Use committed_view.number for Term in ProposeResponse; verify any callers relying on specific term semantics.
- Client IDs: choose stable client_id per node/process; store in engine to allow dedupe and cache hits in RSML.
- Exactly-once: Ensure executor apply path is strictly idempotent on sequence.

## Acceptance Criteria

- Build passes in debug with no warnings for new crates and changed modules.
- In-process 3-node test: primary discovery, successful propose of N operations, and consistent applied_sequence across nodes.
- Optional TCP smoke test succeeds for health, primary discovery, single and batched requests.
- Feature flag rsml-consensus toggles between mock and RSML engines without breaking existing tests.
