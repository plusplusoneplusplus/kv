# Phase 1 Implementation Plan: Happy Path - Basic Working Replication

## Executive Summary

This document provides a detailed implementation plan for Phase 1 of the Paxos-replicated KV store, focusing on achieving basic three-replica consensus with manual testing. Following the "happy path first" philosophy, we will build the simplest possible working implementation before adding complexity.

**Phase 1 Goal**: Get basic SET operations working through Paxos consensus across three in-process replicas with hardcoded configuration.

**Timeline**: 5-7 days of focused development

**Key Principle**: Maximum reuse of existing components with minimal changes to current codebase.

## Current State Assessment

### Already Implemented (from Phase 0)
- ✅ `KvDatabase` trait abstraction (`rust/src/lib/db_trait.rs`)
- ✅ `KvServiceCore` for protocol-agnostic logic (`rust/src/lib/service_core.rs`)
- ✅ `DeploymentMode` enum with Standalone/Replicated (`rust/src/lib/config.rs`)
- ✅ Configuration structure supporting replica endpoints
- ✅ Database factory pattern for mode-based instantiation

### To Be Implemented
- ❌ RSML library integration
- ❌ Consensus operation types and serialization
- ❌ KvStoreExecutor for applying consensus decisions
- ❌ Three-replica manager
- ❌ Primary routing logic
- ❌ Integration with existing Thrift/gRPC handlers

## Prerequisites & Setup

### 1.1 RSML Integration (Day 1)

#### Task: Add RSML as Dependency
```toml
# rust/Cargo.toml
[dependencies]
rsml = { git = "https://github.com/your-org/rsml.git", branch = "main" }
bincode = "1.3"  # For operation serialization
serde = { version = "1.0", features = ["derive"] }
```

#### Task: Create RSML Module Structure
```rust
// rust/src/consensus/mod.rs
pub mod executor;
pub mod operations;
pub mod replica_manager;
pub mod router;

pub use executor::KvStoreExecutor;
pub use operations::KvOperation;
pub use replica_manager::ReplicaManager;
pub use router::ConsensusRouter;
```

## Implementation Tasks

### 1.2 Simple Operation Type (Day 1-2)

#### File: `rust/src/consensus/operations.rs`

```rust
use serde::{Serialize, Deserialize};

/// Simplified operation type for Phase 1
/// Only supports basic SET operations initially
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KvOperation {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        column_family: Option<String>,
    },
    // Phase 2: Add Get, Delete, AtomicCommit, etc.
}

impl KvOperation {
    /// Create a SET operation
    pub fn set(key: Vec<u8>, value: Vec<u8>) -> Self {
        KvOperation::Set {
            key,
            value,
            column_family: None
        }
    }

    /// Serialize operation for consensus
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize operation from consensus
    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}
```

### 1.3 Basic KvStoreExecutor (Day 2)

#### File: `rust/src/consensus/executor.rs`

```rust
use std::sync::Arc;
use async_trait::async_trait;
use rsml::{ExecutionNotifier, LearnerResult, CommittedValue};
use crate::lib::db_trait::KvDatabase;
use super::operations::KvOperation;

/// Executor that applies consensus decisions to the database
pub struct KvStoreExecutor {
    database: Arc<dyn KvDatabase>,
    replica_id: u32,
}

impl KvStoreExecutor {
    pub fn new(database: Arc<dyn KvDatabase>, replica_id: u32) -> Self {
        Self { database, replica_id }
    }
}

#[async_trait]
impl ExecutionNotifier for KvStoreExecutor {
    async fn notify_commit(&mut self, sequence: u64, value: CommittedValue) -> LearnerResult<()> {
        // Log for debugging
        tracing::info!(
            replica_id = self.replica_id,
            sequence = sequence,
            "Applying operation from consensus"
        );

        // Deserialize the operation
        let operation = match KvOperation::deserialize(&value.data) {
            Ok(op) => op,
            Err(e) => {
                tracing::error!("Failed to deserialize operation: {}", e);
                return Ok(()); // Skip malformed operations in Phase 1
            }
        };

        // Apply the operation
        match operation {
            KvOperation::Set { key, value, column_family } => {
                let cf = column_family.as_deref();
                match self.database.put(&key, &value, cf).await {
                    crate::lib::db::OpResult::Ok => {
                        tracing::debug!("Successfully applied SET operation");
                    }
                    crate::lib::db::OpResult::Err(e) => {
                        tracing::error!("Failed to apply SET operation: {}", e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn notify_trim(&mut self, _trim_point: u64) -> LearnerResult<()> {
        // Phase 2: Implement log trimming
        Ok(())
    }
}
```

### 1.4 Three-Replica Manager (Day 3)

#### File: `rust/src/consensus/replica_manager.rs`

```rust
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::RwLock;
use rsml::{ConsensusReplica, ReplicaId, NetworkManager, InMemoryTransport};
use crate::lib::db::TransactionalKvDatabase;
use crate::lib::config::Config;
use super::executor::KvStoreExecutor;
use super::operations::KvOperation;

/// Manages three in-process replicas for Phase 1
pub struct ReplicaManager {
    replicas: Vec<Arc<ConsensusReplica>>,
    executors: Vec<Arc<RwLock<KvStoreExecutor>>>,
    databases: Vec<Arc<TransactionalKvDatabase>>,
    primary_id: ReplicaId,  // Hardcoded to replica 0 in Phase 1
}

impl ReplicaManager {
    /// Create a three-replica cluster in-process
    pub async fn new_three_replicas(base_config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        tracing::info!("Creating three-replica cluster (in-process)");

        // Create shared message bus for in-memory transport
        let message_bus = rsml::create_shared_message_bus();

        let mut replicas = Vec::new();
        let mut executors = Vec::new();
        let mut databases = Vec::new();

        // Hardcoded configuration for Phase 1
        let replica_configs = vec![
            ("replica-0", 0),
            ("replica-1", 1),
            ("replica-2", 2),
        ];

        for (name, id) in replica_configs {
            // Create database for this replica
            let mut db_config = base_config.clone();
            db_config.database.base_path = format!("{}/{}", db_config.database.base_path, name);
            let database = Arc::new(TransactionalKvDatabase::new(db_config));
            databases.push(database.clone());

            // Create executor
            let executor = Arc::new(RwLock::new(
                KvStoreExecutor::new(database.clone(), id)
            ));
            executors.push(executor.clone());

            // Create RSML replica with in-memory transport
            let replica_id = ReplicaId::new(id);
            let transport = Arc::new(InMemoryTransport::new(message_bus.clone()));

            // Simple configuration for Phase 1
            let rsml_config = rsml::Config {
                replica_id,
                cluster_size: 3,
                batch_size: 1,  // No batching in Phase 1
                ..Default::default()
            };

            let network_manager = Arc::new(NetworkManager::new(
                replica_id,
                rsml_config.clone(),
                transport
            )?);

            let replica = Arc::new(ConsensusReplica::new(
                replica_id,
                rsml_config,
                network_manager,
                executor,
            )?);

            replicas.push(replica);

            tracing::info!("Created replica {} with database at {}", id, db_config.database.base_path);
        }

        // Start all replicas
        for replica in &replicas {
            replica.start().await?;
        }

        tracing::info!("All three replicas started successfully");

        Ok(Self {
            replicas,
            executors,
            databases,
            primary_id: ReplicaId::new(0),  // Hardcode replica 0 as primary
        })
    }

    /// Submit an operation to consensus (always through primary)
    pub async fn submit_operation(&self, operation: KvOperation) -> Result<(), Box<dyn std::error::Error>> {
        let data = operation.serialize()?;

        // Always submit to replica 0 (hardcoded primary)
        let primary = &self.replicas[0];

        tracing::debug!("Submitting operation to primary (replica 0)");
        primary.execute_request_async(data).await?;

        Ok(())
    }

    /// Get database reference for a specific replica (for testing)
    pub fn get_database(&self, replica_id: usize) -> Option<&Arc<TransactionalKvDatabase>> {
        self.databases.get(replica_id)
    }

    /// Shutdown all replicas
    pub async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        tracing::info!("Shutting down replica manager");

        for replica in &self.replicas {
            replica.shutdown().await?;
        }

        Ok(())
    }
}
```

### 1.5 Consensus Router (Day 4)

#### File: `rust/src/consensus/router.rs`

```rust
use std::sync::Arc;
use async_trait::async_trait;
use crate::lib::db_trait::KvDatabase;
use crate::lib::db::{GetResult, OpResult, AtomicCommitRequest, AtomicCommitResult};
use super::replica_manager::ReplicaManager;
use super::operations::KvOperation;

/// Routes operations through consensus for replicated mode
pub struct ConsensusRouter {
    replica_manager: Arc<ReplicaManager>,
}

impl ConsensusRouter {
    pub fn new(replica_manager: Arc<ReplicaManager>) -> Self {
        Self { replica_manager }
    }
}

#[async_trait]
impl KvDatabase for ConsensusRouter {
    async fn get(&self, _key: &[u8], _column_family: Option<&str>) -> Result<GetResult, String> {
        // Phase 1: Read from replica 0 directly (no consensus needed for reads)
        Err("GET operations not implemented in Phase 1".to_string())
    }

    async fn put(&self, key: &[u8], value: &[u8], column_family: Option<&str>) -> OpResult {
        let operation = KvOperation::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            column_family: column_family.map(|s| s.to_string()),
        };

        match self.replica_manager.submit_operation(operation).await {
            Ok(_) => OpResult::Ok,
            Err(e) => OpResult::Err(format!("Consensus error: {}", e)),
        }
    }

    async fn delete(&self, _key: &[u8], _column_family: Option<&str>) -> OpResult {
        OpResult::Err("DELETE operations not implemented in Phase 1".to_string())
    }

    async fn list_keys(&self, _prefix: &[u8], _limit: u32, _column_family: Option<&str>) -> Result<Vec<Vec<u8>>, String> {
        Err("LIST_KEYS operations not implemented in Phase 1".to_string())
    }

    async fn get_range(
        &self,
        _start_key: &[u8],
        _end_key: Option<&[u8]>,
        _limit: Option<usize>,
        _reverse: bool,
        _column_family: Option<&str>,
    ) -> Result<crate::lib::db::GetRangeResult, String> {
        Err("GET_RANGE operations not implemented in Phase 1".to_string())
    }

    async fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
        // Phase 1: Only support single SET operations in atomic commits
        if request.operations.len() == 1 {
            if let Some(op) = request.operations.first() {
                if op.op_type == "set" {
                    let result = self.put(&op.key, &op.value, op.column_family.as_deref()).await;
                    match result {
                        OpResult::Ok => AtomicCommitResult {
                            success: true,
                            error_message: None,
                            version: Some(1), // Placeholder version
                        },
                        OpResult::Err(e) => AtomicCommitResult {
                            success: false,
                            error_message: Some(e),
                            version: None,
                        },
                    }
                } else {
                    AtomicCommitResult {
                        success: false,
                        error_message: Some("Only SET operations supported in Phase 1".to_string()),
                        version: None,
                    }
                }
            } else {
                AtomicCommitResult {
                    success: false,
                    error_message: Some("No operations provided".to_string()),
                    version: None,
                }
            }
        } else {
            AtomicCommitResult {
                success: false,
                error_message: Some("Only single operations supported in Phase 1".to_string()),
                version: None,
            }
        }
    }

    async fn get_read_version(&self) -> u64 {
        1 // Placeholder for Phase 1
    }

    async fn snapshot_read(&self, _key: &[u8], _read_version: u64, _column_family: Option<&str>) -> Result<GetResult, String> {
        Err("Snapshot reads not implemented in Phase 1".to_string())
    }

    async fn snapshot_get_range(
        &self,
        _start_key: &[u8],
        _end_key: Option<&[u8]>,
        _limit: Option<usize>,
        _reverse: bool,
        _read_version: u64,
        _column_family: Option<&str>,
    ) -> Result<crate::lib::db::GetRangeResult, String> {
        Err("Snapshot range reads not implemented in Phase 1".to_string())
    }

    async fn set_fault_injection(&self, _config: Option<crate::lib::db::FaultInjectionConfig>) -> OpResult {
        OpResult::Err("Fault injection not supported in replicated mode".to_string())
    }
}
```

### 1.6 Integration with Existing System (Day 5)

#### File: `rust/src/lib/database_factory.rs` (modifications)

```rust
// Add to existing DatabaseFactory implementation
impl DatabaseFactory {
    pub async fn create_database(self) -> Result<Arc<dyn KvDatabase>, Box<dyn std::error::Error>> {
        match self.mode {
            DatabaseMode::Standalone(config) => {
                let db = TransactionalKvDatabase::new(config);
                Ok(Arc::new(db) as Arc<dyn KvDatabase>)
            }
            DatabaseMode::Replicated(config, instance_id) => {
                // Phase 1: Create replicated database
                use crate::consensus::{ReplicaManager, ConsensusRouter};

                tracing::info!("Creating replicated database with {} replicas", 3);

                let replica_manager = ReplicaManager::new_three_replicas(config).await?;
                let router = ConsensusRouter::new(Arc::new(replica_manager));

                Ok(Arc::new(router) as Arc<dyn KvDatabase>)
            }
        }
    }
}
```

#### File: `rust/src/servers/thrift_server.rs` (modifications)

```rust
// Modify main function to support replicated mode
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration
    let config_path = std::env::var("CONFIG_PATH")
        .unwrap_or_else(|_| "config/db_config.toml".to_string());

    let config = Config::load_from_file(&config_path)
        .unwrap_or_else(|_| {
            tracing::warn!("Config file not found, using defaults");
            Config::default()
        });

    // Check deployment mode
    let deployment_mode = config.deployment.mode.clone();
    tracing::info!("Starting server in {:?} mode", deployment_mode);

    // Create database using factory
    let factory = config.create_database_factory();
    let database = factory.create_database().await?;

    // Create service core
    let service_core = Arc::new(KvServiceCore::new(database));

    // Create and start Thrift server
    let handler = ThriftKvStoreHandler::new(service_core);
    // ... rest of server setup

    Ok(())
}
```

## Testing Plan

### Manual Testing Procedure (Day 5-6)

#### Step 1: Environment Setup
```bash
# Set environment for replicated mode
export CONFIG_PATH=config/replicated_config.toml

# Create replicated configuration
cat > config/replicated_config.toml << EOF
[deployment]
mode = "replicated"
instance_id = 0
replica_endpoints = ["localhost:6001", "localhost:6002", "localhost:6003"]

[database]
base_path = "./data/replicated"

# ... rest of configuration
EOF
```

#### Step 2: Start Replicated Server
```bash
# Build with consensus support
cargo build --release

# Run Thrift server in replicated mode
RUST_LOG=info cargo run --bin thrift-server
```

#### Step 3: Test Basic Operations
```bash
# Use existing client to send SET operations
./bin/kv-client set "test_key" "test_value"

# Verify replication (check logs for all 3 replicas applying operation)
```

#### Step 4: Verify Data Consistency
```rust
// Test program to verify all replicas have same data
#[tokio::test]
async fn test_basic_replication() {
    let config = Config::load_from_file("config/replicated_config.toml").unwrap();
    let factory = config.create_database_factory();
    let database = factory.create_database().await.unwrap();

    // Perform SET operation
    database.put(b"key1", b"value1", None).await.unwrap();

    // Wait for consensus
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify all replicas (manual check of database files)
    // In Phase 2, we'll add programmatic verification
}
```

### Integration Tests

#### File: `rust/tests/consensus/basic_replication_test.rs`

```rust
use rocksdb_server::consensus::{ReplicaManager, KvOperation};
use rocksdb_server::lib::config::Config;

#[tokio::test]
async fn test_three_replica_consensus() {
    // Create three-replica cluster
    let config = Config::default();
    let manager = ReplicaManager::new_three_replicas(config).await.unwrap();

    // Submit SET operation
    let op = KvOperation::set(b"test".to_vec(), b"value".to_vec());
    manager.submit_operation(op).await.unwrap();

    // Wait for consensus
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify all three databases have the value
    for i in 0..3 {
        let db = manager.get_database(i).unwrap();
        let result = db.get(b"test", None).unwrap();
        assert_eq!(result.value, b"value");
        assert!(result.found);
    }
}

#[tokio::test]
async fn test_consensus_router() {
    use rocksdb_server::consensus::ConsensusRouter;
    use rocksdb_server::lib::db_trait::KvDatabase;

    let config = Config::default();
    let manager = Arc::new(ReplicaManager::new_three_replicas(config).await.unwrap());
    let router = ConsensusRouter::new(manager);

    // Test PUT through consensus
    let result = router.put(b"key", b"value", None).await;
    assert_eq!(result, OpResult::Ok);

    // Test atomic commit with single SET
    let request = AtomicCommitRequest {
        operations: vec![Operation {
            op_type: "set".to_string(),
            key: b"key2".to_vec(),
            value: b"value2".to_vec(),
            column_family: None,
        }],
        read_conflict_ranges: vec![],
        write_conflict_ranges: vec![],
    };

    let result = router.atomic_commit(request).await;
    assert!(result.success);
}
```

## Success Criteria

### Must Have (End of Phase 1)
- ✅ Three in-process replicas running with RSML
- ✅ Basic SET operations replicated across all replicas
- ✅ Hardcoded primary (replica 0) accepting requests
- ✅ All replicas applying operations in same order
- ✅ Manual verification shows identical data in all replica databases
- ✅ Integration tests passing for basic replication
- ✅ Thrift server works in replicated mode with minimal changes

### Nice to Have (If Time Permits)
- ⭕ Basic GET operations (read from primary)
- ⭕ Simple metrics/logging for debugging
- ⭕ Docker compose setup for three replicas
- ⭕ Basic performance comparison with standalone mode

### Out of Scope for Phase 1
- ❌ Primary election / view changes
- ❌ Network failures / split-brain handling
- ❌ DELETE, RANGE, complex atomic operations
- ❌ TCP networking (using in-memory transport)
- ❌ Configuration via file (using hardcoded values)
- ❌ Production error handling

## Implementation Timeline

| Day | Tasks | Deliverables |
|-----|-------|--------------|
| **Day 1** | RSML integration, project setup | - RSML dependency added<br>- Module structure created<br>- Basic operation types defined |
| **Day 2** | KvStoreExecutor implementation | - Executor applying operations<br>- Basic logging/debugging<br>- Unit tests for executor |
| **Day 3** | ReplicaManager development | - Three replicas starting<br>- In-memory transport working<br>- Operation submission logic |
| **Day 4** | ConsensusRouter & integration | - Router implementing KvDatabase<br>- Integration with factory<br>- Server modifications |
| **Day 5** | Testing & debugging | - Manual testing procedures<br>- Integration tests<br>- Bug fixes |
| **Day 6** | Documentation & cleanup | - Code documentation<br>- Test results documented<br>- Phase 2 planning |
| **Day 7** | Buffer & review | - Performance baseline<br>- Code review<br>- Handoff preparation |

## Risks & Mitigation

### Technical Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| RSML API complexity | High | Start with simplest API usage, add features incrementally |
| In-memory transport issues | Medium | Use RSML examples as reference, add extensive logging |
| Serialization problems | Low | Use simple bincode, test serialization separately |
| Database conflicts | Medium | Use separate database paths, clean state between tests |
| Integration complexity | High | Minimal changes to existing code, use adapter pattern |

### Process Risks

| Risk | Impact | Mitigation |
|------|--------|------------|
| Scope creep | High | Strictly follow Phase 1 limitations, defer features |
| Debugging difficulty | Medium | Extensive logging, in-process replicas for easier debugging |
| Testing complexity | Medium | Start with manual testing, automate incrementally |

## Code Organization

```
rust/
├── src/
│   ├── consensus/           # New module for Phase 1
│   │   ├── mod.rs
│   │   ├── executor.rs      # KvStoreExecutor
│   │   ├── operations.rs    # KvOperation types
│   │   ├── replica_manager.rs # ReplicaManager
│   │   └── router.rs        # ConsensusRouter
│   ├── lib/
│   │   ├── database_factory.rs # Modified for replicated mode
│   │   └── ...              # Existing files unchanged
│   └── servers/
│       └── thrift_server.rs # Minor modifications for replicated mode
├── tests/
│   └── consensus/           # New test directory
│       ├── mod.rs
│       └── basic_replication_test.rs
└── Cargo.toml              # Add RSML dependency
```

## Next Steps (Phase 2 Preview)

After successful Phase 1 completion:
1. Add TCP networking (replace in-memory transport)
2. Implement GET, DELETE, RANGE operations
3. Add primary election via ViewManager
4. Support full atomic commits
5. Add failure detection
6. Implement proper error handling
7. Performance optimization

## Conclusion

This Phase 1 implementation plan focuses on achieving the simplest possible working Paxos replication. By hardcoding configurations, limiting operations to SET only, and using in-process replicas, we can validate the core integration quickly and build a solid foundation for future phases.

The plan emphasizes:
- **Minimal changes** to existing code
- **Maximum reuse** of current abstractions
- **Incremental complexity** - start simple, add features gradually
- **Clear success metrics** - measurable outcomes for Phase 1
- **Risk mitigation** - identify and address challenges early

Success in Phase 1 provides confidence that the Paxos integration is feasible and sets the stage for production-ready features in subsequent phases.