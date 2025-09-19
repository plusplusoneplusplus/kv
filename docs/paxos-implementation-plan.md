# Paxos-Replicated KV Store Implementation Plan

## Executive Summary

This document outlines a detailed implementation plan for integrating Paxos consensus (via RSML) into the existing transactional KV store. The plan follows a "happy path first" approach - starting with basic working replication, then adding robustness, optimization, and failure handling in later phases. This approach emphasizes maximum reuse of existing components through strategic refactoring.

## Implementation Philosophy

**Happy Path First**: Get basic three-replica consensus working with manual testing before adding automatic failure detection, split-brain prevention, and other production features. This allows us to:
1. Validate the core integration quickly
2. Build confidence with working code
3. Add complexity incrementally
4. Identify real issues through testing

## Phase 0: Strategic Refactoring to Prevent Code Duplication (Week 1)

### Duplication Analysis

**Current Duplication Risks**:
1. **Thrift Handler Logic**: Will need separate handlers for standalone vs replicated
2. **Database Operations**: Same operations called in different contexts
3. **Configuration Parsing**: Replica configs vs standalone configs
4. **Error Handling**: Similar patterns across replicas and modes
5. **Testing Setup**: Different test harnesses for each mode
6. **Service Startup**: Different initialization paths

### 0.1 Service Layer Abstraction
**Goal**: Extract core business logic to eliminate handler duplication.

**Current Problem**:
```rust
// This will lead to duplication:
impl TransactionalKVSyncHandler for ThriftHandler {
    fn handle_atomic_commit(&self, req: AtomicCommitRequest) -> thrift::Result<AtomicCommitResponse> {
        // 50+ lines of business logic mixed with Thrift concerns
        let operations = req.operations.into_iter().map(|op| /* conversion */).collect();
        let result = self.database.atomic_commit(operations);
        // Response conversion logic
    }
}
```

**Solution - Extract Service Core**:
```rust
// Pure business logic, no protocol concerns
pub struct KvServiceCore {
    database: Arc<dyn KvDatabase>,
}

impl KvServiceCore {
    pub async fn atomic_commit(&self, request: AtomicCommitRequest) -> Result<AtomicCommitResult, KvError> {
        // All business logic here - can be reused across protocols
    }

    pub async fn get(&self, key: &[u8], cf: Option<&str>) -> Result<GetResult, KvError> {
        // Reusable across gRPC, Thrift, direct calls
    }
}

// Thin Thrift wrapper - no duplication
impl TransactionalKVSyncHandler for ThriftHandler {
    fn handle_atomic_commit(&self, req: AtomicCommitRequest) -> thrift::Result<AtomicCommitResponse> {
        let result = self.service_core.atomic_commit(req).await?;
        Ok(result.into_thrift_response())
    }
}
```

### 0.2 Database Interface Abstraction
**Goal**: Enable database operations to work in both standalone and replicated contexts.

**Current Problem**: Direct coupling to `TransactionalKvDatabase`
```rust
// This creates tight coupling:
pub struct ThriftHandler {
    database: Arc<TransactionalKvDatabase>,  // Can't be replaced
}
```

**Solution**:
```rust
#[async_trait]
pub trait KvDatabase: Send + Sync {
    async fn get(&self, key: &[u8], cf: Option<&str>) -> Result<GetResult, KvError>;
    async fn atomic_commit(&self, request: AtomicCommitRequest) -> Result<AtomicCommitResult, KvError>;
    // ... other operations
}

// Existing database implements the trait
impl KvDatabase for TransactionalKvDatabase {
    // Implementations wrap existing methods
}

// Later: Replicated database can also implement the trait
impl KvDatabase for ReplicatedKvDatabase {
    async fn atomic_commit(&self, request: AtomicCommitRequest) -> Result<AtomicCommitResult, KvError> {
        // Route through consensus
        self.submit_to_consensus(KvOperation::AtomicCommit(request)).await
    }
}
```

### 0.3 Request Router Pattern
**Goal**: Same handler code works for both standalone and replicated modes.

**Problem**: Different routing logic
```rust
// Without abstraction, we'd need:
// - StandaloneThriftHandler
// - ReplicatedThriftHandler
// - Duplicate all the conversion logic
```

**Solution**:
```rust
#[async_trait]
pub trait RequestRouter: Send + Sync {
    async fn route_operation<T>(&self, operation: KvOperation) -> Result<T, KvError>
    where T: DeserializeOwned;
}

pub struct DirectRouter {
    service: Arc<KvServiceCore>,
}

pub struct ConsensusRouter {
    consensus_manager: Arc<ConsensusManager>,
}

// Single handler implementation works for both modes
pub struct UnifiedThriftHandler {
    router: Arc<dyn RequestRouter>,
}

impl TransactionalKVSyncHandler for UnifiedThriftHandler {
    fn handle_atomic_commit(&self, req: AtomicCommitRequest) -> thrift::Result<AtomicCommitResponse> {
        let operation = KvOperation::AtomicCommit(req);
        let result = self.router.route_operation(operation).await?;
        Ok(result)
    }
}
```

### 0.4 Configuration Unification
**Goal**: Single configuration system supporting both modes.

**Problem**: Separate config structures lead to duplication
```rust
// This leads to duplication:
pub struct StandaloneConfig { /* fields */ }
pub struct ReplicatedConfig { /* different fields */ }
```

**Solution**:
```rust
#[derive(Deserialize)]
pub struct Config {
    pub mode: DeploymentMode,
    pub database: DatabaseConfig,     // Shared
    pub server: ServerConfig,         // Shared
    pub replication: Option<ReplicationConfig>, // Only for replicated mode
}

#[derive(Deserialize)]
pub enum DeploymentMode {
    Standalone,
    Replicated { replica_count: u32 },
}

// Single config parser, single validation logic
impl Config {
    pub fn load() -> Result<Self, ConfigError> {
        // Unified loading logic
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        // Shared validation + mode-specific validation
    }
}
```

### 0.5 Testing Framework Unification
**Goal**: Reusable test utilities for both modes.

**Problem**: Separate test setups lead to duplication
```rust
// Without abstraction:
fn setup_standalone_test() -> /* different return type */
fn setup_replicated_test() -> /* different return type */
```

**Solution**:
```rust
pub trait TestCluster {
    async fn client(&self) -> KvStoreClient;
    async fn shutdown(&mut self);
    fn get_database_paths(&self) -> Vec<PathBuf>;
}

pub struct StandaloneTestCluster {
    server: ThriftServer,
    database: Arc<TransactionalKvDatabase>,
}

pub struct ReplicatedTestCluster {
    replicas: Vec<Replica>,
    consensus: ConsensusManager,
}

// Same test can run against both modes
async fn test_atomic_operations(cluster: &dyn TestCluster) {
    let client = cluster.client().await;
    // Test logic works for both modes
}

#[tokio::test]
async fn test_standalone_atomic_operations() {
    let cluster = StandaloneTestCluster::new().await;
    test_atomic_operations(&cluster).await;
}

#[tokio::test]
async fn test_replicated_atomic_operations() {
    let cluster = ReplicatedTestCluster::new().await;
    test_atomic_operations(&cluster).await;
}
```

### 0.6 Error Handling Unification
**Goal**: Consistent error handling across modes.

**Solution**:
```rust
#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("Database error: {0}")]
    Database(String),

    #[error("Consensus error: {0}")]
    Consensus(String),

    #[error("No primary available")]
    NoPrimary,

    #[error("Configuration error: {0}")]
    Config(String),
}

// Consistent error conversion across all layers
impl From<KvError> for thrift::Result<T> {
    // Single conversion logic used everywhere
}
```

### 0.7 Startup/Shutdown Abstraction
**Goal**: Unified server lifecycle management.

**Solution**:
```rust
#[async_trait]
pub trait ServerManager: Send + Sync {
    async fn start(&mut self) -> Result<(), KvError>;
    async fn shutdown(&mut self) -> Result<(), KvError>;
    fn is_healthy(&self) -> bool;
}

pub struct StandaloneServerManager {
    database: Arc<TransactionalKvDatabase>,
    thrift_server: Option<ThriftServer>,
}

pub struct ReplicatedServerManager {
    replicas: Vec<Replica>,
    consensus: ConsensusManager,
    thrift_server: Option<ThriftServer>,
}

// Single main() function works for both modes
#[tokio::main]
async fn main() -> Result<(), KvError> {
    let config = Config::load()?;

    let mut manager: Box<dyn ServerManager> = match config.mode {
        DeploymentMode::Standalone => Box::new(StandaloneServerManager::new(config)?),
        DeploymentMode::Replicated { .. } => Box::new(ReplicatedServerManager::new(config)?),
    };

    manager.start().await?;
    // Single shutdown logic
    signal::ctrl_c().await?;
    manager.shutdown().await?;
    Ok(())
}
```

## Phase 1: Happy Path - Basic Working Replication (Week 2)

### 1.1 Simple Operation Type for Consensus
**Goal**: Get one operation type working through Paxos.

**Start with single operation**:
```rust
#[derive(Serialize, Deserialize)]
pub struct KvOperation {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub op_type: String, // "SET", "GET", "DELETE"
}
```

### 1.2 Basic KvStoreExecutor
**Goal**: Minimal executor that can apply operations to database.

```rust
pub struct KvStoreExecutor {
    database: Arc<TransactionalKvDatabase>,
}

#[async_trait]
impl ExecutionNotifier for KvStoreExecutor {
    async fn notify_commit(&mut self, _sequence: u64, value: CommittedValue) -> LearnerResult<()> {
        let operation: KvOperation = bincode::deserialize(&value.data)?;

        match operation.op_type.as_str() {
            "SET" => {
                self.database.put(&operation.key, &operation.value);
            }
            _ => {} // Ignore other operations for now
        }
        Ok(())
    }
}
```

### 1.3 Three-Replica Setup (Hardcoded)
**Goal**: Start 3 RSML replicas in same process.

```rust
pub struct BasicReplicatedStore {
    replicas: Vec<ConsensusReplica>,
    databases: Vec<Arc<TransactionalKvDatabase>>,
    primary_id: ReplicaId, // Hardcode replica 0 as primary initially
}

impl BasicReplicatedStore {
    pub async fn new() -> Self {
        // Create 3 databases
        let databases = (0..3).map(|i| {
            Arc::new(TransactionalKvDatabase::new_with_instance_id(Config::default(), i))
        }).collect();

        // Create 3 RSML replicas with hardcoded TCP ports
        // Start simple - worry about networking later

        Self {
            replicas,
            databases,
            primary_id: ReplicaId::new(0), // Always use replica 0 as primary
        }
    }

    pub async fn set(&self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let operation = KvOperation {
            key, value, op_type: "SET".to_string()
        };

        // Always submit to replica 0 (hardcoded primary)
        let primary = &self.replicas[0];
        let data = bincode::serialize(&operation)?;
        primary.execute_request_async(data).await?;
        Ok(())
    }
}
```

## Phase 2: Happy Path - Basic Thrift Integration (Week 3)

### 2.1 Simple Router - Always Use Primary
**Goal**: Route Thrift requests to hardcoded primary replica.

```rust
pub struct SimpleRouter {
    replicated_store: Arc<BasicReplicatedStore>,
}

impl SimpleRouter {
    pub async fn handle_atomic_commit(&self, req: AtomicCommitRequest) -> Result<AtomicCommitResponse> {
        // For now, just handle single SET operations
        if req.operations.len() == 1 && req.operations[0].type_ == "SET" {
            self.replicated_store.set(
                req.operations[0].key.clone(),
                req.operations[0].value.clone().unwrap_or_default()
            ).await?;

            Ok(AtomicCommitResponse::new(true, None, Some(1)))
        } else {
            Err("Only single SET operations supported in Phase 2".into())
        }
    }
}
```

### 2.2 Update Thrift Server for Replicated Mode
**Goal**: Minimal changes to support replicated mode.

```rust
#[tokio::main]
async fn main() -> Result<()> {
    let replicated_mode = std::env::var("REPLICATED_MODE").unwrap_or_default() == "true";

    if replicated_mode {
        println!("Starting in replicated mode...");
        let store = Arc::new(BasicReplicatedStore::new().await);
        let router = Arc::new(SimpleRouter::new(store));
        let handler = TransactionalKvStoreThriftHandler::new_with_router(router);
        // Start Thrift server with replicated handler
    } else {
        // Existing standalone mode unchanged
        let db = Arc::new(TransactionalKvDatabase::new(Config::default()));
        let handler = TransactionalKvStoreThriftHandler::new(db, false);
    }

    // Rest of server startup unchanged
}
```

## Phase 3: Happy Path - Basic Network Setup (Week 4)

### 3.1 In-Memory Transport for Testing
**Goal**: Get 3 replicas communicating using RSML InMemoryTransport.

```rust
pub async fn create_test_cluster() -> BasicReplicatedStore {
    let message_bus = create_shared_message_bus();

    let replicas = (0..3).map(|i| {
        let replica_id = ReplicaId::new(i);
        let transport = Arc::new(InMemoryTransport::new(message_bus.clone()));
        let network_manager = Arc::new(NetworkManager::new(replica_id, config, transport)?);

        // Create RSML ConsensusReplica with network manager
        ConsensusReplica::new(replica_id, network_manager)
    }).collect();

    BasicReplicatedStore { replicas, ... }
}
```

### 3.2 Basic TCP Transport (if time permits)
**Goal**: Move from in-memory to real TCP for production.

- Use RSML TcpTransport with hardcoded ports [6001, 6002, 6003]
- No connection pooling or retry logic yet
- Basic send/receive functionality only

## Phase 4: Happy Path - Basic Testing (Week 5)

### 4.1 Manual Testing
**Goal**: Verify replication works with manual testing.

**Test Steps**:
1. Start server in replicated mode
2. Use existing client to send SET operations
3. Manually verify data appears in all 3 replica databases
4. Test basic read operations

### 4.2 Integration Test Suite
**Goal**: Basic automated testing.

```rust
#[tokio::test]
async fn test_basic_replication() {
    let cluster = create_test_cluster().await;

    // Send a SET operation
    cluster.set(b"test_key".to_vec(), b"test_value".to_vec()).await.unwrap();

    // Verify all replicas have the data
    for db in &cluster.databases {
        let result = db.get(b"test_key").unwrap();
        assert_eq!(result.value, b"test_value");
        assert!(result.found);
    }
}
```

## Phase 5: Enhancement - Robustness & Production Features (Week 6-8)

**Now that we have working replication, add production features:**

### 5.1 Proper Primary Detection
- Replace hardcoded primary with RSML ViewManager
- Handle view changes and primary election
- Update routing based on current primary

### 5.2 Failure Handling & Split-Brain Prevention
- Add failure detection mechanisms
- Implement proper view change handling
- Add split-brain prevention
- Request retry and timeout handling

### 5.3 Full Operation Support
- Support all Thrift operations (not just SET)
- Add transaction support
- Handle complex atomic commits
- Add range operations

### 5.4 Performance & Monitoring
- Add batching for multiple operations
- Performance monitoring and metrics
- Connection pooling and resource management
- Comprehensive error handling

### 5.5 Operational Tools
- Admin CLI for cluster management
- Health checking and status reporting
- Manual failover capabilities
- Consistency verification tools

## Implementation Timeline - Happy Path First

| Week | Phase | Focus | Deliverables |
|------|-------|-------|-------------|
| 1 | Phase 0 | Minimal Setup | Environment variable flag, multi-database support |
| 2 | Phase 1 | Basic Replication | Single SET operation through Paxos consensus |
| 3 | Phase 2 | Thrift Integration | Replicated mode in Thrift server, hardcoded primary |
| 4 | Phase 3 | Basic Networking | In-memory transport working, basic TCP if time |
| 5 | Phase 4 | Happy Path Testing | Manual testing + basic integration tests |
| 6-8 | Phase 5 | Production Features | Primary election, failure handling, split-brain prevention |

## Happy Path Success Criteria (Week 5)

**Must Have - Basic Working System**:
- ✅ `REPLICATED_MODE=true` starts 3-replica cluster
- ✅ Single SET operation replicates to all 3 databases
- ✅ Existing Thrift client can write data in replicated mode
- ✅ All 3 replica databases contain identical data after operations
- ✅ Basic integration test passes

**Phase 5 Success Criteria - Production Ready**:
- ✅ Automatic primary detection and failover
- ✅ All Thrift operations supported (not just SET)
- ✅ Split-brain prevention mechanisms
- ✅ TCP networking with proper error handling
- ✅ Performance comparable to standalone mode

## Risk Mitigation - Happy Path Approach

### Technical Risks
1. **RSML Integration Complexity**
   - Mitigation: Start with simplest possible integration (hardcoded primary)
   - Validate core concept before adding complexity

2. **Performance Unknown**
   - Mitigation: Get basic version working first, then measure and optimize
   - Happy path establishes baseline

3. **Too Many Moving Parts**
   - Mitigation: Incremental complexity - one feature at a time
   - Each week builds on previous working version

### Development Risks
1. **Over-Engineering Early**
   - Mitigation: Hardcode everything initially (ports, primary, simple operations)
   - Refactor only when basic version works

2. **Getting Stuck on Edge Cases**
   - Mitigation: Focus on normal operation first
   - Add robustness after core functionality works

## Conclusion

This plan follows a "happy path first" approach to integrating Paxos replication into the existing KV store. By starting with the simplest possible working implementation, we:

1. **Validate the core concept quickly** - Get basic replication working before adding complexity
2. **Maximize code reuse** - Most existing components (TransactionalKvDatabase, Thrift handlers, client SDK) remain unchanged
3. **Build incrementally** - Each phase adds one layer of functionality to a working foundation
4. **Reduce risk** - Happy path success in Week 5 proves feasibility before tackling production concerns

**Key Design Philosophy**:
- Week 1-5: Focus on "does it work?" with hardcoded configurations and simple cases
- Week 6-8: Focus on "is it production ready?" with proper failure handling, split-brain prevention, and operational tools

This approach minimizes development effort while building confidence through working code at each milestone. The existing client interface remains unchanged, ensuring backward compatibility throughout the implementation.