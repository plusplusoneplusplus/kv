use consensus_api::{ConsensusResult, NodeId, ConsensusEngine, StateMachine, ConsensusError, LogEntry};
use consensus_mock::MockConsensusEngine;
use kv_storage_api::KvDatabase;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::debug;

use crate::lib::operations::{KvOperation, OperationResult, DatabaseOperation};
use crate::lib::replication::executor::KvStoreExecutor;

/// KV-specific state machine that delegates to KvStoreExecutor
pub struct KvStateMachine {
    executor: Arc<KvStoreExecutor>,
}

impl std::fmt::Debug for KvStateMachine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvStateMachine")
            .field("applied_sequence", &self.executor.get_applied_sequence())
            .finish()
    }
}

impl KvStateMachine {
    pub fn new(executor: Arc<KvStoreExecutor>) -> Self {
        Self { executor }
    }
}

impl StateMachine for KvStateMachine {
    fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
        debug!("KV state machine applying entry at index {}", entry.index);

        // Deserialize the operation from the log entry
        let operation: KvOperation = bincode::deserialize(&entry.data)
            .map_err(|e| ConsensusError::Other {
                message: format!("Failed to deserialize operation: {}", e),
            })?;

        // For followers in multi-node consensus, we need to actually execute the operation
        // We use a blocking approach since StateMachine::apply is synchronous
        let result = match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                // We're in an async context, use block_in_place to avoid blocking the runtime
                tokio::task::block_in_place(|| {
                    rt.block_on(async {
                        self.executor.apply_operation(entry.index, operation).await
                    })
                })
            }
            Err(_) => {
                // We're not in an async context, create a new runtime
                let rt = tokio::runtime::Runtime::new().map_err(|e| ConsensusError::Other {
                    message: format!("Failed to create runtime: {}", e),
                })?;
                rt.block_on(async {
                    self.executor.apply_operation(entry.index, operation).await
                })
            }
        };

        let operation_result = result.map_err(|e| ConsensusError::Other {
            message: format!("Failed to apply operation: {}", e),
        })?;

        // Serialize the result directly
        // Note: Diagnostic operations (ClusterHealth, DatabaseStats, NodeInfo)
        // should not go through consensus anyway, so this should only handle
        // serializable operation results
        let result_data = bincode::serialize(&operation_result)
            .map_err(|e| ConsensusError::Other {
                message: format!("Failed to serialize result: {}", e),
            })?;

        Ok(result_data)
    }

    fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
        // Create a simple snapshot with the current applied sequence
        let snapshot_data = format!("kv_snapshot:{}", self.executor.get_applied_sequence());
        Ok(snapshot_data.into_bytes())
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
        // Validate snapshot format
        let snapshot_str = String::from_utf8_lossy(snapshot);
        if snapshot_str.starts_with("kv_snapshot:") {
            debug!("KV state machine restored from snapshot: {}", snapshot_str);
            Ok(())
        } else {
            Err(ConsensusError::Other {
                message: "Invalid KV snapshot format".to_string(),
            })
        }
    }
}

/// Consensus-enabled KV database that routes operations appropriately
pub struct ConsensusKvDatabase {
    consensus_engine: Arc<RwLock<Box<dyn ConsensusEngine>>>,
    executor: Arc<KvStoreExecutor>,
}

impl std::fmt::Debug for ConsensusKvDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsensusKvDatabase")
            .field("applied_sequence", &self.executor.get_applied_sequence())
            .finish()
    }
}

impl ConsensusKvDatabase {
    /// Create a new consensus-enabled KV database
    pub fn new(
        consensus_engine: Box<dyn ConsensusEngine>,
        database: Arc<dyn KvDatabase>
    ) -> Self {
        // Create the executor that will handle actual database operations
        let executor = Arc::new(KvStoreExecutor::new(database));

        Self {
            consensus_engine: Arc::new(RwLock::new(consensus_engine)),
            executor,
        }
    }

    /// Create a new consensus-enabled KV database with mock consensus (for testing)
    pub fn new_with_mock(
        node_id: NodeId,
        database: Arc<dyn KvDatabase>
    ) -> Self {
        // Create the executor that will handle actual database operations
        let executor = Arc::new(KvStoreExecutor::new(database.clone()));

        // Create the KV state machine
        let kv_state_machine = Box::new(KvStateMachine::new(executor.clone()));

        // Create the mock consensus engine with the KV state machine
        let consensus_engine = Box::new(MockConsensusEngine::new(node_id, kv_state_machine));

        Self::new(consensus_engine, database)
    }

    /// Start the consensus engine
    pub async fn start(&self) -> ConsensusResult<()> {
        let mut engine = self.consensus_engine.write();
        engine.start().await
    }

    /// Stop the consensus engine
    pub async fn stop(&self) -> ConsensusResult<()> {
        let mut engine = self.consensus_engine.write();
        engine.stop().await
    }

    /// Execute a KV operation through the appropriate path
    pub async fn execute_operation(&self, operation: KvOperation) -> Result<OperationResult, String> {
        debug!("Executing operation: {:?}", operation);

        // Diagnostic operations bypass consensus entirely
        match &operation {
            KvOperation::GetClusterHealth |
            KvOperation::GetDatabaseStats { .. } |
            KvOperation::GetNodeInfo { .. } => {
                // Execute diagnostic operations directly without consensus
                self.executor.execute_on_database(operation).await
                    .map_err(|e| format!("Diagnostic operation failed: {}", e))
            }
            _ => {
                if operation.is_read_only() {
                    // Read operations go directly to the database for eventual consistency
                    self.execute_read_operation(operation).await
                } else {
                    // Write operations go through consensus
                    self.execute_write_operation(operation).await
                }
            }
        }
    }

    /// Execute a read operation directly against the database (eventual consistency)
    async fn execute_read_operation(&self, operation: KvOperation) -> Result<OperationResult, String> {
        debug!("Executing read operation directly: {:?}", operation);

        // For read operations, we can reuse the executor's database execution logic
        // without going through the consensus sequence tracking, since reads don't modify state
        self.executor.execute_on_database(operation).await
            .map_err(|e| format!("Read operation failed: {}", e))
    }

    /// Execute an operation through the consensus protocol
    async fn execute_write_operation(&self, operation: KvOperation) -> Result<OperationResult, String> {
        // Serialize the operation for consensus
        let operation_data = bincode::serialize(&operation)
            .map_err(|e| format!("Failed to serialize operation: {}", e))?;

        // Propose the operation through consensus
        let response = {
            let engine = self.consensus_engine.read();
            engine.propose(operation_data).await
                .map_err(|e| format!("Consensus proposal failed: {}", e))?
        };

        if !response.success {
            return Err(response.error.unwrap_or("Consensus proposal failed".to_string()));
        }

        // Wait for the operation to be committed
        if let Some(index) = response.index {
            let committed_data = {
                let engine = self.consensus_engine.read();
                engine.wait_for_commit(index).await
                    .map_err(|e| format!("Failed to wait for commit: {}", e))?
            };

            // Deserialize the committed operation
            let committed_operation: KvOperation = bincode::deserialize(&committed_data)
                .map_err(|e| format!("Failed to deserialize committed operation: {}", e))?;

            // Now execute the operation through the executor using the consensus index as sequence
            let result = self.executor.apply_operation(index, committed_operation).await
                .map_err(|e| format!("Failed to apply committed operation: {}", e))?;

            Ok(result)
        } else {
            Err("No index returned from consensus proposal".to_string())
        }
    }

    /// Get the current consensus engine information
    pub fn consensus_info(&self) -> (NodeId, bool, u64, u64) {
        let engine = self.consensus_engine.read();
        (
            engine.node_id().clone(),
            engine.is_leader(),
            engine.current_term(),
            self.executor.get_applied_sequence(),
        )
    }

    /// Get direct access to the underlying executor for administrative operations
    pub fn executor(&self) -> &Arc<KvStoreExecutor> {
        &self.executor
    }
}