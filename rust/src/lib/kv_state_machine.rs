use consensus_api::{ConsensusResult, NodeId, ConsensusEngine, StateMachine, ConsensusError, LogEntry};
use consensus_mock::MockConsensusEngine;
use kv_storage_api::KvDatabase;
use parking_lot::RwLock;
use std::sync::Arc;
use tracing::debug;

use crate::lib::operations::{KvOperation, OperationResult};
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
        // For now, just return the entry data (the actual application happens in the executor)
        debug!("KV state machine applying entry at index {}", entry.index);
        Ok(entry.data.clone())
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

use std::sync::atomic::{AtomicU64, Ordering};

/// Consensus-enabled KV database that routes operations appropriately
pub struct ConsensusKvDatabase {
    consensus_engine: Arc<RwLock<Box<dyn ConsensusEngine>>>,
    executor: Arc<KvStoreExecutor>,
    next_sequence: AtomicU64,
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
            next_sequence: AtomicU64::new(1),
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

    /// Execute a KV operation through consensus
    pub async fn execute_operation(&self, operation: KvOperation) -> Result<OperationResult, String> {
        debug!("Executing operation through consensus: {:?}", operation);
        self.execute_write_operation(operation).await
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
            let _committed_data = {
                let engine = self.consensus_engine.read();
                engine.wait_for_commit(index).await
                    .map_err(|e| format!("Failed to wait for commit: {}", e))?
            };

            // Now actually apply the operation to the database using the executor
            // Use our own sequence counter to ensure proper ordering
            let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
            let result = self.executor.apply_operation(sequence, operation).await
                .map_err(|e| format!("Failed to apply operation to database: {}", e))?;

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