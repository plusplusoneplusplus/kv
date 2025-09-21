use consensus_api::{ConsensusResult, ConsensusError, LogEntry, NodeId, ConsensusNode};
use consensus_mock::{MockConsensusNode, StateMachine};
use kv_storage_api::KvDatabase;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
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

    /// Validate that only write operations go through consensus
    fn validate_operation(&self, operation: &KvOperation) -> Result<(), String> {
        if operation.is_read_only() {
            return Err("Read operations should not go through consensus".to_string());
        }
        Ok(())
    }

    /// Create a snapshot of the current state
    fn create_snapshot(&self) -> Result<Vec<u8>, String> {
        let snapshot_data = KvSnapshot {
            last_applied_sequence: self.executor.get_applied_sequence(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        };

        bincode::serialize(&snapshot_data)
            .map_err(|e| format!("Failed to serialize snapshot: {}", e))
    }

    /// Restore from a snapshot (placeholder - real implementation would restore database state)
    fn restore_from_snapshot(&self, snapshot: &[u8]) -> Result<(), String> {
        let _snapshot_data: KvSnapshot = bincode::deserialize(snapshot)
            .map_err(|e| format!("Failed to deserialize snapshot: {}", e))?;

        // In a real implementation, this would restore the database state
        // For now, we just validate that the snapshot is valid
        debug!("Snapshot restore validated");
        Ok(())
    }
}

impl StateMachine for KvStateMachine {
    fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
        // Deserialize the operation from the log entry
        let operation: KvOperation = bincode::deserialize(&entry.data)
            .map_err(|e| ConsensusError::SerializationError {
                message: format!("Failed to deserialize KV operation: {}", e),
            })?;

        // Validate the operation
        self.validate_operation(&operation)
            .map_err(|e| ConsensusError::Other { message: e })?;

        // For the consensus state machine, we just validate and return the operation
        // The actual application to the database happens asynchronously via a different path
        debug!("State machine applying operation at index {}", entry.index);

        // Return the operation data - the async execution will happen in the wrapper
        Ok(entry.data.clone())
    }

    fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
        self.create_snapshot()
            .map_err(|e| ConsensusError::Other { message: e })
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
        self.restore_from_snapshot(snapshot)
            .map_err(|e| ConsensusError::Other { message: e })
    }
}

/// Snapshot format for the KV state machine
#[derive(Debug, Clone, Serialize, Deserialize)]
struct KvSnapshot {
    last_applied_sequence: u64,
    timestamp: u64,
}

/// Wrapper to make Arc<KvStateMachine> implement StateMachine
struct StateMachineWrapper {
    inner: Arc<KvStateMachine>,
}

impl std::fmt::Debug for StateMachineWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateMachineWrapper")
            .field("inner", &self.inner)
            .finish()
    }
}

impl StateMachine for StateMachineWrapper {
    fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
        self.inner.apply(entry)
    }

    fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
        self.inner.snapshot()
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
        self.inner.restore_snapshot(snapshot)
    }
}

use std::sync::atomic::{AtomicU64, Ordering};

/// Consensus-enabled KV database that routes operations appropriately
pub struct ConsensusKvDatabase {
    consensus_node: Arc<RwLock<MockConsensusNode>>,
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
        node_id: NodeId,
        database: Arc<dyn KvDatabase>
    ) -> Self {
        // Create the executor that will handle actual database operations
        let executor = Arc::new(KvStoreExecutor::new(database));

        // Create the state machine that delegates to the executor
        let state_machine = Arc::new(KvStateMachine::new(executor.clone()));

        // Create a wrapper that implements StateMachine for Arc<KvStateMachine>
        let state_machine_wrapper = StateMachineWrapper {
            inner: state_machine.clone(),
        };

        let consensus_node = Arc::new(RwLock::new(
            MockConsensusNode::new(node_id, Box::new(state_machine_wrapper))
        ));

        Self {
            consensus_node,
            executor,
            next_sequence: AtomicU64::new(1),
        }
    }

    /// Start the consensus node
    pub async fn start(&self) -> ConsensusResult<()> {
        let mut node = self.consensus_node.write();
        node.start().await
    }

    /// Stop the consensus node
    pub async fn stop(&self) -> ConsensusResult<()> {
        let mut node = self.consensus_node.write();
        node.stop().await
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
            let node = self.consensus_node.read();
            node.propose(operation_data).await
                .map_err(|e| format!("Consensus proposal failed: {}", e))?
        };

        if !response.success {
            return Err(response.error.unwrap_or("Consensus proposal failed".to_string()));
        }

        // Wait for the operation to be committed
        if let Some(index) = response.index {
            let _committed_data = {
                let node = self.consensus_node.read();
                node.wait_for_commit(index).await
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

    /// Get the current consensus node information
    pub fn consensus_info(&self) -> (NodeId, bool, u64, u64) {
        let node = self.consensus_node.read();
        (
            node.node_id().clone(),
            node.is_leader(),
            node.current_term(),
            self.executor.get_applied_sequence(),
        )
    }

    /// Get direct access to the underlying executor for administrative operations
    pub fn executor(&self) -> &Arc<KvStoreExecutor> {
        &self.executor
    }
}