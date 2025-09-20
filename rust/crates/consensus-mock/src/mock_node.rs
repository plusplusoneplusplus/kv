use async_trait::async_trait;
use consensus_api::{
    ConsensusNode, ConsensusResult, Index, LogEntry, NodeId, ProposeResponse, Term,
};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Debug)]
pub struct MockConsensusNode {
    node_id: NodeId,
    term: AtomicU64,
    log: Arc<RwLock<Vec<LogEntry>>>,
    last_applied_index: AtomicU64,
    is_leader: AtomicBool,
    is_started: AtomicBool,
    cluster_members: Arc<RwLock<HashMap<NodeId, String>>>,
    state_machine: Box<dyn StateMachine>,
}

pub trait StateMachine: Send + Sync + std::fmt::Debug {
    fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>>;
    fn snapshot(&self) -> ConsensusResult<Vec<u8>>;
    fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()>;
}

impl MockConsensusNode {
    pub fn new(node_id: NodeId, state_machine: Box<dyn StateMachine>) -> Self {
        let mut cluster_members = HashMap::new();
        cluster_members.insert(node_id.clone(), "localhost:0".to_string());

        Self {
            node_id,
            term: AtomicU64::new(1),
            log: Arc::new(RwLock::new(Vec::new())),
            last_applied_index: AtomicU64::new(0),
            is_leader: AtomicBool::new(true),
            is_started: AtomicBool::new(false),
            cluster_members: Arc::new(RwLock::new(cluster_members)),
            state_machine,
        }
    }

    fn next_index(&self) -> Index {
        let log = self.log.read();
        log.len() as Index + 1
    }

    fn apply_pending_entries(&self) -> ConsensusResult<()> {
        let log = self.log.read();
        let current_applied = self.last_applied_index.load(Ordering::SeqCst);

        for (i, entry) in log.iter().enumerate() {
            let entry_index = (i + 1) as Index;
            if entry_index > current_applied {
                self.state_machine.apply(entry)?;
                self.last_applied_index.store(entry_index, Ordering::SeqCst);
            }
        }

        Ok(())
    }
}

#[async_trait]
impl ConsensusNode for MockConsensusNode {
    async fn propose(&self, data: Vec<u8>) -> ConsensusResult<ProposeResponse> {
        if !self.is_started.load(Ordering::SeqCst) {
            return Ok(ProposeResponse {
                request_id: Uuid::new_v4(),
                success: false,
                index: None,
                term: None,
                error: Some("Node not started".to_string()),
                leader_id: None,
            });
        }

        if !self.is_leader() {
            return Ok(ProposeResponse {
                request_id: Uuid::new_v4(),
                success: false,
                index: None,
                term: Some(self.current_term()),
                error: Some("Not leader".to_string()),
                leader_id: None,
            });
        }

        let index = self.next_index();
        let term = self.current_term();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let entry = LogEntry {
            index,
            term,
            data,
            timestamp,
        };

        {
            let mut log = self.log.write();
            log.push(entry);
        }

        self.apply_pending_entries()?;

        Ok(ProposeResponse {
            request_id: Uuid::new_v4(),
            success: true,
            index: Some(index),
            term: Some(term),
            error: None,
            leader_id: Some(self.node_id.clone()),
        })
    }

    async fn start(&mut self) -> ConsensusResult<()> {
        self.is_started.store(true, Ordering::SeqCst);
        tracing::info!("Mock consensus node '{}' started", self.node_id);
        Ok(())
    }

    async fn stop(&mut self) -> ConsensusResult<()> {
        self.is_started.store(false, Ordering::SeqCst);
        tracing::info!("Mock consensus node '{}' stopped", self.node_id);
        Ok(())
    }

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    fn current_term(&self) -> Term {
        self.term.load(Ordering::SeqCst)
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn cluster_members(&self) -> Vec<NodeId> {
        self.cluster_members.read().keys().cloned().collect()
    }

    async fn add_node(&mut self, node_id: NodeId, address: String) -> ConsensusResult<()> {
        let mut members = self.cluster_members.write();
        members.insert(node_id.clone(), address);
        tracing::info!("Added node '{}' to cluster", node_id);
        Ok(())
    }

    async fn remove_node(&mut self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut members = self.cluster_members.write();
        members.remove(node_id);
        tracing::info!("Removed node '{}' from cluster", node_id);
        Ok(())
    }

    async fn wait_for_commit(&self, index: Index) -> ConsensusResult<Vec<u8>> {
        let log = self.log.read();
        if let Some(entry) = log.get((index - 1) as usize) {
            Ok(entry.data.clone())
        } else {
            Err(consensus_api::ConsensusError::Other {
                message: format!("Log entry at index {} not found", index),
            })
        }
    }

    async fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
        self.state_machine.apply(entry)
    }

    async fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
        self.state_machine.snapshot()
    }

    async fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
        self.state_machine.restore_snapshot(snapshot)
    }

    fn last_applied_index(&self) -> Index {
        self.last_applied_index.load(Ordering::SeqCst)
    }

    fn set_last_applied_index(&self, index: Index) {
        self.last_applied_index.store(index, Ordering::SeqCst);
    }
}