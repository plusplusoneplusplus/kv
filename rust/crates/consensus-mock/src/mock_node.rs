use async_trait::async_trait;
use consensus_api::{
    ConsensusResult, ConsensusEngine, StateMachine, Index, LogEntry, NodeId, ProposeResponse, Term,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConsensusMessage {
    /// Proposal submitted to consensus
    Proposal { request_id: Uuid, index: Index, term: Term, data_size: usize },
    /// Entry committed to log
    Commit { index: Index, term: Term, data_size: usize },
    /// Leadership change
    LeadershipChange { new_leader: NodeId, term: Term },
    /// Node joined cluster
    NodeJoined { node_id: NodeId, address: String },
    /// Node left cluster
    NodeLeft { node_id: NodeId },
    /// Consensus engine started
    EngineStarted { node_id: NodeId },
    /// Consensus engine stopped
    EngineStopped { node_id: NodeId },
}

/// Message Bus for consensus-level communication and replication
#[derive(Debug)]
pub struct ConsensusMessageBus {
    messages: Arc<Mutex<VecDeque<ConsensusMessage>>>,
}

impl ConsensusMessageBus {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn publish(&self, message: ConsensusMessage) {
        if let Ok(mut messages) = self.messages.lock() {
            messages.push_back(message);
            // Keep only last 100 messages to prevent unbounded growth
            if messages.len() > 100 {
                messages.pop_front();
            }
        }
    }

    pub fn get_messages(&self) -> Vec<ConsensusMessage> {
        if let Ok(messages) = self.messages.lock() {
            messages.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }

    pub fn clear_messages(&self) {
        if let Ok(mut messages) = self.messages.lock() {
            messages.clear();
        }
    }

    pub fn get_messages_since(&self, since_count: usize) -> Vec<ConsensusMessage> {
        if let Ok(messages) = self.messages.lock() {
            if since_count < messages.len() {
                messages.iter().skip(since_count).cloned().collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }
}

#[derive(Debug)]
pub struct MockConsensusEngine {
    node_id: NodeId,
    term: AtomicU64,
    log: Arc<RwLock<Vec<LogEntry>>>,
    last_applied_index: AtomicU64,
    is_leader: AtomicBool,
    is_started: AtomicBool,
    cluster_members: Arc<RwLock<HashMap<NodeId, String>>>,
    state_machine: Box<dyn StateMachine>,
    message_bus: Option<Arc<ConsensusMessageBus>>,
}


impl MockConsensusEngine {
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
            message_bus: None,
        }
    }

    pub fn with_message_bus(node_id: NodeId, state_machine: Box<dyn StateMachine>, message_bus: Arc<ConsensusMessageBus>) -> Self {
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
            message_bus: Some(message_bus),
        }
    }

    pub fn message_bus(&self) -> Option<Arc<ConsensusMessageBus>> {
        self.message_bus.clone()
    }

    fn next_index(&self) -> Index {
        let log = self.log.read();
        log.len() as Index + 1
    }

    fn apply_pending_entries(&self) -> ConsensusResult<()> {
        let log = self.log.read();
        let current_applied = self.last_applied_index.load(Ordering::SeqCst);

        // Apply entries to the state machine
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
impl ConsensusEngine for MockConsensusEngine {
    // === Proposal and State Management ===

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

        // Accept the proposal (mock consensus accepts all proposals)
        let index = self.next_index();
        let term = self.current_term();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let request_id = Uuid::new_v4();
        let data_size = data.len();

        // Publish proposal message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::Proposal {
                request_id,
                index,
                term,
                data_size,
            });
        }

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

        // Publish commit message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::Commit {
                index,
                term,
                data_size,
            });
        }

        self.apply_pending_entries()?;

        Ok(ProposeResponse {
            request_id,
            success: true,
            index: Some(index),
            term: Some(term),
            error: None,
            leader_id: Some(self.node_id.clone()),
        })
    }


    // === Consensus Protocol Management ===

    async fn start(&mut self) -> ConsensusResult<()> {
        self.is_started.store(true, Ordering::SeqCst);
        tracing::info!("Mock consensus engine '{}' started", self.node_id);

        // Publish engine started message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::EngineStarted {
                node_id: self.node_id.clone(),
            });
        }

        Ok(())
    }

    async fn stop(&mut self) -> ConsensusResult<()> {
        self.is_started.store(false, Ordering::SeqCst);
        tracing::info!("Mock consensus engine '{}' stopped", self.node_id);

        // Publish engine stopped message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::EngineStopped {
                node_id: self.node_id.clone(),
            });
        }

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

    // === Node State and Identity ===

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    fn current_term(&self) -> Term {
        self.term.load(Ordering::SeqCst)
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn last_applied_index(&self) -> Index {
        self.last_applied_index.load(Ordering::SeqCst)
    }

    fn set_last_applied_index(&self, index: Index) {
        self.last_applied_index.store(index, Ordering::SeqCst);
    }

    // === Cluster Management ===

    fn cluster_members(&self) -> Vec<NodeId> {
        self.cluster_members.read().keys().cloned().collect()
    }

    async fn add_node(&mut self, node_id: NodeId, address: String) -> ConsensusResult<()> {
        let mut members = self.cluster_members.write();
        members.insert(node_id.clone(), address.clone());
        tracing::info!("Added node '{}' to cluster", node_id);

        // Publish node joined message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::NodeJoined {
                node_id: node_id.clone(),
                address,
            });
        }

        Ok(())
    }

    async fn remove_node(&mut self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut members = self.cluster_members.write();
        members.remove(node_id);
        tracing::info!("Removed node '{}' from cluster", node_id);

        // Publish node left message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::NodeLeft {
                node_id: node_id.clone(),
            });
        }

        Ok(())
    }
}