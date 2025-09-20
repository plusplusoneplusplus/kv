use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

use crate::ConsensusResult;

pub type NodeId = String;
pub type Term = u64;
pub type Index = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposeResponse {
    pub request_id: Uuid,
    pub success: bool,
    pub index: Option<Index>,
    pub term: Option<Term>,
    pub error: Option<String>,
    pub leader_id: Option<NodeId>,
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: Index,
    pub term: Term,
    pub data: Vec<u8>,
    pub timestamp: u64,
}

#[async_trait]
pub trait ConsensusNode: Send + Sync + Debug {
    async fn propose(&self, data: Vec<u8>) -> ConsensusResult<ProposeResponse>;

    async fn start(&mut self) -> ConsensusResult<()>;

    async fn stop(&mut self) -> ConsensusResult<()>;

    fn is_leader(&self) -> bool;

    fn current_term(&self) -> Term;

    fn node_id(&self) -> &NodeId;

    fn cluster_members(&self) -> Vec<NodeId>;

    async fn add_node(&mut self, node_id: NodeId, address: String) -> ConsensusResult<()>;

    async fn remove_node(&mut self, node_id: &NodeId) -> ConsensusResult<()>;

    async fn wait_for_commit(&self, index: Index) -> ConsensusResult<Vec<u8>>;

    async fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>>;

    async fn snapshot(&self) -> ConsensusResult<Vec<u8>>;

    async fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()>;

    fn last_applied_index(&self) -> Index;

    fn set_last_applied_index(&self, index: Index);
}