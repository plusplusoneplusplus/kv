use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::any::Any;
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogEntry {
    pub index: Index,
    pub term: Term,
    pub data: Vec<u8>,
    pub timestamp: u64,
}

/// Trait for application state machine operations
/// Handles applying committed operations to persistent application state
pub trait StateMachine: Send + Sync + Debug {
    /// Apply a committed log entry to the application state
    fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>>;

    /// Create a snapshot of the current application state
    fn snapshot(&self) -> ConsensusResult<Vec<u8>>;

    /// Restore application state from a snapshot
    fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()>;
}

/// Trait for consensus engine operations
/// Handles consensus protocol, leadership, proposals, and cluster management
#[async_trait]
pub trait ConsensusEngine: Send + Sync + Debug {
    // === Consensus Protocol ===

    /// Propose a new operation to be committed through consensus
    async fn propose(&self, data: Vec<u8>) -> ConsensusResult<ProposeResponse>;

    /// Start the consensus engine
    async fn start(&mut self) -> ConsensusResult<()>;

    /// Stop the consensus engine
    async fn stop(&mut self) -> ConsensusResult<()>;

    /// Wait for a specific log index to be committed and return the log entry data
    async fn wait_for_commit(&self, index: Index) -> ConsensusResult<Vec<u8>>;

    // === Node State and Identity ===

    /// Check if this node is currently the leader
    fn is_leader(&self) -> bool;

    /// Get the current term
    fn current_term(&self) -> Term;

    /// Get this node's ID
    fn node_id(&self) -> &NodeId;

    /// Get the last applied log index
    fn last_applied_index(&self) -> Index;

    /// Set the last applied log index
    fn set_last_applied_index(&self, index: Index);

    // === Cluster Management ===

    /// Get list of cluster member IDs
    fn cluster_members(&self) -> Vec<NodeId>;

    /// Add a new node to the cluster
    async fn add_node(&mut self, node_id: NodeId, address: String) -> ConsensusResult<()>;

    /// Remove a node from the cluster
    async fn remove_node(&mut self, node_id: &NodeId) -> ConsensusResult<()>;

    /// Downcast support for implementations that need concrete access
    /// This enables adapters (e.g., Thrift handlers) to reach
    /// implementation-specific functionality in a controlled way.
    fn as_any(&self) -> &dyn Any;
}
