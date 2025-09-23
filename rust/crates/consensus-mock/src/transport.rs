use async_trait::async_trait;
use consensus_api::{ConsensusResult, Index, LogEntry, NodeId, Term};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;

/// Request for appending a log entry to a follower node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntryRequest {
    pub leader_id: NodeId,
    pub entry: LogEntry,
    pub prev_log_index: Index,
    pub prev_log_term: Term,
    pub leader_term: Term,
}

/// Response from a follower node for an append entry request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntryResponse {
    pub node_id: NodeId,
    pub success: bool,
    pub index: Index,
    pub error: Option<String>,
}

/// Request for notifying followers about committed entries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitNotificationRequest {
    pub leader_id: NodeId,
    pub commit_index: Index,
    pub leader_term: Term,
}

/// Response from a follower node for a commit notification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitNotificationResponse {
    pub success: bool,
}

/// Trait for network transport operations between consensus nodes
/// Handles the actual network communication for consensus messages
#[async_trait]
pub trait NetworkTransport: Send + Sync + Debug {
    /// Send an append entry request to a target node
    async fn send_append_entry(
        &self,
        target_node: &NodeId,
        request: AppendEntryRequest,
    ) -> ConsensusResult<AppendEntryResponse>;

    /// Send a commit notification to a target node
    async fn send_commit_notification(
        &self,
        target_node: &NodeId,
        request: CommitNotificationRequest,
    ) -> ConsensusResult<CommitNotificationResponse>;

    /// Get the current node's ID
    fn node_id(&self) -> &NodeId;

    /// Get the list of known node endpoints (node_id -> address)
    fn node_endpoints(&self) -> &HashMap<NodeId, String>;

    /// Update the endpoint for a specific node
    async fn update_node_endpoint(&mut self, node_id: NodeId, endpoint: String) -> ConsensusResult<()>;

    /// Remove a node from the known endpoints
    async fn remove_node_endpoint(&mut self, node_id: &NodeId) -> ConsensusResult<()>;

    /// Check if connection to a node is healthy
    async fn is_node_reachable(&self, node_id: &NodeId) -> bool;
}

/// Trait for handling incoming consensus messages
/// Typically implemented by consensus engines to process network messages
#[async_trait]
pub trait ConsensusMessageHandler: Send + Sync + Debug {
    /// Handle an incoming append entry request
    async fn handle_append_entry(
        &self,
        request: AppendEntryRequest,
    ) -> ConsensusResult<AppendEntryResponse>;

    /// Handle an incoming commit notification
    async fn handle_commit_notification(
        &self,
        request: CommitNotificationRequest,
    ) -> ConsensusResult<CommitNotificationResponse>;
}