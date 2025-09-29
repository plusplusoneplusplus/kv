use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use consensus_api::{ConsensusEngine, NodeId, StateMachine};
use consensus_mock::transport::NetworkTransport;

/// Role of a node in the cluster for engine construction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsensusNodeRole {
    Leader,
    Follower,
}

/// Abstracts the network transport and server binding for consensus node communication
pub trait ConsensusNetwork: Send + Sync {
    /// Build a transport for this node with the given endpoints (node_id -> address)
    fn build_transport(
        &self,
        node_id: NodeId,
        endpoints: HashMap<NodeId, String>,
    ) -> Arc<dyn NetworkTransport>;

    /// Spawn the consensus RPC server for this node
    fn spawn_server(
        &self,
        port: u16,
        node_id: u32,
        engine: Arc<RwLock<Box<dyn ConsensusEngine>>>,
    ) -> Result<std::thread::JoinHandle<()>, Box<dyn std::error::Error>>;
}

/// Pluggable consensus implementation that can provide engines and network
pub trait ConsensusPlugin: Send + Sync {
    /// Access the network implementation used by this plugin
    fn network(&self) -> &dyn ConsensusNetwork;

    /// Construct an engine for the given role using a provided transport and state machine
    fn build_engine(
        &self,
        node_id: NodeId,
        state_machine: Box<dyn StateMachine>,
        transport: Arc<dyn NetworkTransport>,
        role: ConsensusNodeRole,
    ) -> Box<dyn ConsensusEngine>;
}

