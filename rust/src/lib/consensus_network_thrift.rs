use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::info;

use consensus_api::{ConsensusEngine, NodeId};
use consensus_mock::transport::NetworkTransport;

use crate::lib::consensus_plugin::ConsensusNetwork;
// Intentionally avoid importing the concrete types here to prevent namespace collisions with re-exports below

#[derive(Debug, Default)]
pub struct ThriftConsensusNetwork;

impl ThriftConsensusNetwork {
    pub fn new() -> Self {
        Self
    }
}

impl ConsensusNetwork for ThriftConsensusNetwork {
    fn build_transport(
        &self,
        node_id: NodeId,
        endpoints: HashMap<NodeId, String>,
    ) -> Arc<dyn NetworkTransport> {
        // GeneratedThriftTransport already implements NetworkTransport
        // Build it with the provided endpoints
        let rt = tokio::runtime::Runtime::new()
            .expect("failed to create runtime for transport init");
        let transport = rt.block_on(
            crate::lib::consensus_transport::GeneratedThriftTransport::with_endpoints(node_id, endpoints)
        );
        Arc::new(transport)
    }

    fn spawn_server(
        &self,
        port: u16,
        node_id: u32,
        engine: Arc<RwLock<Box<dyn ConsensusEngine>>>,
    ) -> Result<std::thread::JoinHandle<()>, Box<dyn std::error::Error>> {
        info!("Starting Thrift consensus RPC server on port {}", port);
        let server = crate::lib::consensus_thrift::ConsensusThriftServer::with_consensus_engine(port, node_id, engine);
        server.start()
    }
}

// Re-export underlying transport and server from this module for external use
pub use crate::lib::consensus_transport::GeneratedThriftTransport;
pub use crate::lib::consensus_thrift::ConsensusThriftServer;
