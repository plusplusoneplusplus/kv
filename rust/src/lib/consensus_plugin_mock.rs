use std::sync::Arc;

use consensus_api::{ConsensusEngine, NodeId, StateMachine};
use consensus_mock::MockConsensusEngine;
use consensus_mock::transport::NetworkTransport;

use crate::lib::consensus_plugin::{ConsensusNetwork, ConsensusNodeRole, ConsensusPlugin};
use crate::lib::consensus_network_thrift::ThriftConsensusNetwork;

/// Default plugin that wires MockConsensusEngine with a Thrift-based network
#[derive(Debug, Default)]
pub struct MockConsensusPlugin {
    network: ThriftConsensusNetwork,
}

impl MockConsensusPlugin {
    pub fn new() -> Self {
        Self { network: ThriftConsensusNetwork::new() }
    }
}

impl ConsensusPlugin for MockConsensusPlugin {
    fn network(&self) -> &dyn ConsensusNetwork {
        &self.network
    }

    fn build_engine(
        &self,
        node_id: NodeId,
        state_machine: Box<dyn StateMachine>,
        transport: Arc<dyn NetworkTransport>,
        role: ConsensusNodeRole,
    ) -> Box<dyn ConsensusEngine> {
        match role {
            ConsensusNodeRole::Leader => Box::new(MockConsensusEngine::with_network_transport(
                node_id,
                state_machine,
                transport,
            )),
            ConsensusNodeRole::Follower => Box::new(MockConsensusEngine::follower_with_network_transport(
                node_id,
                state_machine,
                transport,
            )),
        }
    }
}
