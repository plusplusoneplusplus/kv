pub mod mock_node;
pub mod transport;
pub mod in_memory_transport;
pub mod mock_consensus_server;
pub mod mock_consensus_client;
pub mod consensus_service;
pub mod types;

pub use mock_node::{MockConsensusEngine, ConsensusMessage, ConsensusMessageBus};
pub use transport::{
    NetworkTransport, ConsensusMessageHandler,
    AppendEntryRequest, AppendEntryResponse,
    CommitNotificationRequest, CommitNotificationResponse,
};
pub use in_memory_transport::{InMemoryTransport, InMemoryTransportRegistry};
pub use mock_consensus_server::MockConsensusServer;
pub use mock_consensus_client::MockConsensusClient;
pub use consensus_service::ConsensusServiceHandler;
pub use types::{
    AppendEntriesRequest, AppendEntriesResponse, LogEntry,
};

/// Initialize test logging with ANSI colors disabled
/// Available for both unit tests and integration tests
pub fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)  // Disable ANSI color codes
        .try_init();
}
