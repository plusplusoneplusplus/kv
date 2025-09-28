use std::net::TcpListener;
use std::sync::Arc;
use std::thread;

use parking_lot::RwLock;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use tracing::{debug, error, info};

use consensus_api::ConsensusEngine;
use consensus_mock::ConsensusServiceHandler as MockConsensusServiceHandler;

use crate::generated::kvstore::{
    AppendEntriesRequest as GenAppendEntriesRequest,
    AppendEntriesResponse as GenAppendEntriesResponse,
    InstallSnapshotRequest as GenInstallSnapshotRequest,
    InstallSnapshotResponse as GenInstallSnapshotResponse,
    RequestVoteRequest as GenRequestVoteRequest,
    RequestVoteResponse as GenRequestVoteResponse,
    ConsensusServiceSyncHandler, ConsensusServiceSyncProcessor,
    LogEntry as GenLogEntry,
};

/// Adapter that bridges the generated Thrift sync handler to the async mock handler
pub struct ConsensusServiceAdapter {
    inner: Arc<MockConsensusServiceHandler>,
}

impl ConsensusServiceAdapter {
    pub fn new(consensus_engine: Arc<RwLock<Box<dyn ConsensusEngine>>>) -> Self {
        let handler = MockConsensusServiceHandler::with_consensus_engine(consensus_engine);
        Self { inner: Arc::new(handler) }
    }

    fn convert_entry_to_mock(entry: &GenLogEntry) -> consensus_mock::types::LogEntry {
        consensus_mock::types::LogEntry::new(
            entry.term,
            entry.index,
            entry.data.clone(),
            entry.entry_type.clone(),
        )
    }

    fn convert_request_to_mock(req: &GenAppendEntriesRequest) -> consensus_mock::types::AppendEntriesRequest {
        let entries = req
            .entries
            .iter()
            .map(Self::convert_entry_to_mock)
            .collect::<Vec<_>>();
        consensus_mock::types::AppendEntriesRequest::new(
            req.term,
            req.leader_id,
            req.prev_log_index,
            req.prev_log_term,
            entries,
            req.leader_commit,
        )
    }

    fn convert_response_to_gen(resp: &consensus_mock::types::AppendEntriesResponse) -> GenAppendEntriesResponse {
        GenAppendEntriesResponse::new(resp.term, resp.success, resp.last_log_index, resp.error.clone())
    }
}

impl ConsensusServiceSyncHandler for ConsensusServiceAdapter {
    fn handle_append_entries(
        &self,
        request: GenAppendEntriesRequest,
    ) -> thrift::Result<GenAppendEntriesResponse> {
        let mock_req = Self::convert_request_to_mock(&request);

        // Execute async handler on a runtime
        let fut = {
            let inner = Arc::clone(&self.inner);
            async move { inner.handle_append_entries(mock_req).await }
        };

        let rt_handle = tokio::runtime::Handle::try_current()
            .or_else(|_| tokio::runtime::Runtime::new().map(|rt| rt.handle().clone()))
            .map_err(|e| thrift::Error::Transport(thrift::TransportError::new(thrift::TransportErrorKind::Unknown, format!("runtime error: {}", e))))?;

        let mock_resp = rt_handle
            .block_on(fut)
            .map_err(|e| thrift::Error::from(std::io::Error::new(std::io::ErrorKind::Other, format!("append_entries error: {}", e))))?;

        Ok(Self::convert_response_to_gen(&mock_resp))
    }

    fn handle_request_vote(
        &self,
        request: GenRequestVoteRequest,
    ) -> thrift::Result<GenRequestVoteResponse> {
        // Simple mock response: grant vote
        Ok(GenRequestVoteResponse::new(request.term, true, None::<String>))
    }

    fn handle_install_snapshot(
        &self,
        request: GenInstallSnapshotRequest,
    ) -> thrift::Result<GenInstallSnapshotResponse> {
        // Simple mock response: accept snapshot
        Ok(GenInstallSnapshotResponse::new(request.term, true, None::<String>))
    }
}

/// Thrift server that uses the generated ConsensusService processor and the adapter handler
pub struct ConsensusThriftServer {
    port: u16,
    node_id: u32,
    adapter: ConsensusServiceAdapter,
}

impl ConsensusThriftServer {
    pub fn with_consensus_engine(
        port: u16,
        node_id: u32,
        consensus_engine: Arc<RwLock<Box<dyn ConsensusEngine>>>,
    ) -> Self {
        Self {
            port,
            node_id,
            adapter: ConsensusServiceAdapter::new(consensus_engine),
        }
    }

    pub fn start(self) -> Result<thread::JoinHandle<()>, Box<dyn std::error::Error>> {
        let listen_address = format!("0.0.0.0:{}", self.port);
        info!("Starting Consensus Thrift server on {}", listen_address);

        let listener = TcpListener::bind(&listen_address)?;
        info!("Consensus server started on {}, waiting for connections...", listen_address);

        let node_id = self.node_id;
        let adapter = self.adapter; // move into thread

        let handle = thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let peer_addr = stream
                            .peer_addr()
                            .unwrap_or_else(|_| "unknown".parse().unwrap());
                        let adapter = ConsensusServiceAdapter { inner: Arc::clone(&adapter.inner) };
                        thread::spawn(move || {
                            info!("Consensus Node {}: Accepted connection from {}", node_id, peer_addr);

                            // Create transports
                            let read_transport = TBufferedReadTransport::new(stream.try_clone().unwrap());
                            let write_transport = TBufferedWriteTransport::new(stream);

                            // Create protocols
                            let mut input_protocol = TBinaryInputProtocol::new(read_transport, true);
                            let mut output_protocol = TBinaryOutputProtocol::new(write_transport, true);

                            debug!("Consensus Node {}: Connection setup complete for {}", node_id, peer_addr);

                            let processor = ConsensusServiceSyncProcessor::new(adapter);

                            // Process until client closes or error
                            let mut request_count = 0;
                            loop {
                                match processor.process(&mut input_protocol, &mut output_protocol) {
                                    Ok(()) => {
                                        request_count += 1;
                                        debug!(
                                            "Consensus Node {}: Request {} from {} processed successfully",
                                            node_id, request_count, peer_addr
                                        );
                                    }
                                    Err(thrift::Error::Transport(ref e))
                                        if e.kind == thrift::TransportErrorKind::EndOfFile =>
                                    {
                                        info!(
                                            "Consensus Node {}: Connection from {} closed after {} requests",
                                            node_id, peer_addr, request_count
                                        );
                                        break;
                                    }
                                    Err(e) => {
                                        error!(
                                            "Consensus Node {}: Error processing request from {}: {}",
                                            node_id, peer_addr, e
                                        );
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        error!("Consensus server error accepting connection: {}", e);
                    }
                }
            }
        });

        Ok(handle)
    }
}

