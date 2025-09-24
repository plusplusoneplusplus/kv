use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use serde::{Deserialize, Serialize};

use consensus_api::{LogEntry, StateMachine, ConsensusResult, ConsensusError};
use crate::transport::{
    AppendEntryRequest, AppendEntryResponse,
    CommitNotificationRequest, CommitNotificationResponse,
};

/// Simple RPC message types for consensus communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusRpcMessage {
    AppendEntryRequest(AppendEntryRequest),
    AppendEntryResponse(AppendEntryResponse),
    CommitNotificationRequest(CommitNotificationRequest),
    CommitNotificationResponse(CommitNotificationResponse),
}

/// A mock consensus server that handles basic consensus operations
/// This is a minimal implementation for integration testing purposes
pub struct MockConsensusServer {
    node_id: String,
    port: u16,
    state_machine: Arc<dyn StateMachine + Send + Sync>,
    current_term: Arc<Mutex<u64>>,
    log: Arc<Mutex<Vec<LogEntry>>>,
    commit_index: Arc<Mutex<u64>>,
    is_leader: bool,
}

impl MockConsensusServer {
    pub fn new(
        node_id: String,
        port: u16,
        state_machine: Arc<dyn StateMachine + Send + Sync>,
        is_leader: bool,
    ) -> Self {
        Self {
            node_id,
            port,
            state_machine,
            current_term: Arc::new(Mutex::new(1)),
            log: Arc::new(Mutex::new(Vec::new())),
            commit_index: Arc::new(Mutex::new(0)),
            is_leader,
        }
    }

    /// Start the consensus server
    pub async fn start(&self) -> ConsensusResult<()> {
        let listen_addr = format!("127.0.0.1:{}", self.port);
        let listener = TcpListener::bind(&listen_addr).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to bind to {}: {}", listen_addr, e))
        })?;

        tracing::info!("Mock consensus server {} started on {}", self.node_id, listen_addr);

        // Handle incoming connections
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    tracing::debug!("Accepted connection from {}", addr);
                    let handler = self.clone_handler();
                    tokio::spawn(async move {
                        if let Err(e) = handler.handle_connection(stream).await {
                            tracing::error!("Error handling connection: {}", e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("Error accepting connection: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Create a handler for connection processing
    fn clone_handler(&self) -> MockConsensusServerHandler {
        MockConsensusServerHandler {
            node_id: self.node_id.clone(),
            state_machine: self.state_machine.clone(),
            current_term: self.current_term.clone(),
            log: self.log.clone(),
            commit_index: self.commit_index.clone(),
            is_leader: self.is_leader,
        }
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub async fn get_log_size(&self) -> usize {
        self.log.lock().await.len()
    }

    pub async fn get_commit_index(&self) -> u64 {
        *self.commit_index.lock().await
    }
}

/// Handler for processing consensus connections
#[derive(Clone)]
struct MockConsensusServerHandler {
    node_id: String,
    state_machine: Arc<dyn StateMachine + Send + Sync>,
    current_term: Arc<Mutex<u64>>,
    log: Arc<Mutex<Vec<LogEntry>>>,
    commit_index: Arc<Mutex<u64>>,
    #[allow(dead_code)]
    is_leader: bool,
}

impl MockConsensusServerHandler {
    /// Handle an incoming TCP connection
    async fn handle_connection(&self, mut stream: TcpStream) -> ConsensusResult<()> {
        // Read message length first (4 bytes)
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to read message length: {}", e))
        })?;

        let msg_len = u32::from_be_bytes(len_buf) as usize;
        if msg_len > 1024 * 1024 {
            return Err(ConsensusError::TransportError(
                "Message too large".to_string()
            ));
        }

        // Read the actual message
        let mut msg_buf = vec![0u8; msg_len];
        stream.read_exact(&mut msg_buf).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to read message: {}", e))
        })?;

        // Deserialize the message
        let message: ConsensusRpcMessage = bincode::deserialize(&msg_buf).map_err(|e| {
            ConsensusError::SerializationError {
                message: format!("Failed to deserialize message: {}", e),
            }
        })?;

        // Process the message and get response
        let response = self.process_message(message).await?;

        // Serialize and send response
        let response_data = bincode::serialize(&response).map_err(|e| {
            ConsensusError::SerializationError {
                message: format!("Failed to serialize response: {}", e),
            }
        })?;

        // Send response length and data
        let response_len = (response_data.len() as u32).to_be_bytes();
        stream.write_all(&response_len).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to write response length: {}", e))
        })?;

        stream.write_all(&response_data).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to write response: {}", e))
        })?;

        Ok(())
    }

    /// Process an incoming consensus message
    async fn process_message(&self, message: ConsensusRpcMessage) -> ConsensusResult<ConsensusRpcMessage> {
        match message {
            ConsensusRpcMessage::AppendEntryRequest(request) => {
                let response = self.handle_append_entry(request).await?;
                Ok(ConsensusRpcMessage::AppendEntryResponse(response))
            }
            ConsensusRpcMessage::CommitNotificationRequest(request) => {
                let response = self.handle_commit_notification(request).await?;
                Ok(ConsensusRpcMessage::CommitNotificationResponse(response))
            }
            _ => Err(ConsensusError::TransportError(
                "Unexpected message type".to_string()
            )),
        }
    }

    /// Handle append entry request
    async fn handle_append_entry(&self, request: AppendEntryRequest) -> ConsensusResult<AppendEntryResponse> {
        let current_term = *self.current_term.lock().await;

        tracing::debug!(
            "Node {} received append entry: term={}, prev_index={}, entry_index={}",
            self.node_id,
            request.leader_term,
            request.prev_log_index,
            request.entry.index
        );

        // Basic term checking
        if request.leader_term < current_term {
            return Ok(AppendEntryResponse {
                node_id: self.node_id.clone(),
                success: false,
                index: 0,
                error: Some("Term too old".to_string()),
            });
        }

        // Update term if necessary
        if request.leader_term > current_term {
            *self.current_term.lock().await = request.leader_term;
        }

        // Simple log consistency check
        let mut log = self.log.lock().await;

        // Check if we can append this entry
        if request.prev_log_index > 0 {
            if log.len() < request.prev_log_index as usize {
                return Ok(AppendEntryResponse {
                    node_id: self.node_id.clone(),
                    success: false,
                    index: log.len() as u64,
                    error: Some("Log too short".to_string()),
                });
            }

            if log.len() >= request.prev_log_index as usize {
                let prev_entry = &log[request.prev_log_index as usize - 1];
                if prev_entry.term != request.prev_log_term {
                    return Ok(AppendEntryResponse {
                        node_id: self.node_id.clone(),
                        success: false,
                        index: log.len() as u64,
                        error: Some("Term mismatch".to_string()),
                    });
                }
            }
        }

        // Append the entry
        if log.len() >= request.entry.index as usize {
            // Truncate conflicting entries
            log.truncate(request.entry.index as usize - 1);
        }

        log.push(request.entry.clone());

        tracing::debug!(
            "Node {} appended entry to log: new log size = {}",
            self.node_id,
            log.len()
        );

        Ok(AppendEntryResponse {
            node_id: self.node_id.clone(),
            success: true,
            index: request.entry.index,
            error: None,
        })
    }

    /// Handle commit notification
    async fn handle_commit_notification(&self, request: CommitNotificationRequest) -> ConsensusResult<CommitNotificationResponse> {
        tracing::debug!(
            "Node {} received commit notification: commit_index={}",
            self.node_id,
            request.commit_index
        );

        let mut commit_index = self.commit_index.lock().await;
        let log = self.log.lock().await;

        // Apply committed entries to state machine
        let old_commit_index = *commit_index;
        let new_commit_index = std::cmp::min(request.commit_index, log.len() as u64);

        for i in (old_commit_index + 1)..=new_commit_index {
            if let Some(entry) = log.get(i as usize - 1) {
                match self.state_machine.apply(entry) {
                    Ok(result) => {
                        tracing::debug!(
                            "Node {} applied entry {} to state machine: result size = {}",
                            self.node_id,
                            i,
                            result.len()
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            "Node {} failed to apply entry {}: {}",
                            self.node_id,
                            i,
                            e
                        );
                        return Ok(CommitNotificationResponse { success: false });
                    }
                }
            }
        }

        *commit_index = new_commit_index;

        tracing::debug!(
            "Node {} updated commit index to {}",
            self.node_id,
            *commit_index
        );

        Ok(CommitNotificationResponse { success: true })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus_api::StateMachine;

    // Simple test state machine
    #[derive(Debug)]
    struct TestStateMachine {
        operations: Arc<Mutex<Vec<String>>>,
    }

    impl TestStateMachine {
        fn new() -> Self {
            Self {
                operations: Arc::new(Mutex::new(Vec::new())),
            }
        }

        async fn get_operations(&self) -> Vec<String> {
            self.operations.lock().await.clone()
        }
    }

    impl StateMachine for TestStateMachine {
        fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
            let operation = String::from_utf8_lossy(&entry.data);
            let mut ops = futures::executor::block_on(self.operations.lock());
            ops.push(operation.to_string());
            Ok(format!("Applied: {}", operation).into_bytes())
        }

        fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
            let ops = futures::executor::block_on(self.operations.lock());
            Ok(bincode::serialize(&*ops).unwrap())
        }

        fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
            let ops: Vec<String> = bincode::deserialize(snapshot).unwrap();
            *futures::executor::block_on(self.operations.lock()) = ops;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_mock_consensus_server_creation() {
        let state_machine = Arc::new(TestStateMachine::new());
        let server = MockConsensusServer::new(
            "test-node".to_string(),
            0, // Use port 0 for testing
            state_machine,
            true,
        );

        assert_eq!(server.node_id(), "test-node");
        assert_eq!(server.get_log_size().await, 0);
        assert_eq!(server.get_commit_index().await, 0);
    }
}