use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use consensus_api::{ConsensusResult, ConsensusError};
use crate::transport::{AppendEntryRequest, AppendEntryResponse, CommitNotificationRequest, CommitNotificationResponse};
use crate::mock_consensus_server::ConsensusRpcMessage;

/// Mock consensus client for communicating with consensus servers
#[derive(Clone)]
pub struct MockConsensusClient {
    server_address: String,
}

impl MockConsensusClient {
    pub fn new(server_address: String) -> Self {
        Self { server_address }
    }

    /// Send an append entry request to the server
    pub async fn send_append_entry(&self, request: AppendEntryRequest) -> ConsensusResult<AppendEntryResponse> {
        let message = ConsensusRpcMessage::AppendEntryRequest(request);
        let response = self.send_message(message).await?;

        match response {
            ConsensusRpcMessage::AppendEntryResponse(response) => Ok(response),
            _ => Err(ConsensusError::TransportError(
                "Unexpected response type".to_string()
            )),
        }
    }

    /// Send a commit notification to the server
    pub async fn send_commit_notification(&self, request: CommitNotificationRequest) -> ConsensusResult<CommitNotificationResponse> {
        let message = ConsensusRpcMessage::CommitNotificationRequest(request);
        let response = self.send_message(message).await?;

        match response {
            ConsensusRpcMessage::CommitNotificationResponse(response) => Ok(response),
            _ => Err(ConsensusError::TransportError(
                "Unexpected response type".to_string()
            )),
        }
    }

    /// Send a message to the server and receive response
    async fn send_message(&self, message: ConsensusRpcMessage) -> ConsensusResult<ConsensusRpcMessage> {
        // Connect to server
        let mut stream = TcpStream::connect(&self.server_address).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to connect to {}: {}", self.server_address, e))
        })?;

        // Serialize message
        let message_data = bincode::serialize(&message).map_err(|e| {
            ConsensusError::SerializationError {
                message: format!("Failed to serialize message: {}", e),
            }
        })?;

        // Send message length and data
        let msg_len = (message_data.len() as u32).to_be_bytes();
        stream.write_all(&msg_len).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to write message length: {}", e))
        })?;

        stream.write_all(&message_data).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to write message: {}", e))
        })?;

        // Read response length
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to read response length: {}", e))
        })?;

        let response_len = u32::from_be_bytes(len_buf) as usize;
        if response_len > 1024 * 1024 {
            return Err(ConsensusError::TransportError(
                "Response too large".to_string()
            ));
        }

        // Read response data
        let mut response_buf = vec![0u8; response_len];
        stream.read_exact(&mut response_buf).await.map_err(|e| {
            ConsensusError::TransportError(format!("Failed to read response: {}", e))
        })?;

        // Deserialize response
        let response: ConsensusRpcMessage = bincode::deserialize(&response_buf).map_err(|e| {
            ConsensusError::SerializationError {
                message: format!("Failed to deserialize response: {}", e),
            }
        })?;

        Ok(response)
    }

    /// Test connection to the server
    pub async fn test_connection(&self) -> bool {
        TcpStream::connect(&self.server_address).await.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_consensus_client_creation() {
        let client = MockConsensusClient::new("localhost:19999".to_string());
        assert_eq!(client.server_address, "localhost:19999");
    }

    #[tokio::test]
    async fn test_client_connection_failure() {
        let client = MockConsensusClient::new("localhost:19999".to_string());

        // Should fail since no server is running
        assert!(!client.test_connection().await);
    }
}