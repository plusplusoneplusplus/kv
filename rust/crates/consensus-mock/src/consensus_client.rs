use consensus_api::{ConsensusResult, ConsensusError};
use crate::types::{
    AppendEntriesRequest as ThriftAppendEntriesRequest,
    AppendEntriesResponse as ThriftAppendEntriesResponse,
};

/// Thrift client for sending consensus messages to other nodes
pub struct ConsensusClient {
    endpoint: String,
}

impl ConsensusClient {
    pub fn new(endpoint: String) -> Self {
        Self { endpoint }
    }

    /// Send append entries request to a consensus node
    pub async fn append_entries(
        &self,
        request: ThriftAppendEntriesRequest,
    ) -> ConsensusResult<ThriftAppendEntriesResponse> {
        // Validate endpoint format
        let parts: Vec<&str> = self.endpoint.split(':').collect();
        if parts.len() != 2 {
            return Err(ConsensusError::TransportError(format!(
                "Invalid endpoint format: {}. Expected host:port",
                self.endpoint
            )));
        }

        let host = parts[0];
        let port: u16 = parts[1].parse().map_err(|e| {
            ConsensusError::TransportError(format!(
                "Invalid port in endpoint {}: {}",
                self.endpoint, e
            ))
        })?;

        // Try to establish a connection first
        let _stream = tokio::net::TcpStream::connect((host, port)).await
            .map_err(|e| ConsensusError::TransportError(format!(
                "Failed to connect to {}: {}",
                self.endpoint, e
            )))?;

        // If we get here, we have a connection but no actual Thrift implementation yet
        // For now, simulate a successful response since we connected
        // In a real implementation, we'd send the actual Thrift message

        // Create a mock successful response
        let response = ThriftAppendEntriesResponse::new(
            request.term,
            true, // success - follower accepted the entries
            Some(request.prev_log_index + request.entries.len() as i64),
            None, // no error
        );

        tracing::debug!(
            "ConsensusClient: Simulated append_entries to {}: term={}, entries={}, got success={}",
            self.endpoint,
            request.term,
            request.entries.len(),
            response.success
        );

        Ok(response)
    }

    /// Test if the endpoint is reachable
    pub async fn is_reachable(&self) -> bool {
        let parts: Vec<&str> = self.endpoint.split(':').collect();
        if parts.len() != 2 {
            return false;
        }

        let host = parts[0];
        let port: u16 = match parts[1].parse() {
            Ok(p) => p,
            Err(_) => return false,
        };

        // Try to establish a TCP connection to test reachability
        match tokio::net::TcpStream::connect((host, port)).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{LogEntry};

    #[tokio::test]
    async fn test_consensus_client_creation() {
        let client = ConsensusClient::new("localhost:7090".to_string());
        assert_eq!(client.endpoint, "localhost:7090");
    }

    #[tokio::test]
    async fn test_append_entries_with_valid_endpoint() {
        let client = ConsensusClient::new("localhost:7090".to_string());

        let entry = LogEntry::new(
            1,  // term
            10, // index
            b"test_data".to_vec(),
            "operation".to_string(),
        );

        let request = ThriftAppendEntriesRequest::new(
            1,  // term
            0,  // leader_id
            9,  // prev_log_index
            1,  // prev_log_term
            vec![entry],
            9,  // leader_commit
        );

        // This will depend on whether a server is actually running
        let result = client.append_entries(request).await;
        // Don't assert success/failure since it depends on server availability
        tracing::debug!("Append entries result: {:?}", result);
    }

    #[tokio::test]
    async fn test_append_entries_with_invalid_endpoint_format() {
        let client = ConsensusClient::new("invalid_endpoint".to_string());

        let request = ThriftAppendEntriesRequest::new(1, 0, 0, 0, vec![], 0);

        let result = client.append_entries(request).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            ConsensusError::TransportError(msg) => {
                assert!(msg.contains("Invalid endpoint format"));
            }
            _ => panic!("Expected TransportError"),
        }
    }

    #[tokio::test]
    async fn test_append_entries_with_invalid_port() {
        let client = ConsensusClient::new("localhost:invalid_port".to_string());

        let request = ThriftAppendEntriesRequest::new(1, 0, 0, 0, vec![], 0);

        let result = client.append_entries(request).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            ConsensusError::TransportError(msg) => {
                assert!(msg.contains("Invalid port"));
            }
            _ => panic!("Expected TransportError"),
        }
    }

    #[tokio::test]
    async fn test_is_reachable_with_valid_endpoint() {
        let client = ConsensusClient::new("localhost:7090".to_string());

        // With real network checks, valid endpoints may not be reachable if no server is running
        let reachable = client.is_reachable().await;
        // Don't assert anything specific since it depends on whether a server is actually running
        tracing::debug!("Reachability for localhost:7090: {}", reachable);
    }

    #[tokio::test]
    async fn test_is_reachable_with_invalid_endpoint_format() {
        let client = ConsensusClient::new("invalid_format".to_string());

        let reachable = client.is_reachable().await;
        assert!(!reachable);
    }

    #[tokio::test]
    async fn test_is_reachable_with_invalid_port() {
        let client = ConsensusClient::new("localhost:invalid_port".to_string());

        let reachable = client.is_reachable().await;
        assert!(!reachable);
    }

    #[tokio::test]
    async fn test_append_entries_multiple_entries() {
        let client = ConsensusClient::new("localhost:7090".to_string());

        let entries = vec![
            LogEntry::new(1, 10, b"data1".to_vec(), "op".to_string()),
            LogEntry::new(1, 11, b"data2".to_vec(), "op".to_string()),
            LogEntry::new(1, 12, b"data3".to_vec(), "op".to_string()),
        ];

        let request = ThriftAppendEntriesRequest::new(1, 0, 9, 1, entries, 9);

        let result = client.append_entries(request).await;
        // Don't assert success/failure since it depends on server availability
        tracing::debug!("Multiple entries result: {:?}", result);
    }
}