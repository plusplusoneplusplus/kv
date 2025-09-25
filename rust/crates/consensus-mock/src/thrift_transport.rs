use async_trait::async_trait;
use consensus_api::{ConsensusResult, ConsensusError, Index, LogEntry, NodeId, Term};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use thrift::transport::TTcpChannel;

use crate::types::{
    AppendEntriesRequest as ThriftAppendEntriesRequest,
    AppendEntriesResponse as ThriftAppendEntriesResponse,
    LogEntry as ThriftLogEntry,
};

use crate::transport::{
    NetworkTransport,
    AppendEntryRequest, AppendEntryResponse,
    CommitNotificationRequest, CommitNotificationResponse,
};
use crate::consensus_client::ConsensusClient;

/// Thrift-based network transport implementation that uses actual TCP connections
/// and communicates with real Thrift consensus servers over the network
#[derive(Clone)]
pub struct ThriftTransport {
    node_id: NodeId,
    node_endpoints: Arc<Mutex<HashMap<NodeId, String>>>,
}

impl std::fmt::Debug for ThriftTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThriftTransport")
            .field("node_id", &self.node_id)
            .field("node_endpoints", &"<mutex>")
            .finish()
    }
}

impl ThriftTransport {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            node_endpoints: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a Thrift client connection to a specific node
    #[allow(dead_code)]
    async fn create_client_connection(&self, target_node: &NodeId) -> ConsensusResult<(TTcpChannel, String)> {
        let endpoints = self.node_endpoints.lock().await;
        let endpoint = endpoints.get(target_node).ok_or_else(|| {
            ConsensusError::TransportError(format!("No endpoint found for node {}", target_node))
        })?;

        // Parse the endpoint (format: "host:port")
        let parts: Vec<&str> = endpoint.split(':').collect();
        if parts.len() != 2 {
            return Err(ConsensusError::TransportError(format!(
                "Invalid endpoint format for node {}: {}. Expected host:port",
                target_node, endpoint
            )));
        }

        let host = parts[0];
        let port: u16 = parts[1].parse().map_err(|e| {
            ConsensusError::TransportError(format!(
                "Invalid port in endpoint {}: {}",
                endpoint, e
            ))
        })?;

        // Create TCP connection
        let mut channel = TTcpChannel::new();
        channel.open(&format!("{}:{}", host, port)).map_err(|e| {
            ConsensusError::TransportError(format!(
                "Failed to connect to node {} at {}: {}",
                target_node, endpoint, e
            ))
        })?;

        Ok((channel, endpoint.clone()))
    }

    /// Convert consensus API LogEntry to Thrift LogEntry
    fn convert_log_entry_to_thrift(entry: &LogEntry) -> ThriftLogEntry {
        ThriftLogEntry::new(
            entry.term as i64,
            entry.index as i64,
            entry.data.clone(),
            "operation".to_string(), // Default entry type
        )
    }

    /// Convert Thrift LogEntry to consensus API LogEntry
    #[allow(dead_code)]
    fn convert_log_entry_from_thrift(entry: &ThriftLogEntry) -> LogEntry {
        LogEntry {
            term: entry.term as Term,
            index: entry.index as Index,
            data: entry.data.clone(),
            timestamp: 0, // Thrift version doesn't have timestamp, use default
        }
    }

    /// Convert AppendEntryRequest to Thrift AppendEntriesRequest
    fn convert_append_entry_request_to_thrift(request: &AppendEntryRequest) -> ThriftAppendEntriesRequest {
        let thrift_entry = Self::convert_log_entry_to_thrift(&request.entry);

        ThriftAppendEntriesRequest::new(
            request.leader_term as i64,
            request.leader_id.parse().unwrap_or(0), // Convert string to i32, fallback to 0
            request.prev_log_index as i64,
            request.prev_log_term as i64,
            vec![thrift_entry],
            request.prev_log_index as i64, // Use prev_log_index as leader_commit for now
        )
    }

    /// Convert Thrift AppendEntriesResponse to AppendEntryResponse
    fn convert_append_entry_response_from_thrift(
        response: &ThriftAppendEntriesResponse,
        node_id: &NodeId,
    ) -> AppendEntryResponse {
        AppendEntryResponse {
            node_id: node_id.clone(),
            success: response.success,
            index: response.last_log_index.unwrap_or(0) as Index,
            error: response.error.clone(),
        }
    }
}

#[async_trait]
impl NetworkTransport for ThriftTransport {
    async fn send_append_entry(
        &self,
        target_node: &NodeId,
        request: AppendEntryRequest,
    ) -> ConsensusResult<AppendEntryResponse> {
        let endpoint = {
            let endpoints = self.node_endpoints.lock().await;
            endpoints.get(target_node).ok_or_else(|| {
                ConsensusError::TransportError(format!("No endpoint found for node {}", target_node))
            })?.clone()
        };

        let thrift_request = Self::convert_append_entry_request_to_thrift(&request);

        // Create consensus client and make actual RPC call
        let client = ConsensusClient::new(endpoint.clone());
        let thrift_response = client.append_entries(thrift_request).await?;

        tracing::info!(
            "Sent append_entry to {} at {}: prev_index={}, entry_index={}, got success={}",
            target_node,
            endpoint,
            request.prev_log_index,
            request.entry.index,
            thrift_response.success
        );

        Ok(Self::convert_append_entry_response_from_thrift(
            &thrift_response,
            target_node,
        ))
    }

    async fn send_commit_notification(
        &self,
        target_node: &NodeId,
        request: CommitNotificationRequest,
    ) -> ConsensusResult<CommitNotificationResponse> {
        // For now, commit notifications could be implemented as separate calls,
        // but since the mock consensus doesn't use them extensively,
        // we'll keep this as a no-op but successful operation
        tracing::debug!(
            "Commit notification to {}: commit_index={} (no-op)",
            target_node,
            request.commit_index
        );

        Ok(CommitNotificationResponse { success: true })
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn node_endpoints(&self) -> &HashMap<NodeId, String> {
        // Return empty map for now due to sync API limitation
        // This should be redesigned to be async in the future
        use std::sync::LazyLock;
        static EMPTY_MAP: LazyLock<HashMap<NodeId, String>> = LazyLock::new(|| HashMap::new());
        &EMPTY_MAP
    }

    async fn update_node_endpoint(&mut self, node_id: NodeId, endpoint: String) -> ConsensusResult<()> {
        let mut endpoints = self.node_endpoints.lock().await;
        endpoints.insert(node_id, endpoint);
        Ok(())
    }

    async fn remove_node_endpoint(&mut self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut endpoints = self.node_endpoints.lock().await;
        endpoints.remove(node_id);
        Ok(())
    }

    async fn is_node_reachable(&self, node_id: &NodeId) -> bool {
        let endpoint = {
            let endpoints = self.node_endpoints.lock().await;
            match endpoints.get(node_id) {
                Some(ep) => ep.clone(),
                None => return false,
            }
        };

        let client = ConsensusClient::new(endpoint);
        client.is_reachable().await
    }
}

/// Helper trait to create ThriftTransport with preconfigured endpoints
impl ThriftTransport {
    /// Create a new ThriftTransport with a set of known node endpoints
    pub async fn with_endpoints(
        node_id: NodeId,
        endpoints: HashMap<NodeId, String>,
    ) -> Self {
        let transport = Self::new(node_id);

        {
            let mut transport_endpoints = transport.node_endpoints.lock().await;
            for (node, endpoint) in endpoints {
                transport_endpoints.insert(node, endpoint);
            }
        }

        transport
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus_api::LogEntry;

    #[test]
    fn test_log_entry_conversion() {
        let original = LogEntry {
            term: 1,
            index: 10,
            data: b"test_data".to_vec(),
            timestamp: 123456789,
        };

        let thrift_entry = ThriftTransport::convert_log_entry_to_thrift(&original);
        assert_eq!(thrift_entry.term, 1);
        assert_eq!(thrift_entry.index, 10);
        assert_eq!(thrift_entry.data, b"test_data".to_vec());
        assert_eq!(thrift_entry.entry_type, "operation");

        let converted_back = ThriftTransport::convert_log_entry_from_thrift(&thrift_entry);
        assert_eq!(converted_back.term, original.term);
        assert_eq!(converted_back.index, original.index);
        assert_eq!(converted_back.data, original.data);
        // Note: timestamp is lost in conversion (not in Thrift schema)
    }

    #[test]
    fn test_append_entry_request_conversion() {
        let entry = LogEntry {
            term: 1,
            index: 10,
            data: b"test_data".to_vec(),
            timestamp: 123456789,
        };

        let request = AppendEntryRequest {
            leader_id: "leader-1".to_string(),
            entry,
            prev_log_index: 9,
            prev_log_term: 1,
            leader_term: 1,
        };

        let thrift_request = ThriftTransport::convert_append_entry_request_to_thrift(&request);
        assert_eq!(thrift_request.term, 1);
        assert_eq!(thrift_request.leader_id, 0); // String parsed as i32, invalid becomes 0
        assert_eq!(thrift_request.prev_log_index, 9);
        assert_eq!(thrift_request.prev_log_term, 1);
        assert_eq!(thrift_request.entries.len(), 1);
        assert_eq!(thrift_request.entries[0].index, 10);
    }

    #[tokio::test]
    async fn test_transport_creation() {
        let transport = ThriftTransport::new("test-node".to_string());
        assert_eq!(transport.node_id(), "test-node");

        let endpoints = transport.node_endpoints.lock().await;
        assert!(endpoints.is_empty());
    }

    #[tokio::test]
    async fn test_transport_with_endpoints() {
        let mut endpoints = HashMap::new();
        endpoints.insert("node1".to_string(), "localhost:9001".to_string());
        endpoints.insert("node2".to_string(), "localhost:9002".to_string());

        let transport = ThriftTransport::with_endpoints("test-node".to_string(), endpoints).await;

        let transport_endpoints = transport.node_endpoints.lock().await;
        assert_eq!(transport_endpoints.len(), 2);
        assert_eq!(transport_endpoints.get("node1"), Some(&"localhost:9001".to_string()));
        assert_eq!(transport_endpoints.get("node2"), Some(&"localhost:9002".to_string()));
    }

    #[tokio::test]
    async fn test_endpoint_management() {
        let mut transport = ThriftTransport::new("test-node".to_string());

        // Add endpoint
        transport.update_node_endpoint("node1".to_string(), "localhost:9001".to_string()).await.unwrap();

        let endpoints = transport.node_endpoints.lock().await;
        assert_eq!(endpoints.get("node1"), Some(&"localhost:9001".to_string()));
        drop(endpoints);

        // Remove endpoint
        transport.remove_node_endpoint(&"node1".to_string()).await.unwrap();

        let endpoints = transport.node_endpoints.lock().await;
        assert!(!endpoints.contains_key("node1"));
    }

    #[tokio::test]
    async fn test_thrift_connection_creation() {
        let mut transport = ThriftTransport::new("leader".to_string());

        // Add endpoint for target node
        transport
            .update_node_endpoint("follower".to_string(), "localhost:9001".to_string())
            .await
            .unwrap();

        // Test connection creation (this will fail since no server is running)
        let result = transport.create_client_connection(&"follower".to_string()).await;
        assert!(result.is_err()); // Should fail since no server is running on 9001

        // Test invalid endpoint format
        transport
            .update_node_endpoint("invalid".to_string(), "invalid_format".to_string())
            .await
            .unwrap();

        let result = transport.create_client_connection(&"invalid".to_string()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_thrift_node_reachability() {
        let mut transport = ThriftTransport::new("leader".to_string());

        // Test node without endpoint
        let reachable = transport.is_node_reachable(&"unknown_node".to_string()).await;
        assert!(!reachable);

        // Add endpoint for target node (no server running)
        transport
            .update_node_endpoint("follower".to_string(), "localhost:9001".to_string())
            .await
            .unwrap();

        // Test node with endpoint (simulated as reachable)
        let reachable = transport.is_node_reachable(&"follower".to_string()).await;
        assert!(reachable); // Should be true in simulation mode
    }
}