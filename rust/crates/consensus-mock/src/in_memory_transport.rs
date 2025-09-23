use async_trait::async_trait;
use consensus_api::{ConsensusResult, NodeId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

use crate::transport::{
    NetworkTransport, ConsensusMessageHandler,
    AppendEntryRequest, AppendEntryResponse,
    CommitNotificationRequest, CommitNotificationResponse,
};
use crate::{ConsensusMessage, ConsensusMessageBus};

/// In-memory transport implementation that uses the existing ConsensusMessageBus
/// for communication between nodes in the same process
#[derive(Debug)]
pub struct InMemoryTransport {
    node_id: NodeId,
    node_endpoints: Arc<Mutex<HashMap<NodeId, String>>>,
    message_bus: Arc<ConsensusMessageBus>,
    message_handlers: Arc<Mutex<HashMap<NodeId, Arc<dyn ConsensusMessageHandler>>>>,
}

impl InMemoryTransport {
    pub fn new(
        node_id: NodeId,
        message_bus: Arc<ConsensusMessageBus>,
    ) -> Self {
        let mut endpoints = HashMap::new();
        endpoints.insert(node_id.clone(), "in-memory".to_string());

        Self {
            node_id,
            node_endpoints: Arc::new(Mutex::new(endpoints)),
            message_bus,
            message_handlers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_message_handler(&self, node_id: NodeId, handler: Arc<dyn ConsensusMessageHandler>) {
        let mut handlers = self.message_handlers.lock().await;
        handlers.insert(node_id, handler);
    }

    pub async fn unregister_message_handler(&self, node_id: &NodeId) {
        let mut handlers = self.message_handlers.lock().await;
        handlers.remove(node_id);
    }

    /// Convert transport AppendEntryRequest to bus ConsensusMessage
    fn convert_append_entry_request(
        &self,
        target_node: &NodeId,
        request: &AppendEntryRequest,
    ) -> ConsensusMessage {
        ConsensusMessage::AppendEntry {
            target_node: target_node.clone(),
            entry: request.entry.clone(),
            leader_id: request.leader_id.clone(),
            prev_log_index: request.prev_log_index,
            prev_log_term: request.prev_log_term,
        }
    }

    /// Convert transport CommitNotificationRequest to bus ConsensusMessage
    fn convert_commit_notification_request(
        &self,
        target_node: &NodeId,
        request: &CommitNotificationRequest,
    ) -> ConsensusMessage {
        ConsensusMessage::CommitNotification {
            target_node: target_node.clone(),
            commit_index: request.commit_index,
            leader_id: request.leader_id.clone(),
        }
    }

    /// Simulate message delivery to the target node by calling its handler directly
    async fn deliver_to_handler(
        &self,
        target_node: &NodeId,
        message: ConsensusMessage,
    ) -> ConsensusResult<Option<ConsensusMessage>> {
        let handler = {
            let handlers = self.message_handlers.lock().await;
            handlers.get(target_node).cloned()
        };

        if let Some(handler) = handler {
            match message {
                ConsensusMessage::AppendEntry {
                    entry,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    ..
                } => {
                    let request = AppendEntryRequest {
                        leader_id,
                        entry,
                        prev_log_index,
                        prev_log_term,
                        leader_term: prev_log_term,
                    };

                    let response = handler.handle_append_entry(request).await?;

                    return Ok(Some(ConsensusMessage::AppendEntryResponse {
                        from_node: response.node_id.clone(),
                        to_leader: self.node_id.clone(),
                        index: response.index,
                        success: response.success,
                    }));
                }
                ConsensusMessage::CommitNotification {
                    commit_index,
                    leader_id,
                    ..
                } => {
                    let request = CommitNotificationRequest {
                        leader_id,
                        commit_index,
                        leader_term: 1, // Mock term
                    };

                    let _response = handler.handle_commit_notification(request).await?;
                    // Commit notifications don't need responses in the current design
                    return Ok(None);
                }
                _ => {}
            }
        }
        Ok(None)
    }
}

#[async_trait]
impl NetworkTransport for InMemoryTransport {
    async fn send_append_entry(
        &self,
        target_node: &NodeId,
        request: AppendEntryRequest,
    ) -> ConsensusResult<AppendEntryResponse> {
        // First publish to message bus for debugging/logging
        let message = self.convert_append_entry_request(target_node, &request);
        self.message_bus.publish(message.clone());

        // Then deliver directly to handler if available
        if let Some(response_message) = self.deliver_to_handler(target_node, message).await? {
            if let ConsensusMessage::AppendEntryResponse {
                from_node,
                index,
                success,
                ..
            } = response_message.clone()
            {
                // Publish the response to the bus for debugging
                // In production, this could be spawned as a task to avoid blocking,
                // but for tests we publish synchronously to ensure deterministic behavior
                self.message_bus.publish(response_message);

                return Ok(AppendEntryResponse {
                    node_id: from_node,
                    success,
                    index,
                    error: if success { None } else { Some("Handler failed".to_string()) },
                });
            }
        }

        // Fallback: simulate success if no handler available
        Ok(AppendEntryResponse {
            node_id: target_node.clone(),
            success: true,
            index: request.entry.index,
            error: None,
        })
    }

    async fn send_commit_notification(
        &self,
        target_node: &NodeId,
        request: CommitNotificationRequest,
    ) -> ConsensusResult<CommitNotificationResponse> {
        // Publish to message bus
        let message = self.convert_commit_notification_request(target_node, &request);
        self.message_bus.publish(message.clone());

        // Deliver to handler if available
        let _ = self.deliver_to_handler(target_node, message).await?;

        // Commit notifications always succeed in mock transport
        Ok(CommitNotificationResponse { success: true })
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn node_endpoints(&self) -> &HashMap<NodeId, String> {
        // For now, return a reference to an empty map
        // This is a limitation of the current sync API design
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
        // In in-memory transport, all nodes are always reachable if they have handlers
        let handlers = self.message_handlers.lock().await;
        handlers.contains_key(node_id)
    }
}

/// Registry for managing shared in-memory transports in tests
#[derive(Debug)]
pub struct InMemoryTransportRegistry {
    message_bus: Arc<ConsensusMessageBus>,
    transports: Arc<Mutex<HashMap<NodeId, Arc<InMemoryTransport>>>>,
}

impl InMemoryTransportRegistry {
    pub fn new() -> Self {
        Self {
            message_bus: Arc::new(ConsensusMessageBus::new()),
            transports: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn create_transport(&self, node_id: NodeId) -> Arc<InMemoryTransport> {
        let transport = Arc::new(InMemoryTransport::new(node_id.clone(), self.message_bus.clone()));

        let mut transports = self.transports.lock().await;
        transports.insert(node_id, transport.clone());

        transport
    }

    pub async fn get_transport(&self, node_id: &NodeId) -> Option<Arc<InMemoryTransport>> {
        let transports = self.transports.lock().await;
        transports.get(node_id).cloned()
    }

    pub async fn connect_nodes(&self, node1: &NodeId, node2: &NodeId) {
        // In in-memory transport, connection just means both nodes know about each other
        // The actual message delivery happens through the shared message bus and handlers
        // For now, we just document that nodes are connected through the registry
        println!("Connected nodes {} and {} via in-memory transport", node1, node2);
    }

    pub fn message_bus(&self) -> Arc<ConsensusMessageBus> {
        self.message_bus.clone()
    }
}

impl Default for InMemoryTransportRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use consensus_api::{LogEntry};

    // Mock message handler for testing
    #[derive(Debug)]
    struct MockMessageHandler {
        node_id: NodeId,
        should_fail: bool,
    }

    impl MockMessageHandler {
        fn new(node_id: NodeId) -> Self {
            Self {
                node_id,
                should_fail: false,
            }
        }

        fn new_failing(node_id: NodeId) -> Self {
            Self {
                node_id,
                should_fail: true,
            }
        }
    }

    #[async_trait]
    impl ConsensusMessageHandler for MockMessageHandler {
        async fn handle_append_entry(
            &self,
            request: AppendEntryRequest,
        ) -> ConsensusResult<AppendEntryResponse> {
            if self.should_fail {
                return Ok(AppendEntryResponse {
                    node_id: self.node_id.clone(),
                    success: false,
                    index: request.entry.index,
                    error: Some("Handler failed".to_string()),
                });
            }

            Ok(AppendEntryResponse {
                node_id: self.node_id.clone(),
                success: true,
                index: request.entry.index,
                error: None,
            })
        }

        async fn handle_commit_notification(
            &self,
            _request: CommitNotificationRequest,
        ) -> ConsensusResult<CommitNotificationResponse> {
            Ok(CommitNotificationResponse {
                success: !self.should_fail,
            })
        }
    }

    #[tokio::test]
    async fn test_in_memory_transport_creation() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let transport = InMemoryTransport::new("test-node".to_string(), message_bus);

        assert_eq!(transport.node_id(), "test-node");
        assert!(!transport.is_node_reachable(&"other-node".to_string()).await);
    }

    #[tokio::test]
    async fn test_send_append_entry_without_handler() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let transport = InMemoryTransport::new("leader".to_string(), message_bus.clone());

        let entry = LogEntry {
            index: 1,
            term: 1,
            data: b"test_data".to_vec(),
            timestamp: 123456789,
        };

        let request = AppendEntryRequest {
            leader_id: "leader".to_string(),
            entry,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_term: 1,
        };

        let response = transport
            .send_append_entry(&"follower".to_string(), request.clone())
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.node_id, "follower");
        assert_eq!(response.index, 1);

        // Verify message was published to bus
        let messages = message_bus.get_messages();
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages[0], ConsensusMessage::AppendEntry { .. }));
    }

    #[tokio::test]
    async fn test_send_append_entry_with_handler() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let transport = InMemoryTransport::new("leader".to_string(), message_bus.clone());

        // Register a mock handler
        let handler = Arc::new(MockMessageHandler::new("follower".to_string()));
        transport.register_message_handler("follower".to_string(), handler).await;

        assert!(transport.is_node_reachable(&"follower".to_string()).await);

        let entry = LogEntry {
            index: 1,
            term: 1,
            data: b"test_data".to_vec(),
            timestamp: 123456789,
        };

        let request = AppendEntryRequest {
            leader_id: "leader".to_string(),
            entry,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_term: 1,
        };

        let response = transport
            .send_append_entry(&"follower".to_string(), request)
            .await
            .unwrap();

        assert!(response.success);
        assert_eq!(response.node_id, "follower");
        assert_eq!(response.index, 1);

        // Should have both the request and response messages
        let messages = message_bus.get_messages();
        assert_eq!(messages.len(), 2);
        assert!(matches!(messages[0], ConsensusMessage::AppendEntry { .. }));
        assert!(matches!(messages[1], ConsensusMessage::AppendEntryResponse { .. }));
    }

    #[tokio::test]
    async fn test_send_append_entry_with_failing_handler() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let transport = InMemoryTransport::new("leader".to_string(), message_bus.clone());

        // Register a failing handler
        let handler = Arc::new(MockMessageHandler::new_failing("follower".to_string()));
        transport.register_message_handler("follower".to_string(), handler).await;

        let entry = LogEntry {
            index: 1,
            term: 1,
            data: b"test_data".to_vec(),
            timestamp: 123456789,
        };

        let request = AppendEntryRequest {
            leader_id: "leader".to_string(),
            entry,
            prev_log_index: 0,
            prev_log_term: 0,
            leader_term: 1,
        };

        let response = transport
            .send_append_entry(&"follower".to_string(), request)
            .await
            .unwrap();

        assert!(!response.success);
        assert_eq!(response.error, Some("Handler failed".to_string()));
    }

    #[tokio::test]
    async fn test_send_commit_notification() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let transport = InMemoryTransport::new("leader".to_string(), message_bus.clone());

        let request = CommitNotificationRequest {
            leader_id: "leader".to_string(),
            commit_index: 1,
            leader_term: 1,
        };

        let response = transport
            .send_commit_notification(&"follower".to_string(), request)
            .await
            .unwrap();

        assert!(response.success);

        // Verify message was published to bus
        let messages = message_bus.get_messages();
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages[0], ConsensusMessage::CommitNotification { .. }));
    }

    #[tokio::test]
    async fn test_transport_registry() {
        let registry = InMemoryTransportRegistry::new();

        let transport1 = registry.create_transport("node1".to_string()).await;
        let transport2 = registry.create_transport("node2".to_string()).await;

        assert_eq!(transport1.node_id(), "node1");
        assert_eq!(transport2.node_id(), "node2");

        // Both should share the same message bus
        let bus1 = transport1.message_bus.clone();
        let bus2 = transport2.message_bus.clone();

        bus1.publish(ConsensusMessage::EngineStarted {
            node_id: "node1".to_string(),
        });

        let messages = bus2.get_messages();
        assert_eq!(messages.len(), 1);
    }

    #[tokio::test]
    async fn test_handler_registration() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let transport = InMemoryTransport::new("node".to_string(), message_bus);

        assert!(!transport.is_node_reachable(&"handler-node".to_string()).await);

        let handler = Arc::new(MockMessageHandler::new("handler-node".to_string()));
        transport.register_message_handler("handler-node".to_string(), handler).await;

        assert!(transport.is_node_reachable(&"handler-node".to_string()).await);

        transport.unregister_message_handler(&"handler-node".to_string()).await;
        assert!(!transport.is_node_reachable(&"handler-node".to_string()).await);
    }
}