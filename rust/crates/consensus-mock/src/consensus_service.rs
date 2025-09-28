use consensus_api::{ConsensusResult, ConsensusError, ConsensusEngine, LogEntry};
use std::sync::Arc;
use parking_lot::RwLock;

use crate::types::{
    AppendEntriesRequest as ThriftAppendEntriesRequest,
    AppendEntriesResponse as ThriftAppendEntriesResponse,
};

/// Service handler for processing consensus messages from other nodes
pub struct ConsensusServiceHandler {
    consensus_engine: Option<Arc<RwLock<Box<dyn ConsensusEngine>>>>,
}

impl ConsensusServiceHandler {
    pub fn new() -> Self {
        Self {
            consensus_engine: None,
        }
    }

    pub fn with_consensus_engine(consensus_engine: Arc<RwLock<Box<dyn ConsensusEngine>>>) -> Self {
        Self {
            consensus_engine: Some(consensus_engine),
        }
    }

    /// Check if consensus engine is set (for testing)
    pub fn has_consensus_engine(&self) -> bool {
        self.consensus_engine.is_some()
    }

    /// Handle append entries request from leader
    pub async fn handle_append_entries(
        &self,
        request: ThriftAppendEntriesRequest,
    ) -> ConsensusResult<ThriftAppendEntriesResponse> {
        let consensus_engine = self.consensus_engine.as_ref().ok_or_else(|| {
            ConsensusError::Other { message: "Consensus engine not initialized".to_string() }
        })?;
        {
            let engine = consensus_engine.read();

            tracing::info!(
                "Node {}: Received append_entries from leader {}: term={}, prev_index={}, entries={}, leader_commit={}",
                engine.node_id(),
                request.leader_id,
                request.term,
                request.prev_log_index,
                request.entries.len(),
                request.leader_commit
            );

            // Downcast to concrete engine to invoke follower-side log/commit helpers
            let mock_engine = engine
                .as_any()
                .downcast_ref::<crate::mock_node::MockConsensusEngine>()
                .ok_or_else(|| ConsensusError::Other { message: "Unsupported consensus engine for Thrift handler".to_string() })?;

            // Append entries to follower log
            for e in &request.entries {
                let entry = LogEntry {
                    index: e.index as u64,
                    term: e.term as u64,
                    data: e.data.clone(),
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0),
                };
                mock_engine.append_entry_from_network(entry);
            }

            // Apply up to leader's commit index
            let leader_commit = if request.leader_commit > 0 { request.leader_commit as u64 } else { 0 };
            if leader_commit > 0 {
                if let Err(e) = mock_engine.apply_up_to_commit(leader_commit) {
                    tracing::error!("Node {}: apply_up_to_commit failed: {:?}", engine.node_id(), e);
                    return Err(e);
                }
            }

            // Compute last log index after appending
            let last_log_index = if let Some(last) = request.entries.last() {
                last.index
            } else {
                request.prev_log_index
            };

            let response = ThriftAppendEntriesResponse::new(
                request.term,
                true,
                Some(last_log_index),
                None::<String>,
            );

            tracing::info!(
                "Node {}: append_entries processed: last_index={}, leader_commit={}",
                engine.node_id(),
                last_log_index,
                request.leader_commit
            );

            Ok(response)
        }
    }
}

impl Default for ConsensusServiceHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MockConsensusEngine;
    use crate::types::LogEntry;
    use std::sync::Arc;
    use parking_lot::RwLock;

    // Simple test state machine for unit tests
    struct TestStateMachine {
        #[allow(dead_code)]
        data: std::collections::HashMap<String, Vec<u8>>,
    }

    impl TestStateMachine {
        fn new() -> Self {
            Self {
                data: std::collections::HashMap::new(),
            }
        }
    }

    impl consensus_api::StateMachine for TestStateMachine {
        fn apply(&self, _entry: &consensus_api::LogEntry) -> consensus_api::ConsensusResult<Vec<u8>> {
            Ok(b"test_result".to_vec())
        }

        fn snapshot(&self) -> consensus_api::ConsensusResult<Vec<u8>> {
            Ok(b"test_snapshot".to_vec())
        }

        fn restore_snapshot(&self, _snapshot: &[u8]) -> consensus_api::ConsensusResult<()> {
            Ok(())
        }
    }

    impl std::fmt::Debug for TestStateMachine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("TestStateMachine").finish()
        }
    }

    fn create_mock_consensus_engine() -> Arc<RwLock<Box<dyn ConsensusEngine>>> {
        // Create a simple mock consensus engine for testing
        let engine = MockConsensusEngine::new(
            "test-node".to_string(),
            Box::new(TestStateMachine::new()),
        );
        Arc::new(RwLock::new(Box::new(engine)))
    }

    #[tokio::test]
    async fn test_consensus_service_handler_creation() {
        let handler = ConsensusServiceHandler::new();
        assert!(!handler.has_consensus_engine());
    }

    #[tokio::test]
    async fn test_consensus_service_handler_with_engine() {
        let consensus_engine = create_mock_consensus_engine();
        let handler = ConsensusServiceHandler::with_consensus_engine(consensus_engine);
        assert!(handler.has_consensus_engine());
    }

    #[tokio::test]
    async fn test_handle_append_entries_without_engine() {
        let handler = ConsensusServiceHandler::new();

        let request = ThriftAppendEntriesRequest::new(1, 0, 0, 0, vec![], 0);

        let result = handler.handle_append_entries(request).await;
        assert!(result.is_err());

        match result.unwrap_err() {
            ConsensusError::Other { message } => {
                assert!(message.contains("Consensus engine not initialized"));
            }
            _ => panic!("Expected Other error"),
        }
    }

    #[tokio::test]
    async fn test_handle_append_entries_with_engine() {
        let consensus_engine = create_mock_consensus_engine();
        let handler = ConsensusServiceHandler::with_consensus_engine(consensus_engine);

        let entry = LogEntry::new(1, 10, b"test_data".to_vec(), "operation".to_string());
        let request = ThriftAppendEntriesRequest::new(
            1,  // term
            0,  // leader_id
            9,  // prev_log_index
            1,  // prev_log_term
            vec![entry],
            9,  // leader_commit
        );

        let result = handler.handle_append_entries(request).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert!(response.success);
        assert_eq!(response.last_log_index, Some(10));
    }

    #[tokio::test]
    async fn test_handle_append_entries_empty_entries() {
        let consensus_engine = create_mock_consensus_engine();
        let handler = ConsensusServiceHandler::with_consensus_engine(consensus_engine);

        let request = ThriftAppendEntriesRequest::new(1, 0, 5, 1, vec![], 5);

        let result = handler.handle_append_entries(request).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert!(response.success);
        assert_eq!(response.last_log_index, Some(5)); // prev_log_index when no entries
    }

    #[tokio::test]
    async fn test_handle_append_entries_multiple_entries() {
        let consensus_engine = create_mock_consensus_engine();
        let handler = ConsensusServiceHandler::with_consensus_engine(consensus_engine);

        let entries = vec![
            LogEntry::new(1, 10, b"data1".to_vec(), "op".to_string()),
            LogEntry::new(1, 11, b"data2".to_vec(), "op".to_string()),
            LogEntry::new(1, 12, b"data3".to_vec(), "op".to_string()),
        ];

        let request = ThriftAppendEntriesRequest::new(1, 0, 9, 1, entries, 9);

        let result = handler.handle_append_entries(request).await;
        assert!(result.is_ok());

        let response = result.unwrap();
        assert!(response.success);
        assert_eq!(response.last_log_index, Some(12)); // prev_log_index + entries.len()
    }

    #[tokio::test]
    async fn test_default_implementation() {
        let handler1 = ConsensusServiceHandler::new();
        let handler2 = ConsensusServiceHandler::default();

        // Both should have no consensus engine
        assert!(!handler1.has_consensus_engine());
        assert!(!handler2.has_consensus_engine());
    }
}
