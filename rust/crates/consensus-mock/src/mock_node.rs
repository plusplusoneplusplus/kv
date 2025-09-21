use async_trait::async_trait;
use consensus_api::{
    ConsensusResult, ConsensusEngine, StateMachine, Index, LogEntry, NodeId, ProposeResponse, Term,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConsensusMessage {
    /// Proposal submitted to consensus
    Proposal { request_id: Uuid, index: Index, term: Term, data_size: usize },
    /// Entry committed to log
    Commit { index: Index, term: Term, data_size: usize },
    /// Leadership change
    LeadershipChange { new_leader: NodeId, term: Term },
    /// Node joined cluster
    NodeJoined { node_id: NodeId, address: String },
    /// Node left cluster
    NodeLeft { node_id: NodeId },
    /// Consensus engine started
    EngineStarted { node_id: NodeId },
    /// Consensus engine stopped
    EngineStopped { node_id: NodeId },
    /// Log entry to be replicated to followers
    AppendEntry {
        target_node: NodeId,
        entry: consensus_api::LogEntry,
        leader_id: NodeId,
        prev_log_index: Index,
        prev_log_term: Term
    },
    /// Acknowledgment from follower for log entry
    AppendEntryResponse {
        from_node: NodeId,
        to_leader: NodeId,
        index: Index,
        success: bool
    },
    /// Leader instructs followers to commit up to this index
    CommitNotification {
        target_node: NodeId,
        commit_index: Index,
        leader_id: NodeId
    },
}

/// Message Bus for consensus-level communication and replication
#[derive(Debug)]
pub struct ConsensusMessageBus {
    messages: Arc<Mutex<VecDeque<ConsensusMessage>>>,
}

impl ConsensusMessageBus {
    pub fn new() -> Self {
        Self {
            messages: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub fn publish(&self, message: ConsensusMessage) {
        if let Ok(mut messages) = self.messages.lock() {
            messages.push_back(message);
            // Keep only last 100 messages to prevent unbounded growth
            if messages.len() > 100 {
                messages.pop_front();
            }
        }
    }

    pub fn get_messages(&self) -> Vec<ConsensusMessage> {
        if let Ok(messages) = self.messages.lock() {
            messages.iter().cloned().collect()
        } else {
            Vec::new()
        }
    }

    pub fn clear_messages(&self) {
        if let Ok(mut messages) = self.messages.lock() {
            messages.clear();
        }
    }

    pub fn get_messages_since(&self, since_count: usize) -> Vec<ConsensusMessage> {
        if let Ok(messages) = self.messages.lock() {
            if since_count < messages.len() {
                messages.iter().skip(since_count).cloned().collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }
}

#[derive(Debug)]
pub struct MockConsensusEngine {
    node_id: NodeId,
    term: AtomicU64,
    log: Arc<RwLock<Vec<LogEntry>>>,
    last_applied_index: AtomicU64,
    is_leader: AtomicBool,
    is_started: AtomicBool,
    cluster_members: Arc<RwLock<HashMap<NodeId, String>>>,
    state_machine: Box<dyn StateMachine>,
    message_bus: Option<Arc<ConsensusMessageBus>>,
    last_processed_message: AtomicU64,
}


impl MockConsensusEngine {
    pub fn new(node_id: NodeId, state_machine: Box<dyn StateMachine>) -> Self {
        let mut cluster_members = HashMap::new();
        cluster_members.insert(node_id.clone(), "localhost:0".to_string());

        Self {
            node_id,
            term: AtomicU64::new(1),
            log: Arc::new(RwLock::new(Vec::new())),
            last_applied_index: AtomicU64::new(0),
            is_leader: AtomicBool::new(true),
            is_started: AtomicBool::new(false),
            cluster_members: Arc::new(RwLock::new(cluster_members)),
            state_machine,
            message_bus: None,
            last_processed_message: AtomicU64::new(0),
        }
    }

    pub fn new_follower(node_id: NodeId, state_machine: Box<dyn StateMachine>) -> Self {
        let mut cluster_members = HashMap::new();
        cluster_members.insert(node_id.clone(), "localhost:0".to_string());

        Self {
            node_id,
            term: AtomicU64::new(1),
            log: Arc::new(RwLock::new(Vec::new())),
            last_applied_index: AtomicU64::new(0),
            is_leader: AtomicBool::new(false), // Start as follower
            is_started: AtomicBool::new(false),
            cluster_members: Arc::new(RwLock::new(cluster_members)),
            state_machine,
            message_bus: None,
            last_processed_message: AtomicU64::new(0),
        }
    }

    pub fn with_message_bus(node_id: NodeId, state_machine: Box<dyn StateMachine>, message_bus: Arc<ConsensusMessageBus>) -> Self {
        let mut cluster_members = HashMap::new();
        cluster_members.insert(node_id.clone(), "localhost:0".to_string());

        Self {
            node_id,
            term: AtomicU64::new(1),
            log: Arc::new(RwLock::new(Vec::new())),
            last_applied_index: AtomicU64::new(0),
            is_leader: AtomicBool::new(true),
            is_started: AtomicBool::new(false),
            cluster_members: Arc::new(RwLock::new(cluster_members)),
            state_machine,
            message_bus: Some(message_bus),
            last_processed_message: AtomicU64::new(0),
        }
    }

    pub fn follower_with_message_bus(node_id: NodeId, state_machine: Box<dyn StateMachine>, message_bus: Arc<ConsensusMessageBus>) -> Self {
        let mut cluster_members = HashMap::new();
        cluster_members.insert(node_id.clone(), "localhost:0".to_string());

        Self {
            node_id,
            term: AtomicU64::new(1),
            log: Arc::new(RwLock::new(Vec::new())),
            last_applied_index: AtomicU64::new(0),
            is_leader: AtomicBool::new(false), // Start as follower
            is_started: AtomicBool::new(false),
            cluster_members: Arc::new(RwLock::new(cluster_members)),
            state_machine,
            message_bus: Some(message_bus),
            last_processed_message: AtomicU64::new(0),
        }
    }

    pub fn message_bus(&self) -> Option<Arc<ConsensusMessageBus>> {
        self.message_bus.clone()
    }

    /// Process incoming messages from the consensus message bus (for followers)
    pub async fn process_messages(&mut self) -> ConsensusResult<()> {
        let messages_to_process = if let Some(ref bus) = self.message_bus {
            let last_processed = self.last_processed_message.load(Ordering::SeqCst) as usize;
            let messages = bus.get_messages_since(last_processed);
            let total_messages = bus.get_messages().len();

            // Update the last processed message count
            self.last_processed_message.store(total_messages as u64, Ordering::SeqCst);

            messages
        } else {
            Vec::new()
        };

        for message in &messages_to_process {
            self.handle_message(message.clone()).await?;
        }

        Ok(())
    }

    /// Handle individual consensus messages
    async fn handle_message(&mut self, message: ConsensusMessage) -> ConsensusResult<()> {
        match message {
            ConsensusMessage::AppendEntry {
                target_node,
                entry,
                leader_id,
                prev_log_index,
                prev_log_term
            } => {
                if target_node == self.node_id {
                    self.handle_append_entry(entry, leader_id, prev_log_index, prev_log_term).await?;
                }
            }
            ConsensusMessage::CommitNotification {
                target_node,
                commit_index,
                leader_id
            } => {
                if target_node == self.node_id {
                    self.handle_commit_notification(commit_index, leader_id).await?;
                }
            }
            _ => {
                // Ignore other message types for now
            }
        }
        Ok(())
    }

    /// Handle append entry from leader (follower behavior)
    async fn handle_append_entry(&mut self, entry: LogEntry, leader_id: NodeId, _prev_log_index: Index, _prev_log_term: Term) -> ConsensusResult<()> {
        if !self.is_started.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Add entry to log
        {
            let mut log = self.log.write();
            log.push(entry.clone());
        }

        // Send acknowledgment back to leader
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::AppendEntryResponse {
                from_node: self.node_id.clone(),
                to_leader: leader_id,
                index: entry.index,
                success: true,
            });
        }

        Ok(())
    }

    /// Handle commit notification from leader (follower behavior)
    async fn handle_commit_notification(&mut self, commit_index: Index, _leader_id: NodeId) -> ConsensusResult<()> {
        if !self.is_started.load(Ordering::SeqCst) {
            return Ok(());
        }

        // Apply all entries up to commit_index
        let log = self.log.read();
        let current_applied = self.last_applied_index.load(Ordering::SeqCst);

        for (i, entry) in log.iter().enumerate() {
            let entry_index = (i + 1) as Index;
            if entry_index > current_applied && entry_index <= commit_index {
                // Apply this entry to the state machine
                let _result = self.state_machine.apply(entry)?;
                self.last_applied_index.store(entry_index, Ordering::SeqCst);
            }
        }

        Ok(())
    }

    fn next_index(&self) -> Index {
        let log = self.log.read();
        log.len() as Index + 1
    }

    fn apply_pending_entries(&self) -> ConsensusResult<()> {
        let log = self.log.read();
        let current_applied = self.last_applied_index.load(Ordering::SeqCst);

        // Apply entries to the state machine
        for (i, entry) in log.iter().enumerate() {
            let entry_index = (i + 1) as Index;
            if entry_index > current_applied {
                self.state_machine.apply(entry)?;
                self.last_applied_index.store(entry_index, Ordering::SeqCst);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio;

    // Simple test state machine that tracks applied operations
    #[derive(Debug)]
    struct TestStateMachine {
        applied_entries: Arc<Mutex<Vec<LogEntry>>>,
    }

    impl TestStateMachine {
        fn new() -> Self {
            Self {
                applied_entries: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn get_applied_entries(&self) -> Vec<LogEntry> {
            self.applied_entries.lock().unwrap().clone()
        }

        fn applied_count(&self) -> usize {
            self.applied_entries.lock().unwrap().len()
        }
    }

    impl StateMachine for TestStateMachine {
        fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
            self.applied_entries.lock().unwrap().push(entry.clone());
            Ok(format!("applied_{}", entry.index).into_bytes())
        }

        fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
            Ok(b"test_snapshot".to_vec())
        }

        fn restore_snapshot(&self, _snapshot: &[u8]) -> ConsensusResult<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_mock_consensus_engine_creation() {
        let state_machine = Box::new(TestStateMachine::new());
        let engine = MockConsensusEngine::new("test-node".to_string(), state_machine);

        assert_eq!(engine.node_id(), "test-node");
        assert_eq!(engine.current_term(), 1);
        assert_eq!(engine.last_applied_index(), 0);
        assert!(engine.is_leader());
        assert!(!engine.is_started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_start_stop_engine() {
        let state_machine = Box::new(TestStateMachine::new());
        let mut engine = MockConsensusEngine::new("test-node".to_string(), state_machine);

        // Initially not started
        assert!(!engine.is_started.load(Ordering::SeqCst));

        // Start engine
        engine.start().await.unwrap();
        assert!(engine.is_started.load(Ordering::SeqCst));

        // Stop engine
        engine.stop().await.unwrap();
        assert!(!engine.is_started.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_single_node_propose_and_apply() {
        let state_machine = Box::new(TestStateMachine::new());
        let state_machine_ref = state_machine.applied_entries.clone();
        let mut engine = MockConsensusEngine::new("leader".to_string(), state_machine);

        engine.start().await.unwrap();

        // Propose an operation
        let operation_data = b"test_operation".to_vec();
        let response = engine.propose(operation_data.clone()).await.unwrap();

        assert!(response.success);
        assert_eq!(response.index, Some(1));
        assert_eq!(response.term, Some(1));
        assert_eq!(response.leader_id, Some("leader".to_string()));

        // Verify operation was applied to state machine
        assert_eq!(engine.last_applied_index(), 1);
        let applied = state_machine_ref.lock().unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].data, operation_data);
        assert_eq!(applied[0].index, 1);
    }

    #[tokio::test]
    async fn test_follower_cannot_propose() {
        let state_machine = Box::new(TestStateMachine::new());
        let mut engine = MockConsensusEngine::new_follower("follower".to_string(), state_machine);

        engine.start().await.unwrap();

        // Follower should reject proposals
        let operation_data = b"test_operation".to_vec();
        let response = engine.propose(operation_data).await.unwrap();

        assert!(!response.success);
        assert_eq!(response.error, Some("Not leader".to_string()));
        assert_eq!(response.index, None);
    }

    #[tokio::test]
    async fn test_message_bus_communication() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let state_machine = Box::new(TestStateMachine::new());
        let engine = MockConsensusEngine::with_message_bus(
            "test-node".to_string(),
            state_machine,
            message_bus.clone()
        );

        // Initially no messages
        assert_eq!(message_bus.get_messages().len(), 0);

        // Add node should publish message
        let mut engine_mut = engine;
        engine_mut.add_node("other-node".to_string(), "localhost:9091".to_string()).await.unwrap();

        let messages = message_bus.get_messages();
        assert_eq!(messages.len(), 1);
        assert!(matches!(messages[0], ConsensusMessage::NodeJoined { .. }));
    }

    #[tokio::test]
    async fn test_cluster_membership() {
        let state_machine = Box::new(TestStateMachine::new());
        let mut engine = MockConsensusEngine::new("leader".to_string(), state_machine);

        // Initially only self in cluster
        let members = engine.cluster_members();
        assert_eq!(members.len(), 1);
        assert!(members.contains(&"leader".to_string()));

        // Add another node
        engine.add_node("follower".to_string(), "localhost:9091".to_string()).await.unwrap();
        let members = engine.cluster_members();
        assert_eq!(members.len(), 2);
        assert!(members.contains(&"leader".to_string()));
        assert!(members.contains(&"follower".to_string()));

        // Remove node
        engine.remove_node(&"follower".to_string()).await.unwrap();
        let members = engine.cluster_members();
        assert_eq!(members.len(), 1);
        assert!(members.contains(&"leader".to_string()));
    }

    #[tokio::test]
    async fn test_leader_follower_replication() {
        let message_bus = Arc::new(ConsensusMessageBus::new());

        // Create leader
        let leader_state_machine = Box::new(TestStateMachine::new());
        let leader_state_ref = leader_state_machine.applied_entries.clone();
        let mut leader = MockConsensusEngine::with_message_bus(
            "leader".to_string(),
            leader_state_machine,
            message_bus.clone()
        );

        // Create follower
        let follower_state_machine = Box::new(TestStateMachine::new());
        let follower_state_ref = follower_state_machine.applied_entries.clone();
        let mut follower = MockConsensusEngine::follower_with_message_bus(
            "follower".to_string(),
            follower_state_machine,
            message_bus.clone()
        );

        // Set up cluster membership
        leader.add_node("follower".to_string(), "localhost:9091".to_string()).await.unwrap();
        follower.add_node("leader".to_string(), "localhost:9090".to_string()).await.unwrap();

        // Start both nodes
        leader.start().await.unwrap();
        follower.start().await.unwrap();

        // Clear initial messages
        message_bus.clear_messages();

        // Leader proposes operation
        let operation_data = b"replicated_operation".to_vec();
        let response = leader.propose(operation_data.clone()).await.unwrap();
        assert!(response.success);
        assert_eq!(response.index, Some(1));

        // Verify messages were sent
        let messages = message_bus.get_messages();
        assert!(messages.len() >= 3); // Should have AppendEntry, CommitNotification, and Commit messages

        // Find AppendEntry message
        let append_entry_msg = messages.iter().find(|msg| matches!(msg, ConsensusMessage::AppendEntry { .. }));
        assert!(append_entry_msg.is_some());

        // Find CommitNotification message
        let commit_msg = messages.iter().find(|msg| matches!(msg, ConsensusMessage::CommitNotification { .. }));
        assert!(commit_msg.is_some());

        // Process messages on follower
        follower.process_messages().await.unwrap();

        // Verify both nodes applied the operation
        assert_eq!(leader.last_applied_index(), 1);
        assert_eq!(follower.last_applied_index(), 1);

        // Verify state machines were called on both nodes
        let leader_applied = leader_state_ref.lock().unwrap();
        let follower_applied = follower_state_ref.lock().unwrap();

        assert_eq!(leader_applied.len(), 1);
        assert_eq!(follower_applied.len(), 1);
        assert_eq!(leader_applied[0].data, operation_data);
        assert_eq!(follower_applied[0].data, operation_data);
    }

    #[tokio::test]
    async fn test_follower_message_processing() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let state_machine = Box::new(TestStateMachine::new());
        let state_ref = state_machine.applied_entries.clone();
        let mut follower = MockConsensusEngine::follower_with_message_bus(
            "follower".to_string(),
            state_machine,
            message_bus.clone()
        );

        follower.start().await.unwrap();

        // Simulate receiving an AppendEntry message
        let log_entry = LogEntry {
            index: 1,
            term: 1,
            data: b"test_data".to_vec(),
            timestamp: 123456789,
        };

        message_bus.publish(ConsensusMessage::AppendEntry {
            target_node: "follower".to_string(),
            entry: log_entry.clone(),
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
        });

        // Process the AppendEntry message
        follower.process_messages().await.unwrap();

        // Verify entry was added to log but not yet applied
        assert_eq!(follower.last_applied_index(), 0);
        {
            let log = follower.log.read();
            assert_eq!(log.len(), 1);
            assert_eq!(log[0].data, b"test_data".to_vec());
        }

        // Simulate receiving CommitNotification
        message_bus.publish(ConsensusMessage::CommitNotification {
            target_node: "follower".to_string(),
            commit_index: 1,
            leader_id: "leader".to_string(),
        });

        // Process the CommitNotification
        follower.process_messages().await.unwrap();

        // Verify entry was applied to state machine
        assert_eq!(follower.last_applied_index(), 1);
        let applied = state_ref.lock().unwrap();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].data, b"test_data".to_vec());
    }

    #[tokio::test]
    async fn test_multiple_operations_ordering() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let leader_state_machine = Box::new(TestStateMachine::new());
        let leader_state_ref = leader_state_machine.applied_entries.clone();
        let mut leader = MockConsensusEngine::with_message_bus(
            "leader".to_string(),
            leader_state_machine,
            message_bus.clone()
        );

        let follower_state_machine = Box::new(TestStateMachine::new());
        let follower_state_ref = follower_state_machine.applied_entries.clone();
        let mut follower = MockConsensusEngine::follower_with_message_bus(
            "follower".to_string(),
            follower_state_machine,
            message_bus.clone()
        );

        // Set up cluster
        leader.add_node("follower".to_string(), "localhost:9091".to_string()).await.unwrap();
        leader.start().await.unwrap();
        follower.start().await.unwrap();

        // Clear initial messages
        message_bus.clear_messages();

        // Propose multiple operations
        let operations = vec![
            b"operation_1".to_vec(),
            b"operation_2".to_vec(),
            b"operation_3".to_vec(),
        ];

        for (i, op_data) in operations.iter().enumerate() {
            let response = leader.propose(op_data.clone()).await.unwrap();
            assert!(response.success);
            assert_eq!(response.index, Some((i + 1) as u64));
        }

        // Process all messages on follower
        follower.process_messages().await.unwrap();

        // Verify ordering is preserved on both nodes
        assert_eq!(leader.last_applied_index(), 3);
        assert_eq!(follower.last_applied_index(), 3);

        let leader_applied = leader_state_ref.lock().unwrap();
        let follower_applied = follower_state_ref.lock().unwrap();

        assert_eq!(leader_applied.len(), 3);
        assert_eq!(follower_applied.len(), 3);

        for (i, op_data) in operations.iter().enumerate() {
            assert_eq!(leader_applied[i].data, *op_data);
            assert_eq!(follower_applied[i].data, *op_data);
            assert_eq!(leader_applied[i].index, (i + 1) as u64);
            assert_eq!(follower_applied[i].index, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn test_message_bus_incremental_processing() {
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let state_machine = Box::new(TestStateMachine::new());
        let state_ref = state_machine.applied_entries.clone();
        let mut follower = MockConsensusEngine::follower_with_message_bus(
            "follower".to_string(),
            state_machine,
            message_bus.clone()
        );

        follower.start().await.unwrap();

        // Add first message
        message_bus.publish(ConsensusMessage::EngineStarted {
            node_id: "test".to_string(),
        });

        // Process messages
        follower.process_messages().await.unwrap();

        // Add second message
        let log_entry = LogEntry {
            index: 1,
            term: 1,
            data: b"test_data".to_vec(),
            timestamp: 123456789,
        };

        message_bus.publish(ConsensusMessage::AppendEntry {
            target_node: "follower".to_string(),
            entry: log_entry.clone(),
            leader_id: "leader".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
        });

        // Process messages again - should only process new messages
        follower.process_messages().await.unwrap();

        // Verify only the AppendEntry was processed (not the EngineStarted again)
        {
            let log = follower.log.read();
            assert_eq!(log.len(), 1);
            assert_eq!(log[0].data, b"test_data".to_vec());
        }

        // State machine should not have been called yet (no commit)
        assert_eq!(state_ref.lock().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_wait_for_commit() {
        let state_machine = Box::new(TestStateMachine::new());
        let mut engine = MockConsensusEngine::new("leader".to_string(), state_machine);

        engine.start().await.unwrap();

        // Propose an operation
        let operation_data = b"test_operation".to_vec();
        let response = engine.propose(operation_data.clone()).await.unwrap();
        assert!(response.success);
        let index = response.index.unwrap();

        // Wait for commit should return the operation data (in mock consensus implementation)
        let committed_data = engine.wait_for_commit(index).await.unwrap();
        assert_eq!(committed_data, operation_data);
    }

    #[tokio::test]
    async fn test_proposal_when_not_started() {
        let state_machine = Box::new(TestStateMachine::new());
        let engine = MockConsensusEngine::new("leader".to_string(), state_machine);

        // Engine not started - proposal should fail
        let operation_data = b"test_operation".to_vec();
        let response = engine.propose(operation_data).await.unwrap();

        assert!(!response.success);
        assert_eq!(response.error, Some("Node not started".to_string()));
        assert_eq!(response.index, None);
    }

    // Tests for ConsensusMessageBus
    mod message_bus_tests {
        use super::*;

        #[test]
        fn test_message_bus_creation() {
            let bus = ConsensusMessageBus::new();
            assert_eq!(bus.get_messages().len(), 0);
        }

        #[test]
        fn test_publish_and_get_messages() {
            let bus = ConsensusMessageBus::new();

            // Publish a message
            let msg = ConsensusMessage::EngineStarted {
                node_id: "test-node".to_string(),
            };
            bus.publish(msg.clone());

            // Verify message was stored
            let messages = bus.get_messages();
            assert_eq!(messages.len(), 1);
            assert_eq!(messages[0], msg);
        }

        #[test]
        fn test_multiple_messages() {
            let bus = ConsensusMessageBus::new();

            // Publish multiple messages
            let messages = vec![
                ConsensusMessage::EngineStarted {
                    node_id: "node1".to_string(),
                },
                ConsensusMessage::NodeJoined {
                    node_id: "node2".to_string(),
                    address: "localhost:9091".to_string(),
                },
                ConsensusMessage::LeadershipChange {
                    new_leader: "node1".to_string(),
                    term: 2,
                },
            ];

            for msg in &messages {
                bus.publish(msg.clone());
            }

            // Verify all messages were stored in order
            let stored_messages = bus.get_messages();
            assert_eq!(stored_messages.len(), 3);
            for (i, expected_msg) in messages.iter().enumerate() {
                assert_eq!(stored_messages[i], *expected_msg);
            }
        }

        #[test]
        fn test_get_messages_since() {
            let bus = ConsensusMessageBus::new();

            // Publish some messages
            for i in 0..5 {
                bus.publish(ConsensusMessage::EngineStarted {
                    node_id: format!("node{}", i),
                });
            }

            // Get messages since index 2
            let recent_messages = bus.get_messages_since(2);
            assert_eq!(recent_messages.len(), 3);

            // Verify the correct messages were returned
            for (i, msg) in recent_messages.iter().enumerate() {
                if let ConsensusMessage::EngineStarted { node_id } = msg {
                    assert_eq!(*node_id, format!("node{}", i + 2));
                } else {
                    panic!("Unexpected message type");
                }
            }
        }

        #[test]
        fn test_get_messages_since_out_of_bounds() {
            let bus = ConsensusMessageBus::new();

            // Publish 3 messages
            for i in 0..3 {
                bus.publish(ConsensusMessage::EngineStarted {
                    node_id: format!("node{}", i),
                });
            }

            // Request messages since index 5 (out of bounds)
            let messages = bus.get_messages_since(5);
            assert_eq!(messages.len(), 0);

            // Request messages since index 3 (equal to length)
            let messages = bus.get_messages_since(3);
            assert_eq!(messages.len(), 0);
        }

        #[test]
        fn test_clear_messages() {
            let bus = ConsensusMessageBus::new();

            // Publish some messages
            for i in 0..3 {
                bus.publish(ConsensusMessage::EngineStarted {
                    node_id: format!("node{}", i),
                });
            }

            assert_eq!(bus.get_messages().len(), 3);

            // Clear messages
            bus.clear_messages();
            assert_eq!(bus.get_messages().len(), 0);
        }

        #[test]
        fn test_message_bus_capacity_limit() {
            let bus = ConsensusMessageBus::new();

            // Publish more than 100 messages (the limit)
            for i in 0..150 {
                bus.publish(ConsensusMessage::EngineStarted {
                    node_id: format!("node{}", i),
                });
            }

            // Should only keep the last 100 messages
            let messages = bus.get_messages();
            assert_eq!(messages.len(), 100);

            // Verify the messages are the most recent ones (50-149)
            for (i, msg) in messages.iter().enumerate() {
                if let ConsensusMessage::EngineStarted { node_id } = msg {
                    assert_eq!(*node_id, format!("node{}", i + 50));
                } else {
                    panic!("Unexpected message type");
                }
            }
        }

        #[test]
        fn test_concurrent_access() {
            use std::thread;
            use std::sync::Arc;

            let bus = Arc::new(ConsensusMessageBus::new());
            let mut handles = vec![];

            // Spawn multiple threads to publish messages
            for thread_id in 0..5 {
                let bus_clone = Arc::clone(&bus);
                let handle = thread::spawn(move || {
                    for i in 0..10 {
                        bus_clone.publish(ConsensusMessage::EngineStarted {
                            node_id: format!("thread{}_msg{}", thread_id, i),
                        });
                    }
                });
                handles.push(handle);
            }

            // Wait for all threads to complete
            for handle in handles {
                handle.join().unwrap();
            }

            // Verify we got all 50 messages
            let messages = bus.get_messages();
            assert_eq!(messages.len(), 50);
        }
    }
}

#[async_trait]
impl ConsensusEngine for MockConsensusEngine {
    // === Proposal and State Management ===

    async fn propose(&self, data: Vec<u8>) -> ConsensusResult<ProposeResponse> {
        if !self.is_started.load(Ordering::SeqCst) {
            return Ok(ProposeResponse {
                request_id: Uuid::new_v4(),
                success: false,
                index: None,
                term: None,
                error: Some("Node not started".to_string()),
                leader_id: None,
            });
        }

        if !self.is_leader() {
            return Ok(ProposeResponse {
                request_id: Uuid::new_v4(),
                success: false,
                index: None,
                term: Some(self.current_term()),
                error: Some("Not leader".to_string()),
                leader_id: None,
            });
        }

        // Accept the proposal (mock consensus accepts all proposals)
        let index = self.next_index();
        let term = self.current_term();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let request_id = Uuid::new_v4();
        let data_size = data.len();

        // Publish proposal message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::Proposal {
                request_id,
                index,
                term,
                data_size,
            });
        }

        let entry = LogEntry {
            index,
            term,
            data,
            timestamp,
        };

        {
            let mut log = self.log.write();
            log.push(entry.clone());
        }

        // Replicate entry to all followers first
        if let Some(ref bus) = self.message_bus {
            let cluster_members = self.cluster_members();
            for member_id in cluster_members {
                if member_id != self.node_id {
                    bus.publish(ConsensusMessage::AppendEntry {
                        target_node: member_id,
                        entry: entry.clone(),
                        leader_id: self.node_id.clone(),
                        prev_log_index: index.saturating_sub(1),
                        prev_log_term: term,
                    });
                }
            }
        }

        // For mock consensus, we simulate immediate replication success
        // In real consensus, we'd wait for majority acknowledgment here

        // Apply to leader's state machine
        self.apply_pending_entries()?;

        // Notify followers to commit
        if let Some(ref bus) = self.message_bus {
            let cluster_members = self.cluster_members();
            for member_id in cluster_members {
                if member_id != self.node_id {
                    bus.publish(ConsensusMessage::CommitNotification {
                        target_node: member_id,
                        commit_index: index,
                        leader_id: self.node_id.clone(),
                    });
                }
            }

            // Publish commit message to bus for logging
            bus.publish(ConsensusMessage::Commit {
                index,
                term,
                data_size,
            });
        }

        Ok(ProposeResponse {
            request_id,
            success: true,
            index: Some(index),
            term: Some(term),
            error: None,
            leader_id: Some(self.node_id.clone()),
        })
    }


    // === Consensus Protocol Management ===

    async fn start(&mut self) -> ConsensusResult<()> {
        self.is_started.store(true, Ordering::SeqCst);
        tracing::info!("Mock consensus engine '{}' started", self.node_id);

        // Publish engine started message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::EngineStarted {
                node_id: self.node_id.clone(),
            });
        }

        Ok(())
    }

    async fn stop(&mut self) -> ConsensusResult<()> {
        self.is_started.store(false, Ordering::SeqCst);
        tracing::info!("Mock consensus engine '{}' stopped", self.node_id);

        // Publish engine stopped message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::EngineStopped {
                node_id: self.node_id.clone(),
            });
        }

        Ok(())
    }

    async fn wait_for_commit(&self, index: Index) -> ConsensusResult<Vec<u8>> {
        let log = self.log.read();
        if let Some(entry) = log.get((index - 1) as usize) {
            Ok(entry.data.clone())
        } else {
            Err(consensus_api::ConsensusError::Other {
                message: format!("Log entry at index {} not found", index),
            })
        }
    }

    // === Node State and Identity ===

    fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    fn current_term(&self) -> Term {
        self.term.load(Ordering::SeqCst)
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn last_applied_index(&self) -> Index {
        self.last_applied_index.load(Ordering::SeqCst)
    }

    fn set_last_applied_index(&self, index: Index) {
        self.last_applied_index.store(index, Ordering::SeqCst);
    }

    // === Cluster Management ===

    fn cluster_members(&self) -> Vec<NodeId> {
        self.cluster_members.read().keys().cloned().collect()
    }

    async fn add_node(&mut self, node_id: NodeId, address: String) -> ConsensusResult<()> {
        let mut members = self.cluster_members.write();
        members.insert(node_id.clone(), address.clone());
        tracing::info!("Added node '{}' to cluster", node_id);

        // Publish node joined message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::NodeJoined {
                node_id: node_id.clone(),
                address,
            });
        }

        Ok(())
    }

    async fn remove_node(&mut self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut members = self.cluster_members.write();
        members.remove(node_id);
        tracing::info!("Removed node '{}' from cluster", node_id);

        // Publish node left message to bus
        if let Some(ref bus) = self.message_bus {
            bus.publish(ConsensusMessage::NodeLeft {
                node_id: node_id.clone(),
            });
        }

        Ok(())
    }
}