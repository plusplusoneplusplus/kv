use consensus_api::{ConsensusNode, ConsensusResult, LogEntry};
use consensus_mock::{MockConsensusNode, StateMachine};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CounterCommand {
    Increment,
    Decrement,
    Set(u64),
    Get,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum CounterResponse {
    Value(u64),
    Ack,
}

#[derive(Debug)]
pub struct CounterStateMachine {
    value: Arc<AtomicU64>,
    last_applied_index: AtomicU64,
}

impl CounterStateMachine {
    pub fn new() -> Self {
        Self {
            value: Arc::new(AtomicU64::new(0)),
            last_applied_index: AtomicU64::new(0),
        }
    }

    pub fn get_value(&self) -> u64 {
        self.value.load(Ordering::SeqCst)
    }

    fn deserialize_command(&self, data: &[u8]) -> ConsensusResult<CounterCommand> {
        bincode::deserialize(data).map_err(|e| consensus_api::ConsensusError::SerializationError {
            message: format!("Failed to deserialize command: {}", e),
        })
    }

    fn serialize_response(&self, response: &CounterResponse) -> ConsensusResult<Vec<u8>> {
        bincode::serialize(response).map_err(|e| consensus_api::ConsensusError::SerializationError {
            message: format!("Failed to serialize response: {}", e),
        })
    }

    fn apply_command(&self, command: CounterCommand) -> ConsensusResult<CounterResponse> {
        let response = match command {
            CounterCommand::Increment => {
                self.value.fetch_add(1, Ordering::SeqCst);
                CounterResponse::Ack
            }
            CounterCommand::Decrement => {
                self.value.fetch_sub(1, Ordering::SeqCst);
                CounterResponse::Ack
            }
            CounterCommand::Set(value) => {
                self.value.store(value, Ordering::SeqCst);
                CounterResponse::Ack
            }
            CounterCommand::Get => {
                let value = self.value.load(Ordering::SeqCst);
                CounterResponse::Value(value)
            }
        };

        Ok(response)
    }
}

impl StateMachine for CounterStateMachine {
    fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
        let command = self.deserialize_command(&entry.data)?;
        let response = self.apply_command(command)?;
        self.serialize_response(&response)
    }

    fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
        let value = self.value.load(Ordering::SeqCst);
        let last_applied = self.last_applied_index.load(Ordering::SeqCst);

        let snapshot_data = (value, last_applied);
        bincode::serialize(&snapshot_data)
            .map_err(|e| consensus_api::ConsensusError::SerializationError {
                message: format!("Failed to serialize snapshot: {}", e),
            })
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
        let (value, last_applied): (u64, u64) = bincode::deserialize(snapshot)
            .map_err(|e| consensus_api::ConsensusError::SerializationError {
                message: format!("Failed to deserialize snapshot: {}", e),
            })?;

        self.value.store(value, Ordering::SeqCst);
        self.last_applied_index.store(last_applied, Ordering::SeqCst);

        Ok(())
    }
}

fn create_command_data(command: CounterCommand) -> Vec<u8> {
    bincode::serialize(&command).unwrap()
}

fn deserialize_response(data: &[u8]) -> CounterResponse {
    bincode::deserialize(data).unwrap()
}

#[tokio::test]
async fn test_mock_consensus_with_counter() {
    let counter_sm = CounterStateMachine::new();
    let mut node = MockConsensusNode::new("node-1".to_string(), Box::new(counter_sm));

    node.start().await.unwrap();
    assert!(node.is_leader());
    assert_eq!(node.node_id(), "node-1");

    let command_data = create_command_data(CounterCommand::Increment);
    let response = node.propose(command_data).await.unwrap();
    assert!(response.success);
    assert_eq!(response.index, Some(1));

    let command_data = create_command_data(CounterCommand::Increment);
    let response = node.propose(command_data).await.unwrap();
    assert!(response.success);
    assert_eq!(response.index, Some(2));

    let command_data = create_command_data(CounterCommand::Get);
    let response = node.propose(command_data).await.unwrap();
    assert!(response.success);
    assert_eq!(response.index, Some(3));

    let commit_data = node.wait_for_commit(3).await.unwrap();
    let response_data = node.apply(&LogEntry {
        index: 3,
        term: 1,
        data: commit_data,
        timestamp: 0,
    }).await.unwrap();
    let counter_response = deserialize_response(&response_data);
    assert_eq!(counter_response, CounterResponse::Value(2));

    node.stop().await.unwrap();
}

#[tokio::test]
async fn test_mock_consensus_not_started() {
    let counter_sm = CounterStateMachine::new();
    let node = MockConsensusNode::new("node-1".to_string(), Box::new(counter_sm));

    let command_data = create_command_data(CounterCommand::Increment);
    let response = node.propose(command_data).await.unwrap();
    assert!(!response.success);
    assert!(response.error.unwrap().contains("not started"));
}

#[tokio::test]
async fn test_mock_consensus_cluster_management() {
    let counter_sm = CounterStateMachine::new();
    let mut node = MockConsensusNode::new("node-1".to_string(), Box::new(counter_sm));

    assert_eq!(node.cluster_members(), vec!["node-1".to_string()]);

    node.add_node("node-2".to_string(), "localhost:8001".to_string()).await.unwrap();
    let members = node.cluster_members();
    assert!(members.contains(&"node-1".to_string()));
    assert!(members.contains(&"node-2".to_string()));

    node.remove_node(&"node-2".to_string()).await.unwrap();
    assert_eq!(node.cluster_members(), vec!["node-1".to_string()]);
}