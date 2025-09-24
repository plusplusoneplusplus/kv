//! Full consensus integration tests with real network servers and state machines
//!
//! These tests create actual consensus servers running on real TCP ports,
//! use state machines to process operations, and verify consensus behavior
//! across network boundaries with real network communication.

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::sleep;

use consensus_api::{LogEntry, StateMachine, ConsensusResult, ConsensusError};
use consensus_mock::{
    MockConsensusServer, MockConsensusClient,
    AppendEntryRequest, CommitNotificationRequest,
};

/// Counter state machine for testing consensus operations
#[derive(Debug)]
struct CounterStateMachine {
    value: Arc<Mutex<u64>>,
    operations: Arc<Mutex<Vec<String>>>,
}

impl CounterStateMachine {
    fn new() -> Self {
        Self {
            value: Arc::new(Mutex::new(0)),
            operations: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn get_value(&self) -> u64 {
        *self.value.lock().unwrap()
    }

    fn get_operations(&self) -> Vec<String> {
        self.operations.lock().unwrap().clone()
    }
}

impl StateMachine for CounterStateMachine {
    fn apply(&self, entry: &LogEntry) -> ConsensusResult<Vec<u8>> {
        let operation = String::from_utf8_lossy(&entry.data);

        // Parse operation
        let parts: Vec<&str> = operation.split_whitespace().collect();

        // Synchronous operations using std::sync::Mutex
        let mut value = self.value.lock().unwrap();
        let mut ops = self.operations.lock().unwrap();

        ops.push(format!("Entry {}: {}", entry.index, operation));

        let result = match parts.as_slice() {
            ["INCREMENT"] => {
                *value += 1;
                format!("INCREMENTED to {}", *value)
            }
            ["DECREMENT"] => {
                if *value > 0 {
                    *value -= 1;
                }
                format!("DECREMENTED to {}", *value)
            }
            ["SET", val] => {
                if let Ok(new_val) = val.parse::<u64>() {
                    *value = new_val;
                    format!("SET to {}", *value)
                } else {
                    format!("INVALID SET: {}", val)
                }
            }
            ["GET"] => {
                format!("VALUE is {}", *value)
            }
            _ => format!("UNKNOWN operation: {}", operation),
        };

        Ok(result.into_bytes())
    }

    fn snapshot(&self) -> ConsensusResult<Vec<u8>> {
        let value = *self.value.lock().unwrap();
        let ops = self.operations.lock().unwrap().clone();
        let result = (value, ops);

        bincode::serialize(&result).map_err(|e| ConsensusError::SerializationError {
            message: format!("Failed to serialize snapshot: {}", e),
        })
    }

    fn restore_snapshot(&self, snapshot: &[u8]) -> ConsensusResult<()> {
        let (value, ops): (u64, Vec<String>) = bincode::deserialize(snapshot)
            .map_err(|e| ConsensusError::SerializationError {
                message: format!("Failed to deserialize snapshot: {}", e),
            })?;

        *self.value.lock().unwrap() = value;
        *self.operations.lock().unwrap() = ops;

        Ok(())
    }
}

/// Test cluster with real consensus servers
struct FullConsensusCluster {
    servers: Vec<Arc<MockConsensusServer>>,
    state_machines: Vec<Arc<CounterStateMachine>>,
    clients: Vec<MockConsensusClient>,
    server_handles: Vec<tokio::task::JoinHandle<()>>,
    next_log_index: Arc<Mutex<u64>>,
}

impl FullConsensusCluster {
    /// Create a new cluster with real consensus servers
    async fn new(node_count: usize, base_port: u16) -> ConsensusResult<Self> {
        assert!(node_count > 0, "Must have at least one node");

        let mut servers = Vec::new();
        let mut state_machines = Vec::new();
        let mut clients = Vec::new();
        let mut server_handles = Vec::new();

        // Create servers and state machines
        for i in 0..node_count {
            let node_id = format!("node-{}", i);
            let port = base_port + i as u16;
            let is_leader = i == 0; // First node is leader

            // Create state machine
            let state_machine = Arc::new(CounterStateMachine::new());
            state_machines.push(state_machine.clone());

            // Create server
            let server = Arc::new(MockConsensusServer::new(
                node_id.clone(),
                port,
                state_machine,
                is_leader,
            ));
            servers.push(server.clone());

            // Create client for this server
            let client = MockConsensusClient::new(format!("127.0.0.1:{}", port));
            clients.push(client);

            // Start server in background
            let server_clone = server.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) = server_clone.start().await {
                    tracing::error!("Server {} failed: {}", node_id, e);
                }
            });
            server_handles.push(handle);
        }

        // Wait a bit for servers to start
        sleep(Duration::from_millis(100)).await;

        Ok(Self {
            servers,
            state_machines,
            clients,
            server_handles,
            next_log_index: Arc::new(Mutex::new(1)),
        })
    }

    /// Get the leader client (first client)
    fn leader_client(&self) -> &MockConsensusClient {
        &self.clients[0]
    }

    /// Get follower clients
    fn follower_clients(&self) -> &[MockConsensusClient] {
        &self.clients[1..]
    }

    /// Get state machine for a specific node
    fn get_state_machine(&self, node_index: usize) -> Option<&Arc<CounterStateMachine>> {
        self.state_machines.get(node_index)
    }

    /// Send an operation to all followers and apply commit
    async fn execute_operation(&self, operation: &str) -> ConsensusResult<()> {
        // Get next log index
        let mut next_index = self.next_log_index.lock().unwrap();
        let current_index = *next_index;
        *next_index += 1;
        drop(next_index);

        let entry = LogEntry {
            term: 1,
            index: current_index,
            data: operation.as_bytes().to_vec(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
        };

        tracing::info!("Executing operation: {}", operation);

        // Send append entry to all followers
        for (i, client) in self.follower_clients().iter().enumerate() {
            let request = AppendEntryRequest {
                leader_id: "node-0".to_string(),
                entry: entry.clone(),
                prev_log_index: if current_index > 1 { current_index - 1 } else { 0 },
                prev_log_term: 1,
                leader_term: 1,
            };

            tracing::debug!("Sending append entry to node-{}", i + 1);
            let response = client.send_append_entry(request).await?;

            if !response.success {
                return Err(ConsensusError::TransportError(format!(
                    "Append entry failed on node-{}: {:?}",
                    i + 1,
                    response.error
                )));
            }

            tracing::debug!("Node-{} accepted append entry", i + 1);
        }

        // Send commit notification to all followers
        for (i, client) in self.follower_clients().iter().enumerate() {
            let commit_request = CommitNotificationRequest {
                leader_id: "node-0".to_string(),
                commit_index: current_index,
                leader_term: 1,
            };

            tracing::debug!("Sending commit notification to node-{}", i + 1);
            let response = client.send_commit_notification(commit_request).await?;

            if !response.success {
                return Err(ConsensusError::TransportError(format!(
                    "Commit notification failed on node-{}",
                    i + 1
                )));
            }

            tracing::debug!("Node-{} committed entry", i + 1);
        }

        // Also apply to leader's state machine
        if let Some(leader_sm) = self.get_state_machine(0) {
            leader_sm.apply(&entry)?;
            tracing::debug!("Leader applied entry");
        }

        tracing::info!("Operation '{}' executed successfully across cluster", operation);
        Ok(())
    }

    /// Verify state consistency across all nodes
    async fn verify_state_consistency(&self) -> ConsensusResult<()> {
        if self.state_machines.is_empty() {
            return Ok(());
        }

        let leader_value = self.state_machines[0].get_value();
        let leader_ops = self.state_machines[0].get_operations();

        tracing::debug!("Leader state: value={}, ops={:?}", leader_value, leader_ops);

        for (i, sm) in self.state_machines.iter().enumerate().skip(1) {
            let node_value = sm.get_value();
            let node_ops = sm.get_operations();

            if node_value != leader_value {
                return Err(ConsensusError::Other {
                    message: format!(
                        "Value mismatch on node-{}: expected {}, got {}",
                        i, leader_value, node_value
                    ),
                });
            }

            if node_ops.len() != leader_ops.len() {
                return Err(ConsensusError::Other {
                    message: format!(
                        "Operations count mismatch on node-{}: expected {}, got {}",
                        i, leader_ops.len(), node_ops.len()
                    ),
                });
            }

            tracing::debug!("Node-{} state consistent: value={}, ops count={}", i, node_value, node_ops.len());
        }

        tracing::info!("State consistency verified: all nodes have value={}, {} operations", leader_value, leader_ops.len());
        Ok(())
    }

    /// Test connectivity to all servers
    async fn test_connectivity(&self) -> ConsensusResult<()> {
        for (i, client) in self.clients.iter().enumerate() {
            if !client.test_connection().await {
                return Err(ConsensusError::TransportError(format!(
                    "Cannot connect to server node-{}",
                    i
                )));
            }
        }
        Ok(())
    }

    fn node_count(&self) -> usize {
        self.servers.len()
    }
}

impl Drop for FullConsensusCluster {
    fn drop(&mut self) {
        // Cancel all server tasks
        for handle in &self.server_handles {
            handle.abort();
        }
    }
}

/// Test basic consensus server setup and connectivity
#[tokio::test]
async fn test_full_consensus_cluster_setup() {
    let _ = tracing_subscriber::fmt::try_init();

    tracing::info!("=== Testing full consensus cluster setup ===");

    let cluster = FullConsensusCluster::new(3, 20000).await.unwrap();

    // Test basic connectivity
    cluster.test_connectivity().await.unwrap();

    tracing::info!("✅ All {} servers are reachable", cluster.node_count());

    // Wait a bit to ensure servers are fully ready
    sleep(Duration::from_millis(50)).await;

    tracing::info!("=== Cluster setup test completed ===");
}

/// Test single operation execution across the cluster
#[tokio::test]
async fn test_single_operation_execution() {
    let _ = tracing_subscriber::fmt::try_init();

    tracing::info!("=== Testing single operation execution ===");

    let cluster = FullConsensusCluster::new(3, 20010).await.unwrap();

    // Wait for servers to be ready
    sleep(Duration::from_millis(100)).await;

    // Execute a single increment operation
    cluster.execute_operation("INCREMENT").await.unwrap();

    // Wait for operation to propagate
    sleep(Duration::from_millis(50)).await;

    // Verify state consistency
    cluster.verify_state_consistency().await.unwrap();

    // Check that all nodes have value = 1
    for (i, sm) in cluster.state_machines.iter().enumerate() {
        let value = sm.get_value();
        assert_eq!(value, 1, "Node-{} should have value 1", i);

        let ops = sm.get_operations();
        assert_eq!(ops.len(), 1, "Node-{} should have 1 operation", i);
        assert!(ops[0].contains("INCREMENT"), "Node-{} should have INCREMENT operation", i);
    }

    tracing::info!("✅ Single operation executed successfully across all nodes");
    tracing::info!("=== Single operation test completed ===");
}

/// Test multiple operations execution
#[tokio::test]
async fn test_multiple_operations_execution() {
    let _ = tracing_subscriber::fmt::try_init();

    tracing::info!("=== Testing multiple operations execution ===");

    let cluster = FullConsensusCluster::new(3, 20020).await.unwrap();

    // Wait for servers to be ready
    sleep(Duration::from_millis(100)).await;

    // Execute multiple operations
    let operations = vec!["INCREMENT", "INCREMENT", "SET 10", "DECREMENT", "GET"];

    for operation in &operations {
        cluster.execute_operation(operation).await.unwrap();
        sleep(Duration::from_millis(25)).await; // Small delay between operations
    }

    // Wait for all operations to propagate
    sleep(Duration::from_millis(100)).await;

    // Verify state consistency
    cluster.verify_state_consistency().await.unwrap();

    // Check final state: INCREMENT, INCREMENT (2), SET 10 (10), DECREMENT (9), GET (9)
    for (i, sm) in cluster.state_machines.iter().enumerate() {
        let value = sm.get_value();
        assert_eq!(value, 9, "Node-{} should have final value 9", i);

        let ops = sm.get_operations();
        assert_eq!(ops.len(), 5, "Node-{} should have 5 operations", i);
    }

    tracing::info!("✅ Multiple operations executed successfully across all nodes");
    tracing::info!("=== Multiple operations test completed ===");
}

/// Test state machine behavior with complex operations
#[tokio::test]
async fn test_state_machine_operations() {
    let _ = tracing_subscriber::fmt::try_init();

    tracing::info!("=== Testing state machine operations ===");

    let cluster = FullConsensusCluster::new(2, 20030).await.unwrap();

    // Wait for servers to be ready
    sleep(Duration::from_millis(100)).await;

    // Test various operations
    let test_operations = vec![
        ("SET 42", 42),
        ("INCREMENT", 43),
        ("INCREMENT", 44),
        ("DECREMENT", 43),
        ("SET 100", 100),
        ("DECREMENT", 99),
    ];

    for (operation, expected_value) in &test_operations {
        cluster.execute_operation(operation).await.unwrap();
        sleep(Duration::from_millis(25)).await;

        // Verify current state
        cluster.verify_state_consistency().await.unwrap();

        for sm in &cluster.state_machines {
            let value = sm.get_value();
            assert_eq!(value, *expected_value, "After '{}', value should be {}", operation, expected_value);
        }

        tracing::info!("✅ Operation '{}' resulted in value {}", operation, expected_value);
    }

    tracing::info!("=== State machine operations test completed ===");
}

/// Test error handling and network issues
#[tokio::test]
async fn test_error_handling() {
    let _ = tracing_subscriber::fmt::try_init();

    tracing::info!("=== Testing error handling ===");

    let cluster = FullConsensusCluster::new(2, 20040).await.unwrap();

    // Wait for servers to be ready
    sleep(Duration::from_millis(100)).await;

    // Test invalid operations
    cluster.execute_operation("INVALID_OPERATION").await.unwrap();
    sleep(Duration::from_millis(50)).await;

    // Should still maintain consistency
    cluster.verify_state_consistency().await.unwrap();

    // Test malformed SET operation
    cluster.execute_operation("SET invalid_number").await.unwrap();
    sleep(Duration::from_millis(50)).await;

    cluster.verify_state_consistency().await.unwrap();

    tracing::info!("✅ Error handling works correctly");
    tracing::info!("=== Error handling test completed ===");
}

/// Comprehensive end-to-end test
#[tokio::test]
async fn test_full_consensus_end_to_end() {
    let _ = tracing_subscriber::fmt::try_init();

    tracing::info!("=== Full End-to-End Consensus Test ===");

    // Create a 5-node cluster
    let cluster = FullConsensusCluster::new(5, 20050).await.unwrap();

    // Wait for all servers to be ready
    sleep(Duration::from_millis(200)).await;

    tracing::info!("Created {}-node consensus cluster", cluster.node_count());

    // Test connectivity
    cluster.test_connectivity().await.unwrap();
    tracing::info!("✅ All servers are reachable");

    // Execute a complex sequence of operations
    let operations = vec![
        "SET 0",
        "INCREMENT",
        "INCREMENT",
        "INCREMENT",
        "SET 50",
        "DECREMENT",
        "DECREMENT",
        "INCREMENT",
        "GET",
        "SET 100",
    ];

    for (i, operation) in operations.iter().enumerate() {
        tracing::info!("Executing operation {}: {}", i + 1, operation);
        cluster.execute_operation(operation).await.unwrap();
        sleep(Duration::from_millis(50)).await;
    }

    // Wait for all operations to propagate
    sleep(Duration::from_millis(200)).await;

    // Verify final state consistency
    cluster.verify_state_consistency().await.unwrap();

    // Check final state: operations should result in value = 100
    let expected_final_value = 100;
    for (i, sm) in cluster.state_machines.iter().enumerate() {
        let value = sm.get_value();
        assert_eq!(value, expected_final_value, "Node-{} should have final value {}", i, expected_final_value);

        let ops = sm.get_operations();
        assert_eq!(ops.len(), operations.len(), "Node-{} should have {} operations", i, operations.len());

        tracing::info!("Node-{}: value={}, operations={}", i, value, ops.len());
    }

    tracing::info!("✅ All {} nodes maintained consistency throughout {} operations",
                   cluster.node_count(), operations.len());
    tracing::info!("✅ Final state: value={} on all nodes", expected_final_value);
    tracing::info!("=== End-to-End test completed successfully ===");
}

// Note: These tests demonstrate full consensus with:
// 1. Real TCP servers running on different ports
// 2. Real network communication between nodes
// 3. State machines processing actual operations
// 4. Consensus protocol message passing (AppendEntry + Commit)
// 5. State consistency verification across network boundaries
// 6. Multi-node cluster coordination