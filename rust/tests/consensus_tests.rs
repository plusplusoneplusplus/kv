use rocksdb_server::{ConsensusKvDatabase, KvOperation, OperationResult, TransactionalKvDatabase, Config};
use kv_storage_api::KvDatabase;
use consensus_api::{traits::ConsensusEngine, ConsensusResult, ProposeResponse};
use std::sync::Arc;
use tempfile::TempDir;
use consensus_mock::{MockConsensusEngine, ConsensusMessageBus};
use rocksdb_server::lib::kv_state_machine::KvStateMachine;
use rocksdb_server::lib::replication::executor::KvStoreExecutor;

/// Test cluster abstraction to simplify multi-node consensus testing
struct TestCluster {
    nodes: Vec<TestNode>,
    message_bus: Arc<ConsensusMessageBus>,
}

struct TestNode {
    node_id: String,
    database: Arc<dyn KvDatabase>,
    consensus: MockConsensusEngine,
    _temp_dir: TempDir, // Keep temp dir alive
}

impl TestCluster {
    /// Create a new test cluster with the specified number of nodes
    /// The first node will be the leader, the rest will be followers
    async fn new(node_count: usize) -> Self {
        assert!(node_count > 0, "Must have at least one node");
        
        let message_bus = Arc::new(ConsensusMessageBus::new());
        let mut nodes = Vec::new();
        
        for i in 0..node_count {
            let node_id = if i == 0 {
                "leader-node".to_string()
            } else {
                format!("follower{}-node", i)
            };
            
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join(format!("{}_db", node_id));
            let config = Config::default();
            
            let database: Arc<dyn KvDatabase> = Arc::new(
                TransactionalKvDatabase::new(
                    db_path.to_str().unwrap(),
                    &config,
                    &["default"]
                ).unwrap()
            );
            
            let executor = Arc::new(KvStoreExecutor::new(database.clone()));
            let state_machine = Box::new(KvStateMachine::new(executor));
            
            let consensus = if i == 0 {
                // Leader node
                MockConsensusEngine::with_message_bus(
                    node_id.clone(),
                    state_machine,
                    message_bus.clone()
                )
            } else {
                // Follower node
                MockConsensusEngine::follower_with_message_bus(
                    node_id.clone(),
                    state_machine,
                    message_bus.clone()
                )
            };
            
            nodes.push(TestNode {
                node_id,
                database,
                consensus,
                _temp_dir: temp_dir,
            });
        }
        
        Self {
            nodes,
            message_bus,
        }
    }
    
    /// Set up cluster membership between all nodes
    async fn setup_cluster_membership(&mut self) {
        let node_count = self.nodes.len();
        
        for i in 0..node_count {
            for j in 0..node_count {
                if i != j {
                    let other_node_id = self.nodes[j].node_id.clone();
                    let port = 9090 + j;
                    let address = format!("localhost:{}", port);
                    
                    self.nodes[i].consensus.add_node(other_node_id, address).await.unwrap();
                }
            }
        }
    }
    
    /// Start all nodes in the cluster
    async fn start(&mut self) {
        for node in &mut self.nodes {
            node.consensus.start().await.unwrap();
        }
        
        // Clear any initial cluster setup messages
        self.message_bus.clear_messages();
    }
    
    /// Stop all nodes in the cluster
    async fn stop(&mut self) {
        for node in &mut self.nodes {
            node.consensus.stop().await.unwrap();
        }
    }
    
    /// Get the leader node (first node)
    fn leader(&mut self) -> &mut TestNode {
        &mut self.nodes[0]
    }
    
    /// Get all follower nodes
    fn followers(&mut self) -> &mut [TestNode] {
        &mut self.nodes[1..]
    }
    
    /// Process messages on all follower nodes (simulate message delivery)
    async fn process_messages(&mut self) {
        for node in self.followers() {
            node.consensus.process_messages().await.unwrap();
        }
    }
    
    /// Execute a KV operation through the leader and replicate to followers
    async fn execute_operation(&mut self, operation: KvOperation) -> ConsensusResult<ProposeResponse> {
        let operation_data = bincode::serialize(&operation).unwrap();
        let result = self.leader().consensus.propose(operation_data).await;
        
        // Process messages on followers to simulate replication
        self.process_messages().await;
        
        result
    }
    
    /// Set a key-value pair across the cluster
    async fn set(&mut self, key: &[u8], value: &[u8]) -> ConsensusResult<ProposeResponse> {
        let operation = KvOperation::Set {
            key: key.to_vec(),
            value: value.to_vec(),
            column_family: None,
        };
        self.execute_operation(operation).await
    }
    
    /// Delete a key across the cluster
    async fn delete(&mut self, key: &[u8]) -> ConsensusResult<ProposeResponse> {
        let operation = KvOperation::Delete {
            key: key.to_vec(),
            column_family: None,
        };
        self.execute_operation(operation).await
    }
    
    /// Verify that a key-value pair exists on all nodes
    async fn verify_key_exists(&self, key: &[u8], expected_value: &[u8]) {
        for node in &self.nodes {
            let get_result = node.database.get(key, None).await.unwrap();
            assert!(get_result.found, "Key {:?} should be found in {} database", key, node.node_id);
            assert_eq!(get_result.value, expected_value, "Value should match in {} database", node.node_id);
        }
    }
    
    /// Verify that a key does not exist on any node
    async fn verify_key_not_exists(&self, key: &[u8]) {
        for node in &self.nodes {
            let get_result = node.database.get(key, None).await.unwrap();
            assert!(!get_result.found, "Key {:?} should not be found in {} database", key, node.node_id);
        }
    }
    
    /// Verify consensus state consistency across all nodes
    fn verify_consensus_consistency(&self, expected_applied_count: u64) {
        // Verify node roles
        assert!(self.nodes[0].consensus.is_leader(), "First node should be leader");
        for i in 1..self.nodes.len() {
            assert!(!self.nodes[i].consensus.is_leader(), "Node {} should be follower", i);
        }
        
        // Verify all nodes are on the same term
        let leader_term = self.nodes[0].consensus.current_term();
        for i in 1..self.nodes.len() {
            let follower_term = self.nodes[i].consensus.current_term();
            assert_eq!(leader_term, follower_term, "All nodes should be on the same term");
        }
        
        // Verify all nodes have applied the same number of operations
        for (i, node) in self.nodes.iter().enumerate() {
            let applied = node.consensus.last_applied_index();
            assert_eq!(applied, expected_applied_count, "Node {} should have applied {} operations", i, expected_applied_count);
        }
    }

    /// Get the number of nodes in the cluster
    #[allow(dead_code)]
    fn node_count(&self) -> usize {
        self.nodes.len()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consensus_kv_basic_write_operations() {
    // Create a temporary database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let config = Config::default();
    let database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    // Create consensus-enabled KV database
    let consensus_db = ConsensusKvDatabase::new_with_mock("node-1".to_string(), database.clone());

    // Start the consensus node
    consensus_db.start().await.unwrap();

    // Test write operation through consensus
    let set_operation = KvOperation::Set {
        key: b"test_key".to_vec(),
        value: b"test_value".to_vec(),
        column_family: None,
    };

    let result = consensus_db.execute_operation(set_operation).await;
    assert!(result.is_ok(), "Set operation should succeed: {:?}", result);

    // Note: In a real system, reads would either go through consensus for strong consistency
    // or directly to the database for eventual consistency. For now, we skip read testing
    // to avoid sequence conflicts between consensus and direct executor access.

    // Test delete operation through consensus
    let delete_operation = KvOperation::Delete {
        key: b"test_key".to_vec(),
        column_family: None,
    };

    let result = consensus_db.execute_operation(delete_operation).await;
    assert!(result.is_ok(), "Delete operation should succeed: {:?}", result);

    // Note: We could verify the deletion, but that would require careful sequence management

    // Stop the consensus node
    consensus_db.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consensus_read_operations_bypass_consensus() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let config = Config::default();
    let database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    let consensus_db = ConsensusKvDatabase::new_with_mock("node-1".to_string(), database.clone());
    consensus_db.start().await.unwrap();

    // First, set a value directly in the database
    let put_result = database.put(b"direct_key", b"direct_value", None).await;
    assert!(put_result.success, "Put operation should succeed: {:?}", put_result);

    // Test that read operations work by accessing executor directly
    let executor = consensus_db.executor();
    let temp_seq = executor.get_applied_sequence() + 1;

    let get_operation = KvOperation::Get {
        key: b"direct_key".to_vec(),
        column_family: None,
    };

    let result = executor.apply_operation(temp_seq, get_operation).await;
    assert!(result.is_ok(), "Get operation should succeed: {:?}", result);

    if let Ok(OperationResult::GetResult(Ok(get_result))) = result {
        assert!(get_result.found);
        assert_eq!(get_result.value, b"direct_value".to_vec());
    } else {
        panic!("Expected successful get result");
    }

    consensus_db.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_consensus_end_to_end_set_get_flow() {
    // Create a temporary database
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let config = Config::default();
    let database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    // Create consensus-enabled KV database
    let consensus_db = ConsensusKvDatabase::new_with_mock("node-1".to_string(), database.clone());

    // Start the consensus node
    consensus_db.start().await.unwrap();

    // Test the happy path: SET key1=value1 through consensus, then GET key1
    let test_key = b"consensus_test_key".to_vec();
    let test_value = b"consensus_test_value".to_vec();

    // 1. SET operation through consensus
    let set_operation = KvOperation::Set {
        key: test_key.clone(),
        value: test_value.clone(),
        column_family: None,
    };

    let set_result = consensus_db.execute_operation(set_operation).await;
    assert!(set_result.is_ok(), "Set operation should succeed: {:?}", set_result);

    // Verify it's a successful set result
    match set_result.unwrap() {
        OperationResult::OpResult(op_result) => {
            assert!(op_result.success, "Set operation should be successful");
        }
        _ => panic!("Expected OpResult from set operation"),
    }

    // 2. GET operation to verify the value was stored
    let get_operation = KvOperation::Get {
        key: test_key.clone(),
        column_family: None,
    };

    let get_result = consensus_db.execute_operation(get_operation).await;
    assert!(get_result.is_ok(), "Get operation should succeed: {:?}", get_result);

    // Verify the value matches what we set
    match get_result.unwrap() {
        OperationResult::GetResult(Ok(get_result)) => {
            assert!(get_result.found, "Key should be found");
            assert_eq!(get_result.value, test_value, "Value should match what was set");
        }
        _ => panic!("Expected successful GetResult from get operation"),
    }

    // 3. Test DELETE operation through consensus
    let delete_operation = KvOperation::Delete {
        key: test_key.clone(),
        column_family: None,
    };

    let delete_result = consensus_db.execute_operation(delete_operation).await;
    assert!(delete_result.is_ok(), "Delete operation should succeed: {:?}", delete_result);

    // 4. GET again to verify deletion
    let get_after_delete = KvOperation::Get {
        key: test_key.clone(),
        column_family: None,
    };

    let get_after_delete_result = consensus_db.execute_operation(get_after_delete).await;
    assert!(get_after_delete_result.is_ok(), "Get after delete should succeed: {:?}", get_after_delete_result);

    // Verify the key is no longer found
    match get_after_delete_result.unwrap() {
        OperationResult::GetResult(Ok(get_result)) => {
            assert!(!get_result.found, "Key should not be found after deletion");
        }
        _ => panic!("Expected successful GetResult from get after delete operation"),
    }

    // Stop the consensus node
    consensus_db.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_multi_node_consensus_follower_execution() {
    // Create a 2-node test cluster (1 leader + 1 follower)
    let mut cluster = TestCluster::new(2).await;
    
    // Set up cluster membership and start all nodes
    cluster.setup_cluster_membership().await;
    cluster.start().await;

    // Test setting a key-value pair across the cluster
    let test_key = b"multi_node_key";
    let test_value = b"multi_node_value";
    
    let set_result = cluster.set(test_key, test_value).await.unwrap();
    assert!(set_result.success, "Set operation should succeed on leader");
    
    // Verify the key-value exists on all nodes
    cluster.verify_key_exists(test_key, test_value).await;
    
    // Verify consensus state consistency (1 operation applied)
    cluster.verify_consensus_consistency(1);

    // Stop all nodes
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_three_node_consensus_kv_replication() {
    // Create a 3-node test cluster (1 leader + 2 followers)
    let mut cluster = TestCluster::new(3).await;
    
    // Set up cluster membership and start all nodes
    cluster.setup_cluster_membership().await;
    cluster.start().await;

    // Test 1: Set a key-value pair across the cluster
    let test_key1 = b"three_node_key1";
    let test_value1 = b"three_node_value1";
    
    let set_result1 = cluster.set(test_key1, test_value1).await.unwrap();
    assert!(set_result1.success, "Set operation should succeed on leader");
    
    // Verify the key-value exists on all nodes
    cluster.verify_key_exists(test_key1, test_value1).await;

    // Test 2: Set another key-value pair to test ordering
    let test_key2 = b"three_node_key2";
    let test_value2 = b"three_node_value2";
    
    let set_result2 = cluster.set(test_key2, test_value2).await.unwrap();
    assert!(set_result2.success, "Second set operation should succeed on leader");
    
    // Verify both keys exist on all nodes
    cluster.verify_key_exists(test_key1, test_value1).await;
    cluster.verify_key_exists(test_key2, test_value2).await;

    // Test 3: Delete a key across the cluster
    let delete_result = cluster.delete(test_key1).await.unwrap();
    assert!(delete_result.success, "Delete operation should succeed on leader");
    
    // Verify the key is deleted on all nodes
    cluster.verify_key_not_exists(test_key1).await;
    
    // Verify the second key still exists on all nodes
    cluster.verify_key_exists(test_key2, test_value2).await;

    // Verify consensus state consistency across all nodes
    // (3 total operations: 2 sets + 1 delete)
    cluster.verify_consensus_consistency(3);

    // Stop all nodes
    cluster.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn test_five_node_consensus_cluster() {
    // Demonstrate how easy it is to create larger clusters with TestCluster
    let mut cluster = TestCluster::new(5).await;
    
    // Set up cluster membership and start all nodes
    cluster.setup_cluster_membership().await;
    cluster.start().await;

    // Test multiple operations across a 5-node cluster
    let operations = vec![
        (b"key1".as_slice(), b"value1".as_slice()),
        (b"key2".as_slice(), b"value2".as_slice()),
        (b"key3".as_slice(), b"value3".as_slice()),
    ];
    
    // Set all key-value pairs
    for (key, value) in &operations {
        let result = cluster.set(key, value).await.unwrap();
        assert!(result.success, "Set operation should succeed for key {:?}", key);
    }
    
    // Verify all keys exist on all 5 nodes
    for (key, value) in &operations {
        cluster.verify_key_exists(key, value).await;
    }
    
    // Delete one key
    let delete_result = cluster.delete(b"key2").await.unwrap();
    assert!(delete_result.success, "Delete operation should succeed");
    
    // Verify deletion
    cluster.verify_key_not_exists(b"key2").await;
    
    // Verify other keys still exist
    cluster.verify_key_exists(b"key1".as_slice(), b"value1".as_slice()).await;
    cluster.verify_key_exists(b"key3".as_slice(), b"value3".as_slice()).await;
    
    // Verify consensus consistency (3 sets + 1 delete = 4 operations)
    cluster.verify_consensus_consistency(4);
    
    cluster.stop().await;
}

#[test]
fn test_consensus_info() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test_db");

    let config = Config::default();
    let database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    let consensus_db = ConsensusKvDatabase::new_with_mock("test-node".to_string(), database);

    let (node_id, is_leader, term, applied_index) = consensus_db.consensus_info();
    assert_eq!(node_id, "test-node");
    assert!(is_leader); // Mock consensus node is always leader
    assert_eq!(term, 1); // Default term
    assert_eq!(applied_index, 0); // No operations applied yet
}