use rocksdb_server::{ConsensusKvDatabase, KvOperation, OperationResult, TransactionalKvDatabase, Config};
use kv_storage_api::KvDatabase;
use consensus_api::traits::ConsensusEngine;
use std::sync::Arc;
use tempfile::TempDir;

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
    use consensus_mock::{MockConsensusEngine, ConsensusMessageBus};
    use rocksdb_server::lib::kv_state_machine::KvStateMachine;
    use rocksdb_server::lib::replication::executor::KvStoreExecutor;
    use std::sync::Arc;

    // Create shared message bus for communication between nodes
    let message_bus = Arc::new(ConsensusMessageBus::new());

    // Create temporary databases for leader and follower
    let leader_temp_dir = TempDir::new().unwrap();
    let leader_db_path = leader_temp_dir.path().join("leader_db");
    let config = Config::default();
    let leader_database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            leader_db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    let follower_temp_dir = TempDir::new().unwrap();
    let follower_db_path = follower_temp_dir.path().join("follower_db");
    let follower_database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            follower_db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    // Create leader consensus node
    let leader_executor = Arc::new(KvStoreExecutor::new(leader_database.clone()));
    let leader_state_machine = Box::new(KvStateMachine::new(leader_executor.clone()));
    let mut leader_consensus = MockConsensusEngine::with_message_bus(
        "leader-node".to_string(),
        leader_state_machine,
        message_bus.clone()
    );

    // Create follower consensus node
    let follower_executor = Arc::new(KvStoreExecutor::new(follower_database.clone()));
    let follower_state_machine = Box::new(KvStateMachine::new(follower_executor.clone()));
    let mut follower_consensus = MockConsensusEngine::follower_with_message_bus(
        "follower-node".to_string(),
        follower_state_machine,
        message_bus.clone()
    );

    // Add follower to leader's cluster membership
    leader_consensus.add_node("follower-node".to_string(), "localhost:9091".to_string()).await.unwrap();
    follower_consensus.add_node("leader-node".to_string(), "localhost:9090".to_string()).await.unwrap();

    // Start both nodes
    leader_consensus.start().await.unwrap();
    follower_consensus.start().await.unwrap();

    // Create the operation to propose
    let operation = KvOperation::Set {
        key: b"multi_node_key".to_vec(),
        value: b"multi_node_value".to_vec(),
        column_family: None,
    };

    // Serialize the operation
    let operation_data = bincode::serialize(&operation).unwrap();

    // Leader proposes the operation
    let response = leader_consensus.propose(operation_data).await.unwrap();
    assert!(response.success, "Proposal should succeed on leader");

    // Process messages on follower (simulate message delivery)
    follower_consensus.process_messages().await.unwrap();

    // Verify leader applied the operation
    assert_eq!(leader_consensus.last_applied_index(), 1);

    // Verify follower applied the operation
    assert_eq!(follower_consensus.last_applied_index(), 1);

    // Verify the operation was actually executed on follower's database
    let get_result = follower_database.get(b"multi_node_key", None).await.unwrap();
    assert!(get_result.found, "Key should be found in follower database");
    assert_eq!(get_result.value, b"multi_node_value", "Value should match in follower database");

    // Verify the operation was also executed on leader's database
    let leader_get_result = leader_database.get(b"multi_node_key", None).await.unwrap();
    assert!(leader_get_result.found, "Key should be found in leader database");
    assert_eq!(leader_get_result.value, b"multi_node_value", "Value should match in leader database");

    // Stop both nodes
    leader_consensus.stop().await.unwrap();
    follower_consensus.stop().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn test_three_node_consensus_kv_replication() {
    use consensus_mock::{MockConsensusEngine, ConsensusMessageBus};
    use rocksdb_server::lib::kv_state_machine::KvStateMachine;
    use rocksdb_server::lib::replication::executor::KvStoreExecutor;
    use std::sync::Arc;

    // Create shared message bus for communication between all nodes
    let message_bus = Arc::new(ConsensusMessageBus::new());

    // Create temporary databases for leader and two followers
    let leader_temp_dir = TempDir::new().unwrap();
    let leader_db_path = leader_temp_dir.path().join("leader_db");

    let follower1_temp_dir = TempDir::new().unwrap();
    let follower1_db_path = follower1_temp_dir.path().join("follower1_db");

    let follower2_temp_dir = TempDir::new().unwrap();
    let follower2_db_path = follower2_temp_dir.path().join("follower2_db");

    let config = Config::default();

    // Create databases for all three nodes
    let leader_database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            leader_db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    let follower1_database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            follower1_db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    let follower2_database: Arc<dyn KvDatabase> = Arc::new(
        TransactionalKvDatabase::new(
            follower2_db_path.to_str().unwrap(),
            &config,
            &["default"]
        ).unwrap()
    );

    // Create executors and state machines for all three nodes
    let leader_executor = Arc::new(KvStoreExecutor::new(leader_database.clone()));
    let leader_state_machine = Box::new(KvStateMachine::new(leader_executor.clone()));
    let mut leader_consensus = MockConsensusEngine::with_message_bus(
        "leader-node".to_string(),
        leader_state_machine,
        message_bus.clone()
    );

    let follower1_executor = Arc::new(KvStoreExecutor::new(follower1_database.clone()));
    let follower1_state_machine = Box::new(KvStateMachine::new(follower1_executor.clone()));
    let mut follower1_consensus = MockConsensusEngine::follower_with_message_bus(
        "follower1-node".to_string(),
        follower1_state_machine,
        message_bus.clone()
    );

    let follower2_executor = Arc::new(KvStoreExecutor::new(follower2_database.clone()));
    let follower2_state_machine = Box::new(KvStateMachine::new(follower2_executor.clone()));
    let mut follower2_consensus = MockConsensusEngine::follower_with_message_bus(
        "follower2-node".to_string(),
        follower2_state_machine,
        message_bus.clone()
    );

    // Set up cluster membership - each node knows about the others
    leader_consensus.add_node("follower1-node".to_string(), "localhost:9091".to_string()).await.unwrap();
    leader_consensus.add_node("follower2-node".to_string(), "localhost:9092".to_string()).await.unwrap();

    follower1_consensus.add_node("leader-node".to_string(), "localhost:9090".to_string()).await.unwrap();
    follower1_consensus.add_node("follower2-node".to_string(), "localhost:9092".to_string()).await.unwrap();

    follower2_consensus.add_node("leader-node".to_string(), "localhost:9090".to_string()).await.unwrap();
    follower2_consensus.add_node("follower1-node".to_string(), "localhost:9091".to_string()).await.unwrap();

    // Start all three nodes
    leader_consensus.start().await.unwrap();
    follower1_consensus.start().await.unwrap();
    follower2_consensus.start().await.unwrap();

    // Clear any initial cluster setup messages
    message_bus.clear_messages();

    // Test 1: Leader sets a key-value pair
    let test_key1 = b"three_node_key1".to_vec();
    let test_value1 = b"three_node_value1".to_vec();

    let set_operation1 = KvOperation::Set {
        key: test_key1.clone(),
        value: test_value1.clone(),
        column_family: None,
    };

    // Serialize the operation for consensus
    let operation1_data = bincode::serialize(&set_operation1).unwrap();

    // Leader proposes the operation
    let set_result1 = leader_consensus.propose(operation1_data).await.unwrap();
    assert!(set_result1.success, "Set operation should succeed on leader");

    // Process messages on both followers (simulate message delivery)
    follower1_consensus.process_messages().await.unwrap();
    follower2_consensus.process_messages().await.unwrap();

    // Verify the key-value exists on all databases
    for (name, database) in [
        ("leader", &leader_database),
        ("follower1", &follower1_database),
        ("follower2", &follower2_database)
    ] {
        let get_result = database.get(&test_key1, None).await.unwrap();
        assert!(get_result.found, "Key should be found in {} database", name);
        assert_eq!(get_result.value, test_value1, "Value should match in {} database", name);
    }

    // Test 2: Leader sets another key-value pair to test ordering
    let test_key2 = b"three_node_key2".to_vec();
    let test_value2 = b"three_node_value2".to_vec();

    let set_operation2 = KvOperation::Set {
        key: test_key2.clone(),
        value: test_value2.clone(),
        column_family: None,
    };

    let operation2_data = bincode::serialize(&set_operation2).unwrap();
    let set_result2 = leader_consensus.propose(operation2_data).await.unwrap();
    assert!(set_result2.success, "Second set operation should succeed on leader");

    // Process messages on both followers again
    follower1_consensus.process_messages().await.unwrap();
    follower2_consensus.process_messages().await.unwrap();

    // Verify both keys exist on all nodes
    for (key, expected_value) in [(&test_key1, &test_value1), (&test_key2, &test_value2)] {
        for (name, database) in [
            ("leader", &leader_database),
            ("follower1", &follower1_database),
            ("follower2", &follower2_database)
        ] {
            let get_result = database.get(key, None).await.unwrap();
            assert!(get_result.found, "Key {:?} should be found in {} database", key, name);
            assert_eq!(get_result.value, *expected_value, "Value should match in {} database", name);
        }
    }

    // Test 3: Leader deletes a key
    let delete_operation = KvOperation::Delete {
        key: test_key1.clone(),
        column_family: None,
    };

    let delete_data = bincode::serialize(&delete_operation).unwrap();
    let delete_result = leader_consensus.propose(delete_data).await.unwrap();
    assert!(delete_result.success, "Delete operation should succeed on leader");

    // Process messages on both followers
    follower1_consensus.process_messages().await.unwrap();
    follower2_consensus.process_messages().await.unwrap();

    // Verify the key is deleted on all nodes
    for (name, database) in [
        ("leader", &leader_database),
        ("follower1", &follower1_database),
        ("follower2", &follower2_database)
    ] {
        let get_result = database.get(&test_key1, None).await.unwrap();
        assert!(!get_result.found, "Key should be deleted from {} database", name);
    }

    // Verify the second key still exists on all nodes
    for (name, database) in [
        ("leader", &leader_database),
        ("follower1", &follower1_database),
        ("follower2", &follower2_database)
    ] {
        let get_result = database.get(&test_key2, None).await.unwrap();
        assert!(get_result.found, "Second key should still exist on {} database", name);
        assert_eq!(get_result.value, test_value2, "Second key value should match on {} database", name);
    }

    // Verify consensus state consistency across all nodes
    assert_eq!(leader_consensus.node_id(), "leader-node");
    assert_eq!(follower1_consensus.node_id(), "follower1-node");
    assert_eq!(follower2_consensus.node_id(), "follower2-node");

    assert!(leader_consensus.is_leader(), "Leader should be marked as leader");
    assert!(!follower1_consensus.is_leader(), "Follower1 should not be marked as leader");
    assert!(!follower2_consensus.is_leader(), "Follower2 should not be marked as leader");

    // All nodes should be on the same term
    let leader_term = leader_consensus.current_term();
    let follower1_term = follower1_consensus.current_term();
    let follower2_term = follower2_consensus.current_term();
    assert_eq!(leader_term, follower1_term, "All nodes should be on the same term");
    assert_eq!(leader_term, follower2_term, "All nodes should be on the same term");

    // All nodes should have applied the same number of operations (3 total: 2 sets + 1 delete)
    let leader_applied = leader_consensus.last_applied_index();
    let follower1_applied = follower1_consensus.last_applied_index();
    let follower2_applied = follower2_consensus.last_applied_index();

    assert_eq!(leader_applied, 3, "Leader should have applied 3 operations");
    assert_eq!(follower1_applied, 3, "Follower1 should have applied 3 operations");
    assert_eq!(follower2_applied, 3, "Follower2 should have applied 3 operations");

    // Stop all nodes
    leader_consensus.stop().await.unwrap();
    follower1_consensus.stop().await.unwrap();
    follower2_consensus.stop().await.unwrap();
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