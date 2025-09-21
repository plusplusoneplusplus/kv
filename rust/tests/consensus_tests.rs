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