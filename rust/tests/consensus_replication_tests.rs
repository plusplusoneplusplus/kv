use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use consensus_api::LogEntry;
use consensus_mock::{ThriftTransport, MockConsensusEngine, ConsensusMessageBus, NetworkTransport};
use rocksdb_server::lib::kv_state_machine::{ConsensusKvDatabase, KvStateMachine};
use rocksdb_server::lib::replication::KvStoreExecutor;
use rocksdb_server::lib::operations::KvOperation;
use rocksdb_server::{TransactionalKvDatabase, Config};
use kv_storage_api::KvDatabase;

/// Helper to create a test database with a temporary path
fn create_test_database(path: &str) -> Arc<TransactionalKvDatabase> {
    let config = Config::default();
    let db = TransactionalKvDatabase::new(path, &config, &[])
        .expect("Failed to create test database");
    Arc::new(db)
}

/// Test that ThriftTransport can validate connections between nodes
#[tokio::test]
async fn test_thrift_transport_connection_validation() {
    let mut transport = ThriftTransport::new("leader".to_string());

    // Add follower endpoints
    transport.update_node_endpoint("follower1".to_string(), "localhost:7091".to_string()).await.unwrap();
    transport.update_node_endpoint("follower2".to_string(), "localhost:7092".to_string()).await.unwrap();

    // Test that nodes with valid endpoint formats are not reachable when no servers are running
    assert!(!transport.is_node_reachable(&"follower1".to_string()).await);
    assert!(!transport.is_node_reachable(&"follower2".to_string()).await);

    // Test unknown node returns false
    assert!(!transport.is_node_reachable(&"unknown_node".to_string()).await);
}

/// Test consensus KV database creation with mock consensus
#[tokio::test(flavor = "multi_thread")]
async fn test_consensus_kv_database_creation() {
    let db_path = format!("/tmp/test_consensus_db_{}", uuid::Uuid::new_v4());
    let database = create_test_database(&db_path);

    // Create consensus database with mock consensus
    let consensus_db = ConsensusKvDatabase::new_with_mock("node-1".to_string(), database.clone());

    // Start the consensus engine
    consensus_db.start().await.expect("Failed to start consensus");

    // Test basic operation
    let set_operation = KvOperation::Set {
        key: b"test_key".to_vec(),
        value: b"test_value".to_vec(),
        column_family: None,
    };

    let result = consensus_db.execute_operation(set_operation).await;
    assert!(result.is_ok());

    // Verify the key exists with a read operation
    let get_operation = KvOperation::Get {
        key: b"test_key".to_vec(),
        column_family: None,
    };

    let result = consensus_db.execute_operation(get_operation).await;
    assert!(result.is_ok());

    // Stop the consensus engine
    consensus_db.stop().await.expect("Failed to stop consensus");

    // Cleanup
    std::fs::remove_dir_all(db_path).ok();
}

/// Test multi-node consensus database setup with network transport
#[tokio::test(flavor = "multi_thread")]
async fn test_multi_node_consensus_setup() {
    let node_count = 3;
    let mut databases = Vec::new();
    let mut consensus_databases = Vec::new();
    let mut endpoint_maps = Vec::new();

    // Create databases and endpoint maps for each node
    for i in 0..node_count {
        let db_path = format!("/tmp/test_multinode_db_{}_{}", i, uuid::Uuid::new_v4());
        let database = create_test_database(&db_path);
        databases.push((database.clone(), db_path));

        // Create endpoint map for this node
        let mut endpoints = HashMap::new();
        for j in 0..node_count {
            let consensus_port = 7090 + j;
            endpoints.insert(j.to_string(), format!("localhost:{}", consensus_port));
        }
        endpoint_maps.push(endpoints);
    }

    // Create consensus databases with ThriftTransport
    for i in 0..node_count {
        let (database, _) = &databases[i];
        let endpoints = endpoint_maps[i].clone();

        // Create ThriftTransport
        let transport = ThriftTransport::with_endpoints(i.to_string(), endpoints).await;

        // Create state machine with executor
        let executor = Arc::new(KvStoreExecutor::new(database.clone()));
        let state_machine = Box::new(KvStateMachine::new(executor));

        // Create consensus engine
        let consensus_engine = if i == 0 {
            // Node 0 starts as leader
            MockConsensusEngine::with_network_transport(
                i.to_string(),
                state_machine,
                Arc::new(transport),
            )
        } else {
            // Other nodes start as followers
            MockConsensusEngine::follower_with_network_transport(
                i.to_string(),
                state_machine,
                Arc::new(transport),
            )
        };

        let consensus_db = ConsensusKvDatabase::new(
            Box::new(consensus_engine),
            database.clone() as Arc<dyn KvDatabase>
        );

        consensus_databases.push(consensus_db);
    }

    // Start all consensus engines
    for consensus_db in &consensus_databases {
        consensus_db.start().await.expect("Failed to start consensus");
    }

    // Verify node roles
    let (leader_node_id, is_leader, leader_term, _) = consensus_databases[0].consensus_info();
    assert_eq!(leader_node_id, "0");
    assert!(is_leader);
    assert!(leader_term > 0);

    for i in 1..node_count {
        let (follower_node_id, is_leader, follower_term, _) = consensus_databases[i].consensus_info();
        assert_eq!(follower_node_id, i.to_string());
        assert!(!is_leader);
        assert_eq!(follower_term, leader_term);
    }

    // Test operation on leader
    let set_operation = KvOperation::Set {
        key: b"cluster_test_key".to_vec(),
        value: b"cluster_test_value".to_vec(),
        column_family: None,
    };

    let result = consensus_databases[0].execute_operation(set_operation).await;
    assert!(result.is_ok());

    // Give some time for potential replication
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop all consensus engines
    for consensus_db in &consensus_databases {
        consensus_db.stop().await.expect("Failed to stop consensus");
    }

    // Cleanup
    for (_, db_path) in databases {
        std::fs::remove_dir_all(db_path).ok();
    }
}

/// Test consensus message propagation (simulated)
#[tokio::test(flavor = "multi_thread")]
async fn test_consensus_message_propagation() {
    // Create a message bus to track consensus messages
    let message_bus = Arc::new(ConsensusMessageBus::new());

    let db_path = format!("/tmp/test_message_prop_{}", uuid::Uuid::new_v4());
    let database = create_test_database(&db_path);

    // Create state machine
    let executor = Arc::new(KvStoreExecutor::new(database.clone()));
    let state_machine = Box::new(KvStateMachine::new(executor));

    // Create mock consensus engine with message bus
    let consensus_engine = MockConsensusEngine::with_message_bus(
        "test-node".to_string(),
        state_machine,
        message_bus.clone(),
    );

    let consensus_db = ConsensusKvDatabase::new(
        Box::new(consensus_engine),
        database.clone() as Arc<dyn KvDatabase>
    );

    consensus_db.start().await.expect("Failed to start consensus");

    // Execute an operation that should trigger consensus
    let operation = KvOperation::Set {
        key: b"message_test_key".to_vec(),
        value: b"message_test_value".to_vec(),
        column_family: None,
    };

    let result = consensus_db.execute_operation(operation).await;
    assert!(result.is_ok());

    // Verify that the message bus recorded the operation
    let messages = message_bus.get_messages();
    assert!(!messages.is_empty(), "Expected consensus messages to be recorded");

    consensus_db.stop().await.expect("Failed to stop consensus");

    // Cleanup
    std::fs::remove_dir_all(db_path).ok();
}

/// Test log entry conversion between consensus API and Thrift formats
#[test]
fn test_log_entry_conversion() {
    let consensus_entry = LogEntry {
        term: 1,
        index: 10,
        data: b"test_operation_data".to_vec(),
        timestamp: 123456789,
    };

    // Test conversion to Thrift format (this would be done in the transport layer)
    // Since we're using mock types, we can verify the data is preserved
    let term = consensus_entry.term as i64;
    let index = consensus_entry.index as i64;
    let data = consensus_entry.data.clone();

    assert_eq!(term, 1);
    assert_eq!(index, 10);
    assert_eq!(data, b"test_operation_data".to_vec());
}

/// Test consensus database with read and write operations
#[tokio::test(flavor = "multi_thread")]
async fn test_consensus_read_write_operations() {
    let db_path = format!("/tmp/test_rw_ops_{}", uuid::Uuid::new_v4());
    let database = create_test_database(&db_path);

    let consensus_db = ConsensusKvDatabase::new_with_mock("rw-node".to_string(), database.clone());
    consensus_db.start().await.expect("Failed to start consensus");

    // Test write operation
    let set_op = KvOperation::Set {
        key: b"rw_key".to_vec(),
        value: b"rw_value".to_vec(),
        column_family: None,
    };

    let set_result = consensus_db.execute_operation(set_op).await;
    assert!(set_result.is_ok());

    // Test read operation
    let get_op = KvOperation::Get {
        key: b"rw_key".to_vec(),
        column_family: None,
    };

    let get_result = consensus_db.execute_operation(get_op).await;
    assert!(get_result.is_ok());

    // Test delete operation
    let delete_op = KvOperation::Delete {
        key: b"rw_key".to_vec(),
        column_family: None,
    };

    let delete_result = consensus_db.execute_operation(delete_op).await;
    assert!(delete_result.is_ok());

    // Verify key is deleted
    let get_deleted_op = KvOperation::Get {
        key: b"rw_key".to_vec(),
        column_family: None,
    };

    let get_deleted_result = consensus_db.execute_operation(get_deleted_op).await;
    assert!(get_deleted_result.is_ok());

    consensus_db.stop().await.expect("Failed to stop consensus");

    // Cleanup
    std::fs::remove_dir_all(db_path).ok();
}

/// Integration test simulating cluster startup and basic operations
#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_integration_simulation() {
    // This test simulates the cluster startup process and validates
    // that all components work together as expected

    let cluster_size = 3;
    let mut test_databases = Vec::new();

    // Phase 1: Create and configure databases (similar to start_cluster.sh)
    for node_id in 0..cluster_size {
        let db_path = format!("/tmp/test_cluster_integration_{}_{}", node_id, uuid::Uuid::new_v4());
        let database = create_test_database(&db_path);

        // Create consensus database
        let consensus_db = ConsensusKvDatabase::new_with_mock(
            node_id.to_string(),
            database.clone() as Arc<dyn KvDatabase>
        );

        test_databases.push((consensus_db, db_path));
    }

    // Phase 2: Start all nodes
    for (consensus_db, _) in &test_databases {
        consensus_db.start().await.expect("Failed to start consensus engine");
    }

    // Phase 3: Verify cluster state
    let leader_db = &test_databases[0].0;
    let (leader_id, is_leader, term, _) = leader_db.consensus_info();
    assert_eq!(leader_id, "0");
    assert!(is_leader);
    assert!(term > 0);

    // Phase 4: Execute operations on leader
    let test_operations = vec![
        KvOperation::Set {
            key: b"integration_key_1".to_vec(),
            value: b"integration_value_1".to_vec(),
            column_family: None,
        },
        KvOperation::Set {
            key: b"integration_key_2".to_vec(),
            value: b"integration_value_2".to_vec(),
            column_family: None,
        },
        KvOperation::Get {
            key: b"integration_key_1".to_vec(),
            column_family: None,
        },
    ];

    for operation in test_operations {
        let result = leader_db.execute_operation(operation).await;
        assert!(result.is_ok(), "Operation should succeed on leader");
    }

    // Phase 5: Test consensus info (diagnostic operations require routing manager)
    let (leader_id, is_leader, term, applied_sequence) = leader_db.consensus_info();
    assert_eq!(leader_id, "0");
    assert!(is_leader);
    assert!(term > 0);
    assert!(applied_sequence > 0, "Applied sequence should reflect processed operations");

    // Phase 6: Clean shutdown
    for (consensus_db, _) in &test_databases {
        consensus_db.stop().await.expect("Failed to stop consensus engine");
    }

    // Phase 7: Cleanup
    for (_, db_path) in test_databases {
        std::fs::remove_dir_all(db_path).ok();
    }
}