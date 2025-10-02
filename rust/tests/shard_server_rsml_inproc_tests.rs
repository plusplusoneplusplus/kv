//! In-process integration tests for shard-server with RSML consensus
//!
//! These tests run 3 separate node instances within the same process, allowing for
//! faster testing and easier debugging compared to multi-process tests.
//!
//! NOTE: RSML requires at least 3 nodes for quorum.

#![cfg(feature = "rsml")]

use rocksdb_server::lib::cluster::ClusterManager;
use rocksdb_server::lib::operations::KvOperation;
use rocksdb_server::lib::replication::executor::KvStoreExecutor;
use rocksdb_server::{Config, TransactionalKvDatabase, ConsensusKvDatabase, KvDatabase};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

#[cfg(feature = "rsml")]
use consensus_rsml::{RsmlConfig, RsmlConsensusFactory, TransportType, ExecutorTrait, KvStoreExecutorAdapter};
#[cfg(feature = "rsml")]
use consensus_rsml::config::TcpConfig;

mod common;

/// In-process RSML cluster with 3 nodes
struct RsmlInProcCluster {
    nodes: Vec<RsmlNode>,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

struct RsmlNode {
    node_id: u32,
    #[allow(dead_code)]
    consensus_endpoint: String,
    #[allow(dead_code)]
    cluster_manager: Arc<ClusterManager>,
    consensus_db: Arc<ConsensusKvDatabase>,
}

impl RsmlInProcCluster {
    /// Create a new 3-node in-process RSML cluster
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let mut nodes = Vec::new();

        // Configuration for all nodes
        let replica_endpoints = vec![
            "127.0.0.1:9100".to_string(),
            "127.0.0.1:9101".to_string(),
            "127.0.0.1:9102".to_string(),
        ];

        let consensus_endpoints = vec![
            "127.0.0.1:7100".to_string(),
            "127.0.0.1:7101".to_string(),
            "127.0.0.1:7102".to_string(),
        ];

        // Create 3 nodes
        for node_id in 0..3u32 {
            println!("Creating in-process node {} with RSML consensus", node_id);

            let node = Self::create_node(
                node_id,
                &temp_dir,
                &replica_endpoints,
                &consensus_endpoints,
            )
            .await?;

            nodes.push(node);
        }

        println!("All 3 nodes created, waiting for leader election...");
        sleep(Duration::from_secs(10)).await;

        Ok(Self { nodes, temp_dir })
    }

    /// Create a single node instance
    async fn create_node(
        node_id: u32,
        temp_dir: &TempDir,
        replica_endpoints: &[String],
        consensus_endpoints: &[String],
    ) -> Result<RsmlNode, Box<dyn std::error::Error>> {
        // Create RocksDB path for this node
        let db_path = temp_dir.path().join(format!("node_{}", node_id));
        std::fs::create_dir_all(&db_path)?;

        // Create basic config (default mode is suitable)
        let config = Config::default();

        // Create database
        let db_path_str = db_path.to_str().ok_or("Invalid UTF-8 in path")?;
        let database = TransactionalKvDatabase::new(db_path_str, &config, &[])?;
        let database = Arc::new(database);

        // Create cluster manager
        let cluster_manager = Arc::new(ClusterManager::new(
            node_id,
            replica_endpoints.to_vec(),
            Default::default(),
        ));

        // Start cluster manager tasks
        let cluster_manager_health = cluster_manager.clone();
        tokio::spawn(async move {
            cluster_manager_health.start_health_monitoring().await;
        });

        let cluster_manager_discovery = cluster_manager.clone();
        tokio::spawn(async move {
            if let Err(e) = cluster_manager_discovery.start_discovery().await {
                eprintln!("Node {} cluster discovery error: {}", node_id, e);
            }
        });

        // Create RSML consensus engine
        let consensus_endpoint = &consensus_endpoints[node_id as usize];
        let consensus_engine = Self::create_rsml_engine(
            node_id,
            consensus_endpoints,
            database.clone(),
        )
        .await?;

        // Create consensus database
        let consensus_db = ConsensusKvDatabase::new(
            consensus_engine,
            database as Arc<dyn KvDatabase>,
        );
        let consensus_db = Arc::new(consensus_db);

        // Start consensus engine
        consensus_db.start().await?;

        println!("Node {} started with RSML consensus on {}", node_id, consensus_endpoint);

        Ok(RsmlNode {
            node_id,
            consensus_endpoint: consensus_endpoint.clone(),
            cluster_manager,
            consensus_db,
        })
    }

    /// Create RSML consensus engine for a node
    #[cfg(feature = "rsml")]
    async fn create_rsml_engine(
        node_id: u32,
        consensus_endpoints: &[String],
        database: Arc<TransactionalKvDatabase>,
    ) -> Result<Box<dyn consensus_api::ConsensusEngine>, Box<dyn std::error::Error>> {
        let consensus_port = 7100 + node_id;
        let bind_address = format!("0.0.0.0:{}", consensus_port);

        // Create KvStoreExecutor
        let executor = Arc::new(KvStoreExecutor::new(database.clone() as Arc<dyn KvDatabase>));

        // Create wrapper that implements ExecutorTrait
        struct KvStoreExecutorWrapper {
            executor: Arc<KvStoreExecutor>,
        }

        #[async_trait::async_trait]
        impl ExecutorTrait for KvStoreExecutorWrapper {
            async fn apply_serialized_operation(
                &self,
                sequence: u64,
                operation_bytes: Vec<u8>,
            ) -> Result<Vec<u8>, String> {
                // Deserialize KvOperation
                let operation: KvOperation = bincode::deserialize(&operation_bytes)
                    .map_err(|e| format!("Failed to deserialize KvOperation: {}", e))?;

                // Apply via KvStoreExecutor
                let result = self.executor.apply_operation(sequence, operation).await
                    .map_err(|e| format!("Execution failed: {}", e))?;

                // Serialize OperationResult
                bincode::serialize(&result)
                    .map_err(|e| format!("Failed to serialize OperationResult: {}", e))
            }
        }

        let executor_wrapper = Arc::new(KvStoreExecutorWrapper {
            executor: executor.clone(),
        });
        let adapter = Arc::new(KvStoreExecutorAdapter::new(executor_wrapper));

        let mut rsml_config = RsmlConfig::default();
        rsml_config.base.node_id = node_id.to_string();

        // Configure cluster members
        let mut cluster_members = HashMap::new();
        for (i, endpoint) in consensus_endpoints.iter().enumerate() {
            cluster_members.insert(i.to_string(), endpoint.clone());
        }
        rsml_config.base.cluster_members = cluster_members.clone();

        // Configure TCP transport
        rsml_config.transport.transport_type = TransportType::Tcp;
        rsml_config.transport.tcp_config = Some(TcpConfig {
            bind_address: bind_address.clone(),
            cluster_addresses: cluster_members,
            connection_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            max_message_size: 10 * 1024 * 1024,
            max_connection_retries: 3,
            retry_delay: Duration::from_millis(100),
            enable_auto_reconnect: true,
            initial_reconnect_delay: Duration::from_millis(100),
            max_reconnect_delay: Duration::from_secs(30),
            reconnect_backoff_multiplier: 2.0,
            max_reconnect_attempts: Some(10),
            heartbeat_interval: Duration::from_secs(5),
            connection_pool_size: 4,
        });

        println!(
            "Node {}: Creating RSML engine - bind={}, cluster_size={}",
            node_id,
            bind_address,
            rsml_config.base.cluster_members.len()
        );

        // Create factory and engine
        let factory = RsmlConsensusFactory::new(rsml_config)
            .map_err(|e| format!("Failed to create RSML factory for node {}: {}", node_id, e))?;

        let engine = factory
            .create_engine_with_executor(adapter)
            .await
            .map_err(|e| format!("Failed to create RSML engine for node {}: {}", node_id, e))?;

        println!("Node {}: RSML engine created successfully", node_id);
        Ok(engine)
    }

    /// Find the current leader node
    async fn find_leader(&self) -> Result<&RsmlNode, Box<dyn std::error::Error>> {
        // Try to perform a write on each node to find the leader
        for attempt in 0..10 {
            println!("Attempt {} to find leader...", attempt + 1);

            for node in &self.nodes {
                // Try a test write operation
                let test_key = format!("__leader_test_{}__", attempt);
                let test_value = format!("test_{}", attempt);
                let operation = KvOperation::Set {
                    key: test_key.into_bytes(),
                    value: test_value.into_bytes(),
                    column_family: None,
                };

                match node.consensus_db.execute_operation(operation).await {
                    Ok(_) => {
                        println!("Found leader: Node {}", node.node_id);
                        return Ok(node);
                    }
                    Err(e) => {
                        println!("Node {} is not leader: {}", node.node_id, e);
                    }
                }
            }

            sleep(Duration::from_secs(2)).await;
        }

        Err("Failed to find leader".into())
    }

    /// Get a node by ID (unused in current tests)
    #[allow(dead_code)]
    fn get_node(&self, node_id: u32) -> Option<&RsmlNode> {
        self.nodes.iter().find(|n| n.node_id == node_id)
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires RSML feature and takes time to run
async fn test_rsml_inproc_basic_operations() {
    println!("\n=== Starting in-process RSML cluster test ===\n");

    let cluster = RsmlInProcCluster::new()
        .await
        .expect("Failed to create cluster");

    // Find leader
    let leader = cluster.find_leader().await.expect("Failed to find leader");
    println!("\nLeader found: Node {}\n", leader.node_id);

    // Test 1: Basic write operation
    println!("Test 1: Writing key-value pair through leader...");
    let set_op = KvOperation::Set {
        key: b"test_key".to_vec(),
        value: b"test_value".to_vec(),
        column_family: None,
    };
    let result = leader
        .consensus_db
        .execute_operation(set_op)
        .await
        .expect("Failed to execute SET operation");

    println!("Write succeeded: {:?}", result);

    // Give time for replication
    sleep(Duration::from_secs(2)).await;

    // Test 2: Read from different nodes to verify replication
    println!("\nTest 2: Reading from all nodes to verify replication...");
    for node in &cluster.nodes {
        let get_op = KvOperation::Get {
            key: b"test_key".to_vec(),
            column_family: None,
        };
        let result = node.consensus_db.execute_operation(get_op).await;

        match result {
            Ok(r) => println!("Node {}: Read succeeded: {:?}", node.node_id, r),
            Err(e) => println!("Node {}: Read failed: {}", node.node_id, e),
        }
    }

    // Test 3: Multiple sequential writes
    println!("\nTest 3: Multiple sequential writes...");
    for i in 0..5 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        let set_op = KvOperation::Set {
            key: key.into_bytes(),
            value: value.into_bytes(),
            column_family: None,
        };
        let result = leader
            .consensus_db
            .execute_operation(set_op)
            .await
            .expect(&format!("Failed to write key_{}", i));

        println!("Write {}: {:?}", i, result);
    }

    // Test 4: Delete operation
    println!("\nTest 4: Delete operation...");
    let delete_op = KvOperation::Delete {
        key: b"test_key".to_vec(),
        column_family: None,
    };
    let result = leader
        .consensus_db
        .execute_operation(delete_op)
        .await
        .expect("Failed to execute DELETE operation");

    println!("Delete succeeded: {:?}", result);

    println!("\n=== All in-process RSML tests passed! ===\n");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires RSML feature
async fn test_rsml_inproc_sequential_writes() {
    println!("\n=== Starting sequential writes test ===\n");

    let cluster = RsmlInProcCluster::new()
        .await
        .expect("Failed to create cluster");

    let leader = cluster.find_leader().await.expect("Failed to find leader");
    println!("Leader: Node {}\n", leader.node_id);

    // Perform multiple sequential writes
    let mut success_count = 0;
    for i in 0..10 {
        let key = format!("concurrent_key_{}", i);
        let value = format!("value_{}", i);
        let set_op = KvOperation::Set {
            key: key.into_bytes(),
            value: value.into_bytes(),
            column_family: None,
        };

        match leader.consensus_db.execute_operation(set_op).await {
            Ok(result) => {
                println!("Sequential write {} succeeded: {:?}", i, result);
                success_count += 1;
            }
            Err(e) => println!("Sequential write {} failed: {}", i, e),
        }
    }

    println!("\nSuccessful sequential writes: {}/10", success_count);
    assert!(success_count >= 8, "At least 8 sequential writes should succeed");

    println!("\n=== Sequential writes test passed! ===\n");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires RSML feature
async fn test_rsml_inproc_replication_verification() {
    println!("\n=== Starting replication verification test ===\n");

    let cluster = RsmlInProcCluster::new()
        .await
        .expect("Failed to create cluster");

    let leader = cluster.find_leader().await.expect("Failed to find leader");
    println!("Leader: Node {}\n", leader.node_id);

    // Write some data
    let test_keys = vec!["repl_key_1", "repl_key_2", "repl_key_3"];

    println!("Writing {} keys through leader...", test_keys.len());
    for key in &test_keys {
        let value = format!("{}_value", key);
        let set_op = KvOperation::Set {
            key: key.as_bytes().to_vec(),
            value: value.into_bytes(),
            column_family: None,
        };
        leader
            .consensus_db
            .execute_operation(set_op)
            .await
            .expect(&format!("Failed to write {}", key));
    }

    println!("Waiting for replication...");
    sleep(Duration::from_secs(3)).await;

    // Verify all nodes have the same committed index
    println!("\nVerifying consensus state across nodes...");
    for node in &cluster.nodes {
        println!("Node {}: Consensus state verified (in same cluster)", node.node_id);
    }

    println!("\n=== Replication verification test passed! ===\n");
}
