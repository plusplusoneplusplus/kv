//! Integration tests for shard-server with RSML consensus
//!
//! These tests verify that the shard-server works correctly when configured with RSML consensus
//! instead of the default mock consensus implementation.
//!
//! NOTE: RSML requires at least 3 nodes for quorum, so these tests run a 3-node cluster.

#![cfg(feature = "rsml")]

use rocksdb_server::client::KvStoreClient;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

mod common;

/// Test harness for running a 3-node shard-server cluster with RSML consensus
struct RsmlClusterTest {
    nodes: Vec<RsmlNode>,
    #[allow(dead_code)] // Keeps temp directory alive
    temp_dir: TempDir,
    client: Option<KvStoreClient>,
}

struct RsmlNode {
    node_id: u32,
    process: Option<Child>,
    endpoint: String,
    consensus_endpoint: String,
    config_path: PathBuf,
    #[allow(dead_code)] // Useful for debugging
    data_path: PathBuf,
}

impl RsmlClusterTest {
    /// Create a new 3-node RSML cluster test instance
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let mut nodes = Vec::new();

        // Create 3 nodes with sequential ports
        for node_id in 0..3u32 {
            let node_port = 9100 + node_id as u16;
            let consensus_port = 7100 + node_id as u16;
            let data_path = temp_dir.path().join(format!("node_{}", node_id));
            let config_path = temp_dir.path().join(format!("config_{}.toml", node_id));

            fs::create_dir_all(&data_path)?;

            // Create configuration file for this node
            let config_content = Self::create_config(node_id, node_port, consensus_port, &data_path)?;

            let mut file = fs::File::create(&config_path)?;
            file.write_all(config_content.as_bytes())?;
            file.flush()?;

            nodes.push(RsmlNode {
                node_id,
                process: None,
                endpoint: format!("127.0.0.1:{}", node_port),
                consensus_endpoint: format!("127.0.0.1:{}", consensus_port),
                config_path,
                data_path,
            });
        }

        Ok(Self {
            nodes,
            temp_dir,
            client: None,
        })
    }

    /// Create configuration content for a node
    fn create_config(
        node_id: u32,
        _node_port: u16,
        _consensus_port: u16,
        data_path: &PathBuf,
    ) -> Result<String, Box<dyn std::error::Error>> {
        Ok(format!(
            r#"# Configuration for node {} in 3-node RSML cluster

[database]
base_path = "{}"

[rocksdb]
write_buffer_size_mb = 32
max_write_buffer_number = 3
block_cache_size_mb = 64
block_size_kb = 4
max_background_jobs = 6
bytes_per_sync = 0
dynamic_level_bytes = true

[bloom_filter]
enabled = true
bits_per_key = 10

[compression]
l0_compression = "lz4"
l1_compression = "lz4"
bottom_compression = "zstd"

[concurrency]
max_read_concurrency = 32

[compaction]
compaction_priority = "min_overlapping_ratio"
target_file_size_base_mb = 64
target_file_size_multiplier = 2
max_bytes_for_level_base_mb = 256
max_bytes_for_level_multiplier = 10

[cache]
cache_index_and_filter_blocks = true
pin_l0_filter_and_index_blocks_in_cache = true
high_priority_pool_ratio = 0.2

[memory]
write_buffer_manager_limit_mb = 256
enable_write_buffer_manager = true

[logging]
log_level = "info"
max_log_file_size_mb = 10
keep_log_file_num = 5

[performance]
statistics_level = "except_detailed_timers"
enable_statistics = false
stats_dump_period_sec = 600

[deployment]
mode = "replicated"
instance_id = {}
replica_endpoints = ["127.0.0.1:9100", "127.0.0.1:9101", "127.0.0.1:9102"]

[consensus]
algorithm = "rsml"
election_timeout_ms = 5000
heartbeat_interval_ms = 1000
max_batch_size = 100
max_outstanding_proposals = 1000
endpoints = ["127.0.0.1:7100", "127.0.0.1:7101", "127.0.0.1:7102"]

[reads]
consistency_level = "strong"
allow_stale_reads = false
max_staleness_ms = 1000

[cluster]
health_check_interval_ms = 1000
leader_discovery_timeout_ms = 5000
node_timeout_ms = 10000
"#,
            node_id,
            data_path.to_string_lossy(),
            node_id
        ))
    }

    /// Start all nodes in the cluster
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting 3-node RSML cluster...");

        // Get the shard-server binary path from test configuration
        let binary = common::test_config::shard_server_binary();

        // Validate the binary exists
        if let Err(e) = common::test_config::TestConfig::global().validate_shard_server() {
            return Err(format!("Shard server binary validation failed: {}", e).into());
        }

        // Start all nodes
        for node in &mut self.nodes {
            println!(
                "Starting node {} on {} (consensus: {})",
                node.node_id, node.endpoint, node.consensus_endpoint
            );

            let process = Command::new(&binary)
                .arg("--config")
                .arg(&node.config_path)
                .arg("--node-id")
                .arg(&node.node_id.to_string())
                .arg("--port")
                .arg(&(9100 + node.node_id).to_string())
                .arg("--use-rsml")
                .env("RUST_LOG", "info")
                .spawn()?;

            node.process = Some(process);
        }

        // Wait for cluster to be ready
        self.wait_for_ready().await?;

        // Find and connect to the leader
        let client = self.find_leader().await?;
        self.client = Some(client);

        println!("3-node RSML cluster started successfully");
        Ok(())
    }

    /// Wait for the cluster to be ready to accept connections
    async fn wait_for_ready(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Waiting for cluster to be ready...");

        // Wait for all nodes to start accepting connections
        for node in &self.nodes {
            let mut retries = 30; // 30 seconds timeout per node
            while retries > 0 {
                sleep(Duration::from_secs(1)).await;

                match KvStoreClient::connect(&node.endpoint) {
                    Ok(_) => {
                        println!("Node {} is ready at {}", node.node_id, node.endpoint);
                        break;
                    }
                    Err(_) => {
                        if retries == 1 {
                            return Err(format!(
                                "Node {} failed to start within timeout on {}",
                                node.node_id, node.endpoint
                            )
                            .into());
                        }
                        retries -= 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Find and connect to the current RSML leader
    async fn find_leader(&self) -> Result<KvStoreClient, Box<dyn std::error::Error>> {
        println!("Waiting for RSML leader election to stabilize...");

        // RSML needs time for:
        // 1. Initial leader election
        // 2. View changes to stabilize
        // 3. Leader to be ready to accept requests
        sleep(Duration::from_secs(12)).await;

        // Try each node until we find one that can handle write requests
        for attempt in 0..5 {
            println!("Attempt {} to find leader...", attempt + 1);

            for node in &self.nodes {
                println!("Trying node {} at {}...", node.node_id, node.endpoint);

                let client = match KvStoreClient::connect(&node.endpoint) {
                    Ok(c) => c,
                    Err(e) => {
                        println!("Failed to connect to node {}: {}", node.node_id, e);
                        continue;
                    }
                };

                // Try a test write
                let tx_future = client.begin_transaction(None, Some(30));
                let mut tx = match tx_future.await_result().await {
                    Ok(t) => t,
                    Err(e) => {
                        println!("Failed to begin transaction on node {}: {}", node.node_id, e);
                        continue;
                    }
                };

                if tx.set(b"__leader_test__", b"test", None).is_err() {
                    println!("Failed to set key on node {}", node.node_id);
                    continue;
                }

                let commit_future = tx.commit();
                match commit_future.await_result().await {
                    Ok(_) => {
                        println!("Successfully found leader at node {} ({})", node.node_id, node.endpoint);
                        return Ok(client);
                    }
                    Err(e) => {
                        println!("Failed to commit on node {}: {}", node.node_id, e);
                    }
                }
            }

            println!("No leader found in this round, waiting before retry...");
            sleep(Duration::from_secs(3)).await;
        }

        Err("Failed to find cluster leader after multiple attempts".into())
    }

    /// Stop all nodes in the cluster
    async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Stopping RSML cluster...");

        self.client = None;

        for node in &mut self.nodes {
            if let Some(mut process) = node.process.take() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }

        println!("RSML cluster stopped");
        Ok(())
    }

    /// Get the client for this cluster
    fn client(&self) -> Result<&KvStoreClient, Box<dyn std::error::Error>> {
        self.client
            .as_ref()
            .ok_or_else(|| "Cluster not started".into())
    }
}

impl Drop for RsmlClusterTest {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut process) = node.process.take() {
                let _ = process.kill();
                let _ = process.wait();
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires RSML feature to be enabled and takes longer to run
async fn test_rsml_consensus_basic_operations() {
    let mut cluster = RsmlClusterTest::new()
        .await
        .expect("Failed to create test cluster");

    cluster.start().await.expect("Failed to start cluster");

    let client = cluster.client().expect("Failed to get client");

    // Test basic SET operation
    let tx_future = client.begin_transaction(None, Some(60));
    let mut tx = tx_future
        .await_result()
        .await
        .expect("Failed to begin transaction");

    tx.set(b"rsml_test_key", b"rsml_test_value", None)
        .expect("Failed to set key");

    let commit_future = tx.commit();
    commit_future
        .await_result()
        .await
        .expect("Failed to commit transaction");

    // Test GET operation
    let read_tx_future = client.begin_read_transaction(None);
    let read_tx = read_tx_future
        .await_result()
        .await
        .expect("Failed to begin read transaction");

    let get_future = read_tx.snapshot_get(b"rsml_test_key", None);
    let value = get_future
        .await_result()
        .await
        .expect("Failed to get value");

    assert!(value.is_some(), "Key should be found");
    assert_eq!(
        value.unwrap(),
        b"rsml_test_value",
        "Value should match what was set"
    );

    cluster.stop().await.expect("Failed to stop cluster");

    println!("RSML consensus basic operations test passed");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires RSML feature to be enabled and takes longer to run
async fn test_rsml_consensus_multiple_operations() {
    let mut cluster = RsmlClusterTest::new()
        .await
        .expect("Failed to create test cluster");

    cluster.start().await.expect("Failed to start cluster");

    let client = cluster.client().expect("Failed to get client");

    // Test multiple operations in sequence
    let test_data = vec![
        ("rsml_key_1", "rsml_value_1"),
        ("rsml_key_2", "rsml_value_2"),
        ("rsml_key_3", "rsml_value_3"),
    ];

    for (key, value) in &test_data {
        let tx_future = client.begin_transaction(None, Some(60));
        let mut tx = tx_future
            .await_result()
            .await
            .expect("Failed to begin transaction");

        tx.set(key.as_bytes(), value.as_bytes(), None)
            .expect("Failed to set key");

        let commit_future = tx.commit();
        commit_future
            .await_result()
            .await
            .expect("Failed to commit transaction");
    }

    // Verify all keys were set
    let read_tx_future = client.begin_read_transaction(None);
    let read_tx = read_tx_future
        .await_result()
        .await
        .expect("Failed to begin read transaction");

    for (key, expected_value) in &test_data {
        let get_future = read_tx.snapshot_get(key.as_bytes(), None);
        let value = get_future
            .await_result()
            .await
            .expect("Failed to get value");

        assert!(value.is_some(), "Key {} should be found", key);
        assert_eq!(
            value.unwrap(),
            expected_value.as_bytes(),
            "Value should match for key {}",
            key
        );
    }

    // Test delete operation
    let tx_future = client.begin_transaction(None, Some(60));
    let mut tx = tx_future
        .await_result()
        .await
        .expect("Failed to begin transaction");

    tx.delete(b"rsml_key_2", None)
        .expect("Failed to delete key");

    let commit_future = tx.commit();
    commit_future
        .await_result()
        .await
        .expect("Failed to commit delete transaction");

    // Verify deletion
    let read_tx_future = client.begin_read_transaction(None);
    let read_tx = read_tx_future
        .await_result()
        .await
        .expect("Failed to begin read transaction");

    let get_future = read_tx.snapshot_get(b"rsml_key_2", None);
    let value = get_future
        .await_result()
        .await
        .expect("Failed to get deleted key");

    assert!(value.is_none(), "Deleted key should not be found");

    cluster.stop().await.expect("Failed to stop cluster");

    println!("RSML consensus multiple operations test passed");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore] // Requires RSML feature to be enabled and takes longer to run
async fn test_rsml_consensus_cluster_replication() {
    let mut cluster = RsmlClusterTest::new()
        .await
        .expect("Failed to create test cluster");

    cluster.start().await.expect("Failed to start cluster");

    let client = cluster.client().expect("Failed to get client");

    // Write data through the leader
    let tx_future = client.begin_transaction(None, Some(60));
    let mut tx = tx_future
        .await_result()
        .await
        .expect("Failed to begin transaction");

    tx.set(b"replication_key", b"replication_value", None)
        .expect("Failed to set key");

    let commit_future = tx.commit();
    commit_future
        .await_result()
        .await
        .expect("Failed to commit transaction");

    // Give time for replication
    sleep(Duration::from_secs(2)).await;

    // Verify data is accessible (implicitly checking replication worked)
    let read_tx_future = client.begin_read_transaction(None);
    let read_tx = read_tx_future
        .await_result()
        .await
        .expect("Failed to begin read transaction");

    let get_future = read_tx.snapshot_get(b"replication_key", None);
    let value = get_future
        .await_result()
        .await
        .expect("Failed to get value");

    assert!(value.is_some(), "Key should be found after replication");
    assert_eq!(
        value.unwrap(),
        b"replication_value",
        "Value should match what was replicated"
    );

    cluster.stop().await.expect("Failed to stop cluster");

    println!("RSML consensus cluster replication test passed");
}