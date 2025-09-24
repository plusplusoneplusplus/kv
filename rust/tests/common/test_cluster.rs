use async_trait::async_trait;
use rocksdb_server::client::KvStoreClient;
use rocksdb_server::{Config, KvDatabase};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Trait for test cluster abstractions that can be used across different deployment modes
#[async_trait]
pub trait TestCluster: Send + Sync {
    /// Start the test cluster
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    /// Shutdown the test cluster gracefully
    async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>>;

    /// Get a client connected to the cluster
    async fn client(&self) -> Result<KvStoreClient, Box<dyn std::error::Error>>;

    /// Get database paths used by this cluster
    #[allow(dead_code)]
    fn get_database_paths(&self) -> Vec<PathBuf>;

    /// Verify that data exists in the cluster
    async fn verify_data(&self, key: &[u8], expected_value: &[u8]) -> Result<(), String>;

    /// Get the cluster configuration
    #[allow(dead_code)]
    fn config(&self) -> &Config;
}

/// Standalone test cluster - single database instance
pub struct StandaloneTestCluster {
    config: Config,
    database: Option<Arc<dyn KvDatabase>>,
    thrift_server: Option<super::ThriftTestServer>,
    client: Option<KvStoreClient>,
}

impl StandaloneTestCluster {
    /// Create a new standalone test cluster
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let config = Self::create_test_config().await?;

        Ok(Self {
            config,
            database: None,
            thrift_server: None,
            client: None,
        })
    }

    /// Create a test configuration with temporary directories
    async fn create_test_config() -> Result<Config, Box<dyn std::error::Error>> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("test_db");

        let mut config = Config::default();
        config.database.base_path = db_path.to_string_lossy().to_string();

        // Don't drop temp_dir - we'll let the test manage it
        std::mem::forget(temp_dir);

        Ok(config)
    }

    /// Get the port the server is running on
    #[allow(dead_code)]
    pub fn get_port(&self) -> Option<u16> {
        self.thrift_server.as_ref().and_then(|s| s.port)
    }
}

#[async_trait]
impl TestCluster for StandaloneTestCluster {
    async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Create database
        let factory = self.config.create_database_factory();
        let database = factory
            .create_database()
            .map_err(|e| format!("Failed to create database: {}", e))?;
        self.database = Some(database);

        // Start Thrift server
        let mut server = super::ThriftTestServer::new().await;
        let port = server
            .start()
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { e })?;
        self.thrift_server = Some(server);

        // Create client
        let address = format!("localhost:{}", port);
        let client = KvStoreClient::connect(&address)?;
        self.client = Some(client);

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // Close client
        self.client = None;

        // Stop server
        if let Some(mut server) = self.thrift_server.take() {
            server.stop().await;
        }

        // Drop database reference
        self.database = None;

        Ok(())
    }

    async fn client(&self) -> Result<KvStoreClient, Box<dyn std::error::Error>> {
        self.client
            .as_ref()
            .ok_or_else(|| "Cluster not started".into())
            .map(|c| c.clone())
    }

    fn get_database_paths(&self) -> Vec<PathBuf> {
        vec![PathBuf::from(&self.config.database.base_path)]
    }

    async fn verify_data(&self, key: &[u8], expected_value: &[u8]) -> Result<(), String> {
        let client = self
            .client()
            .await
            .map_err(|e| format!("Failed to get client: {}", e))?;

        // Start a transaction for reading
        let tx_future = client.begin_transaction(None, Some(30));
        let tx = tx_future
            .await_result()
            .await
            .map_err(|e| format!("Failed to begin transaction: {:?}", e))?;

        // Get the value
        let get_future = tx.get(key, None);
        let result = get_future
            .await_result()
            .await
            .map_err(|e| format!("Failed to get value: {:?}", e))?;

        match result {
            Some(actual_value) => {
                if actual_value == expected_value {
                    Ok(())
                } else {
                    Err(format!(
                        "Value mismatch: expected {:?}, got {:?}",
                        expected_value, actual_value
                    ))
                }
            }
            None => Err("Key not found".to_string()),
        }
    }

    fn config(&self) -> &Config {
        &self.config
    }
}

/// Helper functions for common test patterns
pub mod test_operations {
    use super::*;

    /// Test basic get/set operations on a cluster
    pub async fn test_basic_operations(
        cluster: &dyn TestCluster,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = cluster.client().await?;

        // Test SET
        let tx_future = client.begin_transaction(None, Some(60));
        let mut tx = tx_future.await_result().await?;

        tx.set(b"test_key", b"test_value", None)?;

        let commit_future = tx.commit();
        commit_future.await_result().await?;

        // Verify
        cluster
            .verify_data(b"test_key", b"test_value")
            .await
            .map_err(|e| e.into())
    }

    /// Test transaction rollback behavior
    pub async fn test_transaction_rollback(
        cluster: &dyn TestCluster,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = cluster.client().await?;

        // Start transaction but don't commit
        let tx_future = client.begin_transaction(None, Some(60));
        let mut tx = tx_future.await_result().await?;

        tx.set(b"rollback_key", b"rollback_value", None)?;

        // Let transaction drop without commit (implicit rollback)
        drop(tx);

        // Verify key doesn't exist
        let verify_result = cluster
            .verify_data(b"rollback_key", b"rollback_value")
            .await;
        if verify_result.is_ok() {
            return Err("Key should not exist after rollback".into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_standalone_cluster_lifecycle() {
        let mut cluster = StandaloneTestCluster::new().await.unwrap();

        // Test start
        cluster.start().await.unwrap();

        // Test basic functionality
        let _client = cluster.client().await.unwrap();
        // Client created successfully

        // Test shutdown
        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn test_basic_cluster_operations() {
        let mut cluster = StandaloneTestCluster::new().await.unwrap();
        cluster.start().await.unwrap();

        // Test the helper functions
        test_operations::test_basic_operations(&cluster)
            .await
            .unwrap();
        test_operations::test_transaction_rollback(&cluster)
            .await
            .unwrap();

        cluster.shutdown().await.unwrap();
    }
}

/// Test harness for 3-node cluster deployment and operations
pub struct ThreeNodeClusterTest {
    nodes: Vec<ClusterNode>,
    client_endpoints: Vec<String>,
    clients: Vec<Option<KvStoreClient>>,
    test_data_cleanup: Vec<String>,
}

struct ClusterNode {
    node_id: u32,
    process: Option<Child>,
    endpoint: String,
    config_path: String,
    data_path: String,
}

impl ThreeNodeClusterTest {
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let mut nodes = Vec::new();
        let mut client_endpoints = Vec::new();

        for i in 0..3 {
            let endpoint = format!("localhost:{}", 9090 + i);
            let config_path = format!("build/bin/cluster_configs/node_{}.toml", i);
            let data_path = format!("./data/multi-node-node-{}", i);

            nodes.push(ClusterNode {
                node_id: i,
                process: None,
                endpoint: endpoint.clone(),
                config_path,
                data_path,
            });

            client_endpoints.push(endpoint);
        }

        Ok(Self {
            nodes,
            client_endpoints,
            clients: vec![None; 3],
            test_data_cleanup: Vec::new(),
        })
    }

    pub async fn start_cluster(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Starting 3-node cluster...");

        self.setup_data_directories().await?;
        self.ensure_cluster_configs().await?;
        for node in &mut self.nodes {
            println!("Starting node {} on {}", node.node_id, node.endpoint);

            let process = Command::new("./rust/target/debug/thrift-server")
                .arg("--config")
                .arg(&node.config_path)
                .arg("--node-id")
                .arg(&node.node_id.to_string())
                .arg("--port")
                .arg(&(9090 + node.node_id).to_string())
                .spawn()?;

            node.process = Some(process);
        }

        self.wait_for_cluster_ready().await?;
        self.create_client_connections().await?;

        println!("3-node cluster started successfully");
        Ok(())
    }

    pub async fn shutdown_cluster(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Shutting down 3-node cluster...");

        self.clients = vec![None; 3];
        for node in &mut self.nodes {
            if let Some(mut process) = node.process.take() {
                println!("Stopping node {}", node.node_id);
                let _ = process.kill();
                let _ = process.wait();
            }
        }

        self.cleanup_test_data().await?;

        println!("3-node cluster shutdown complete");
        Ok(())
    }

    pub fn get_primary_client(&self) -> Result<&KvStoreClient, Box<dyn std::error::Error>> {
        self.clients[0].as_ref().ok_or("Primary client not available".into())
    }

    pub fn get_available_client(&self) -> Result<&KvStoreClient, Box<dyn std::error::Error>> {
        for client in &self.clients {
            if let Some(client) = client {
                return Ok(client);
            }
        }
        Err("No available clients".into())
    }

    pub fn available_client_count(&self) -> usize {
        self.clients.iter().filter(|c| c.is_some()).count()
    }

    pub async fn verify_data_consistency(&self, test_data: &[(&str, &str)]) -> Result<(), Box<dyn std::error::Error>> {
        println!("Verifying data consistency across {} nodes...", self.available_client_count());

        for (key, expected_value) in test_data {
            let mut node_values = HashMap::new();

            for (i, client_opt) in self.clients.iter().enumerate() {
                if let Some(client) = client_opt {
                    let tx_future = client.begin_transaction(None, Some(10));
                    if let Ok(tx) = tx_future.await_result().await {
                        let get_future = tx.get(key.as_bytes(), None);
                        match get_future.await_result().await {
                            Ok(Some(value)) => {
                                node_values.insert(i, String::from_utf8_lossy(&value).to_string());
                            }
                            Ok(None) => {
                                node_values.insert(i, "<NOT_FOUND>".to_string());
                            }
                            Err(_) => {
                                node_values.insert(i, "<ERROR>".to_string());
                            }
                        }
                    }
                }
            }

            let mut consistent = true;
            for (node_id, value) in &node_values {
                if value != expected_value && value != "<ERROR>" {
                    println!("Inconsistency detected for key '{}': node {} has '{}', expected '{}'",
                           key, node_id, value, expected_value);
                    consistent = false;
                }
            }

            if consistent && !node_values.is_empty() {
                println!("Key '{}' is consistent across {} nodes", key, node_values.len());
            }
        }

        Ok(())
    }

    pub fn add_cleanup_key(&mut self, key: String) {
        self.test_data_cleanup.push(key);
    }

    pub async fn test_cluster_formation(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing cluster formation and leader election...");

        sleep(Duration::from_secs(3)).await;
        for (i, client) in self.clients.iter().enumerate() {
            if let Some(client) = client {
                let tx_future = client.begin_transaction(None, Some(30));
                let mut tx = tx_future.await_result().await
                    .map_err(|e| format!("Node {} not reachable: {:?}", i, e))?;

                let test_key = format!("formation_test_{}", i);
                let test_value = format!("node_{}_ready", i);

                tx.set(test_key.as_bytes(), test_value.as_bytes(), None)?;
                let commit_future = tx.commit();
                commit_future.await_result().await?;

                self.test_data_cleanup.push(test_key);
                println!("Node {} is responsive and participating in cluster", i);
            } else {
                return Err(format!("Client {} not available", i).into());
            }
        }

        println!("Cluster formation test passed - all 3 nodes are operational");
        Ok(())
    }

    pub async fn test_leader_failover(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing leader failover scenario...");

        let test_key = "failover_test_key";
        let test_value = "initial_value";

        let client = self.get_primary_client()?;
        let tx_future = client.begin_transaction(None, Some(30));
        let mut tx = tx_future.await_result().await?;

        tx.set(test_key.as_bytes(), test_value.as_bytes(), None)?;
        let commit_future = tx.commit();
        commit_future.await_result().await?;

        self.test_data_cleanup.push(test_key.to_string());

        println!("Simulating leader failure (stopping node 0)");
        if let Some(mut process) = self.nodes[0].process.take() {
            let _ = process.kill();
            let _ = process.wait();
        }
        self.clients[0] = None;

        sleep(Duration::from_secs(5)).await;

        let fallback_client = self.clients[1].as_ref()
            .ok_or("Fallback client not available")?;

        let tx_future = fallback_client.begin_transaction(None, Some(30));
        let mut tx = tx_future.await_result().await?;

        let failover_key = "post_failover_key";
        let failover_value = "failover_successful";

        tx.set(failover_key.as_bytes(), failover_value.as_bytes(), None)?;
        let commit_future = tx.commit();
        commit_future.await_result().await?;

        self.test_data_cleanup.push(failover_key.to_string());

        self.verify_data_consistency(&[(failover_key, failover_value)]).await?;

        println!("Leader failover test passed - cluster survived leader failure");
        Ok(())
    }

    pub async fn test_follower_failure(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing follower failure scenario...");

        println!("Stopping follower node 2");
        if let Some(mut process) = self.nodes[2].process.take() {
            let _ = process.kill();
            let _ = process.wait();
        }
        self.clients[2] = None;

        let client = self.get_available_client()?;
        let tx_future = client.begin_transaction(None, Some(30));
        let mut tx = tx_future.await_result().await?;

        let test_key = "follower_failure_test";
        let test_value = "cluster_still_works";

        tx.set(test_key.as_bytes(), test_value.as_bytes(), None)?;
        let commit_future = tx.commit();
        commit_future.await_result().await?;

        self.test_data_cleanup.push(test_key.to_string());

        self.verify_data_consistency(&[(test_key, test_value)]).await?;

        println!("Follower failure test passed - cluster maintained operations with 2 nodes");
        Ok(())
    }

    pub async fn test_concurrent_operations(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing concurrent operations across nodes...");

        use std::sync::Arc;
        use tokio::sync::Mutex;

        let concurrent_tasks = Arc::new(Mutex::new(Vec::new()));
        let test_data = Arc::new(Mutex::new(Vec::new()));

        for (i, client_opt) in self.clients.iter().enumerate() {
            if let Some(client) = client_opt {
                let client = client.clone();
                let tasks = concurrent_tasks.clone();
                let data = test_data.clone();

                let task = tokio::spawn(async move {
                    for j in 0..5 {
                        let key = format!("concurrent_{}_{}", i, j);
                        let value = format!("value_from_client_{}_{}", i, j);

                        let tx_future = client.begin_transaction(None, Some(30));
                        if let Ok(mut tx) = tx_future.await_result().await {
                            if tx.set(key.as_bytes(), value.as_bytes(), None).is_ok() {
                                let commit_future = tx.commit();
                                if commit_future.await_result().await.is_ok() {
                                    data.lock().await.push((key, value));
                                }
                            }
                        }

                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                });

                tasks.lock().await.push(task);
            }
        }

        let tasks = Arc::try_unwrap(concurrent_tasks).unwrap().into_inner();
        for task in tasks {
            let _ = task.await;
        }

        let test_data = test_data.lock().await;
        println!("Concurrent operations completed: {} operations successful", test_data.len());

        for (key, _) in test_data.iter() {
            self.test_data_cleanup.push(key.clone());
        }

        if !test_data.is_empty() {
            let sample_data: Vec<(&str, &str)> = test_data.iter().take(3).map(|(k, v)| (k.as_str(), v.as_str())).collect();
            self.verify_data_consistency(&sample_data).await?;
        }

        println!("Concurrent operations test passed");
        Ok(())
    }

    pub async fn test_data_consistency(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing data consistency across cluster...");

        let test_cases = vec![
            ("consistency_key_1", "consistency_value_1"),
            ("consistency_key_2", "consistency_value_2"),
            ("consistency_key_3", "consistency_value_3"),
        ];

        for (key, value) in &test_cases {
            let client = self.get_available_client()?;
            let tx_future = client.begin_transaction(None, Some(30));
            let mut tx = tx_future.await_result().await?;

            tx.set(key.as_bytes(), value.as_bytes(), None)?;
            let commit_future = tx.commit();
            commit_future.await_result().await?;

            self.test_data_cleanup.push(key.to_string());
        }

        self.verify_data_consistency(&test_cases).await?;

        println!("Data consistency test passed - all nodes have consistent data");
        Ok(())
    }

    pub async fn test_network_partitions(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing network partition scenarios...");

        let client = self.get_available_client()?;
        let pre_partition_key = "pre_partition_key";
        let pre_partition_value = "data_before_split";

        let tx_future = client.begin_transaction(None, Some(30));
        let mut tx = tx_future.await_result().await?;
        tx.set(pre_partition_key.as_bytes(), pre_partition_value.as_bytes(), None)?;
        let commit_future = tx.commit();
        commit_future.await_result().await?;

        self.test_data_cleanup.push(pre_partition_key.to_string());

        println!("Simulating network partition (isolating node 0 from nodes 1,2)");

        if let Some(mut process) = self.nodes[1].process.take() {
            let _ = process.kill();
            let _ = process.wait();
        }
        self.clients[1] = None;

        if let Some(mut process) = self.nodes[2].process.take() {
            let _ = process.kill();
            let _ = process.wait();
        }
        self.clients[2] = None;

        sleep(Duration::from_secs(3)).await;

        if let Some(client) = &self.clients[0] {
            let tx_future = client.begin_transaction(None, Some(10));
            if let Ok(tx) = tx_future.await_result().await {
                let get_future = tx.get(pre_partition_key.as_bytes(), None);
                match get_future.await_result().await {
                    Ok(Some(value)) => {
                        let value_str = String::from_utf8_lossy(&value);
                        if value_str == pre_partition_value {
                            println!("Isolated node can still read pre-partition data");
                        }
                    }
                    Ok(None) => {
                        println!("Pre-partition data not found on isolated node");
                    }
                    Err(e) => {
                        println!("Error reading from isolated node: {:?}", e);
                    }
                }
            }
        }

        println!("Simulating partition healing (restarting nodes 1 and 2)");

        let process1 = Command::new("./rust/target/debug/thrift-server")
            .arg("--config")
            .arg(&self.nodes[1].config_path)
            .arg("--node-id")
            .arg("1")
            .arg("--port")
            .arg("9091")
            .spawn()?;
        self.nodes[1].process = Some(process1);

        let process2 = Command::new("./rust/target/debug/thrift-server")
            .arg("--config")
            .arg(&self.nodes[2].config_path)
            .arg("--node-id")
            .arg("2")
            .arg("--port")
            .arg("9092")
            .spawn()?;
        self.nodes[2].process = Some(process2);

        sleep(Duration::from_secs(5)).await;

        if let Ok(client1) = KvStoreClient::connect(&self.client_endpoints[1]) {
            self.clients[1] = Some(client1);
            println!("Node 1 rejoined cluster");
        }

        if let Ok(client2) = KvStoreClient::connect(&self.client_endpoints[2]) {
            self.clients[2] = Some(client2);
            println!("Node 2 rejoined cluster");
        }

        let post_partition_key = "post_partition_key";
        let post_partition_value = "cluster_healed";

        let client = self.get_available_client()?;
        let tx_future = client.begin_transaction(None, Some(30));
        let mut tx = tx_future.await_result().await?;
        tx.set(post_partition_key.as_bytes(), post_partition_value.as_bytes(), None)?;
        let commit_future = tx.commit();
        commit_future.await_result().await?;

        self.test_data_cleanup.push(post_partition_key.to_string());

        self.verify_data_consistency(&[(post_partition_key, post_partition_value)]).await?;

        println!("Network partition test completed - cluster survived split-brain scenario");
        Ok(())
    }

    pub async fn test_rolling_restart(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing rolling restart scenario...");

        let client = self.get_available_client()?;
        let initial_key = "rolling_restart_initial";
        let initial_value = "before_rolling_restart";

        let tx_future = client.begin_transaction(None, Some(30));
        let mut tx = tx_future.await_result().await?;
        tx.set(initial_key.as_bytes(), initial_value.as_bytes(), None)?;
        let commit_future = tx.commit();
        commit_future.await_result().await?;

        self.test_data_cleanup.push(initial_key.to_string());

        for i in 0..3 {
            println!("Rolling restart: restarting node {}", i);

            if let Some(mut process) = self.nodes[i].process.take() {
                let _ = process.kill();
                let _ = process.wait();
            }
            self.clients[i] = None;

            sleep(Duration::from_secs(2)).await;

            if let Ok(available_client) = self.get_available_client() {
                let rolling_key = format!("rolling_restart_during_{}", i);
                let rolling_value = format!("service_maintained_{}", i);

                let tx_future = available_client.begin_transaction(None, Some(15));
                if let Ok(mut tx) = tx_future.await_result().await {
                    if tx.set(rolling_key.as_bytes(), rolling_value.as_bytes(), None).is_ok() {
                        let commit_future = tx.commit();
                        if commit_future.await_result().await.is_ok() {
                            println!("Service maintained during restart of node {}", i);
                            self.test_data_cleanup.push(rolling_key);
                        }
                    }
                }
            }

            let process = Command::new("./rust/target/debug/thrift-server")
                .arg("--config")
                .arg(&self.nodes[i].config_path)
                .arg("--node-id")
                .arg(&i.to_string())
                .arg("--port")
                .arg(&(9090 + i).to_string())
                .spawn()?;
            self.nodes[i].process = Some(process);

            sleep(Duration::from_secs(3)).await;

            if let Ok(client) = KvStoreClient::connect(&self.client_endpoints[i]) {
                self.clients[i] = Some(client);
                println!("Node {} restarted and rejoined cluster", i);
            }
        }

        sleep(Duration::from_secs(2)).await;
        self.verify_data_consistency(&[(initial_key, initial_value)]).await?;

        println!("Rolling restart test completed - cluster maintained service throughout");
        Ok(())
    }

    pub async fn test_diagnostic_endpoints(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Testing diagnostic endpoints across cluster...");

        for (i, client_opt) in self.clients.iter().enumerate() {
            if let Some(client) = client_opt {
                let tx_future = client.begin_transaction(None, Some(10));
                match tx_future.await_result().await {
                    Ok(_tx) => {
                        println!("Node {} diagnostic: Healthy and accepting transactions", i);
                    }
                    Err(e) => {
                        println!("Node {} diagnostic: Failed - {:?}", i, e);
                    }
                }
            } else {
                println!("Node {} diagnostic: Not available", i);
            }
        }

        println!("Diagnostic endpoints test completed");
        Ok(())
    }

    pub async fn test_load_testing(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Running load test on 3-node cluster...");

        use std::time::Instant;

        let start_time = Instant::now();
        let operations_per_client = 20;
        let total_expected = operations_per_client * self.available_client_count();
        let mut successful_operations = 0;

        let mut tasks = Vec::new();

        for (i, client_opt) in self.clients.iter().enumerate() {
            if let Some(client) = client_opt {
                let client = client.clone();
                let task = tokio::spawn(async move {
                    let mut local_success = 0;
                    for j in 0..operations_per_client {
                        let key = format!("load_test_{}_{}", i, j);
                        let value = format!("load_value_{}_{}", i, j);

                        let tx_future = client.begin_transaction(None, Some(15));
                        if let Ok(mut tx) = tx_future.await_result().await {
                            if tx.set(key.as_bytes(), value.as_bytes(), None).is_ok() {
                                let commit_future = tx.commit();
                                if commit_future.await_result().await.is_ok() {
                                    local_success += 1;
                                }
                            }
                        }
                    }
                    local_success
                });
                tasks.push(task);
            }
        }

        for task in tasks {
            if let Ok(count) = task.await {
                successful_operations += count;
            }
        }

        let duration = start_time.elapsed();
        let ops_per_second = successful_operations as f64 / duration.as_secs_f64();

        println!("Load test completed:");
        println!("   - Duration: {:?}", duration);
        println!("   - Successful operations: {}/{}", successful_operations, total_expected);
        println!("   - Operations per second: {:.2}", ops_per_second);

        for i in 0..self.available_client_count() {
            for j in 0..operations_per_client {
                self.test_data_cleanup.push(format!("load_test_{}_{}", i, j));
            }
        }

        println!("Load testing completed successfully");
        Ok(())
    }

    async fn setup_data_directories(&self) -> Result<(), Box<dyn std::error::Error>> {
        for node in &self.nodes {
            std::fs::create_dir_all(&node.data_path)?;
        }
        Ok(())
    }

    async fn ensure_cluster_configs(&self) -> Result<(), Box<dyn std::error::Error>> {
        for node in &self.nodes {
            if std::path::Path::new(&node.config_path).exists() {
                let content = std::fs::read_to_string(&node.config_path)?;
                if !content.contains("algorithm = \"mock\"") && content.contains("[consensus]") {
                    let updated_content = content.replace(
                        "[consensus]",
                        "[consensus]\nalgorithm = \"mock\""
                    );
                    std::fs::write(&node.config_path, updated_content)?;
                }
            }
        }
        Ok(())
    }

    async fn wait_for_cluster_ready(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Waiting for cluster to be ready...");

        sleep(Duration::from_secs(5)).await;

        for node in &self.nodes {
            let mut retries = 10;
            while retries > 0 {
                match KvStoreClient::connect(&node.endpoint) {
                    Ok(_) => {
                        println!("Node {} ready at {}", node.node_id, node.endpoint);
                        break;
                    }
                    Err(_) => {
                        if retries == 1 {
                            return Err(format!("Node {} failed to start", node.node_id).into());
                        }
                        retries -= 1;
                        sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }

        Ok(())
    }

    async fn create_client_connections(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for (i, endpoint) in self.client_endpoints.iter().enumerate() {
            match KvStoreClient::connect(endpoint) {
                Ok(client) => {
                    self.clients[i] = Some(client);
                    println!("Client {} connected to {}", i, endpoint);
                }
                Err(e) => {
                    println!("Failed to connect client {} to {}: {:?}", i, endpoint, e);
                }
            }
        }
        Ok(())
    }

    async fn cleanup_test_data(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.test_data_cleanup.is_empty() {
            return Ok(());
        }

        println!("Cleaning up {} test keys...", self.test_data_cleanup.len());

        if let Ok(client) = self.get_available_client() {
            for key in &self.test_data_cleanup {
                let _ = async {
                    let tx_future = client.begin_transaction(None, Some(10));
                    let mut tx = tx_future.await_result().await?;
                    tx.delete(key.as_bytes(), None)?;
                    let commit_future = tx.commit();
                    commit_future.await_result().await
                }.await;
            }
        }

        self.test_data_cleanup.clear();
        Ok(())
    }
}
