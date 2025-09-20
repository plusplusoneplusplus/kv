use async_trait::async_trait;
use rocksdb_server::client::KvStoreClient;
use rocksdb_server::{Config, KvDatabase};
use std::path::PathBuf;
use std::sync::Arc;

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
