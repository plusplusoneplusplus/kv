// Cluster test utilities - provides reusable components for testing cluster functionality
// This module contains utilities for setting up and managing test clusters

use std::sync::Arc;
use rocksdb_server::lib::cluster::ClusterManager;
use rocksdb_server::lib::config::ClusterConfig;

/// Configuration for cluster test setup
#[derive(Debug, Clone)]
pub struct ClusterTestConfig {
    pub node_count: u32,
    pub base_port: u16,
    pub health_check_interval_ms: u64,
    pub node_timeout_ms: u64,
    pub leader_discovery_timeout_ms: u64,
}

impl Default for ClusterTestConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            base_port: 9090,
            health_check_interval_ms: 100,
            node_timeout_ms: 5000,
            leader_discovery_timeout_ms: 2000,
        }
    }
}

/// Multi-node cluster test utility for testing cluster functionality
pub struct MultiNodeClusterTest {
    cluster_managers: Vec<Arc<ClusterManager>>,
    config: ClusterTestConfig,
}

impl MultiNodeClusterTest {
    /// Create a new multi-node cluster test with default configuration
    pub async fn new(node_count: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let config = ClusterTestConfig {
            node_count,
            ..Default::default()
        };
        Self::new_with_config(config).await
    }

    /// Create a new multi-node cluster test with custom configuration
    pub async fn new_with_config(config: ClusterTestConfig) -> Result<Self, Box<dyn std::error::Error>> {
        if config.node_count < 2 {
            return Err("Multi-node cluster must have at least 2 nodes".into());
        }

        // Create cluster configuration with endpoints
        let mut endpoints = Vec::new();
        for i in 0..config.node_count {
            endpoints.push(format!("localhost:{}", config.base_port + i as u16));
        }

        let cluster_config = ClusterConfig {
            health_check_interval_ms: config.health_check_interval_ms,
            leader_discovery_timeout_ms: config.leader_discovery_timeout_ms,
            node_timeout_ms: config.node_timeout_ms,
        };

        // Create cluster managers for each node
        let mut cluster_managers = Vec::new();
        for i in 0..config.node_count {
            let manager = Arc::new(ClusterManager::new(
                i,
                endpoints.clone(),
                cluster_config.clone(),
            ));
            cluster_managers.push(manager);
        }

        Ok(Self {
            cluster_managers,
            config,
        })
    }

    /// Start cluster discovery for all nodes
    #[allow(dead_code)]
    pub async fn start_discovery(&self) -> Result<(), Box<dyn std::error::Error>> {
        for manager in &self.cluster_managers {
            manager.start_discovery().await?;
        }
        Ok(())
    }

    /// Get a cluster manager by node ID
    pub fn get_manager(&self, node_id: u32) -> Option<&Arc<ClusterManager>> {
        if node_id < self.config.node_count {
            self.cluster_managers.get(node_id as usize)
        } else {
            None
        }
    }

    /// Get all cluster managers
    pub fn get_all_managers(&self) -> &[Arc<ClusterManager>] {
        &self.cluster_managers
    }

    /// Get the test configuration
    pub fn get_config(&self) -> &ClusterTestConfig {
        &self.config
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_test_config_default() {
        let config = ClusterTestConfig::default();
        assert_eq!(config.node_count, 3);
        assert_eq!(config.base_port, 9090);
        assert!(config.health_check_interval_ms > 0);
        assert!(config.node_timeout_ms > 0);
        assert!(config.leader_discovery_timeout_ms > 0);
    }

    #[tokio::test]
    async fn test_multi_node_cluster_test_creation() {
        let cluster = MultiNodeClusterTest::new(3).await.unwrap();
        assert_eq!(cluster.get_config().node_count, 3);
        assert_eq!(cluster.get_all_managers().len(), 3);

        for i in 0..3 {
            let manager = cluster.get_manager(i).unwrap();
            assert_eq!(manager.get_node_id(), i);
        }
    }

    #[tokio::test]
    async fn test_multi_node_cluster_test_invalid_size() {
        let result = MultiNodeClusterTest::new(1).await;
        assert!(result.is_err());
        let error = result.err().unwrap();
        assert!(error.to_string().contains("at least 2 nodes"));
    }
}
