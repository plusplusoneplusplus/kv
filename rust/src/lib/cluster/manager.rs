use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::lib::config::ClusterConfig;
use super::errors::ClusterResult;
use super::node_info::{NodeInfo, NodeStatus};

pub struct ClusterManager {
    node_id: u32,
    cluster_nodes: Arc<RwLock<HashMap<u32, NodeInfo>>>,
    config: ClusterConfig,
    current_leader: Arc<RwLock<Option<u32>>>,
    current_term: Arc<RwLock<u64>>,
}

impl ClusterManager {
    pub fn new(node_id: u32, endpoints: Vec<String>, config: ClusterConfig) -> Self {
        let mut cluster_nodes = HashMap::new();

        if endpoints.is_empty() {
            // Single-node case: create a cluster with just this node
            let endpoint = format!("localhost:{}", 9090); // Default port for single node
            let node_info = NodeInfo::new(node_id, endpoint);
            cluster_nodes.insert(node_id, node_info);
        } else {
            // Multi-node case: use provided endpoints
            for (id, endpoint) in endpoints.iter().enumerate() {
                let node_info = NodeInfo::new(id as u32, endpoint.clone());
                cluster_nodes.insert(id as u32, node_info);
            }
        }

        Self {
            node_id,
            cluster_nodes: Arc::new(RwLock::new(cluster_nodes)),
            config,
            current_leader: Arc::new(RwLock::new(None)),
            current_term: Arc::new(RwLock::new(0)),
        }
    }

    /// Create a single-node cluster manager
    pub fn single_node(node_id: u32) -> Self {
        Self::new(node_id, vec![], ClusterConfig::default())
    }

    /// Start cluster discovery process
    pub async fn start_discovery(&self) -> ClusterResult<()> {
        info!("Node {}: Starting cluster discovery", self.node_id);

        // For now, this is a simple implementation without consensus
        // In Phase 1, we just mark ourselves as healthy and try to discover other nodes

        {
            let mut nodes = self.cluster_nodes.write().await;
            if let Some(this_node) = nodes.get_mut(&self.node_id) {
                this_node.update_status(NodeStatus::Healthy);
                info!("Node {}: Marked self as healthy", self.node_id);
            }
        }

        // Try to ping other nodes to check if they're available
        self.discover_peers().await?;

        // Check if this is a single-node cluster
        let cluster_size = {
            let nodes = self.cluster_nodes.read().await;
            nodes.len()
        };

        if cluster_size == 1 {
            // Single-node cluster: we are always the leader
            self.become_leader().await;
        } else {
            // Multi-node cluster: For Phase 1, node 0 becomes the leader by default
            // This is a placeholder until real consensus is implemented
            if self.node_id == 0 {
                self.become_leader().await;
            } else {
                self.discover_leader().await?;
            }
        }

        Ok(())
    }

    /// Start health monitoring background task
    pub async fn start_health_monitoring(&self) {
        info!("Node {}: Starting health monitoring", self.node_id);

        let mut health_check_interval = interval(Duration::from_millis(
            self.config.health_check_interval_ms
        ));

        let cluster_nodes = self.cluster_nodes.clone();
        let node_id = self.node_id;
        let node_timeout = self.config.node_timeout_ms;

        loop {
            health_check_interval.tick().await;

            let mut nodes = cluster_nodes.write().await;
            let _now = SystemTime::now();

            for (id, node) in nodes.iter_mut() {
                if *id == node_id {
                    // Keep ourselves healthy
                    node.update_status(NodeStatus::Healthy);
                    continue;
                }

                // Check if node has timed out
                if let Ok(elapsed) = node.time_since_last_seen() {
                    if elapsed.as_millis() > node_timeout as u128 {
                        if node.status != NodeStatus::Unreachable {
                            warn!("Node {}: Marking node {} as unreachable (last seen {:?} ago)",
                                  node_id, id, elapsed);
                            node.update_status(NodeStatus::Unreachable);
                        }
                    }
                }
            }
        }
    }

    /// Get current leader information
    pub async fn get_current_leader(&self) -> Option<NodeInfo> {
        let leader_id = self.current_leader.read().await;
        if let Some(id) = *leader_id {
            let nodes = self.cluster_nodes.read().await;
            nodes.get(&id).cloned()
        } else {
            None
        }
    }

    /// Check if this node is the leader
    pub async fn is_leader(&self) -> bool {
        let leader_id = self.current_leader.read().await;
        leader_id.map_or(false, |id| id == self.node_id)
    }

    /// Get all cluster nodes
    pub async fn get_all_nodes(&self) -> HashMap<u32, NodeInfo> {
        self.cluster_nodes.read().await.clone()
    }

    /// Get healthy nodes for read load balancing
    pub async fn get_healthy_nodes(&self) -> Vec<NodeInfo> {
        let nodes = self.cluster_nodes.read().await;
        nodes.values()
            .filter(|node| node.is_healthy())
            .cloned()
            .collect()
    }

    /// Update node status (called by health checks or network operations)
    pub async fn update_node_status(&self, node_id: u32, status: NodeStatus) {
        let mut nodes = self.cluster_nodes.write().await;
        if let Some(node) = nodes.get_mut(&node_id) {
            debug!("Node {}: Updated node {} status to {:?}", self.node_id, node_id, status);
            node.update_status(status);
        }
    }

    /// Discover other nodes in the cluster (Phase 1 implementation)
    async fn discover_peers(&self) -> ClusterResult<()> {
        info!("Node {}: Discovering cluster peers", self.node_id);

        let nodes = self.cluster_nodes.read().await;
        let peer_count = nodes.len() - 1; // Exclude ourselves

        if peer_count > 0 {
            info!("Node {}: Found {} peers in configuration", self.node_id, peer_count);

            // In Phase 1, we just log the peer endpoints
            // Real health checks would be implemented here
            for (id, node) in nodes.iter() {
                if *id != self.node_id {
                    debug!("Node {}: Peer node {} at {}", self.node_id, id, node.endpoint);
                }
            }
        } else {
            info!("Node {}: Running as single-node cluster", self.node_id);
        }

        Ok(())
    }

    /// Discover the current leader (Phase 1 implementation)
    async fn discover_leader(&self) -> ClusterResult<()> {
        info!("Node {}: Discovering cluster leader", self.node_id);

        // In Phase 1, node 0 is always the leader by convention
        // This will be replaced with real leader election in later phases
        let leader_id = 0;

        {
            let mut current_leader = self.current_leader.write().await;
            *current_leader = Some(leader_id);
        }

        {
            let mut nodes = self.cluster_nodes.write().await;
            if let Some(leader_node) = nodes.get_mut(&leader_id) {
                leader_node.update_leader_status(true, 1);
                info!("Node {}: Discovered leader: Node {}", self.node_id, leader_id);
            }
        }

        Ok(())
    }

    /// Become the leader (Phase 1 implementation)
    async fn become_leader(&self) {
        info!("Node {}: Becoming cluster leader", self.node_id);

        {
            let mut current_leader = self.current_leader.write().await;
            *current_leader = Some(self.node_id);
        }

        {
            let mut current_term = self.current_term.write().await;
            *current_term += 1;
        }

        {
            let mut nodes = self.cluster_nodes.write().await;
            if let Some(this_node) = nodes.get_mut(&self.node_id) {
                this_node.update_leader_status(true, *self.current_term.read().await);
            }
        }

        info!("Node {}: Successfully became cluster leader", self.node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cluster_manager_creation() {
        let endpoints = vec![
            "localhost:9090".to_string(),
            "localhost:9091".to_string(),
            "localhost:9092".to_string(),
        ];
        let config = ClusterConfig::default();

        let manager = ClusterManager::new(0, endpoints.clone(), config);
        let nodes = manager.get_all_nodes().await;

        assert_eq!(nodes.len(), 3);
        assert!(nodes.contains_key(&0));
        assert!(nodes.contains_key(&1));
        assert!(nodes.contains_key(&2));
    }

    #[tokio::test]
    async fn test_leader_discovery() {
        let endpoints = vec![
            "localhost:9090".to_string(),
            "localhost:9091".to_string(),
        ];
        let config = ClusterConfig::default();

        let manager = ClusterManager::new(1, endpoints, config);

        // Start discovery process
        manager.start_discovery().await.unwrap();

        // Node 0 should be the leader
        let leader = manager.get_current_leader().await;
        assert!(leader.is_some());
        assert_eq!(leader.unwrap().id, 0);

        // This node should not be the leader
        assert!(!manager.is_leader().await);
    }

    #[tokio::test]
    async fn test_become_leader() {
        let endpoints = vec!["localhost:9090".to_string()];
        let config = ClusterConfig::default();

        let manager = ClusterManager::new(0, endpoints, config);

        // Start discovery process
        manager.start_discovery().await.unwrap();

        // Node 0 should become the leader
        assert!(manager.is_leader().await);

        let leader = manager.get_current_leader().await;
        assert!(leader.is_some());
        assert_eq!(leader.unwrap().id, 0);
    }
}