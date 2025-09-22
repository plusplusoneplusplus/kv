// Diagnostic test utilities
// Utilities specifically for testing diagnostic endpoints and cluster health monitoring

use crate::common::cluster_test_utils::MultiNodeClusterTest;

/// Summary of cluster health across all nodes
#[derive(Debug, Clone)]
pub struct ClusterHealthSummary {
    pub total_nodes: u32,
    pub healthy_nodes: u32,
    pub leader_count: u32,
    pub node_statuses: Vec<NodeHealthInfo>,
}

/// Health information for a single node
#[derive(Debug, Clone)]
pub struct NodeHealthInfo {
    pub node_id: u32,
    pub is_leader: bool,
    #[allow(dead_code)]
    pub is_healthy: bool,
    #[allow(dead_code)]
    pub current_term: u64,
}

impl MultiNodeClusterTest {
    /// Get cluster health summary across all nodes (diagnostic-specific extension)
    pub async fn get_cluster_health_summary(&self) -> ClusterHealthSummary {
        let mut healthy_nodes = 0;
        let mut total_nodes = 0;
        let mut leader_count = 0;
        let mut node_statuses = Vec::new();

        for (i, manager) in self.get_all_managers().iter().enumerate() {
            let cluster_status = manager.get_cluster_status().await;
            total_nodes = cluster_status.total_nodes;
            healthy_nodes = cluster_status.healthy_nodes;

            if manager.is_leader().await {
                leader_count += 1;
            }

            node_statuses.push(NodeHealthInfo {
                node_id: i as u32,
                is_leader: manager.is_leader().await,
                is_healthy: cluster_status.nodes.get(&(i as u32))
                    .map(|n| n.is_healthy())
                    .unwrap_or(false),
                current_term: cluster_status.current_term,
            });
        }

        ClusterHealthSummary {
            total_nodes,
            healthy_nodes,
            leader_count,
            node_statuses,
        }
    }
}
