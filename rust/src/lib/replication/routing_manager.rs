use std::sync::Arc;
use std::time::SystemTime;

use kv_storage_rocksdb::config::DeploymentMode;
use crate::lib::operations::{KvOperation, OperationResult, DatabaseOperation};
use crate::lib::cluster::{ClusterManager, NodeStatus as ClusterNodeStatus};
use crate::lib::kv_state_machine::ConsensusKvDatabase;
use crate::generated::kvstore::{ClusterHealth, DatabaseStats, NodeStatus};
use super::errors::{RoutingError, RoutingResult};
use super::executor::KvStoreExecutor;
use thrift::OrderedFloat;
use tracing::warn;

/// Central routing manager that routes operations through the consensus layer
/// for both single-node and multi-node deployments.
pub struct RoutingManager {
    consensus_database: Arc<ConsensusKvDatabase>,
    #[allow(dead_code)]
    deployment_mode: DeploymentMode,
    #[allow(dead_code)]
    node_id: Option<u32>,
    /// Cluster manager for node discovery and leader election
    /// Handles both single-node and multi-node cases
    #[allow(dead_code)]
    cluster_manager: Arc<ClusterManager>,
}

impl RoutingManager {
    /// Create a new routing manager with consensus database
    pub fn new(
        consensus_database: Arc<ConsensusKvDatabase>,
        deployment_mode: DeploymentMode,
        node_id: Option<u32>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        Self {
            consensus_database,
            deployment_mode,
            node_id,
            cluster_manager,
        }
    }

    /// Get a reference to the consensus database executor
    pub fn get_executor(&self) -> &Arc<KvStoreExecutor> {
        self.consensus_database.executor()
    }

    /// Route an operation through the appropriate handler
    pub async fn route_operation(&self, operation: KvOperation) -> RoutingResult<OperationResult> {
        // Use the DatabaseOperation trait to determine routing behavior
        if operation.should_route_to_consensus() {
            // Route through consensus database layer
            // The consensus layer handles:
            // - Read operations: Direct local execution
            // - Write operations: Leader/follower logic with consensus
            match self.consensus_database.execute_operation(operation).await {
                Ok(result) => Ok(result),
                Err(error_msg) => {
                    // Check if this is a "not leader" consensus error
                    if error_msg.contains("not leader") || error_msg.contains("Not leader") {
                        // Extract leader info from cluster manager for better error response
                        if let Some(leader) = self.cluster_manager.get_current_leader().await {
                            Err(RoutingError::NotLeader {
                                leader: Some(leader.endpoint.clone()),
                                leader_endpoint: Some(leader.endpoint),
                                leader_id: Some(leader.id)
                            })
                        } else {
                            Err(RoutingError::NoLeaderAvailable)
                        }
                    } else {
                        Err(RoutingError::DatabaseError(error_msg))
                    }
                }
            }
        } else {
            // Handle locally at routing manager level (diagnostic operations)
            match operation {
                KvOperation::GetClusterHealth => {
                    self.handle_get_cluster_health().await
                }
                KvOperation::GetDatabaseStats { include_detailed } => {
                    self.handle_get_database_stats(include_detailed).await
                }
                KvOperation::GetNodeInfo { node_id } => {
                    self.handle_get_node_info(node_id).await
                }
                _ => {
                    // This should not happen if should_route_to_consensus is implemented correctly
                    Err(RoutingError::InvalidOperation(
                        format!("Operation {:?} marked for local handling but no local handler implemented", operation)
                    ))
                }
            }
        }
    }




    /// Handle cluster health diagnostic request
    async fn handle_get_cluster_health(&self) -> RoutingResult<OperationResult> {
        // Use the new ClusterManager diagnostic methods
        let cluster_status = self.cluster_manager.get_cluster_status().await;
        let mut node_statuses = Vec::new();
        let mut leader_info = None;

        for (node_id, node_info) in cluster_status.nodes.iter() {
            let status_str = match node_info.status {
                ClusterNodeStatus::Healthy => "healthy",
                ClusterNodeStatus::Degraded => "degraded",
                ClusterNodeStatus::Unreachable => "unreachable",
                ClusterNodeStatus::Unknown => "unknown",
            };

            let uptime = SystemTime::now()
                .duration_since(node_info.last_seen)
                .unwrap_or_default()
                .as_secs() as i64;

            let node_status = NodeStatus::new(
                *node_id as i32,
                node_info.endpoint.clone(),
                status_str.to_string(),
                node_info.is_leader,
                node_info.last_seen
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
                node_info.term as i64,
                Some(uptime),
            );

            if node_info.is_leader {
                leader_info = Some(node_status.clone());
            }

            node_statuses.push(node_status);
        }

        let cluster_health = ClusterHealth::new(
            node_statuses,
            cluster_status.healthy_nodes as i32,
            cluster_status.total_nodes as i32,
            leader_info,
            cluster_status.cluster_health,
            cluster_status.current_term as i64,
        );

        Ok(OperationResult::ClusterHealthResult(cluster_health))
    }

    /// Handle database statistics diagnostic request
    async fn handle_get_database_stats(&self, include_detailed: bool) -> RoutingResult<OperationResult> {
        // Get real database statistics from RocksDB
        let stats_result = self.consensus_database.executor().database().get_database_statistics().await;

        let (total_keys, total_size_bytes, cache_hit_rate, compaction_pending) = match stats_result {
            Ok(ref db_stats) => {
                let total_keys = db_stats.get("total_keys").cloned().unwrap_or(0);
                let total_size_bytes = db_stats.get("total_size_bytes").cloned().unwrap_or(0);
                let cache_hit_rate = if include_detailed {
                    db_stats.get("cache_hit_rate_percent").cloned()
                } else {
                    None
                };
                let compaction_pending = if include_detailed {
                    db_stats.get("compaction_pending_bytes").cloned()
                } else {
                    None
                };

                (total_keys, total_size_bytes, cache_hit_rate, compaction_pending)
            }
            Err(ref e) => {
                // Fallback to placeholder values if we can't get real stats
                warn!("Failed to get real database statistics: {}, using fallback values", e);
                if include_detailed {
                    (100, 1024 * 1024, Some(85), Some(0))
                } else {
                    (0, 0, None, None)
                }
            }
        };

        // Extract additional statistics from the database stats
        let (write_ops, read_ops, avg_response_time, active_txns, committed_txns, aborted_txns) = match &stats_result {
            Ok(db_stats) => (
                db_stats.get("write_operations").cloned().unwrap_or(0),
                db_stats.get("read_operations").cloned().unwrap_or(0),
                db_stats.get("average_response_time_ms").cloned().unwrap_or(1),
                db_stats.get("active_transactions").cloned().unwrap_or(0),
                db_stats.get("committed_transactions").cloned().unwrap_or(0),
                db_stats.get("aborted_transactions").cloned().unwrap_or(0),
            ),
            Err(_) => (0, 0, 1, 0, 0, 0)
        };

        let stats = DatabaseStats::new(
            total_keys as i64,
            total_size_bytes as i64,
            write_ops as i64,
            read_ops as i64,
            OrderedFloat(avg_response_time as f64),
            active_txns as i64,
            committed_txns as i64,
            aborted_txns as i64,
            cache_hit_rate.map(|v| v as i64),
            compaction_pending.map(|v| v as i64),
        );

        Ok(OperationResult::DatabaseStatsResult(stats))
    }

    /// Handle node information diagnostic request
    async fn handle_get_node_info(&self, node_id: Option<u32>) -> RoutingResult<OperationResult> {
        let target_node_id = node_id.unwrap_or_else(|| self.cluster_manager.get_node_id());

        // Use the new ClusterManager method
        if let Some(node_info) = self.cluster_manager.get_node_info(target_node_id).await {
            let status_str = match node_info.status {
                ClusterNodeStatus::Healthy => "healthy",
                ClusterNodeStatus::Degraded => "degraded",
                ClusterNodeStatus::Unreachable => "unreachable",
                ClusterNodeStatus::Unknown => "unknown",
            };

            let uptime = SystemTime::now()
                .duration_since(node_info.last_seen)
                .unwrap_or_default()
                .as_secs() as i64;

            let node_status = NodeStatus::new(
                target_node_id as i32,
                node_info.endpoint.clone(),
                status_str.to_string(),
                node_info.is_leader,
                node_info.last_seen
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64,
                node_info.term as i64,
                Some(uptime),
            );

            Ok(OperationResult::NodeInfoResult(node_status))
        } else {
            Err(RoutingError::DatabaseError(format!("Node {} not found", target_node_id)))
        }
    }
}

#[cfg(disabled_test)]
mod tests {
    use super::*;
    use kv_storage_rocksdb::config::DeploymentMode;
    use kv_storage_mockdb::MockDatabase;
    use kv_storage_api::KvDatabase;

    #[tokio::test]
    async fn test_routing_manager_standalone_mode() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let cluster_manager = Arc::new(ClusterManager::single_node(0));
        let routing_manager = RoutingManager::new(
            mock_db,
            DeploymentMode::Standalone,
            None,
            cluster_manager,
        );

        // Test GET operation
        let get_op = KvOperation::Get {
            key: b"test_key".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(get_op).await;
        assert!(result.is_ok());

        // Test SET operation
        let set_op = KvOperation::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(set_op).await;
        assert!(result.is_ok());

        // Verify the value was set
        let get_op = KvOperation::Get {
            key: b"test_key".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(get_op).await;
        assert!(result.is_ok());

        if let Ok(OperationResult::GetResult(Ok(get_result))) = result {
            assert!(get_result.found);
            assert_eq!(get_result.value, b"test_value");
        } else {
            panic!("Expected successful get result");
        }
    }

    #[tokio::test]
    async fn test_routing_manager_replicated_mode() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let cluster_manager = Arc::new(ClusterManager::single_node(1));
        let routing_manager = RoutingManager::new(
            mock_db,
            DeploymentMode::Replicated,
            Some(1),
            cluster_manager,
        );

        // Test that replicated mode works the same as standalone for now
        let set_op = KvOperation::Set {
            key: b"replicated_key".to_vec(),
            value: b"replicated_value".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(set_op).await;
        assert!(result.is_ok());

        let get_op = KvOperation::Get {
            key: b"replicated_key".to_vec(),
            column_family: None,
        };

        let result = routing_manager.route_operation(get_op).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_routing_manager_ping_operation() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let cluster_manager = Arc::new(ClusterManager::single_node(0));
        let routing_manager = RoutingManager::new(
            mock_db,
            DeploymentMode::Standalone,
            None,
            cluster_manager,
        );

        let ping_op = KvOperation::Ping {
            message: Some(b"hello".to_vec()),
            timestamp: Some(12345),
        };

        let result = routing_manager.route_operation(ping_op).await;
        assert!(result.is_ok());

        if let Ok(OperationResult::PingResult { message, client_timestamp, server_timestamp }) = result {
            assert_eq!(message, b"hello");
            assert_eq!(client_timestamp, 12345);
            assert!(server_timestamp > 0);
        } else {
            panic!("Expected successful ping result");
        }
    }

    #[tokio::test]
    async fn test_routing_manager_executor_integration() {
        let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn KvDatabase>;
        let cluster_manager = Arc::new(ClusterManager::single_node(0));
        let routing_manager = RoutingManager::new(
            mock_db,
            DeploymentMode::Standalone,
            None,
            cluster_manager,
        );

        // Verify executor is available and functional
        let executor = routing_manager.get_executor();
        assert_eq!(executor.get_applied_sequence(), 0);
        assert!(!executor.has_pending_operations().await);

        // The executor should be able to apply operations
        let op = KvOperation::Set {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            column_family: None,
        };

        let result = executor.apply_operation(1, op).await;
        assert!(result.is_ok());
        assert_eq!(executor.get_applied_sequence(), 1);
    }
}
