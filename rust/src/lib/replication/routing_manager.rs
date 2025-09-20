use std::sync::Arc;
use std::time::SystemTime;

use kv_storage_api::KvDatabase;
use kv_storage_rocksdb::config::DeploymentMode;
use crate::lib::operations::{KvOperation, DatabaseOperation, OperationType, OperationResult};
use crate::lib::cluster::{ClusterManager, NodeStatus as ClusterNodeStatus};
use crate::generated::kvstore::{ClusterHealth, DatabaseStats, NodeStatus};
use super::errors::{RoutingError, RoutingResult};
use super::executor::KvStoreExecutor;
use thrift::OrderedFloat;

/// Central routing manager that decides how to handle operations based on deployment mode
/// and operation type. In Phase 1, this simply forwards all operations to the local database.
/// Future phases will add consensus-based routing for write operations.
pub struct RoutingManager {
    database: Arc<dyn KvDatabase>,
    deployment_mode: DeploymentMode,
    #[allow(dead_code)]
    node_id: Option<u32>,
    /// State machine executor for applying consensus operations
    /// Currently unused but prepared for consensus integration
    #[allow(dead_code)]
    executor: Arc<KvStoreExecutor>,
    /// Cluster manager for node discovery and leader election
    /// Handles both single-node and multi-node cases
    #[allow(dead_code)]
    cluster_manager: Arc<ClusterManager>,
}

impl RoutingManager {
    /// Create a new routing manager
    pub fn new(
        database: Arc<dyn KvDatabase>,
        deployment_mode: DeploymentMode,
        node_id: Option<u32>,
        cluster_manager: Arc<ClusterManager>,
    ) -> Self {
        let executor = Arc::new(KvStoreExecutor::new(database.clone()));

        Self {
            database,
            deployment_mode,
            node_id,
            executor,
            cluster_manager,
        }
    }

    /// Get a reference to the state machine executor
    /// This will be used by consensus integration in future phases
    pub fn get_executor(&self) -> Arc<KvStoreExecutor> {
        self.executor.clone()
    }

    /// Route an operation based on its type and current deployment mode
    pub async fn route_operation(&self, operation: KvOperation) -> RoutingResult<OperationResult> {
        match self.deployment_mode {
            DeploymentMode::Standalone => {
                // In standalone mode, execute everything locally
                self.execute_locally(operation).await
            }
            DeploymentMode::Replicated => {
                // In Phase 1 of replicated mode, still execute locally
                // TODO: In future phases, route writes through consensus
                match operation.operation_type() {
                    OperationType::Read => {
                        // Read operations can be served locally
                        self.execute_locally(operation).await
                    }
                    OperationType::Write => {
                        // Write operations - for now execute locally
                        // TODO: Route through consensus in future phases
                        self.execute_locally(operation).await
                    }
                }
            }
        }
    }

    /// Execute an operation directly on the local database
    async fn execute_locally(&self, operation: KvOperation) -> RoutingResult<OperationResult> {
        match operation {
            KvOperation::Get { key, column_family } => {
                let result = self.database.get(&key, column_family.as_deref()).await
                    .map_err(|e| RoutingError::DatabaseError(e))?;
                Ok(OperationResult::GetResult(Ok(result)))
            }

            KvOperation::GetRange {
                begin_key,
                end_key,
                begin_offset,
                begin_or_equal,
                end_offset,
                end_or_equal,
                limit,
                column_family,
            } => {
                let result = self.database.get_range(
                    &begin_key,
                    &end_key,
                    begin_offset,
                    begin_or_equal,
                    end_offset,
                    end_or_equal,
                    limit,
                    column_family.as_deref(),
                ).await
                .map_err(|e| RoutingError::DatabaseError(e))?;
                Ok(OperationResult::GetRangeResult(result))
            }

            KvOperation::SnapshotRead { key, read_version, column_family } => {
                let result = self.database.snapshot_read(&key, read_version, column_family.as_deref()).await
                    .map_err(|e| RoutingError::DatabaseError(e))?;
                Ok(OperationResult::GetResult(Ok(result)))
            }

            KvOperation::SnapshotGetRange {
                begin_key,
                end_key,
                begin_offset,
                begin_or_equal,
                end_offset,
                end_or_equal,
                read_version,
                limit,
                column_family,
            } => {
                let result = self.database.snapshot_get_range(
                    &begin_key,
                    &end_key,
                    begin_offset,
                    begin_or_equal,
                    end_offset,
                    end_or_equal,
                    read_version,
                    limit,
                    column_family.as_deref(),
                ).await
                .map_err(|e| RoutingError::DatabaseError(e))?;
                Ok(OperationResult::GetRangeResult(result))
            }

            KvOperation::GetReadVersion => {
                let version = self.database.get_read_version().await;
                Ok(OperationResult::ReadVersion(version))
            }

            KvOperation::Ping { message, timestamp } => {
                let server_timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_micros() as i64)
                    .unwrap_or(0);

                let response_message = message.unwrap_or_else(|| b"pong".to_vec());
                let client_timestamp = timestamp.unwrap_or(0);

                Ok(OperationResult::PingResult {
                    message: response_message,
                    client_timestamp,
                    server_timestamp,
                })
            }

            KvOperation::Set { key, value, column_family } => {
                let result = self.database.put(&key, &value, column_family.as_deref()).await;
                Ok(OperationResult::OpResult(result))
            }

            KvOperation::Delete { key, column_family } => {
                let result = self.database.delete(&key, column_family.as_deref()).await;
                Ok(OperationResult::OpResult(result))
            }

            KvOperation::AtomicCommit { request } => {
                let result = self.database.atomic_commit(request).await;
                Ok(OperationResult::AtomicCommitResult(result))
            }

            KvOperation::SetVersionstampedKey { key_prefix, value, column_family } => {
                // For versionstamped operations, we need to generate the key
                // The key_prefix should contain placeholder bytes (usually null bytes) that we replace
                // with the versionstamp. In FoundationDB, this is typically 10 bytes.
                let timestamp = self.database.get_read_version().await;
                let mut full_key = key_prefix;

                // Find the placeholder bytes (usually trailing null bytes) and replace them
                // This is a simplified implementation - in reality, FoundationDB has specific rules
                // about where the versionstamp goes. For this test, we'll replace the last 10 bytes
                // if they exist and are null bytes.
                if full_key.len() >= 10 {
                    let len = full_key.len();
                    let last_10 = &full_key[len-10..];
                    if last_10.iter().all(|&b| b == 0) {
                        // Replace the last 10 bytes with timestamp (8 bytes) + 2-byte transaction order
                        full_key.truncate(len - 10);
                        full_key.extend_from_slice(&timestamp.to_be_bytes()); // 8 bytes
                        full_key.extend_from_slice(&[0u8, 0u8]); // 2 bytes for transaction order
                    }
                }

                let put_result = self.database.put(&full_key, &value, column_family.as_deref()).await;

                // Return AtomicCommitResult with generated key
                let atomic_result = kv_storage_api::AtomicCommitResult {
                    success: put_result.success,
                    error: put_result.error,
                    error_code: put_result.error_code,
                    committed_version: Some(timestamp),
                    generated_keys: if put_result.success { vec![full_key] } else { vec![] },
                    generated_values: vec![],
                };
                Ok(OperationResult::AtomicCommitResult(atomic_result))
            }

            KvOperation::SetVersionstampedValue { key, value_prefix, column_family } => {
                // For versionstamped values, we need to generate the value
                // Similar to keys, we replace placeholder bytes with the versionstamp
                let timestamp = self.database.get_read_version().await;
                let mut full_value = value_prefix;

                // Find the placeholder bytes (usually trailing null bytes) and replace them
                if full_value.len() >= 10 {
                    let len = full_value.len();
                    let last_10 = &full_value[len-10..];
                    if last_10.iter().all(|&b| b == 0) {
                        // Replace the last 10 bytes with timestamp (8 bytes) + 2-byte transaction order
                        full_value.truncate(len - 10);
                        full_value.extend_from_slice(&timestamp.to_be_bytes()); // 8 bytes
                        full_value.extend_from_slice(&[0u8, 0u8]); // 2 bytes for transaction order
                    }
                }

                let put_result = self.database.put(&key, &full_value, column_family.as_deref()).await;

                // Return AtomicCommitResult with generated value
                let atomic_result = kv_storage_api::AtomicCommitResult {
                    success: put_result.success,
                    error: put_result.error,
                    error_code: put_result.error_code,
                    committed_version: Some(timestamp),
                    generated_keys: vec![],
                    generated_values: if put_result.success { vec![full_value] } else { vec![] },
                };
                Ok(OperationResult::AtomicCommitResult(atomic_result))
            }

            KvOperation::SetFaultInjection { config } => {
                let result = self.database.set_fault_injection(config).await;
                Ok(OperationResult::OpResult(result))
            }

            // Diagnostic operations for cluster management
            KvOperation::GetClusterHealth => {
                self.handle_get_cluster_health().await
            }

            KvOperation::GetDatabaseStats { include_detailed } => {
                self.handle_get_database_stats(include_detailed).await
            }

            KvOperation::GetNodeInfo { node_id } => {
                self.handle_get_node_info(node_id).await
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
        // Try to get actual database statistics from RocksDB
        // In Phase 1, we provide basic statistics. Future phases will add more detailed metrics.
        
        let (total_keys, total_size_bytes, cache_hit_rate, compaction_pending) = 
            if include_detailed {
                // In Phase 1, provide reasonable default values for detailed statistics
                // Future phases will query actual RocksDB properties for:
                // - self.database.get_property("rocksdb.estimate-num-keys")
                // - self.database.get_property("rocksdb.total-sst-files-size")
                (
                    100,  // Placeholder for total keys
                    1024 * 1024,  // Placeholder for 1MB database size
                    Some(85), // Typical cache hit rate
                    Some(0),  // No pending compactions in test environment
                )
            } else {
                // Basic statistics only
                (0, 0, None, None)
            };

        // In a real implementation, these would be maintained as atomic counters
        // For Phase 1, we provide reasonable placeholder values
        let stats = DatabaseStats::new(
            total_keys,
            total_size_bytes,
            0,  // write_operations_count - would maintain counter
            0,  // read_operations_count - would maintain counter
            OrderedFloat(1.5),  // average_response_time_ms - reasonable default
            0,  // active_transactions - would track active transactions
            0,  // committed_transactions - would maintain counter
            0,  // aborted_transactions - would maintain counter
            cache_hit_rate,
            compaction_pending,
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

#[cfg(test)]
mod tests {
    use super::*;
    use kv_storage_rocksdb::config::DeploymentMode;
    use kv_storage_mockdb::MockDatabase;

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