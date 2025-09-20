use rocksdb_server::lib::operations::{KvOperation, OperationResult};
use rocksdb_server::lib::replication::RoutingManager;
use rocksdb_server::lib::cluster::ClusterManager;
use kv_storage_rocksdb::config::DeploymentMode;
use kv_storage_mockdb::MockDatabase;
use std::sync::Arc;

#[tokio::test]
async fn test_get_cluster_health() {
    let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn kv_storage_api::KvDatabase>;
    let cluster_manager = Arc::new(ClusterManager::single_node(0));
    
    // Initialize the cluster discovery to mark the node as healthy
    cluster_manager.start_discovery().await.expect("Failed to start discovery");
    
    let routing_manager = RoutingManager::new(
        mock_db,
        DeploymentMode::Standalone,
        Some(0),
        cluster_manager,
    );

    let operation = KvOperation::GetClusterHealth;
    let result = routing_manager.route_operation(operation).await;
    
    assert!(result.is_ok());
    if let Ok(OperationResult::ClusterHealthResult(cluster_health)) = result {
        assert_eq!(cluster_health.total_nodes_count, 1);
        assert_eq!(cluster_health.healthy_nodes_count, 1);
        assert_eq!(cluster_health.cluster_status, "healthy");
        assert!(!cluster_health.nodes.is_empty());
        assert_eq!(cluster_health.nodes[0].node_id, 0);
        assert_eq!(cluster_health.nodes[0].status, "healthy");
        assert!(cluster_health.nodes[0].is_leader);
    } else {
        panic!("Expected ClusterHealthResult");
    }
}

#[tokio::test]
async fn test_get_database_stats() {
    let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn kv_storage_api::KvDatabase>;
    let cluster_manager = Arc::new(ClusterManager::single_node(0));
    let routing_manager = RoutingManager::new(
        mock_db,
        DeploymentMode::Standalone,
        Some(0),
        cluster_manager,
    );

    let operation = KvOperation::GetDatabaseStats { include_detailed: false };
    let result = routing_manager.route_operation(operation).await;
    
    assert!(result.is_ok());
    if let Ok(OperationResult::DatabaseStatsResult(stats)) = result {
        // For now, we expect default/mock values
        assert_eq!(stats.total_keys, 0);
        assert_eq!(stats.total_size_bytes, 0);
        assert_eq!(stats.active_transactions, 0);
    } else {
        panic!("Expected DatabaseStatsResult");
    }
}


#[tokio::test]
async fn test_get_node_info() {
    let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn kv_storage_api::KvDatabase>;
    let cluster_manager = Arc::new(ClusterManager::single_node(0));
    let routing_manager = RoutingManager::new(
        mock_db,
        DeploymentMode::Standalone,
        Some(0),
        cluster_manager,
    );

    let operation = KvOperation::GetNodeInfo { node_id: Some(0) };
    let result = routing_manager.route_operation(operation).await;
    
    assert!(result.is_ok());
    if let Ok(OperationResult::NodeInfoResult(node_info)) = result {
        assert_eq!(node_info.node_id, 0);
        assert!(!node_info.endpoint.is_empty());
        assert!(node_info.uptime_seconds.is_some());
    } else {
        panic!("Expected NodeInfoResult");
    }
}

#[tokio::test]
async fn test_get_node_info_not_found() {
    let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn kv_storage_api::KvDatabase>;
    let cluster_manager = Arc::new(ClusterManager::single_node(0));
    let routing_manager = RoutingManager::new(
        mock_db,
        DeploymentMode::Standalone,
        Some(0),
        cluster_manager,
    );

    let operation = KvOperation::GetNodeInfo { node_id: Some(999) };
    let result = routing_manager.route_operation(operation).await;
    
    assert!(result.is_err());
}


#[tokio::test]
async fn test_multi_node_cluster_health() {
    let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn kv_storage_api::KvDatabase>;
    
    // Create a multi-node cluster
    let endpoints = vec![
        "localhost:9090".to_string(),
        "localhost:9091".to_string(),
        "localhost:9092".to_string(),
    ];
    let cluster_config = rocksdb_server::lib::config::ClusterConfig::default();
    let cluster_manager = Arc::new(ClusterManager::new(0, endpoints, cluster_config));
    
    let routing_manager = RoutingManager::new(
        mock_db,
        DeploymentMode::Replicated,
        Some(0),
        cluster_manager,
    );

    let operation = KvOperation::GetClusterHealth;
    let result = routing_manager.route_operation(operation).await;
    
    assert!(result.is_ok());
    if let Ok(OperationResult::ClusterHealthResult(cluster_health)) = result {
        assert_eq!(cluster_health.total_nodes_count, 3);
        assert_eq!(cluster_health.nodes.len(), 3);
        // All nodes should be in unknown state initially
        for node in &cluster_health.nodes {
            assert_eq!(node.status, "unknown");
        }
    } else {
        panic!("Expected ClusterHealthResult");
    }
}
