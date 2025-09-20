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

#[tokio::test]
async fn test_cluster_manager_get_cluster_status() {
    let cluster_manager = ClusterManager::single_node(0);
    
    // Start discovery to initialize the cluster state
    cluster_manager.start_discovery().await.expect("Failed to start discovery");
    
    let cluster_status = cluster_manager.get_cluster_status().await;
    
    assert_eq!(cluster_status.total_nodes, 1);
    assert_eq!(cluster_status.healthy_nodes, 1);
    assert_eq!(cluster_status.cluster_health, "healthy");
    assert!(cluster_status.current_leader.is_some());
    assert_eq!(cluster_status.current_leader.unwrap(), 0);
    assert!(cluster_status.current_term > 0);
    assert_eq!(cluster_status.nodes.len(), 1);
}

#[tokio::test]
async fn test_cluster_manager_get_replication_status() {
    let cluster_manager = ClusterManager::single_node(0);
    
    // Start discovery to initialize the cluster state
    cluster_manager.start_discovery().await.expect("Failed to start discovery");
    
    let replication_status = cluster_manager.get_replication_status().await;
    
    assert!(replication_status.current_term > 0);
    assert!(replication_status.leader_id.is_some());
    assert_eq!(replication_status.leader_id.unwrap(), 0);
    assert!(!replication_status.consensus_active); // Single node doesn't need consensus
    assert_eq!(replication_status.node_states.len(), 1);
    assert_eq!(replication_status.replication_lag.len(), 1);
    
    // In single-node mode, this node should be the leader
    assert_eq!(replication_status.node_states.get(&0), Some(&"leader".to_string()));
    assert_eq!(replication_status.replication_lag.get(&0), Some(&0));
}

#[tokio::test]
async fn test_multi_node_cluster_status() {
    let endpoints = vec![
        "localhost:9090".to_string(),
        "localhost:9091".to_string(),
        "localhost:9092".to_string(),
    ];
    let cluster_config = rocksdb_server::lib::config::ClusterConfig::default();
    let cluster_manager = ClusterManager::new(0, endpoints, cluster_config);
    
    // Start discovery to initialize the cluster state
    cluster_manager.start_discovery().await.expect("Failed to start discovery");
    
    let cluster_status = cluster_manager.get_cluster_status().await;
    
    assert_eq!(cluster_status.total_nodes, 3);
    assert_eq!(cluster_status.healthy_nodes, 1); // Only this node is healthy initially
    assert_eq!(cluster_status.cluster_health, "unhealthy"); // Majority not healthy
    assert!(cluster_status.current_leader.is_some());
    assert_eq!(cluster_status.current_leader.unwrap(), 0); // Node 0 should be leader
    assert_eq!(cluster_status.nodes.len(), 3);
}

#[tokio::test]
async fn test_multi_node_replication_status() {
    let endpoints = vec![
        "localhost:9090".to_string(),
        "localhost:9091".to_string(),
    ];
    let cluster_config = rocksdb_server::lib::config::ClusterConfig::default();
    let cluster_manager = ClusterManager::new(1, endpoints, cluster_config);
    
    // Start discovery to initialize the cluster state
    cluster_manager.start_discovery().await.expect("Failed to start discovery");
    
    let replication_status = cluster_manager.get_replication_status().await;
    
    assert!(replication_status.current_term > 0); // Term should be > 0 after discovery
    assert!(replication_status.leader_id.is_some());
    assert_eq!(replication_status.leader_id.unwrap(), 0); // Node 0 should be leader
    assert!(replication_status.consensus_active); // Multi-node needs consensus
    assert_eq!(replication_status.node_states.len(), 2);
    assert_eq!(replication_status.replication_lag.len(), 2);
    
    // Node 0 should be leader, node 1 should be follower
    assert_eq!(replication_status.node_states.get(&0), Some(&"leader".to_string()));
    assert_eq!(replication_status.node_states.get(&1), Some(&"follower".to_string()));
}

#[tokio::test]
async fn test_get_database_stats_detailed() {
    let mock_db = Arc::new(MockDatabase::new()) as Arc<dyn kv_storage_api::KvDatabase>;
    let cluster_manager = Arc::new(ClusterManager::single_node(0));
    let routing_manager = RoutingManager::new(
        mock_db,
        DeploymentMode::Standalone,
        Some(0),
        cluster_manager,
    );

    let operation = KvOperation::GetDatabaseStats { include_detailed: true };
    let result = routing_manager.route_operation(operation).await;
    
    assert!(result.is_ok());
    if let Ok(OperationResult::DatabaseStatsResult(stats)) = result {
        // With detailed stats, we should get some placeholder values
        assert_eq!(stats.total_keys, 100); // Placeholder value
        assert_eq!(stats.total_size_bytes, 1024 * 1024); // 1MB placeholder
        assert_eq!(stats.cache_hit_rate_percent, Some(85));
        assert_eq!(stats.compaction_pending_bytes, Some(0));
        assert_eq!(stats.average_response_time_ms.into_inner(), 1.5);
    } else {
        panic!("Expected DatabaseStatsResult");
    }
}

#[tokio::test]
async fn test_cluster_manager_node_operations() {
    let cluster_manager = ClusterManager::single_node(0);
    
    // Test getting node ID
    assert_eq!(cluster_manager.get_node_id(), 0);
    
    // Start discovery to initialize the cluster state
    cluster_manager.start_discovery().await.expect("Failed to start discovery");
    
    // Test getting current term
    let term = cluster_manager.get_current_term().await;
    assert!(term > 0);
    
    // Test getting node info
    let node_info = cluster_manager.get_node_info(0).await;
    assert!(node_info.is_some());
    let node = node_info.unwrap();
    assert_eq!(node.id, 0);
    assert!(node.is_leader);
    
    // Test getting non-existent node
    let missing_node = cluster_manager.get_node_info(999).await;
    assert!(missing_node.is_none());
}
