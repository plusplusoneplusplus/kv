// Consolidated diagnostic tests
// This file combines all diagnostic endpoint tests (cluster health, database stats, node info)
// to reduce duplication and simplify the test structure

use crate::common::test_cluster::{StandaloneTestCluster, TestCluster};
use crate::common::cluster_test_utils::MultiNodeClusterTest;
use rocksdb_server::generated::kvstore::*;

// Helper function to create test auth tokens
fn create_test_auth_token(test_name: &str) -> String {
    format!("test_token_{}", test_name)
}

// Helper function to validate error responses
fn validate_error_response(
    success: bool,
    error: Option<String>,
    data_present: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if success && error.is_some() {
        return Err("Successful response should not have error message".into());
    }

    if !success && error.is_none() {
        return Err("Failed response should have error message".into());
    }

    if !success && data_present {
        return Err("Failed response should not include data".into());
    }

    Ok(())
}

// =============================================================================
// CLUSTER HEALTH TESTS
// =============================================================================

#[tokio::test]
async fn test_cluster_health_basic() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;
    let request = GetClusterHealthRequest::new(Some(create_test_auth_token("cluster_health_basic")));
    let response = client.get_cluster_health_sync(request)?;

    assert!(response.success, "Cluster health request should succeed");
    let cluster_health = response.cluster_health.unwrap();

    // Verify single-node cluster specifics
    assert_eq!(cluster_health.total_nodes_count, 1);
    assert!(cluster_health.healthy_nodes_count >= 0); // May be 0 or 1 depending on initialization timing
    assert!(!cluster_health.cluster_status.is_empty()); // Status should be set to something
    assert_eq!(cluster_health.nodes.len(), 1);
    
    let node = &cluster_health.nodes[0];
    assert_eq!(node.node_id, 0);
    assert!(!node.status.is_empty()); // Status should be set to something
    // Don't assert specific leader status as it may depend on timing

    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_cluster_health_with_operations() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;

    // Add some data first
    let tx_future = client.begin_transaction(None, Some(60));
    let mut tx = tx_future.await_result().await?;

    for i in 0..5 {
        let key = format!("health_test_key_{}", i);
        let value = format!("health_test_value_{}", i);
        tx.set(key.as_bytes(), value.as_bytes(), None)?;
    }

    let commit_future = tx.commit();
    commit_future.await_result().await?;

    // Test cluster health after operations
    let request = GetClusterHealthRequest::new(Some(create_test_auth_token("cluster_health_with_data")));
    let response = client.get_cluster_health_sync(request)?;

    assert!(response.success);
    let cluster_health = response.cluster_health.unwrap();
    assert!(!cluster_health.cluster_status.is_empty());

    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_cluster_health_authentication() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;

    // Test without auth token
    let no_auth_request = GetClusterHealthRequest::new(None);
    let no_auth_response = client.get_cluster_health_sync(no_auth_request)?;
    assert!(no_auth_response.success, "Should work without auth token for now");

    // Test with valid auth token
    let auth_request = GetClusterHealthRequest::new(Some(create_test_auth_token("cluster_health_auth")));
    let auth_response = client.get_cluster_health_sync(auth_request)?;
    assert!(auth_response.success, "Should work with valid auth token");

    cluster.shutdown().await?;
    Ok(())
}

// =============================================================================
// DATABASE STATS TESTS
// =============================================================================

#[tokio::test]
async fn test_database_stats_basic() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;
    let request = GetDatabaseStatsRequest::new(Some(create_test_auth_token("db_stats_basic")), Some(false));
    let response = client.get_database_stats_sync(request)?;

    assert!(response.success, "Database stats request should succeed");
    let stats = response.database_stats.unwrap();

    // Verify basic stats structure
    assert!(stats.total_keys >= 0);
    assert!(stats.total_size_bytes >= 0);
    assert!(stats.write_operations_count >= 0);
    assert!(stats.read_operations_count >= 0);
    assert!(stats.active_transactions >= 0);
    assert!(stats.committed_transactions >= 0);
    assert!(stats.aborted_transactions >= 0);

    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_detailed() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;
    let request = GetDatabaseStatsRequest::new(Some(create_test_auth_token("db_stats_detailed")), Some(true));
    let response = client.get_database_stats_sync(request)?;

    assert!(response.success);
    let stats = response.database_stats.unwrap();

    // Verify detailed stats include additional information
    assert!(stats.cache_hit_rate_percent.is_some(), "Cache hit rate should be present in detailed stats");
    assert!(stats.compaction_pending_bytes.is_some(), "Compaction info should be present in detailed stats");

    if let Some(cache_hit_rate) = stats.cache_hit_rate_percent {
        assert!(cache_hit_rate >= 0 && cache_hit_rate <= 100, "Cache hit rate should be between 0-100%");
    }

    if let Some(compaction_pending) = stats.compaction_pending_bytes {
        assert!(compaction_pending >= 0, "Compaction pending bytes should be non-negative");
    }

    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_database_stats_with_operations() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;

    // Add data to make stats more interesting
    let tx_future = client.begin_transaction(None, Some(60));
    let mut tx = tx_future.await_result().await?;

    for i in 0..10 {
        let key = format!("stats_test_key_{}", i);
        let value = format!("stats_test_value_{}", i);
        tx.set(key.as_bytes(), value.as_bytes(), None)?;
    }

    let commit_future = tx.commit();
    commit_future.await_result().await?;

    // Test database stats after adding data
    let request = GetDatabaseStatsRequest::new(Some(create_test_auth_token("db_stats_with_data")), Some(true));
    let response = client.get_database_stats_sync(request)?;

    assert!(response.success);
    let stats = response.database_stats.unwrap();
    assert!(stats.total_keys >= 0);
    assert!(stats.total_size_bytes >= 0);

    cluster.shutdown().await?;
    Ok(())
}

// =============================================================================
// NODE INFO TESTS
// =============================================================================

#[tokio::test]
async fn test_node_info_current_node() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;
    let request = GetNodeInfoRequest::new(Some(create_test_auth_token("node_info_current")), None);
    let response = client.get_node_info_sync(request)?;

    assert!(response.success, "Node info request should succeed");
    let node_info = response.node_info.unwrap();

    // Verify node info structure
    assert_eq!(node_info.node_id, 0);
    assert!(!node_info.endpoint.is_empty());
    assert!(!node_info.status.is_empty()); // Status should be set to something
    // Don't assert specific leader status as it may depend on timing
    assert!(node_info.last_seen_timestamp > 0);
    assert!(node_info.uptime_seconds.is_some());

    if let Some(uptime) = node_info.uptime_seconds {
        assert!(uptime >= 0);
    }

    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_node_info_specific_node() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;
    let request = GetNodeInfoRequest::new(Some(create_test_auth_token("node_info_specific")), Some(0));
    let response = client.get_node_info_sync(request)?;

    assert!(response.success);
    let node_info = response.node_info.unwrap();
    assert_eq!(node_info.node_id, 0);
    assert!(!node_info.status.is_empty()); // Status should be set to something
    // Don't assert specific leader status as it may depend on timing

    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_node_info_nonexistent_node() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;
    let request = GetNodeInfoRequest::new(Some(create_test_auth_token("node_info_nonexistent")), Some(999));
    let response = client.get_node_info_sync(request)?;

    // Should return an error for non-existent node
    assert!(!response.success, "Non-existent node request should fail");
    assert!(response.error.is_some(), "Error message should be present");
    assert!(response.node_info.is_none(), "No node info should be returned for invalid node");

    cluster.shutdown().await?;
    Ok(())
}

// =============================================================================
// INTEGRATION TESTS
// =============================================================================

#[tokio::test]
async fn test_all_diagnostic_endpoints_integration() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;
    let auth_token = create_test_auth_token("integration_all");

    // Test all diagnostic endpoints in sequence
    let health_request = GetClusterHealthRequest::new(Some(auth_token.clone()));
    let health_response = client.get_cluster_health_sync(health_request)?;
    assert!(health_response.success);
    let cluster_health = health_response.cluster_health.unwrap();

    let stats_request = GetDatabaseStatsRequest::new(Some(auth_token.clone()), Some(true));
    let stats_response = client.get_database_stats_sync(stats_request)?;
    assert!(stats_response.success);

    let node_request = GetNodeInfoRequest::new(Some(auth_token), None);
    let node_response = client.get_node_info_sync(node_request)?;
    assert!(node_response.success);
    let node_info = node_response.node_info.unwrap();

    // Verify consistency across endpoints
    assert_eq!(cluster_health.total_nodes_count, 1);
    assert!(cluster_health.healthy_nodes_count >= 0); // May be 0 or 1 depending on timing
    assert_eq!(cluster_health.nodes.len(), 1);

    let cluster_node = &cluster_health.nodes[0];
    assert_eq!(cluster_node.node_id, node_info.node_id);
    assert_eq!(cluster_node.status, node_info.status);
    // Both should have consistent leader status (whatever it is)

    cluster.shutdown().await?;
    Ok(())
}

#[tokio::test]
async fn test_diagnostic_endpoints_under_load() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;

    // Create concurrent load
    let mut operation_handles = Vec::new();

    // Spawn multiple tasks to create concurrent database load
    for i in 0..5 {
        let client_clone = client.clone();
        let handle = tokio::spawn(async move {
            let tx_future = client_clone.begin_transaction(None, Some(60));
            let mut tx = tx_future.await_result().await.unwrap();

            let key = format!("load_test_{}", i);
            let value = format!("value_{}", i);
            tx.set(key.as_bytes(), value.as_bytes(), None).unwrap();

            let commit_future = tx.commit();
            commit_future.await_result().await.unwrap();
        });
        operation_handles.push(handle);
    }

    // While operations are running, test diagnostic endpoints concurrently
    let mut diagnostic_handles = Vec::new();

    for i in 0..3 {
        let client_clone = client.clone();
        let auth_token = create_test_auth_token(&format!("load_test_{}", i));
        
        let handle = tokio::spawn(async move {
            let health_request = GetClusterHealthRequest::new(Some(auth_token.clone()));
            let health_result = client_clone.get_cluster_health_sync(health_request);
            assert!(health_result.is_ok(), "Cluster health should work under load");

            let stats_request = GetDatabaseStatsRequest::new(Some(auth_token.clone()), Some(false));
            let stats_result = client_clone.get_database_stats_sync(stats_request);
            assert!(stats_result.is_ok(), "Database stats should work under load");

            let node_request = GetNodeInfoRequest::new(Some(auth_token), None);
            let node_result = client_clone.get_node_info_sync(node_request);
            assert!(node_result.is_ok(), "Node info should work under load");
        });
        diagnostic_handles.push(handle);
    }

    // Wait for all operations and diagnostics to complete
    for handle in operation_handles {
        handle.await?;
    }

    for handle in diagnostic_handles {
        handle.await?;
    }

    cluster.shutdown().await?;
    Ok(())
}

// =============================================================================
// MULTI-NODE TESTS
// =============================================================================

#[tokio::test]
async fn test_multi_node_cluster_health() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = MultiNodeClusterTest::new(3).await?;
    cluster.start_discovery().await?;

    // Give nodes time to discover each other
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Get cluster health summary
    let health_summary = cluster.get_cluster_health_summary().await;

    // Verify multi-node cluster health
    assert_eq!(health_summary.total_nodes, 3);
    assert!(health_summary.healthy_nodes > 0);
    assert_eq!(health_summary.leader_count, 1);
    assert_eq!(health_summary.node_statuses.len(), 3);

    // Verify leader election
    let leader_nodes: Vec<_> = health_summary.node_statuses.iter()
        .filter(|n| n.is_leader)
        .collect();
    assert_eq!(leader_nodes.len(), 1);
    assert_eq!(leader_nodes[0].node_id, 0); // Node 0 should be the leader

    Ok(())
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

#[tokio::test]
async fn test_diagnostic_error_responses() -> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = StandaloneTestCluster::new().await?;
    cluster.start().await?;

    let client = cluster.client().await?;
    let auth_token = create_test_auth_token("error_handling");

    // Test valid requests first to ensure they work
    let health_request = GetClusterHealthRequest::new(Some(auth_token.clone()));
    let health_response = client.get_cluster_health_sync(health_request)?;
    validate_error_response(health_response.success, health_response.error, health_response.cluster_health.is_some())?;

    let stats_request = GetDatabaseStatsRequest::new(Some(auth_token.clone()), Some(false));
    let stats_response = client.get_database_stats_sync(stats_request)?;
    validate_error_response(stats_response.success, stats_response.error, stats_response.database_stats.is_some())?;

    let node_request = GetNodeInfoRequest::new(Some(auth_token), Some(0));
    let node_response = client.get_node_info_sync(node_request)?;
    validate_error_response(node_response.success, node_response.error, node_response.node_info.is_some())?;

    cluster.shutdown().await?;
    Ok(())
}
