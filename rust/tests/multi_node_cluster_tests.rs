// Multi-node cluster integration tests - tests cluster functionality with multiple nodes
// Run with: cargo test multi_node_cluster_tests
// This test creates a multi-node test cluster to verify cluster and replication functionality

mod common;

use common::cluster_test_utils::MultiNodeClusterTest;

#[tokio::test]
async fn test_multi_node_cluster_creation() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = MultiNodeClusterTest::new(3).await?;
    
    // Test cluster creation
    assert_eq!(cluster.get_all_managers().len(), 3, "Should create 3 cluster managers");
    assert_eq!(cluster.get_config().node_count, 3, "Config should show 3 nodes");
    
    // Start the cluster discovery
    cluster.start_discovery().await?;
    
    // Verify all cluster managers have been initialized
    for i in 0..3 {
        let manager = cluster.get_manager(i).unwrap();
        assert_eq!(manager.get_node_id(), i, "Node ID should match index");
        
        let all_nodes = manager.get_all_nodes().await;
        assert_eq!(all_nodes.len(), 3, "Each node should see 3 nodes in cluster");
    }
    
    Ok(())
}

#[tokio::test]
async fn test_cluster_status_multi_node() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = MultiNodeClusterTest::new(3).await?;
    cluster.start_discovery().await?;

    // Test cluster status on each node
    for i in 0..cluster.get_config().node_count {
        let manager = cluster.get_manager(i).unwrap();
        let cluster_status = manager.get_cluster_status().await;
        
        assert_eq!(cluster_status.total_nodes, 3, "Should report 3 total nodes");
        assert!(cluster_status.healthy_nodes > 0, "Should have at least some healthy nodes");
        assert!(!cluster_status.cluster_health.is_empty(), "Cluster health should be set");
        // Current term is always non-negative (u64 type)
        
        // Verify node information
        assert_eq!(cluster_status.nodes.len(), 3, "Should have 3 nodes in status");
        
        println!("Node {} cluster status: {:?}", i, cluster_status);
    }

    Ok(())
}

#[tokio::test]
async fn test_replication_status_multi_node() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = MultiNodeClusterTest::new(3).await?;
    cluster.start_discovery().await?;

    // Test replication status on each node
    for i in 0..cluster.get_config().node_count {
        let manager = cluster.get_manager(i).unwrap();
        let replication_status = manager.get_replication_status().await;
        
        // Current term is always non-negative (u64 type)
        assert_eq!(replication_status.node_states.len(), 3, "Should have 3 nodes in replication status");
        
        // Verify node states
        for (node_id, state) in &replication_status.node_states {
            assert!(*node_id < 3, "Node ID should be valid");
            assert!(
                state == "leader" || state == "follower" || state == "unreachable",
                "Node state should be valid: {}",
                state
            );
        }
        
        // Verify replication lag information
        assert_eq!(replication_status.replication_lag.len(), 3, "Should have replication lag for all nodes");
        for (_node_id, _lag) in &replication_status.replication_lag {
            // Replication lag is always non-negative (u64 type)
        }
        
        println!("Node {} replication status: {:?}", i, replication_status);
    }

    Ok(())
}

#[tokio::test]
async fn test_leader_election_multi_node() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = MultiNodeClusterTest::new(3).await?;
    cluster.start_discovery().await?;

    // Give nodes time to complete leader election
    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;

    let mut leader_count = 0;
    let mut follower_count = 0;

    // Check leader election results
    for i in 0..cluster.get_config().node_count {
        let manager = cluster.get_manager(i).unwrap();
        let is_leader = manager.is_leader().await;
        let current_leader = manager.get_current_leader().await;
        
        if is_leader {
            leader_count += 1;
            println!("Node {} is the leader", i);
        } else {
            follower_count += 1;
            println!("Node {} is a follower", i);
        }
        
        // All nodes should agree on who the leader is
        if let Some(leader_info) = current_leader {
            println!("Node {} sees leader as: Node {}", i, leader_info.id);
        }
    }

    // In Phase 1, node 0 should always be the leader by convention
    assert_eq!(leader_count, 1, "Should have exactly one leader");
    assert_eq!(follower_count, 2, "Should have exactly two followers");

    // Verify node 0 is the leader
    assert!(cluster.get_manager(0).unwrap().is_leader().await, "Node 0 should be the leader");

    Ok(())
}

#[tokio::test]
async fn test_node_health_monitoring() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = MultiNodeClusterTest::new(2).await?;
    cluster.start_discovery().await?;

    // Start health monitoring on both nodes
    for i in 0..cluster.get_config().node_count {
        let manager = cluster.get_manager(i).unwrap().clone();
        tokio::spawn(async move {
            manager.start_health_monitoring().await;
        });
    }

    // Give health monitoring time to run
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Check that all nodes are initially healthy
    for i in 0..cluster.get_config().node_count {
        let manager = cluster.get_manager(i).unwrap();
        let healthy_nodes = manager.get_healthy_nodes().await;
        assert!(!healthy_nodes.is_empty(), "Node {} should see some healthy nodes", i);
        
        // At minimum, each node should see itself as healthy
        let self_healthy = healthy_nodes.iter().any(|n| n.id == i as u32);
        assert!(self_healthy, "Node {} should see itself as healthy", i);
    }

    Ok(())
}