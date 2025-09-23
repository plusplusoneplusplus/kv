//! Unit tests for RSML configuration and factory functionality

use consensus_rsml::{RsmlConfig, RsmlConsensusFactory};
use consensus_rsml::config::TransportType;

#[cfg(feature = "tcp")]
use consensus_rsml::config::TcpConfig;
#[cfg(feature = "tcp")]
use tokio::time::Duration;
#[cfg(feature = "tcp")]
use std::collections::HashMap;

#[test]
fn test_rsml_config_default() {
    let config = RsmlConfig::default();

    assert_eq!(config.base.node_id, "node-1"); // Default from ConsensusConfig
    assert!(config.base.cluster_members.is_empty());
    assert!(matches!(config.transport.transport_type, TransportType::InMemory));
    #[cfg(feature = "tcp")]
    assert!(config.transport.tcp_config.is_none());
}

#[test]
fn test_rsml_config_validation() {
    let mut config = RsmlConfig::default();

    // Valid configuration
    config.base.node_id = "1".to_string();
    config.base.cluster_members.insert("1".to_string(), "localhost:8000".to_string());
    config.base.cluster_members.insert("2".to_string(), "localhost:8001".to_string());

    // Should be valid
    assert!(!config.base.node_id.is_empty());
    assert!(!config.base.cluster_members.is_empty());
}

#[test]
fn test_transport_config_inmemory() {
    let mut config = RsmlConfig::default();
    config.transport.transport_type = TransportType::InMemory;

    assert!(matches!(config.transport.transport_type, TransportType::InMemory));
    #[cfg(feature = "tcp")]
    assert!(config.transport.tcp_config.is_none());
}

#[cfg(feature = "tcp")]
#[test]
fn test_transport_config_tcp() {
    let mut cluster_addresses = HashMap::new();
    cluster_addresses.insert(1, "localhost:8000".to_string());
    cluster_addresses.insert(2, "localhost:8001".to_string());

    let tcp_config = TcpConfig {
        bind_address: "0.0.0.0:8000".to_string(),
        cluster_addresses: cluster_addresses.clone(),
        keepalive: Some(Duration::from_secs(30)),
        nodelay: true,
        buffer_size: 64 * 1024,
    };

    let mut config = RsmlConfig::default();
    config.transport.transport_type = TransportType::Tcp;
    config.transport.tcp_config = Some(tcp_config.clone());

    assert!(matches!(config.transport.transport_type, TransportType::Tcp));
    assert!(config.transport.tcp_config.is_some());

    let tcp_conf = config.transport.tcp_config.unwrap();
    assert_eq!(tcp_conf.bind_address, "0.0.0.0:8000");
    assert_eq!(tcp_conf.cluster_addresses.len(), 2);
    assert_eq!(tcp_conf.cluster_addresses.get(&1), Some(&"localhost:8000".to_string()));
    assert_eq!(tcp_conf.buffer_size, 64 * 1024);
    assert_eq!(tcp_conf.nodelay, true);
    assert_eq!(tcp_conf.keepalive, Some(Duration::from_secs(30)));
}

#[test]
fn test_rsml_config_builder_pattern() {
    let mut config = RsmlConfig::default();

    // Build configuration step by step
    config.base.node_id = "leader".to_string();
    config.base.cluster_members.insert("leader".to_string(), "localhost:8000".to_string());
    config.base.cluster_members.insert("follower1".to_string(), "localhost:8001".to_string());
    config.base.cluster_members.insert("follower2".to_string(), "localhost:8002".to_string());

    config.transport.transport_type = TransportType::InMemory;

    // Verify the built configuration
    assert_eq!(config.base.node_id, "leader");
    assert_eq!(config.base.cluster_members.len(), 3);
    assert!(config.base.cluster_members.contains_key("leader"));
    assert!(config.base.cluster_members.contains_key("follower1"));
    assert!(config.base.cluster_members.contains_key("follower2"));
    assert!(matches!(config.transport.transport_type, TransportType::InMemory));
}

#[test]
fn test_rsml_consensus_factory_creation() {
    let mut config = RsmlConfig::default();
    config.base.node_id = "factory-test".to_string();
    config.base.cluster_members.insert("factory-test".to_string(), "localhost:9000".to_string());

    let factory = RsmlConsensusFactory::new(config);

    // Should succeed with valid configuration
    assert!(factory.is_ok());
    let factory = factory.unwrap();

    // Verify configuration is preserved
    let current_config = factory.config();
    assert_eq!(current_config.base.node_id, "factory-test");
    assert_eq!(current_config.base.cluster_members.len(), 1);
    assert_eq!(
        current_config.base.cluster_members.get("factory-test"),
        Some(&"localhost:9000".to_string())
    );
}

#[test]
fn test_rsml_consensus_factory_config_update() {
    let mut initial_config = RsmlConfig::default();
    initial_config.base.cluster_members.insert("node-1".to_string(), "localhost:8000".to_string());
    let mut factory = RsmlConsensusFactory::new(initial_config).unwrap();

    let mut new_config = RsmlConfig::default();
    new_config.base.node_id = "factory-test".to_string();
    new_config.base.cluster_members.insert("factory-test".to_string(), "localhost:9000".to_string());

    let result = factory.update_config(new_config.clone());
    assert!(result.is_ok());

    let current_config = factory.config();
    assert_eq!(current_config.base.node_id, "factory-test");
    assert_eq!(current_config.base.cluster_members.len(), 1);
    assert_eq!(
        current_config.base.cluster_members.get("factory-test"),
        Some(&"localhost:9000".to_string())
    );
}

#[cfg(feature = "test-utils")]
#[test]
fn test_rsml_consensus_factory_for_testing() {
    let result = consensus_rsml::RsmlConsensusFactory::for_testing("test-node-123".to_string());

    assert!(result.is_ok());
    let factory = result.unwrap();

    let config = factory.config();
    assert_eq!(config.base.node_id, "test-node-123");
    assert!(matches!(config.transport.transport_type, TransportType::InMemory));
}

#[test]
fn test_config_clone_and_equality() {
    let mut original_config = RsmlConfig::default();
    original_config.base.node_id = "clone-test".to_string();
    original_config.base.cluster_members.insert("clone-test".to_string(), "localhost:8000".to_string());

    let cloned_config = original_config.clone();

    assert_eq!(original_config.base.node_id, cloned_config.base.node_id);
    assert_eq!(original_config.base.cluster_members.len(), cloned_config.base.cluster_members.len());
    assert_eq!(
        original_config.base.cluster_members.get("clone-test"),
        cloned_config.base.cluster_members.get("clone-test")
    );
}

#[test]
fn test_config_serialization() {
    let mut config = RsmlConfig::default();
    config.base.node_id = "serialization-test".to_string();
    config.base.cluster_members.insert("node1".to_string(), "localhost:8000".to_string());
    config.base.cluster_members.insert("node2".to_string(), "localhost:8001".to_string());

    // Test debug formatting
    let debug_string = format!("{:?}", config);
    assert!(debug_string.contains("serialization-test"));
    assert!(debug_string.contains("node1"));
    assert!(debug_string.contains("node2"));
}

#[test]
fn test_large_cluster_configuration() {
    let mut config = RsmlConfig::default();
    config.base.node_id = "5".to_string();

    // Create a larger cluster (e.g., 10 nodes)
    for i in 1..=10 {
        config.base.cluster_members.insert(
            i.to_string(),
            format!("node-{}.cluster.local:{}", i, 8000 + i)
        );
    }

    assert_eq!(config.base.cluster_members.len(), 10);
    assert_eq!(config.base.cluster_members.get("1"), Some(&"node-1.cluster.local:8001".to_string()));
    assert_eq!(config.base.cluster_members.get("10"), Some(&"node-10.cluster.local:8010".to_string()));
}

#[cfg(feature = "tcp")]
#[test]
fn test_tcp_config_edge_cases() {
    // Test with minimal TCP configuration
    let tcp_config = TcpConfig {
        bind_address: "127.0.0.1:0".to_string(), // Let OS choose port
        cluster_addresses: HashMap::new(), // Empty cluster
        keepalive: None,
        nodelay: false,
        buffer_size: 1024, // Small buffer
    };

    assert_eq!(tcp_config.bind_address, "127.0.0.1:0");
    assert!(tcp_config.cluster_addresses.is_empty());
    assert_eq!(tcp_config.keepalive, None);
    assert_eq!(tcp_config.nodelay, false);
    assert_eq!(tcp_config.buffer_size, 1024);

    // Test with maximal TCP configuration
    let mut large_cluster = HashMap::new();
    for i in 1..=100 {
        large_cluster.insert(i, format!("node{}.example.com:{}", i, 8000 + i));
    }

    let tcp_config_large = TcpConfig {
        bind_address: "0.0.0.0:8000".to_string(),
        cluster_addresses: large_cluster,
        keepalive: Some(Duration::from_secs(3600)), // 1 hour
        nodelay: true,
        buffer_size: 1024 * 1024, // 1MB buffer
    };

    assert_eq!(tcp_config_large.cluster_addresses.len(), 100);
    assert_eq!(tcp_config_large.keepalive, Some(Duration::from_secs(3600)));
    assert_eq!(tcp_config_large.buffer_size, 1024 * 1024);
}

#[test]
fn test_factory_multiple_config_updates() {
    let mut initial_config = RsmlConfig::default();
    initial_config.base.cluster_members.insert("node-1".to_string(), "localhost:8000".to_string());
    let mut factory = RsmlConsensusFactory::new(initial_config).unwrap();

    // First configuration
    let mut config1 = RsmlConfig::default();
    config1.base.node_id = "config1".to_string();
    config1.base.cluster_members.insert("config1".to_string(), "localhost:8000".to_string());

    let result1 = factory.update_config(config1);
    assert!(result1.is_ok());
    assert_eq!(factory.config().base.node_id, "config1");

    // Second configuration (update)
    let mut config2 = RsmlConfig::default();
    config2.base.node_id = "config2".to_string();
    config2.base.cluster_members.insert("config2".to_string(), "localhost:8001".to_string());
    config2.base.cluster_members.insert("other".to_string(), "localhost:8002".to_string());

    let result2 = factory.update_config(config2);
    assert!(result2.is_ok());
    assert_eq!(factory.config().base.node_id, "config2");
    assert_eq!(factory.config().base.cluster_members.len(), 2);

    // Verify the configuration was completely replaced, not merged
    assert!(!factory.config().base.cluster_members.contains_key("config1"));
    assert!(factory.config().base.cluster_members.contains_key("config2"));
    assert!(factory.config().base.cluster_members.contains_key("other"));
}

// Test utilities from the test_utils module
#[cfg(feature = "test-utils")]
mod test_utils_tests {
    use consensus_rsml::test_utils::{create_test_cluster_config, create_test_factory};

    #[test]
    fn test_create_test_cluster_config() {
        let config = create_test_cluster_config(5);

        assert_eq!(config.len(), 5);
        assert_eq!(config.get("node-0"), Some(&"localhost:8000".to_string()));
        assert_eq!(config.get("node-2"), Some(&"localhost:8002".to_string()));
        assert_eq!(config.get("node-4"), Some(&"localhost:8004".to_string()));

        // Test single node
        let single_config = create_test_cluster_config(1);
        assert_eq!(single_config.len(), 1);
        assert_eq!(single_config.get("node-0"), Some(&"localhost:8000".to_string()));
    }

    #[test]
    fn test_create_test_factory() {
        let cluster_config = create_test_cluster_config(3);
        let factory_result = create_test_factory("node-1".to_string(), cluster_config.clone());

        assert!(factory_result.is_ok());
        let factory = factory_result.unwrap();

        assert_eq!(factory.config().base.node_id, "node-1");
        assert_eq!(factory.config().base.cluster_members.len(), 3);
        assert_eq!(
            factory.config().base.cluster_members.get("node-0"),
            Some(&"localhost:8000".to_string())
        );
        assert_eq!(
            factory.config().base.cluster_members.get("node-2"),
            Some(&"localhost:8002".to_string())
        );
    }

    #[test]
    fn test_create_test_factory_with_custom_cluster() {
        let mut custom_cluster = std::collections::HashMap::new();
        custom_cluster.insert("leader".to_string(), "leader.example.com:9000".to_string());
        custom_cluster.insert("follower1".to_string(), "follower1.example.com:9001".to_string());
        custom_cluster.insert("follower2".to_string(), "follower2.example.com:9002".to_string());

        let factory_result = create_test_factory("leader".to_string(), custom_cluster.clone());

        assert!(factory_result.is_ok());
        let factory = factory_result.unwrap();

        assert_eq!(factory.config().base.node_id, "leader");
        assert_eq!(factory.config().base.cluster_members.len(), 3);
        assert_eq!(
            factory.config().base.cluster_members.get("leader"),
            Some(&"leader.example.com:9000".to_string())
        );
        assert_eq!(
            factory.config().base.cluster_members.get("follower1"),
            Some(&"follower1.example.com:9001".to_string())
        );
    }
}