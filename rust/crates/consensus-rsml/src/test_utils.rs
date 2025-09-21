//! Test utilities for RSML consensus factory
//!
//! This module provides utilities and helpers for testing RSML-specific
//! factory and configuration functionality. Only available when the 'test-utils' feature is enabled.

use std::collections::HashMap;
use crate::{RsmlConsensusFactory, RsmlConfig};

/// Create a test cluster configuration
pub fn create_test_cluster_config(cluster_size: usize) -> HashMap<String, String> {
    (0..cluster_size)
        .map(|i| (format!("node-{}", i), format!("localhost:{}", 8000 + i)))
        .collect()
}

/// Create a test RSML factory for a specific node in a cluster
pub fn create_test_factory(node_id: String, cluster_members: HashMap<String, String>) -> crate::RsmlResult<RsmlConsensusFactory> {
    RsmlConsensusFactory::for_testing(node_id)
        .and_then(|mut factory| {
            let mut config = factory.config().clone();
            config.base.cluster_members = cluster_members;
            factory.update_config(config)?;
            Ok(factory)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_cluster_config() {
        let config = create_test_cluster_config(3);
        assert_eq!(config.len(), 3);
        assert_eq!(config.get("node-0"), Some(&"localhost:8000".to_string()));
        assert_eq!(config.get("node-2"), Some(&"localhost:8002".to_string()));
    }

    #[test]
    fn test_create_test_factory() {
        let cluster_config = create_test_cluster_config(3);
        let factory = create_test_factory("node-0".to_string(), cluster_config);
        assert!(factory.is_ok());

        let factory = factory.unwrap();
        assert_eq!(factory.config().base.node_id, "node-0");
        assert_eq!(factory.config().base.cluster_members.len(), 3);
    }
}