//! RSML consensus engine factory implementation
//!
//! This module implements the factory pattern for creating RSML consensus engines
//! with proper configuration validation and feature flag management.

use consensus_api::{ConsensusEngine, StateMachine};
use std::sync::Arc;
use tracing::info;
#[cfg(feature = "test-utils")]
use tracing::warn;

use crate::{RsmlConfig, RsmlResult, RsmlError};

#[cfg(feature = "rsml")]
use crate::RsmlConsensusEngine;

/// Factory for creating RSML consensus engines
///
/// This factory provides engine-agnostic creation of consensus engines
/// while preserving RSML-specific configuration and error handling.
#[derive(Debug)]
pub struct RsmlConsensusFactory {
    config: RsmlConfig,
}

impl RsmlConsensusFactory {
    /// Create a new RSML consensus factory with the given configuration
    ///
    /// # Arguments
    /// * `config` - RSML-specific configuration
    ///
    /// # Returns
    /// * `Ok(RsmlConsensusFactory)` - Successfully created factory
    /// * `Err(RsmlError)` - Configuration validation failed
    pub fn new(config: RsmlConfig) -> RsmlResult<Self> {
        // Validate configuration before creating factory
        config.validate()?;

        info!("Creating RSML consensus factory for node: {}", config.base.node_id);

        // Check feature availability
        Self::validate_feature_availability(&config)?;

        Ok(Self { config })
    }

    /// Create a consensus engine with the given state machine
    ///
    /// # Arguments
    /// * `state_machine` - Application state machine implementation
    ///
    /// # Returns
    /// * `Ok(Box<dyn ConsensusEngine>)` - Successfully created consensus engine
    /// * `Err(RsmlError)` - Engine creation failed
    #[cfg(feature = "rsml")]
    pub async fn create_engine(
        &self,
        state_machine: Arc<dyn StateMachine>,
    ) -> RsmlResult<Box<dyn ConsensusEngine>> {
        info!("Creating RSML consensus engine for node: {}", self.config.base.node_id);

        // Create the appropriate engine based on configuration
        match self.config.transport.transport_type {
            crate::config::TransportType::InMemory => {
                self.create_in_memory_engine(state_machine).await
            }
            #[cfg(feature = "tcp")]
            crate::config::TransportType::Tcp => {
                self.create_tcp_engine(state_machine).await
            }
        }
    }

    /// Create a consensus engine with the given state machine (stub for when rsml feature is disabled)
    ///
    /// # Arguments
    /// * `state_machine` - Application state machine implementation
    ///
    /// # Returns
    /// * `Err(RsmlError)` - RSML feature not enabled
    #[cfg(not(feature = "rsml"))]
    pub async fn create_engine(
        &self,
        _state_machine: Arc<dyn StateMachine>,
    ) -> RsmlResult<Box<dyn ConsensusEngine>> {
        Err(crate::RsmlError::ConfigurationError {
            field: "rsml".to_string(),
            message: "RSML feature not enabled. Enable with --features rsml".to_string(),
        })
    }

    /// Create a consensus engine with a pre-configured KvExecutor
    ///
    /// This method is designed for shard server integration where the KvStoreExecutor
    /// is already created and needs to be connected to RSML's execution notifier.
    ///
    /// # Arguments
    /// * `executor` - Pre-configured KV executor (typically wrapped KvStoreExecutor)
    ///
    /// # Returns
    /// * `Ok(Box<dyn ConsensusEngine>)` - Successfully created consensus engine
    /// * `Err(RsmlError)` - Engine creation failed
    #[cfg(feature = "rsml")]
    pub async fn create_engine_with_executor(
        &self,
        executor: Arc<dyn crate::execution::KvExecutor>,
    ) -> RsmlResult<Box<dyn ConsensusEngine>> {
        info!("Creating RSML consensus engine with external executor for node: {}", self.config.base.node_id);

        // Create the appropriate engine based on configuration
        match self.config.transport.transport_type {
            crate::config::TransportType::InMemory => {
                self.create_in_memory_engine_with_executor(executor).await
            }
            #[cfg(feature = "tcp")]
            crate::config::TransportType::Tcp => {
                self.create_tcp_engine_with_executor(executor).await
            }
        }
    }

    /// Create a consensus engine with a pre-configured KvExecutor (stub for when rsml feature is disabled)
    ///
    /// # Arguments
    /// * `executor` - Pre-configured KV executor
    ///
    /// # Returns
    /// * `Err(RsmlError)` - RSML feature not enabled
    #[cfg(not(feature = "rsml"))]
    pub async fn create_engine_with_executor(
        &self,
        _executor: Arc<dyn crate::execution::KvExecutor>,
    ) -> RsmlResult<Box<dyn ConsensusEngine>> {
        Err(crate::RsmlError::ConfigurationError {
            field: "rsml".to_string(),
            message: "RSML feature not enabled. Enable with --features rsml".to_string(),
        })
    }

    /// Get a reference to the factory's configuration
    pub fn config(&self) -> &RsmlConfig {
        &self.config
    }

    /// Update the factory's configuration
    ///
    /// # Arguments
    /// * `config` - New RSML configuration
    ///
    /// # Returns
    /// * `Ok(())` - Configuration updated successfully
    /// * `Err(RsmlError)` - Configuration validation failed
    pub fn update_config(&mut self, config: RsmlConfig) -> RsmlResult<()> {
        config.validate()?;
        Self::validate_feature_availability(&config)?;

        info!("Updating RSML consensus factory configuration for node: {}", config.base.node_id);
        self.config = config;
        Ok(())
    }

    /// Validate that required features are available for the configuration
    fn validate_feature_availability(_config: &RsmlConfig) -> RsmlResult<()> {
        // Feature validation is handled at compile time through conditional compilation
        // of the TransportType enum variants and config fields
        Ok(())
    }

    /// Create an in-memory consensus engine for testing
    #[cfg(feature = "rsml")]
    async fn create_in_memory_engine(
        &self,
        state_machine: Arc<dyn StateMachine>,
    ) -> RsmlResult<Box<dyn ConsensusEngine>> {
        info!("Creating in-memory RSML consensus engine");

        // Create the RSML consensus engine
        let engine = RsmlConsensusEngine::new(self.config.clone(), state_machine).await?;

        Ok(Box::new(engine))
    }

    /// Create a TCP-based consensus engine for production
    #[cfg(all(feature = "rsml", feature = "tcp"))]
    async fn create_tcp_engine(
        &self,
        state_machine: Arc<dyn StateMachine>,
    ) -> RsmlResult<Box<dyn ConsensusEngine>> {
        info!("Creating TCP RSML consensus engine");

        let tcp_config = self.config.transport.tcp_config.as_ref()
            .ok_or_else(|| RsmlError::ConfigurationError {
                field: "transport.tcp_config".to_string(),
                message: "TCP configuration required for TCP transport".to_string(),
            })?;

        info!("TCP engine will bind to: {}", tcp_config.bind_address);
        info!("TCP engine cluster size: {}", tcp_config.cluster_addresses.len());

        // Create the RSML consensus engine with TCP transport
        let engine = RsmlConsensusEngine::new(self.config.clone(), state_machine).await?;

        Ok(Box::new(engine))
    }

    /// Create an in-memory consensus engine with external executor
    #[cfg(feature = "rsml")]
    async fn create_in_memory_engine_with_executor(
        &self,
        executor: Arc<dyn crate::execution::KvExecutor>,
    ) -> RsmlResult<Box<dyn ConsensusEngine>> {
        info!("Creating in-memory RSML consensus engine with external executor");

        // Create the RSML consensus engine with external executor
        let engine = RsmlConsensusEngine::new_with_executor(self.config.clone(), executor).await?;

        Ok(Box::new(engine))
    }

    /// Create a TCP-based consensus engine with external executor
    #[cfg(all(feature = "rsml", feature = "tcp"))]
    async fn create_tcp_engine_with_executor(
        &self,
        executor: Arc<dyn crate::execution::KvExecutor>,
    ) -> RsmlResult<Box<dyn ConsensusEngine>> {
        info!("Creating TCP RSML consensus engine with external executor");

        let tcp_config = self.config.transport.tcp_config.as_ref()
            .ok_or_else(|| RsmlError::ConfigurationError {
                field: "transport.tcp_config".to_string(),
                message: "TCP configuration required for TCP transport".to_string(),
            })?;

        info!("TCP engine will bind to: {}", tcp_config.bind_address);
        info!("TCP engine cluster size: {}", tcp_config.cluster_addresses.len());

        // Create the RSML consensus engine with TCP transport and external executor
        let engine = RsmlConsensusEngine::new_with_executor(self.config.clone(), executor).await?;

        Ok(Box::new(engine))
    }

    /// Create a factory with default configuration for the given node
    ///
    /// This is a convenience method for quick setup with sensible defaults.
    ///
    /// # Arguments
    /// * `node_id` - Identifier for this consensus node
    /// * `cluster_members` - Map of node IDs to their addresses
    ///
    /// # Returns
    /// * `Ok(RsmlConsensusFactory)` - Successfully created factory
    /// * `Err(RsmlError)` - Configuration validation failed
    pub fn with_defaults(
        node_id: String,
        cluster_members: std::collections::HashMap<String, String>,
    ) -> RsmlResult<Self> {
        let mut config = RsmlConfig::default();
        config.base.node_id = node_id;
        config.base.cluster_members = cluster_members;

        Self::new(config)
    }

    /// Create a factory configured for testing
    ///
    /// This creates a factory with in-memory transport and testing-friendly
    /// timeouts and settings.
    #[cfg(feature = "test-utils")]
    pub fn for_testing(node_id: String) -> RsmlResult<Self> {
        let mut config = RsmlConfig::default();
        config.base.node_id = node_id.clone();
        config.base.cluster_members.insert(node_id, "localhost:0".to_string());

        // Configure for fast testing
        config.transport.transport_type = crate::config::TransportType::InMemory;
        config.view_config.view_change_timeout = std::time::Duration::from_millis(100);
        config.performance.batch_timeout = std::time::Duration::from_millis(1);

        warn!("Creating RSML factory for testing - not suitable for production");
        Self::new(config)
    }
}

/// Builder pattern for creating RSML consensus factory with fluent API
#[derive(Debug)]
pub struct RsmlFactoryBuilder {
    config: RsmlConfig,
}

impl RsmlFactoryBuilder {
    /// Create a new factory builder with default configuration
    pub fn new() -> Self {
        Self {
            config: RsmlConfig::default(),
        }
    }

    /// Set the node ID
    pub fn node_id(mut self, node_id: String) -> Self {
        self.config.base.node_id = node_id;
        self
    }

    /// Add a cluster member
    pub fn cluster_member(mut self, node_id: String, address: String) -> Self {
        self.config.base.cluster_members.insert(node_id, address);
        self
    }

    /// Set cluster members
    pub fn cluster_members(mut self, members: std::collections::HashMap<String, String>) -> Self {
        self.config.base.cluster_members = members;
        self
    }

    /// Set transport type to in-memory
    pub fn in_memory_transport(mut self) -> Self {
        self.config.transport.transport_type = crate::config::TransportType::InMemory;
        self
    }

    /// Set transport type to TCP
    #[cfg(feature = "tcp")]
    pub fn tcp_transport(mut self, bind_address: String) -> Self {
        use std::time::Duration;
        self.config.transport.transport_type = crate::config::TransportType::Tcp;
        self.config.transport.tcp_config = Some(crate::config::TcpConfig {
            bind_address,
            cluster_addresses: self.config.base.cluster_members.clone(),
            connection_timeout: Duration::from_secs(10),
            read_timeout: Duration::from_secs(30),
            max_message_size: 10 * 1024 * 1024, // 10MB
            max_connection_retries: 3,
            retry_delay: Duration::from_millis(100),
            enable_auto_reconnect: true,
            initial_reconnect_delay: Duration::from_millis(100),
            max_reconnect_delay: Duration::from_secs(30),
            reconnect_backoff_multiplier: 2.0,
            max_reconnect_attempts: Some(10),
            heartbeat_interval: Duration::from_secs(5),
            connection_pool_size: 4,
        });
        self
    }

    /// Enable batch processing
    pub fn batch_processing(mut self, enabled: bool, max_size: usize) -> Self {
        self.config.performance.batch_processing = enabled;
        self.config.performance.max_batch_size = max_size;
        self
    }

    /// Enable WAL
    #[cfg(feature = "wal")]
    pub fn with_wal(mut self, wal_dir: String) -> Self {
        self.config.wal_config = Some(crate::config::WalConfig {
            wal_dir,
            max_file_size: 64 * 1024 * 1024, // 64MB
            sync_mode: crate::config::WalSyncMode::PerWrite,
            compression: false,
            retention: crate::config::WalRetention::Count(1000),
        });
        self
    }

    /// Build the factory
    pub fn build(self) -> RsmlResult<RsmlConsensusFactory> {
        RsmlConsensusFactory::new(self.config)
    }
}

impl Default for RsmlFactoryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_factory_creation_with_valid_config() {
        let mut cluster_members = HashMap::new();
        cluster_members.insert("node-1".to_string(), "localhost:8080".to_string());

        let factory = RsmlConsensusFactory::with_defaults("node-1".to_string(), cluster_members);
        assert!(factory.is_ok());
    }

    #[test]
    fn test_factory_creation_with_invalid_config() {
        let cluster_members = HashMap::new(); // Empty cluster

        let factory = RsmlConsensusFactory::with_defaults("node-1".to_string(), cluster_members);
        assert!(factory.is_err());

        if let Err(crate::RsmlError::ConfigurationError { field, .. }) = factory {
            assert_eq!(field, "base.cluster_members");
        }
    }

    #[test]
    fn test_factory_builder() {
        let factory = RsmlFactoryBuilder::new()
            .node_id("test-node".to_string())
            .cluster_member("test-node".to_string(), "localhost:9000".to_string())
            .in_memory_transport()
            .batch_processing(true, 50)
            .build();

        assert!(factory.is_ok());
        let factory = factory.unwrap();
        assert_eq!(factory.config().base.node_id, "test-node");
        assert_eq!(factory.config().performance.max_batch_size, 50);
    }

    #[cfg(feature = "test-utils")]
    #[test]
    fn test_factory_for_testing() {
        let factory = RsmlConsensusFactory::for_testing("test-node".to_string());
        assert!(factory.is_ok());

        let factory = factory.unwrap();
        assert_eq!(factory.config().base.node_id, "test-node");
        assert_eq!(factory.config().transport.transport_type, crate::config::TransportType::InMemory);
    }

    #[test]
    fn test_feature_validation() {
        let mut config = RsmlConfig::default();
        config.base.node_id = "node-1".to_string();
        config.base.cluster_members.insert("node-1".to_string(), "localhost:8080".to_string());

        // Test with in-memory transport (should work)
        config.transport.transport_type = crate::config::TransportType::InMemory;
        let result = RsmlConsensusFactory::validate_feature_availability(&config);
        assert!(result.is_ok());
    }
}
