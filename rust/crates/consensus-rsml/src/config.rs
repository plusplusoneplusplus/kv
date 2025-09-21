//! RSML-specific configuration types
//!
//! This module defines configuration structures that extend the generic
//! consensus configuration with RSML-specific options and validation.

use consensus_api::{ConsensusConfig, NodeId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::{RsmlError, RsmlResult};

/// RSML-specific consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RsmlConfig {
    /// Base consensus configuration
    pub base: ConsensusConfig,

    /// RSML-specific transport configuration
    pub transport: TransportConfig,

    /// RSML view management configuration
    pub view_config: ViewConfig,

    /// RSML performance optimization settings
    pub performance: PerformanceConfig,

    /// WAL (Write-Ahead Log) configuration
    #[cfg(feature = "wal")]
    pub wal_config: Option<WalConfig>,
}

/// Transport configuration for RSML networking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransportConfig {
    /// Transport type selection
    pub transport_type: TransportType,

    /// TCP-specific configuration (when tcp feature is enabled)
    #[cfg(feature = "tcp")]
    pub tcp_config: Option<TcpConfig>,

    /// Connection retry configuration
    pub retry_config: RetryConfig,

    /// Message timeouts
    pub message_timeouts: MessageTimeouts,
}

/// Available transport types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransportType {
    /// In-memory transport for testing
    InMemory,

    /// TCP transport for production
    #[cfg(feature = "tcp")]
    Tcp,
}

/// TCP transport configuration
#[cfg(feature = "tcp")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TcpConfig {
    /// Bind address for this node
    pub bind_address: String,

    /// Addresses of all cluster members
    pub cluster_addresses: HashMap<NodeId, String>,

    /// TCP keepalive settings
    pub keepalive: bool,

    /// TCP nodelay setting
    pub nodelay: bool,

    /// Connection buffer sizes
    pub buffer_size: usize,
}

/// Connection retry configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    /// Maximum number of connection retries
    pub max_retries: usize,

    /// Initial retry delay
    pub initial_delay: Duration,

    /// Maximum retry delay
    pub max_delay: Duration,

    /// Retry backoff multiplier
    pub backoff_multiplier: f64,
}

/// Message timeout configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageTimeouts {
    /// Timeout for 1a (prepare) messages
    pub prepare_timeout: Duration,

    /// Timeout for 2a (accept) messages
    pub accept_timeout: Duration,

    /// Timeout for view change messages
    pub view_change_timeout: Duration,

    /// Timeout for client request responses
    pub client_timeout: Duration,
}

/// RSML view management configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ViewConfig {
    /// View change timeout
    pub view_change_timeout: Duration,

    /// Maximum number of view changes to attempt
    pub max_view_changes: usize,

    /// Leader lease duration
    pub leader_lease_duration: Duration,

    /// View number advancement strategy
    pub view_advancement: ViewAdvancement,
}

/// View number advancement strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ViewAdvancement {
    /// Increment by 1
    Sequential,
    /// Jump to next available view
    Exponential { base: u64 },
}

/// Performance optimization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Enable batch processing of messages
    pub batch_processing: bool,

    /// Maximum batch size
    pub max_batch_size: usize,

    /// Batch timeout
    pub batch_timeout: Duration,

    /// Enable parallel processing where safe
    pub parallel_processing: bool,

    /// Thread pool size for parallel operations
    pub thread_pool_size: Option<usize>,
}

/// Write-Ahead Log configuration
#[cfg(feature = "wal")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    /// WAL directory path
    pub wal_dir: String,

    /// Maximum WAL file size
    pub max_file_size: usize,

    /// Sync mode for WAL writes
    pub sync_mode: WalSyncMode,

    /// Compression for WAL entries
    pub compression: bool,

    /// Retention policy
    pub retention: WalRetention,
}

/// WAL synchronization modes
#[cfg(feature = "wal")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalSyncMode {
    /// No explicit sync (OS buffered)
    None,
    /// Sync after each write
    PerWrite,
    /// Sync periodically
    Periodic { interval: Duration },
}

/// WAL retention policies
#[cfg(feature = "wal")]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRetention {
    /// Keep WAL entries for a duration
    Duration(Duration),
    /// Keep a fixed number of WAL entries
    Count(usize),
    /// Keep WAL entries until a certain disk usage
    DiskUsage { max_bytes: usize },
}

impl Default for RsmlConfig {
    fn default() -> Self {
        Self {
            base: ConsensusConfig::default(),
            transport: TransportConfig::default(),
            view_config: ViewConfig::default(),
            performance: PerformanceConfig::default(),
            #[cfg(feature = "wal")]
            wal_config: None,
        }
    }
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            transport_type: TransportType::InMemory,
            #[cfg(feature = "tcp")]
            tcp_config: None,
            retry_config: RetryConfig::default(),
            message_timeouts: MessageTimeouts::default(),
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

impl Default for MessageTimeouts {
    fn default() -> Self {
        Self {
            prepare_timeout: Duration::from_millis(1000),
            accept_timeout: Duration::from_millis(1000),
            view_change_timeout: Duration::from_millis(2000),
            client_timeout: Duration::from_secs(30),
        }
    }
}

impl Default for ViewConfig {
    fn default() -> Self {
        Self {
            view_change_timeout: Duration::from_millis(2000),
            max_view_changes: 10,
            leader_lease_duration: Duration::from_secs(10),
            view_advancement: ViewAdvancement::Sequential,
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            batch_processing: true,
            max_batch_size: 100,
            batch_timeout: Duration::from_millis(10),
            parallel_processing: false,
            thread_pool_size: None,
        }
    }
}

impl RsmlConfig {
    /// Validate the configuration and return detailed errors
    pub fn validate(&self) -> RsmlResult<()> {
        // Validate base configuration
        self.validate_base_config()?;

        // Validate transport configuration
        self.validate_transport_config()?;

        // Validate view configuration
        self.validate_view_config()?;

        // Validate performance configuration
        self.validate_performance_config()?;

        // Validate WAL configuration if present
        #[cfg(feature = "wal")]
        if let Some(ref wal_config) = self.wal_config {
            self.validate_wal_config(wal_config)?;
        }

        Ok(())
    }

    fn validate_base_config(&self) -> RsmlResult<()> {
        if self.base.node_id.is_empty() {
            return Err(RsmlError::ConfigurationError {
                field: "base.node_id".to_string(),
                message: "Node ID cannot be empty".to_string(),
            });
        }

        if self.base.cluster_members.is_empty() {
            return Err(RsmlError::ConfigurationError {
                field: "base.cluster_members".to_string(),
                message: "Cluster must have at least one member".to_string(),
            });
        }

        if !self.base.cluster_members.contains_key(&self.base.node_id) {
            return Err(RsmlError::ConfigurationError {
                field: "base.cluster_members".to_string(),
                message: "Node ID must be present in cluster members".to_string(),
            });
        }

        Ok(())
    }

    fn validate_transport_config(&self) -> RsmlResult<()> {
        match self.transport.transport_type {
            TransportType::InMemory => {
                // In-memory transport has no additional validation
                Ok(())
            }
            #[cfg(feature = "tcp")]
            TransportType::Tcp => {
                if self.transport.tcp_config.is_none() {
                    return Err(RsmlError::ConfigurationError {
                        field: "transport.tcp_config".to_string(),
                        message: "TCP configuration required when using TCP transport".to_string(),
                    });
                }

                if let Some(ref tcp_config) = self.transport.tcp_config {
                    if tcp_config.bind_address.is_empty() {
                        return Err(RsmlError::ConfigurationError {
                            field: "transport.tcp_config.bind_address".to_string(),
                            message: "Bind address cannot be empty".to_string(),
                        });
                    }

                    if tcp_config.cluster_addresses.is_empty() {
                        return Err(RsmlError::ConfigurationError {
                            field: "transport.tcp_config.cluster_addresses".to_string(),
                            message: "Cluster addresses cannot be empty".to_string(),
                        });
                    }
                }

                Ok(())
            }
        }
    }

    fn validate_view_config(&self) -> RsmlResult<()> {
        if self.view_config.max_view_changes == 0 {
            return Err(RsmlError::ConfigurationError {
                field: "view_config.max_view_changes".to_string(),
                message: "Maximum view changes must be greater than 0".to_string(),
            });
        }

        if self.view_config.view_change_timeout.is_zero() {
            return Err(RsmlError::ConfigurationError {
                field: "view_config.view_change_timeout".to_string(),
                message: "View change timeout must be greater than 0".to_string(),
            });
        }

        Ok(())
    }

    fn validate_performance_config(&self) -> RsmlResult<()> {
        if self.performance.max_batch_size == 0 {
            return Err(RsmlError::ConfigurationError {
                field: "performance.max_batch_size".to_string(),
                message: "Maximum batch size must be greater than 0".to_string(),
            });
        }

        if let Some(pool_size) = self.performance.thread_pool_size {
            if pool_size == 0 {
                return Err(RsmlError::ConfigurationError {
                    field: "performance.thread_pool_size".to_string(),
                    message: "Thread pool size must be greater than 0".to_string(),
                });
            }
        }

        Ok(())
    }

    #[cfg(feature = "wal")]
    fn validate_wal_config(&self, wal_config: &WalConfig) -> RsmlResult<()> {
        if wal_config.wal_dir.is_empty() {
            return Err(RsmlError::ConfigurationError {
                field: "wal_config.wal_dir".to_string(),
                message: "WAL directory cannot be empty".to_string(),
            });
        }

        if wal_config.max_file_size == 0 {
            return Err(RsmlError::ConfigurationError {
                field: "wal_config.max_file_size".to_string(),
                message: "Maximum WAL file size must be greater than 0".to_string(),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_default_config() {
        let config = RsmlConfig::default();
        assert_eq!(config.transport.transport_type, TransportType::InMemory);
        assert!(config.performance.batch_processing);
    }

    #[test]
    fn test_config_validation_empty_node_id() {
        let mut config = RsmlConfig::default();
        config.base.node_id = "".to_string();

        let result = config.validate();
        assert!(result.is_err());
        if let Err(RsmlError::ConfigurationError { field, .. }) = result {
            assert_eq!(field, "base.node_id");
        }
    }

    #[test]
    fn test_config_validation_missing_cluster_members() {
        let mut config = RsmlConfig::default();
        config.base.node_id = "node-1".to_string();
        config.base.cluster_members = HashMap::new();

        let result = config.validate();
        assert!(result.is_err());
        if let Err(RsmlError::ConfigurationError { field, .. }) = result {
            assert_eq!(field, "base.cluster_members");
        }
    }

    #[test]
    fn test_config_validation_valid() {
        let mut config = RsmlConfig::default();
        config.base.node_id = "node-1".to_string();
        config.base.cluster_members.insert("node-1".to_string(), "localhost:8080".to_string());

        let result = config.validate();
        assert!(result.is_ok());
    }
}