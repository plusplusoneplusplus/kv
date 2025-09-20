use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::NodeId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    pub node_id: NodeId,
    pub cluster_members: HashMap<NodeId, String>,
    pub timeouts: TimeoutConfig,
    pub algorithm: AlgorithmConfig,
    pub storage: StorageConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_max: Duration,
    pub heartbeat_interval: Duration,
    pub append_entries_timeout: Duration,
    pub request_timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgorithmConfig {
    pub algorithm_type: AlgorithmType,
    pub batch_size: usize,
    pub max_log_entries_per_request: usize,
    pub snapshot_threshold: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlgorithmType {
    Raft,
    Pbft,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub log_dir: String,
    pub snapshot_dir: String,
    pub sync_writes: bool,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".to_string(),
            cluster_members: HashMap::new(),
            timeouts: TimeoutConfig::default(),
            algorithm: AlgorithmConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            heartbeat_interval: Duration::from_millis(50),
            append_entries_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_secs(5),
        }
    }
}

impl Default for AlgorithmConfig {
    fn default() -> Self {
        Self {
            algorithm_type: AlgorithmType::Raft,
            batch_size: 100,
            max_log_entries_per_request: 1000,
            snapshot_threshold: 10000,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            log_dir: "./consensus_logs".to_string(),
            snapshot_dir: "./consensus_snapshots".to_string(),
            sync_writes: false,
        }
    }
}