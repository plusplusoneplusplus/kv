use serde::{Deserialize, Serialize};
use std::path::Path;
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub deployment: DeploymentConfig,
    pub database: DatabaseConfig,
    pub rocksdb: RocksDbConfig,
    pub bloom_filter: BloomFilterConfig,
    pub compression: CompressionConfig,
    pub concurrency: ConcurrencyConfig,
    pub compaction: CompactionConfig,
    pub cache: CacheConfig,
    pub memory: MemoryConfig,
    pub logging: LoggingConfig,
    pub performance: PerformanceConfig,
    pub consensus: Option<ConsensusConfig>,
    pub reads: Option<ReadsConfig>,
    pub cluster: Option<ClusterConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeploymentConfig {
    pub mode: DeploymentMode,
    pub instance_id: Option<u32>,
    pub replica_endpoints: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeploymentMode {
    Standalone,
    Replicated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub base_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbConfig {
    pub write_buffer_size_mb: u64,
    pub max_write_buffer_number: u32,
    pub block_cache_size_mb: u64,
    pub block_size_kb: u32,
    pub max_background_jobs: u32,
    pub bytes_per_sync: u64,
    pub dynamic_level_bytes: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BloomFilterConfig {
    pub enabled: bool,
    pub bits_per_key: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    pub l0_compression: String,
    pub l1_compression: String,
    pub bottom_compression: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConcurrencyConfig {
    pub max_read_concurrency: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionConfig {
    pub compaction_priority: String,
    pub target_file_size_base_mb: u64,
    pub target_file_size_multiplier: u32,
    pub max_bytes_for_level_base_mb: u64,
    pub max_bytes_for_level_multiplier: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    pub cache_index_and_filter_blocks: bool,
    pub pin_l0_filter_and_index_blocks_in_cache: bool,
    pub high_priority_pool_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConfig {
    pub write_buffer_manager_limit_mb: u64,
    pub enable_write_buffer_manager: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub log_level: String,
    pub max_log_file_size_mb: u64,
    pub keep_log_file_num: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub statistics_level: String,
    pub enable_statistics: bool,
    pub stats_dump_period_sec: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    pub algorithm: String,
    pub election_timeout_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub max_batch_size: u32,
    pub max_outstanding_proposals: u32,
    pub endpoints: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadsConfig {
    pub consistency_level: String,
    pub allow_stale_reads: bool,
    pub max_staleness_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub health_check_interval_ms: u64,
    pub leader_discovery_timeout_ms: u64,
    pub node_timeout_ms: u64,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(path)?;
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }

    pub fn default() -> Self {
        Config {
            deployment: DeploymentConfig {
                mode: DeploymentMode::Standalone,
                instance_id: None,
                replica_endpoints: None,
            },
            database: DatabaseConfig {
                base_path: "./data/rocksdb".to_string(),
            },
            rocksdb: RocksDbConfig {
                write_buffer_size_mb: 64,
                max_write_buffer_number: 3,
                block_cache_size_mb: 512,
                block_size_kb: 16,
                max_background_jobs: 6,
                bytes_per_sync: 1048576,
                dynamic_level_bytes: true,
            },
            bloom_filter: BloomFilterConfig {
                enabled: true,
                bits_per_key: 10,
            },
            compression: CompressionConfig {
                l0_compression: "lz4".to_string(),
                l1_compression: "lz4".to_string(),
                bottom_compression: "zstd".to_string(),
            },
            concurrency: ConcurrencyConfig {
                max_read_concurrency: 32,
            },
            compaction: CompactionConfig {
                compaction_priority: "min_overlapping_ratio".to_string(),
                target_file_size_base_mb: 64,
                target_file_size_multiplier: 2,
                max_bytes_for_level_base_mb: 256,
                max_bytes_for_level_multiplier: 10,
            },
            cache: CacheConfig {
                cache_index_and_filter_blocks: true,
                pin_l0_filter_and_index_blocks_in_cache: true,
                high_priority_pool_ratio: 0.2,
            },
            memory: MemoryConfig {
                write_buffer_manager_limit_mb: 256,
                enable_write_buffer_manager: true,
            },
            logging: LoggingConfig {
                log_level: "info".to_string(),
                max_log_file_size_mb: 10,
                keep_log_file_num: 5,
            },
            performance: PerformanceConfig {
                statistics_level: "except_detailed_timers".to_string(),
                enable_statistics: true,
                stats_dump_period_sec: 600,
            },
            consensus: None,
            reads: None,
            cluster: None,
        }
    }

    pub fn get_db_path(&self, suffix: &str) -> String {
        format!("{}-{}", self.database.base_path, suffix)
    }

    /// Convert to kv-storage-rocksdb Config type for database creation
    pub fn to_rocksdb_config(&self) -> kv_storage_rocksdb::config::Config {
        kv_storage_rocksdb::config::Config {
            deployment: kv_storage_rocksdb::config::DeploymentConfig {
                mode: match self.deployment.mode {
                    DeploymentMode::Standalone => kv_storage_rocksdb::config::DeploymentMode::Standalone,
                    DeploymentMode::Replicated => kv_storage_rocksdb::config::DeploymentMode::Replicated,
                },
                instance_id: self.deployment.instance_id,
                replica_endpoints: self.deployment.replica_endpoints.clone().unwrap_or_default(),
            },
            database: kv_storage_rocksdb::config::DatabaseConfig {
                base_path: self.database.base_path.clone(),
            },
            rocksdb: kv_storage_rocksdb::config::RocksDbConfig {
                write_buffer_size_mb: self.rocksdb.write_buffer_size_mb,
                max_write_buffer_number: self.rocksdb.max_write_buffer_number,
                block_cache_size_mb: self.rocksdb.block_cache_size_mb,
                block_size_kb: self.rocksdb.block_size_kb,
                max_background_jobs: self.rocksdb.max_background_jobs,
                bytes_per_sync: self.rocksdb.bytes_per_sync,
                dynamic_level_bytes: self.rocksdb.dynamic_level_bytes,
            },
            bloom_filter: kv_storage_rocksdb::config::BloomFilterConfig {
                enabled: self.bloom_filter.enabled,
                bits_per_key: self.bloom_filter.bits_per_key,
            },
            compression: kv_storage_rocksdb::config::CompressionConfig {
                l0_compression: self.compression.l0_compression.clone(),
                l1_compression: self.compression.l1_compression.clone(),
                bottom_compression: self.compression.bottom_compression.clone(),
            },
            concurrency: kv_storage_rocksdb::config::ConcurrencyConfig {
                max_read_concurrency: self.concurrency.max_read_concurrency,
            },
            compaction: kv_storage_rocksdb::config::CompactionConfig {
                compaction_priority: self.compaction.compaction_priority.clone(),
                target_file_size_base_mb: self.compaction.target_file_size_base_mb,
                target_file_size_multiplier: self.compaction.target_file_size_multiplier,
                max_bytes_for_level_base_mb: self.compaction.max_bytes_for_level_base_mb,
                max_bytes_for_level_multiplier: self.compaction.max_bytes_for_level_multiplier,
            },
            cache: kv_storage_rocksdb::config::CacheConfig {
                cache_index_and_filter_blocks: self.cache.cache_index_and_filter_blocks,
                pin_l0_filter_and_index_blocks_in_cache: self.cache.pin_l0_filter_and_index_blocks_in_cache,
                high_priority_pool_ratio: self.cache.high_priority_pool_ratio,
            },
            memory: kv_storage_rocksdb::config::MemoryConfig {
                write_buffer_manager_limit_mb: self.memory.write_buffer_manager_limit_mb,
                enable_write_buffer_manager: self.memory.enable_write_buffer_manager,
            },
            logging: kv_storage_rocksdb::config::LoggingConfig {
                log_level: self.logging.log_level.clone(),
                max_log_file_size_mb: self.logging.max_log_file_size_mb,
                keep_log_file_num: self.logging.keep_log_file_num,
            },
            performance: kv_storage_rocksdb::config::PerformanceConfig {
                statistics_level: self.performance.statistics_level.clone(),
                enable_statistics: self.performance.enable_statistics,
                stats_dump_period_sec: self.performance.stats_dump_period_sec,
            },
        }
    }

}

impl CompressionConfig {
    pub fn parse_compression_type(&self, compression: &str) -> rocksdb::DBCompressionType {
        match compression.to_lowercase().as_str() {
            "none" => rocksdb::DBCompressionType::None,
            "snappy" => rocksdb::DBCompressionType::Snappy,
            "lz4" => rocksdb::DBCompressionType::Lz4,
            "zstd" => rocksdb::DBCompressionType::Zstd,
            "zlib" => rocksdb::DBCompressionType::Zlib,
            _ => rocksdb::DBCompressionType::Lz4, // default to lz4
        }
    }

    pub fn get_compression_per_level(&self) -> Vec<rocksdb::DBCompressionType> {
        vec![
            self.parse_compression_type(&self.l0_compression),  // L0
            self.parse_compression_type(&self.l1_compression),  // L1
            self.parse_compression_type(&self.bottom_compression), // L2
            self.parse_compression_type(&self.bottom_compression), // L3
            self.parse_compression_type(&self.bottom_compression), // L4
            self.parse_compression_type(&self.bottom_compression), // L5
            self.parse_compression_type(&self.bottom_compression), // L6
        ]
    }
}

impl CompactionConfig {
    // Note: CompactionPriority not available in rust-rocksdb 0.21
    // This is a placeholder for future RocksDB Rust binding updates
    pub fn get_compaction_priority_comment(&self) -> String {
        format!("# Compaction priority: {}", self.compaction_priority)
    }
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            algorithm: "raft".to_string(),
            election_timeout_ms: 5000,
            heartbeat_interval_ms: 1000,
            max_batch_size: 100,
            max_outstanding_proposals: 1000,
            endpoints: None,
        }
    }
}

impl Default for ReadsConfig {
    fn default() -> Self {
        Self {
            consistency_level: "local".to_string(),
            allow_stale_reads: true,
            max_staleness_ms: 1000,
        }
    }
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            health_check_interval_ms: 2000,
            leader_discovery_timeout_ms: 10000,
            node_timeout_ms: 5000,
        }
    }
}