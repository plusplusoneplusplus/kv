use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbConfig {
    pub database: DatabaseConfig,
    pub rocksdb: RocksDbSettings,
    pub bloom_filter: BloomFilterConfig,
    pub compression: CompressionConfig,
    pub concurrency: ConcurrencyConfig,
    pub compaction: CompactionConfig,
    pub cache: CacheConfig,
    pub memory: MemoryConfig,
    pub logging: LoggingConfig,
    pub performance: PerformanceConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub base_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RocksDbSettings {
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
    pub max_read_concurrency: u32,
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

pub fn load_config_from_file(
    config_path: Option<String>,
) -> anyhow::Result<(RocksDbConfig, String)> {
    if let Some(path) = config_path {
        if !std::path::Path::new(&path).exists() {
            return Err(anyhow::anyhow!("config file not found: {}", path));
        }

        let data = fs::read_to_string(&path)?;
        let config: RocksDbConfig = toml::from_str(&data)?;
        return Ok((config, path));
    }

    // Try to find config file relative to the executable
    let config_path = "db_config.toml";
    if std::path::Path::new(config_path).exists() {
        let data = fs::read_to_string(config_path)?;
        let config: RocksDbConfig = toml::from_str(&data)?;
        return Ok((config, config_path.to_string()));
    }

    Ok((get_default_config(), String::new()))
}

pub fn get_default_config() -> RocksDbConfig {
    RocksDbConfig {
        database: DatabaseConfig {
            base_path: "./data/rocksdb".to_string(),
        },
        rocksdb: RocksDbSettings {
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
    }
}

impl RocksDbConfig {
    pub fn get_db_path(&self, suffix: &str) -> String {
        if suffix.is_empty() {
            self.database.base_path.clone()
        } else {
            format!("{}-{}", self.database.base_path, suffix)
        }
    }
}
