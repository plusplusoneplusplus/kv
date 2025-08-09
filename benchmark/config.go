package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/BurntSushi/toml"
)

// RocksDBConfig holds the RocksDB configuration settings
type RocksDBConfig struct {
	Database    DatabaseConfig    `toml:"database"`
	RocksDB     RocksDBSettings   `toml:"rocksdb"`
	BloomFilter BloomFilterConfig `toml:"bloom_filter"`
	Compression CompressionConfig `toml:"compression"`
	Concurrency ConcurrencyConfig `toml:"concurrency"`
	Compaction  CompactionConfig  `toml:"compaction"`
	Cache       CacheConfig       `toml:"cache"`
	Memory      MemoryConfig      `toml:"memory"`
	Logging     LoggingConfig     `toml:"logging"`
	Performance PerformanceConfig `toml:"performance"`
}

type DatabaseConfig struct {
	BasePath string `toml:"base_path"`
}

type RocksDBSettings struct {
	WriteBufferSizeMB       uint64 `toml:"write_buffer_size_mb"`
	MaxWriteBufferNumber    uint32 `toml:"max_write_buffer_number"`
	BlockCacheSizeMB        uint64 `toml:"block_cache_size_mb"`
	BlockSizeKB             uint32 `toml:"block_size_kb"`
	MaxBackgroundJobs       uint32 `toml:"max_background_jobs"`
	BytesPerSync            uint64 `toml:"bytes_per_sync"`
	DynamicLevelBytes       bool   `toml:"dynamic_level_bytes"`
}

type BloomFilterConfig struct {
	Enabled    bool   `toml:"enabled"`
	BitsPerKey uint32 `toml:"bits_per_key"`
}

type CompressionConfig struct {
	L0Compression     string `toml:"l0_compression"`
	L1Compression     string `toml:"l1_compression"`
	BottomCompression string `toml:"bottom_compression"`
}

type ConcurrencyConfig struct {
	MaxReadConcurrency uint32 `toml:"max_read_concurrency"`
}

type CompactionConfig struct {
	CompactionPriority           string `toml:"compaction_priority"`
	TargetFileSizeBaseMB         uint64 `toml:"target_file_size_base_mb"`
	TargetFileSizeMultiplier     uint32 `toml:"target_file_size_multiplier"`
	MaxBytesForLevelBaseMB       uint64 `toml:"max_bytes_for_level_base_mb"`
	MaxBytesForLevelMultiplier   uint32 `toml:"max_bytes_for_level_multiplier"`
}

type CacheConfig struct {
	CacheIndexAndFilterBlocks             bool    `toml:"cache_index_and_filter_blocks"`
	PinL0FilterAndIndexBlocksInCache      bool    `toml:"pin_l0_filter_and_index_blocks_in_cache"`
	HighPriorityPoolRatio                 float64 `toml:"high_priority_pool_ratio"`
}

type MemoryConfig struct {
	WriteBufferManagerLimitMB   uint64 `toml:"write_buffer_manager_limit_mb"`
	EnableWriteBufferManager    bool   `toml:"enable_write_buffer_manager"`
}

type LoggingConfig struct {
	LogLevel            string `toml:"log_level"`
	MaxLogFileSizeMB    uint64 `toml:"max_log_file_size_mb"`
	KeepLogFileNum      uint32 `toml:"keep_log_file_num"`
}

type PerformanceConfig struct {
	StatisticsLevel     string `toml:"statistics_level"`
	EnableStatistics    bool   `toml:"enable_statistics"`
	StatsDumpPeriodSec  uint64 `toml:"stats_dump_period_sec"`
}

// LoadConfigWithPath loads the configuration and returns the path used
func LoadConfigFromFile(configPath string) (*RocksDBConfig, string, error) {
	if configPath == "" {
		return LoadConfigWithPath()
	}
	
	// Check if file exists
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return nil, "", fmt.Errorf("config file not found: %s", configPath)
	}
	
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read config file %s: %v", configPath, err)
	}
	
	var config RocksDBConfig
	if err := toml.Unmarshal(data, &config); err != nil {
		return nil, "", fmt.Errorf("failed to parse config file %s: %v", configPath, err)
	}
	
	return &config, configPath, nil
}

func LoadConfigWithPath() (*RocksDBConfig, string, error) {
	// Try to find config file relative to the executable
	execPath, err := os.Executable()
	if err != nil {
		execPath = "."
	}
	
	execDir := filepath.Dir(execPath)
	configPath := filepath.Join(execDir, "db_config.toml")
	
	// Check if file exists, if not try current directory
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		configPath = "db_config.toml"
		
		// If still not found, use defaults
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			return GetDefaultConfig(), "", nil
		}
	}
	
	var config RocksDBConfig
	if _, err := toml.DecodeFile(configPath, &config); err != nil {
		return nil, "", err
	}
	
	return &config, configPath, nil
}

// LoadConfig loads the configuration from the db_config.toml file (backwards compatibility)
func LoadConfig() (*RocksDBConfig, error) {
	config, _, err := LoadConfigWithPath()
	return config, err
}

// GetDefaultConfig returns default configuration values
func GetDefaultConfig() *RocksDBConfig {
	return &RocksDBConfig{
		Database: DatabaseConfig{
			BasePath: "./data/rocksdb",
		},
		RocksDB: RocksDBSettings{
			WriteBufferSizeMB:       64,
			MaxWriteBufferNumber:    3,
			BlockCacheSizeMB:        512,
			BlockSizeKB:             16,
			MaxBackgroundJobs:       6,
			BytesPerSync:            1048576,
			DynamicLevelBytes:       true,
		},
		BloomFilter: BloomFilterConfig{
			Enabled:    true,
			BitsPerKey: 10,
		},
		Compression: CompressionConfig{
			L0Compression:     "lz4",
			L1Compression:     "lz4",
			BottomCompression: "zstd",
		},
		Concurrency: ConcurrencyConfig{
			MaxReadConcurrency: 32,
		},
		Compaction: CompactionConfig{
			CompactionPriority:           "min_overlapping_ratio",
			TargetFileSizeBaseMB:         64,
			TargetFileSizeMultiplier:     2,
			MaxBytesForLevelBaseMB:       256,
			MaxBytesForLevelMultiplier:   10,
		},
		Cache: CacheConfig{
			CacheIndexAndFilterBlocks:             true,
			PinL0FilterAndIndexBlocksInCache:      true,
			HighPriorityPoolRatio:                 0.2,
		},
		Memory: MemoryConfig{
			WriteBufferManagerLimitMB:   256,
			EnableWriteBufferManager:    true,
		},
		Logging: LoggingConfig{
			LogLevel:            "info",
			MaxLogFileSizeMB:    10,
			KeepLogFileNum:      5,
		},
		Performance: PerformanceConfig{
			StatisticsLevel:     "except_detailed_timers",
			EnableStatistics:    true,
			StatsDumpPeriodSec:  600,
		},
	}
}

// GetDBPath returns the database path with the specified suffix
func (c *RocksDBConfig) GetDBPath(suffix string) string {
	if suffix == "" {
		return c.Database.BasePath
	}
	return c.Database.BasePath + "-" + suffix
}