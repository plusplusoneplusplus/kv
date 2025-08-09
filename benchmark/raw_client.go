package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
)

// RawClient implements KVOperations interface using direct RocksDB operations
type RawClient struct {
	txnDB          *grocksdb.TransactionDB
	ro             *grocksdb.ReadOptions
	wo             *grocksdb.WriteOptions
	txnOptions     *grocksdb.TransactionOptions
	config         *BenchmarkConfig
	cleanup        func() // cleanup function for shared connections
	
	// Concurrency control similar to server
	readSemaphore  chan struct{}
	writeSemaphore chan struct{}
	mu             sync.RWMutex
}

// RawClientFactory implements ClientFactory for raw RocksDB operations
type RawClientFactory struct {
	sharedDB *RawClient
	mu       sync.Mutex
}

func (f *RawClientFactory) CreateClient(dbPath string, config *BenchmarkConfig) (KVOperations, func(), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	
	// Create shared database connection if it doesn't exist
	if f.sharedDB == nil {
		rawClient, cleanup, err := NewRawClient(dbPath, config)
		if err != nil {
			return nil, nil, err
		}
		f.sharedDB = rawClient
		
		// Store cleanup function for later use
		f.sharedDB.cleanup = cleanup
	}
	
	// Return the shared client with a no-op cleanup function for individual workers
	return f.sharedDB, func() {}, nil
}

// NewRawClient creates a new raw RocksDB client
func NewRawClient(dbPath string, config *BenchmarkConfig) (*RawClient, func(), error) {
	// Load RocksDB configuration
	rocksConfig, configPath, err := LoadConfigFromFile(config.ConfigFile)
	if err != nil {
		// Log the error but continue with defaults
		fmt.Printf("Warning: Failed to load config file (%v), using default configuration\n", err)
		rocksConfig = GetDefaultConfig()
	} else if configPath == "" {
		fmt.Printf("No config file specified, using default configuration\n")
		rocksConfig = GetDefaultConfig()
	} else {
		fmt.Printf("Loaded RocksDB configuration from %s\n", configPath)
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, nil, fmt.Errorf("failed to create database directory %s: %v", dbPath, err)
	}

	// Set up RocksDB options with configuration
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	
	// Apply configuration settings
	opts.SetWriteBufferSize(rocksConfig.RocksDB.WriteBufferSizeMB * 1024 * 1024)
	opts.SetMaxWriteBufferNumber(int(rocksConfig.RocksDB.MaxWriteBufferNumber))
	opts.SetMaxBackgroundJobs(int(rocksConfig.RocksDB.MaxBackgroundJobs))
	opts.SetBytesPerSync(rocksConfig.RocksDB.BytesPerSync)
	
	// Configure block-based table options
	blockOpts := grocksdb.NewDefaultBlockBasedTableOptions()
	
	// Set block cache size
	cache := grocksdb.NewLRUCache(rocksConfig.RocksDB.BlockCacheSizeMB * 1024 * 1024)
	blockOpts.SetBlockCache(cache)
	
	// Set block size
	blockOpts.SetBlockSize(int(rocksConfig.RocksDB.BlockSizeKB * 1024))
	
	// Configure bloom filter
	if rocksConfig.BloomFilter.Enabled {
		blockOpts.SetFilterPolicy(grocksdb.NewBloomFilter(int(rocksConfig.BloomFilter.BitsPerKey)))
	}
	
	// Configure cache settings
	blockOpts.SetCacheIndexAndFilterBlocks(rocksConfig.Cache.CacheIndexAndFilterBlocks)
	blockOpts.SetPinL0FilterAndIndexBlocksInCache(rocksConfig.Cache.PinL0FilterAndIndexBlocksInCache)
	
	// Apply block-based table options
	opts.SetBlockBasedTableFactory(blockOpts)
	
	// Set up transaction database options
	txnDBOptions := grocksdb.NewDefaultTransactionDBOptions()
	
	// Open transaction database (this enables pessimistic locking)
	txnDB, err := grocksdb.OpenTransactionDb(opts, txnDBOptions, dbPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open transaction database: %v", err)
	}

	// Create read and write options
	ro := grocksdb.NewDefaultReadOptions()
	if config.NoCacheRead {
		// Avoid filling the block cache on reads; still allow existing cache hits
		// This mimics a colder read pattern by preventing cache warming
		ro.SetFillCache(false)
	}
	wo := grocksdb.NewDefaultWriteOptions()
	
	// Create transaction options for pessimistic locking
	txnOptions := grocksdb.NewDefaultTransactionOptions()

	// Configure concurrency limits from config
	maxReadConcurrency := int(rocksConfig.Concurrency.MaxReadConcurrency)
	maxWriteConcurrency := 16  // Keep fixed for writes (same as Rust server)

	rawClient := &RawClient{
		txnDB:          txnDB,
		ro:             ro,
		wo:             wo,
		txnOptions:     txnOptions,
		config:         config,
		readSemaphore:  make(chan struct{}, maxReadConcurrency),
		writeSemaphore: make(chan struct{}, maxWriteConcurrency),
	}

	// Return cleanup function
	cleanup := func() {
		rawClient.Close()
	}

	return rawClient, cleanup, nil
}

// Close closes the database and cleans up resources
func (r *RawClient) Close() {
	if r.txnDB != nil {
		r.txnDB.Close()
	}
	if r.ro != nil {
		r.ro.Destroy()
	}
	if r.wo != nil {
		r.wo.Destroy()
	}
	if r.txnOptions != nil {
		r.txnOptions.Destroy()
	}
}

// Put implements KVOperations interface for raw RocksDB
func (r *RawClient) Put(key, value string) *BenchmarkResult {
	if key == "" {
		return &BenchmarkResult{
			Operation: "write",
			Latency:   0,
			Success:   false,
			Error:     fmt.Errorf("key cannot be empty"),
		}
	}

	// Acquire write semaphore to limit concurrent write transactions
	select {
	case r.writeSemaphore <- struct{}{}:
		defer func() { <-r.writeSemaphore }()
	default:
		return &BenchmarkResult{
			Operation: "write",
			Latency:   0,
			Success:   false,
			Error:     fmt.Errorf("timeout waiting for write transaction slot"),
		}
	}

	start := time.Now()
	
	// Create a transaction for pessimistic locking
	txn := r.txnDB.TransactionBegin(r.wo, r.txnOptions, nil)
	defer txn.Destroy()

	// Put the key-value pair within the transaction
	err := txn.Put([]byte(key), []byte(value))
	if err != nil {
		txn.Rollback()
		latency := time.Since(start)
		return &BenchmarkResult{
			Operation: "write",
			Latency:   latency,
			Success:   false,
			Error:     fmt.Errorf("failed to put value: %v", err),
		}
	}

	// Commit the transaction
	err = txn.Commit()
	latency := time.Since(start)
	
	if err != nil {
		return &BenchmarkResult{
			Operation: "write",
			Latency:   latency,
			Success:   false,
			Error:     fmt.Errorf("failed to commit transaction: %v", err),
		}
	}

	return &BenchmarkResult{
		Operation: "write",
		Latency:   latency,
		Success:   true,
		Error:     nil,
	}
}

// Get implements KVOperations interface for raw RocksDB
func (r *RawClient) Get(key string) *BenchmarkResult {
	if key == "" {
		return &BenchmarkResult{
			Operation: "read",
			Latency:   0,
			Success:   false,
			Error:     fmt.Errorf("key cannot be empty"),
		}
	}

	// Acquire read semaphore to limit concurrent read transactions
	select {
	case r.readSemaphore <- struct{}{}:
		defer func() { <-r.readSemaphore }()
	default:
		return &BenchmarkResult{
			Operation: "read",
			Latency:   0,
			Success:   false,
			Error:     fmt.Errorf("timeout waiting for read transaction slot"),
		}
	}

	start := time.Now()
	
	// Create a read-only transaction
	txn := r.txnDB.TransactionBegin(r.wo, r.txnOptions, nil)
	defer txn.Destroy()

	value, err := txn.Get(r.ro, []byte(key))
	latency := time.Since(start)
	
	if err != nil {
		return &BenchmarkResult{
			Operation: "read",
			Latency:   latency,
			Success:   false,
			Error:     fmt.Errorf("failed to get value: %v", err),
		}
	}
	defer value.Free()

	// For benchmark purposes, we consider both found and not found as successful operations
	return &BenchmarkResult{
		Operation: "read",
		Latency:   latency,
		Success:   true,
		Error:     nil,
	}
}

// Ping implements KVOperations interface for raw RocksDB (simulates ping operation)
func (r *RawClient) Ping(workerID int) *BenchmarkResult {
	// For raw database operations, we simulate a ping by doing a lightweight operation
	// We'll perform a quick iterator seek to check database accessibility
	
	start := time.Now()
	
	// Create a read-only transaction for the ping
	txn := r.txnDB.TransactionBegin(r.wo, r.txnOptions, nil)
	defer txn.Destroy()
	
	// Create an iterator and do a quick seek operation
	it := txn.NewIterator(r.ro)
	defer it.Close()
	
	// Seek to first key as a database connectivity test
	it.SeekToFirst()
	
	latency := time.Since(start)
	
	// Check if there were any errors with the iterator
	if err := it.Err(); err != nil {
		return &BenchmarkResult{
			Operation: "ping",
			Latency:   latency,
			Success:   false,
			Error:     fmt.Errorf("database ping failed: %v", err),
		}
	}

	return &BenchmarkResult{
		Operation: "ping",
		Latency:   latency,
		Success:   true,
		Error:     nil,
	}
}