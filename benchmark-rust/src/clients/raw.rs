use async_trait::async_trait;
use rocksdb::{Options, TransactionDB, TransactionDBOptions};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

use crate::{BenchmarkConfig, BenchmarkResult};
use crate::config::{load_config_from_file, get_default_config};
use super::{ClientFactory, KvOperations};

pub struct RawClient {
    db: Arc<TransactionDB>,
    read_semaphore: Arc<Semaphore>,
    write_semaphore: Arc<Semaphore>,
    config: BenchmarkConfig,
}

pub struct RawClientFactory {
    shared_client: Option<Arc<RawClient>>,
}

impl RawClientFactory {
    pub fn new() -> Self {
        Self {
            shared_client: None,
        }
    }
}

#[async_trait]
impl ClientFactory for RawClientFactory {
    async fn create_client(&self, db_path: &str, config: &BenchmarkConfig) 
        -> anyhow::Result<Arc<dyn KvOperations>> {
        // Use shared client for raw mode to avoid multiple DB instances
        if let Some(client) = &self.shared_client {
            return Ok(client.clone());
        }

        let rocks_config = if let Some(config_file) = &config.config_file {
            match load_config_from_file(Some(config_file.clone())) {
                Ok((config, path)) => {
                    if !path.is_empty() {
                        println!("Loaded RocksDB configuration from {}", path);
                    } else {
                        println!("No config file specified, using default configuration");
                    }
                    config
                }
                Err(e) => {
                    println!("Warning: Failed to load config file ({:?}), using default configuration", e);
                    get_default_config()
                }
            }
        } else {
            println!("No config file specified, using default configuration");
            get_default_config()
        };

        // Create directory if it doesn't exist
        std::fs::create_dir_all(db_path)?;

        // Set up RocksDB options with configuration
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_write_buffer_size((rocks_config.rocksdb.write_buffer_size_mb * 1024 * 1024) as usize);
        opts.set_max_write_buffer_number(rocks_config.rocksdb.max_write_buffer_number as i32);
        opts.set_max_background_jobs(rocks_config.rocksdb.max_background_jobs as i32);

        // Set up transaction database options
        let txn_db_opts = TransactionDBOptions::default();

        // Open transaction database
        let db = TransactionDB::open(&opts, &txn_db_opts, db_path)?;

        // Configure concurrency limits from config
        let max_read_concurrency = rocks_config.concurrency.max_read_concurrency as usize;
        let max_write_concurrency = 16; // Keep fixed for writes (same as other implementations)

        let client = Arc::new(RawClient {
            db: Arc::new(db),
            read_semaphore: Arc::new(Semaphore::new(max_read_concurrency)),
            write_semaphore: Arc::new(Semaphore::new(max_write_concurrency)),
            config: config.clone(),
        });

        // Store shared client
        // Note: This is a simplified implementation - in a real scenario, 
        // you might want to use a mutex or other synchronization mechanism
        Ok(client)
    }
}

#[async_trait]
impl KvOperations for RawClient {
    async fn put(&self, key: &str, value: &str) -> BenchmarkResult {
        if key.is_empty() {
            return BenchmarkResult {
                operation: "write".to_string(),
                latency: Duration::ZERO,
                success: false,
                error: Some("key cannot be empty".to_string()),
            };
        }

        // Acquire write semaphore
        let _permit = match self.write_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                return BenchmarkResult {
                    operation: "write".to_string(),
                    latency: Duration::ZERO,
                    success: false,
                    error: Some("timeout waiting for write transaction slot".to_string()),
                };
            }
        };

        let start = Instant::now();

        // Create a transaction for pessimistic locking
        let txn = self.db.transaction();
        
        // Put the key-value pair within the transaction
        let result = txn.put(key.as_bytes(), value.as_bytes());
        
        if let Err(e) = result {
            let latency = start.elapsed();
            return BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: false,
                error: Some(format!("failed to put value: {}", e)),
            };
        }

        // Commit the transaction
        let commit_result = txn.commit();
        let latency = start.elapsed();

        match commit_result {
            Ok(_) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: true,
                error: None,
            },
            Err(e) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: false,
                error: Some(format!("failed to commit transaction: {}", e)),
            },
        }
    }

    async fn get(&self, key: &str) -> BenchmarkResult {
        if key.is_empty() {
            return BenchmarkResult {
                operation: "read".to_string(),
                latency: Duration::ZERO,
                success: false,
                error: Some("key cannot be empty".to_string()),
            };
        }

        // Acquire read semaphore
        let _permit = match self.read_semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                return BenchmarkResult {
                    operation: "read".to_string(),
                    latency: Duration::ZERO,
                    success: false,
                    error: Some("timeout waiting for read transaction slot".to_string()),
                };
            }
        };

        let start = Instant::now();

        // Create a read-only transaction
        let txn = self.db.transaction();
        let result = txn.get(key.as_bytes());
        let latency = start.elapsed();

        match result {
            Ok(_) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: true,
                error: None,
            },
            Err(e) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: false,
                error: Some(format!("failed to get value: {}", e)),
            },
        }
    }

    async fn ping(&self, _worker_id: usize) -> BenchmarkResult {
        let start = Instant::now();

        // For raw database operations, we simulate a ping by doing a lightweight operation
        // We'll create an iterator and do a quick seek operation
        let txn = self.db.transaction();
        let mut iter = txn.iterator(rocksdb::IteratorMode::Start);
        
        // Try to get the first key as a database connectivity test
        let _first = iter.next();
        
        let latency = start.elapsed();

        BenchmarkResult {
            operation: "ping".to_string(),
            latency,
            success: true,
            error: None,
        }
    }
}