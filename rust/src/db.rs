use rocksdb::{TransactionDB, TransactionDBOptions, Options, TransactionOptions, IteratorMode, BlockBasedOptions, Cache};
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tracing::error;
use crate::config::Config;

pub struct KvDatabase {
    db: Arc<TransactionDB>,
    read_semaphore: Arc<Semaphore>,
    write_queue_tx: mpsc::UnboundedSender<WriteRequest>,
}

#[derive(Debug)]
pub struct GetResult {
    pub value: String,
    pub found: bool,
}

#[derive(Debug)]
pub struct OpResult {
    pub success: bool,
    pub error: String,
}

#[derive(Debug)]
enum WriteOperation {
    Put { key: String, value: String },
    Delete { key: String },
}

struct WriteRequest {
    operation: WriteOperation,
    response_tx: oneshot::Sender<OpResult>,
}

impl KvDatabase {
    pub fn new(db_path: &str, config: &Config) -> Result<Self, Box<dyn std::error::Error>> {
        // Set up RocksDB options from configuration
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        // Configure write buffer settings
        opts.set_write_buffer_size((config.rocksdb.write_buffer_size_mb * 1024 * 1024) as usize);
        opts.set_max_write_buffer_number(config.rocksdb.max_write_buffer_number as i32);
        
        // Configure background jobs
        opts.set_max_background_jobs(config.rocksdb.max_background_jobs as i32);
        
        // Configure level compaction dynamic sizing
        if config.rocksdb.dynamic_level_bytes {
            opts.set_level_compaction_dynamic_level_bytes(true);
        }
        
        // Configure bytes per sync
        opts.set_bytes_per_sync(config.rocksdb.bytes_per_sync);
        
        // Configure compression per level
        opts.set_compression_per_level(&config.compression.get_compression_per_level());
        
        // Configure compaction settings
        // Note: set_compaction_priority not available in rust-rocksdb 0.21
        opts.set_target_file_size_base((config.compaction.target_file_size_base_mb * 1024 * 1024) as u64);
        opts.set_target_file_size_multiplier(config.compaction.target_file_size_multiplier as i32);
        opts.set_max_bytes_for_level_base((config.compaction.max_bytes_for_level_base_mb * 1024 * 1024) as u64);
        opts.set_max_bytes_for_level_multiplier(config.compaction.max_bytes_for_level_multiplier as f64);
        
        // Set up block-based table options
        let mut table_opts = BlockBasedOptions::default();
        
        // Configure block cache
        let cache = Cache::new_lru_cache((config.rocksdb.block_cache_size_mb * 1024 * 1024) as usize);
        table_opts.set_block_cache(&cache);
        
        // Configure block size
        table_opts.set_block_size((config.rocksdb.block_size_kb * 1024) as usize);
        
        // Configure bloom filter
        if config.bloom_filter.enabled {
            table_opts.set_bloom_filter(config.bloom_filter.bits_per_key as f64, false);
        }
        
        // Configure cache settings
        table_opts.set_cache_index_and_filter_blocks(config.cache.cache_index_and_filter_blocks);
        table_opts.set_pin_l0_filter_and_index_blocks_in_cache(config.cache.pin_l0_filter_and_index_blocks_in_cache);
        
        // Apply table options to column family options
        opts.set_block_based_table_factory(&table_opts);
        
        // Configure memory settings
        if config.memory.enable_write_buffer_manager && config.memory.write_buffer_manager_limit_mb > 0 {
            // Note: Write buffer manager requires more complex setup in Rust bindings
            // This is a simplified version - full implementation would need custom write buffer manager
        }
        
        // Set up transaction database options
        let txn_db_opts = TransactionDBOptions::default();
        
        // Open transaction database
        let db = TransactionDB::open(&opts, &txn_db_opts, db_path)?;
        let db = Arc::new(db);
        
        // Configure concurrency limits for reads from config
        let max_read_concurrency = config.concurrency.max_read_concurrency;
        
        // Create write queue channel
        let (write_queue_tx, write_queue_rx) = mpsc::unbounded_channel::<WriteRequest>();
        
        // Spawn write worker task to serialize write operations
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            Self::write_worker(db_clone, write_queue_rx).await;
        });
        
        Ok(Self {
            db,
            read_semaphore: Arc::new(Semaphore::new(max_read_concurrency)),
            write_queue_tx,
        })
    }

    async fn write_worker(db: Arc<TransactionDB>, mut write_queue_rx: mpsc::UnboundedReceiver<WriteRequest>) {
        while let Some(request) = write_queue_rx.recv().await {
            let result = match request.operation {
                WriteOperation::Put { key, value } => Self::execute_put(&db, &key, &value),
                WriteOperation::Delete { key } => Self::execute_delete(&db, &key),
            };
            
            // Send the result back, ignoring if the receiver is dropped
            let _ = request.response_tx.send(result);
        }
    }

    fn execute_put(db: &TransactionDB, key: &str, value: &str) -> OpResult {
        if key.is_empty() {
            return OpResult {
                success: false,
                error: "key cannot be empty".to_string(),
            };
        }

        // Create a transaction for pessimistic locking
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&Default::default(), &txn_opts);
        
        // Put the key-value pair within the transaction
        match txn.put(key, value) {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => OpResult {
                        success: true,
                        error: String::new(),
                    },
                    Err(e) => {
                        error!("Failed to commit transaction: {}", e);
                        OpResult {
                            success: false,
                            error: format!("failed to commit transaction: {}", e),
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to put value: {}", e);
                let _ = txn.rollback();
                OpResult {
                    success: false,
                    error: format!("failed to put value: {}", e),
                }
            }
        }
    }

    fn execute_delete(db: &TransactionDB, key: &str) -> OpResult {
        if key.is_empty() {
            return OpResult {
                success: false,
                error: "key cannot be empty".to_string(),
            };
        }

        // Create a transaction for pessimistic locking
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&Default::default(), &txn_opts);
        
        // Delete the key within the transaction
        match txn.delete(key) {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => OpResult {
                        success: true,
                        error: String::new(),
                    },
                    Err(e) => {
                        error!("Failed to commit transaction: {}", e);
                        OpResult {
                            success: false,
                            error: format!("failed to commit transaction: {}", e),
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to delete key: {}", e);
                let _ = txn.rollback();
                OpResult {
                    success: false,
                    error: format!("failed to delete key: {}", e),
                }
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

        // Acquire read semaphore to limit concurrent read transactions
        let _permit = self.read_semaphore
            .acquire()
            .await
            .map_err(|_| "timeout waiting for read transaction slot".to_string())?;

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        match txn.get(key) {
            Ok(Some(value)) => {
                let value_str = String::from_utf8_lossy(&value).to_string();
                Ok(GetResult {
                    value: value_str,
                    found: true,
                })
            }
            Ok(None) => {
                Ok(GetResult {
                    value: String::new(),
                    found: false,
                })
            }
            Err(e) => {
                error!("Failed to get value: {}", e);
                Err(format!("failed to get value: {}", e))
            }
        }
    }

    pub async fn put(&self, key: &str, value: &str) -> OpResult {
        let (response_tx, response_rx) = oneshot::channel();
        let write_request = WriteRequest {
            operation: WriteOperation::Put {
                key: key.to_string(),
                value: value.to_string(),
            },
            response_tx,
        };

        // Send write request to the queue
        if let Err(_) = self.write_queue_tx.send(write_request) {
            return OpResult {
                success: false,
                error: "write queue channel closed".to_string(),
            };
        }

        // Wait for the response
        match response_rx.await {
            Ok(result) => result,
            Err(_) => OpResult {
                success: false,
                error: "failed to receive response from write worker".to_string(),
            },
        }
    }

    pub async fn delete(&self, key: &str) -> OpResult {
        let (response_tx, response_rx) = oneshot::channel();
        let write_request = WriteRequest {
            operation: WriteOperation::Delete {
                key: key.to_string(),
            },
            response_tx,
        };

        // Send write request to the queue
        if let Err(_) = self.write_queue_tx.send(write_request) {
            return OpResult {
                success: false,
                error: "write queue channel closed".to_string(),
            };
        }

        // Wait for the response
        match response_rx.await {
            Ok(result) => result,
            Err(_) => OpResult {
                success: false,
                error: "failed to receive response from write worker".to_string(),
            },
        }
    }

    pub async fn list_keys(&self, prefix: &str, limit: u32) -> Result<Vec<String>, String> {
        // Acquire read semaphore to limit concurrent read transactions
        let _permit = self.read_semaphore
            .acquire()
            .await
            .map_err(|_| "timeout waiting for read transaction slot".to_string())?;

        // Create a read-only transaction for consistent snapshot
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        let iter = txn.iterator(IteratorMode::Start);
        let mut keys = Vec::new();
        let mut count = 0;
        let limit = if limit > 0 { limit as usize } else { 1000 };
        
        for item in iter {
            if count >= limit {
                break;
            }
            
            match item {
                Ok((key, _value)) => {
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    
                    // If prefix is specified, check if key starts with prefix
                    if !prefix.is_empty() && !key_str.starts_with(prefix) {
                        continue;
                    }
                    
                    keys.push(key_str);
                    count += 1;
                }
                Err(e) => {
                    error!("Iterator error: {}", e);
                    return Err(format!("iterator error: {}", e));
                }
            }
        }

        Ok(keys)
    }
}