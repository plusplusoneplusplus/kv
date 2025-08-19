use rocksdb::{TransactionDB, TransactionDBOptions, Options, TransactionOptions, IteratorMode, BlockBasedOptions, Cache};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::{Semaphore, mpsc, oneshot, RwLock};
use tracing::error;
use uuid::Uuid;
use std::time::{SystemTime, Duration};
use crate::config::Config;

pub struct TransactionalKvDatabase {
    db: Arc<TransactionDB>,
    cf_handles: HashMap<String, String>, // Store CF names instead of handles for now
    active_transactions: Arc<RwLock<HashMap<String, ActiveTransaction>>>,
    read_semaphore: Arc<Semaphore>,
    write_queue_tx: mpsc::UnboundedSender<WriteRequest>,
    config: Config,
}

#[derive(Debug)]
pub struct ActiveTransaction {
    pub id: String,
    pub created_at: SystemTime,
    pub timeout_duration: Duration,
    pub column_families: Vec<String>,
    pub read_version: Option<i64>,
    pub read_conflicts: Vec<String>,
    pub read_conflict_ranges: Vec<(String, String)>,
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
pub struct TransactionResult {
    pub transaction_id: String,
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

impl TransactionalKvDatabase {
    pub fn new(db_path: &str, config: &Config, column_families: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
        // Set up RocksDB options from configuration (reuse existing logic)
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
        
        // Set up transaction database options
        let txn_db_opts = TransactionDBOptions::default();
        
        // For now, create database without column families to avoid complexity
        // Column families can be added in a later iteration
        let db = TransactionDB::open(&opts, &txn_db_opts, db_path)?;
        let db = Arc::new(db);
        
        // Store column family names (for future implementation)
        let mut cf_handles = HashMap::new();
        cf_handles.insert("default".to_string(), "default".to_string());
        for cf_name in column_families {
            cf_handles.insert(cf_name.to_string(), cf_name.to_string());
        }
        
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
            cf_handles,
            active_transactions: Arc::new(RwLock::new(HashMap::new())),
            read_semaphore: Arc::new(Semaphore::new(max_read_concurrency)),
            write_queue_tx,
            config: config.clone(),
        })
    }

    // Transaction lifecycle methods
    pub async fn begin_transaction(&self, column_families: Vec<String>, timeout_seconds: u64) -> TransactionResult {
        let transaction_id = Uuid::new_v4().to_string();
        let timeout_duration = Duration::from_secs(timeout_seconds);
        
        let transaction = ActiveTransaction {
            id: transaction_id.clone(),
            created_at: SystemTime::now(),
            timeout_duration,
            column_families,
            read_version: None,
            read_conflicts: Vec::new(),
            read_conflict_ranges: Vec::new(),
        };
        
        // Store the transaction state
        let mut active_txns = self.active_transactions.write().await;
        active_txns.insert(transaction_id.clone(), transaction);
        
        TransactionResult {
            transaction_id,
            success: true,
            error: String::new(),
        }
    }

    pub async fn commit_transaction(&self, transaction_id: &str) -> OpResult {
        let mut active_txns = self.active_transactions.write().await;
        
        if active_txns.remove(transaction_id).is_some() {
            OpResult {
                success: true,
                error: String::new(),
            }
        } else {
            OpResult {
                success: false,
                error: "transaction not found".to_string(),
            }
        }
    }

    pub async fn abort_transaction(&self, transaction_id: &str) -> OpResult {
        let mut active_txns = self.active_transactions.write().await;
        
        if active_txns.remove(transaction_id).is_some() {
            OpResult {
                success: true,
                error: String::new(),
            }
        } else {
            OpResult {
                success: false,
                error: "transaction not found".to_string(),
            }
        }
    }

    // Transactional operations
    pub async fn transactional_get(&self, transaction_id: &str, key: &str, column_family: Option<&str>) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

        // Verify transaction exists
        let active_txns = self.active_transactions.read().await;
        if !active_txns.contains_key(transaction_id) {
            return Err("transaction not found".to_string());
        }
        drop(active_txns);

        // Acquire read semaphore to limit concurrent read transactions
        let _permit = self.read_semaphore
            .acquire()
            .await
            .map_err(|_| "timeout waiting for read transaction slot".to_string())?;

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        // Perform get operation (with or without column family)
        let result = if let Some(cf_name) = column_family {
            if let Some(_cf_handle) = self.cf_handles.get(cf_name) {
                // For now, use default CF until we can store actual CF handles
                txn.get(key)
            } else {
                return Err(format!("column family '{}' not found", cf_name));
            }
        } else {
            txn.get(key)
        };
        
        match result {
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

    // Additional transactional operations
    pub async fn transactional_set(&self, transaction_id: &str, key: &str, value: &str, column_family: Option<&str>) -> OpResult {
        if key.is_empty() {
            return OpResult {
                success: false,
                error: "key cannot be empty".to_string(),
            };
        }

        // Verify transaction exists
        let active_txns = self.active_transactions.read().await;
        if !active_txns.contains_key(transaction_id) {
            return OpResult {
                success: false,
                error: "transaction not found".to_string(),
            };
        }
        drop(active_txns);

        // Create a transaction for the set operation
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        // Perform set operation (with or without column family)
        let result = if let Some(cf_name) = column_family {
            if let Some(_cf_handle) = self.cf_handles.get(cf_name) {
                // For now, use default CF until we can store actual CF handles
                txn.put(key, value)
            } else {
                return OpResult {
                    success: false,
                    error: format!("column family '{}' not found", cf_name),
                };
            }
        } else {
            txn.put(key, value)
        };
        
        match result {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => OpResult {
                        success: true,
                        error: String::new(),
                    },
                    Err(e) => {
                        error!("Failed to commit transactional set: {}", e);
                        OpResult {
                            success: false,
                            error: format!("failed to commit transactional set: {}", e),
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to set value in transaction: {}", e);
                let _ = txn.rollback();
                OpResult {
                    success: false,
                    error: format!("failed to set value in transaction: {}", e),
                }
            }
        }
    }

    pub async fn transactional_delete(&self, transaction_id: &str, key: &str, column_family: Option<&str>) -> OpResult {
        if key.is_empty() {
            return OpResult {
                success: false,
                error: "key cannot be empty".to_string(),
            };
        }

        // Verify transaction exists
        let active_txns = self.active_transactions.read().await;
        if !active_txns.contains_key(transaction_id) {
            return OpResult {
                success: false,
                error: "transaction not found".to_string(),
            };
        }
        drop(active_txns);

        // Create a transaction for the delete operation
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        // Perform delete operation (with or without column family)
        let result = if let Some(cf_name) = column_family {
            if let Some(_cf_handle) = self.cf_handles.get(cf_name) {
                // For now, use default CF until we can store actual CF handles
                txn.delete(key)
            } else {
                return OpResult {
                    success: false,
                    error: format!("column family '{}' not found", cf_name),
                };
            }
        } else {
            txn.delete(key)
        };
        
        match result {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => OpResult {
                        success: true,
                        error: String::new(),
                    },
                    Err(e) => {
                        error!("Failed to commit transactional delete: {}", e);
                        OpResult {
                            success: false,
                            error: format!("failed to commit transactional delete: {}", e),
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to delete key in transaction: {}", e);
                let _ = txn.rollback();
                OpResult {
                    success: false,
                    error: format!("failed to delete key in transaction: {}", e),
                }
            }
        }
    }

    pub async fn transactional_get_range(&self, transaction_id: &str, start_key: &str, end_key: Option<&str>, limit: u32, column_family: Option<&str>) -> Result<Vec<(String, String)>, String> {
        // Verify transaction exists
        let active_txns = self.active_transactions.read().await;
        if !active_txns.contains_key(transaction_id) {
            return Err("transaction not found".to_string());
        }
        drop(active_txns);

        // Acquire read semaphore to limit concurrent read transactions
        let _permit = self.read_semaphore
            .acquire()
            .await
            .map_err(|_| "timeout waiting for read transaction slot".to_string())?;

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        // Use prefix-based iterator for efficiency
        let iter = txn.iterator(IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward));
        
        let mut key_values = Vec::new();
        let mut count = 0;
        let limit = if limit > 0 { limit as usize } else { 1000 };
        
        for item in iter {
            if count >= limit {
                break;
            }
            
            match item {
                Ok((key, value)) => {
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    let value_str = String::from_utf8_lossy(&value).to_string();
                    
                    // Check if we're past the end key (if specified)
                    if let Some(end) = end_key {
                        if key_str.as_str() >= end {
                            break;
                        }
                    }
                    
                    // Check if key starts with start_key prefix
                    if !key_str.starts_with(start_key) {
                        break;
                    }
                    
                    key_values.push((key_str, value_str));
                    count += 1;
                }
                Err(e) => {
                    error!("Iterator error in transactional_get_range: {}", e);
                    return Err(format!("iterator error: {}", e));
                }
            }
        }

        Ok(key_values)
    }

    // Conflict detection methods
    pub async fn add_read_conflict(&self, transaction_id: &str, key: &str, _column_family: Option<&str>) -> OpResult {
        let mut active_txns = self.active_transactions.write().await;
        if let Some(txn) = active_txns.get_mut(transaction_id) {
            txn.read_conflicts.push(key.to_string());
            OpResult {
                success: true,
                error: String::new(),
            }
        } else {
            OpResult {
                success: false,
                error: "transaction not found".to_string(),
            }
        }
    }

    pub async fn add_read_conflict_range(&self, transaction_id: &str, start_key: &str, end_key: &str, _column_family: Option<&str>) -> OpResult {
        let mut active_txns = self.active_transactions.write().await;
        if let Some(txn) = active_txns.get_mut(transaction_id) {
            txn.read_conflict_ranges.push((start_key.to_string(), end_key.to_string()));
            OpResult {
                success: true,
                error: String::new(),
            }
        } else {
            OpResult {
                success: false,
                error: "transaction not found".to_string(),
            }
        }
    }

    // Version management methods
    pub async fn set_read_version(&self, transaction_id: &str, version: i64) -> OpResult {
        let mut active_txns = self.active_transactions.write().await;
        if let Some(txn) = active_txns.get_mut(transaction_id) {
            txn.read_version = Some(version);
            OpResult {
                success: true,
                error: String::new(),
            }
        } else {
            OpResult {
                success: false,
                error: "transaction not found".to_string(),
            }
        }
    }

    pub async fn get_committed_version(&self, transaction_id: &str) -> Result<i64, String> {
        let active_txns = self.active_transactions.read().await;
        if let Some(txn) = active_txns.get(transaction_id) {
            // For now, return a basic timestamp-based version
            // In a real implementation, this would be the actual committed version from RocksDB
            let version = txn.created_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .map_err(|_| "failed to calculate version".to_string())?
                .as_millis() as i64;
            Ok(version)
        } else {
            Err("transaction not found".to_string())
        }
    }

    // Snapshot operations
    pub async fn snapshot_get(&self, transaction_id: &str, key: &str, _read_version: i64, column_family: Option<&str>) -> Result<GetResult, String> {
        // For simplicity, delegate to regular transactional_get
        // In a full implementation, this would use the read_version for snapshot isolation
        self.transactional_get(transaction_id, key, column_family).await
    }

    pub async fn snapshot_get_range(&self, transaction_id: &str, start_key: &str, end_key: Option<&str>, _read_version: i64, limit: u32, column_family: Option<&str>) -> Result<Vec<(String, String)>, String> {
        // For simplicity, delegate to regular transactional_get_range
        // In a full implementation, this would use the read_version for snapshot isolation
        self.transactional_get_range(transaction_id, start_key, end_key, limit, column_family).await
    }

    // Versionstamped operations (basic implementation)
    pub async fn set_versionstamped_key(&self, transaction_id: &str, key_prefix: &str, value: &str, column_family: Option<&str>) -> Result<String, String> {
        // Generate a version stamp based on current time
        let version_stamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| "failed to generate version stamp".to_string())?
            .as_millis();
        
        let generated_key = format!("{}{:016x}", key_prefix, version_stamp);
        
        let result = self.transactional_set(transaction_id, &generated_key, value, column_family).await;
        if result.success {
            Ok(generated_key)
        } else {
            Err(result.error)
        }
    }

    pub async fn set_versionstamped_value(&self, transaction_id: &str, key: &str, value_prefix: &str, column_family: Option<&str>) -> Result<String, String> {
        // Generate a version stamp based on current time
        let version_stamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|_| "failed to generate version stamp".to_string())?
            .as_millis();
        
        let generated_value = format!("{}{:016x}", value_prefix, version_stamp);
        
        let result = self.transactional_set(transaction_id, key, &generated_value, column_family).await;
        if result.success {
            Ok(generated_value)
        } else {
            Err(result.error)
        }
    }

    // Transaction cleanup and management
    pub async fn cleanup_expired_transactions(&self) -> Result<(), String> {
        let mut active_txns = self.active_transactions.write().await;
        let now = SystemTime::now();
        
        active_txns.retain(|_id, txn| {
            if let Ok(elapsed) = now.duration_since(txn.created_at) {
                elapsed < txn.timeout_duration
            } else {
                false // Remove transactions with invalid timestamps
            }
        });
        
        Ok(())
    }

    // Keep write worker logic from original implementation
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

    // Non-transactional operations for backward compatibility
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
        
        // Use prefix-based iterator for efficiency
        let iter = if prefix.is_empty() {
            txn.iterator(IteratorMode::Start)
        } else {
            txn.iterator(IteratorMode::From(prefix.as_bytes(), rocksdb::Direction::Forward))
        };
        
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
                    // This is important because IteratorMode::From continues past the prefix
                    if !prefix.is_empty() && !key_str.starts_with(prefix) {
                        break; // Stop iterating once we're past the prefix
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