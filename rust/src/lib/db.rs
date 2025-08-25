use rocksdb::{TransactionDB, TransactionDBOptions, Options, TransactionOptions, BlockBasedOptions, Cache, WriteOptions, ReadOptions};
use std::sync::{Arc, RwLock, mpsc};
use std::collections::HashMap;
use tracing::error;
use std::time::Duration;
use super::config::Config;

pub struct TransactionalKvDatabase {
    db: Arc<TransactionDB>,
    cf_handles: HashMap<String, String>,
    _config: Config,
    write_queue_tx: mpsc::Sender<WriteRequest>,
    fault_injection: Arc<RwLock<Option<FaultInjectionConfig>>>,
    // Global version counter for read versioning (FoundationDB-style)
    current_version: Arc<std::sync::atomic::AtomicU64>,
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
    pub error_code: Option<String>,
}

#[derive(Debug)]
pub enum WriteOperation {
    Put { key: String, value: String },
    Delete { key: String },
}

#[derive(Debug)]
pub struct WriteRequest {
    pub operation: WriteOperation,
    pub response_tx: mpsc::Sender<OpResult>,
}

#[derive(Debug, Clone)]
pub struct FaultInjectionConfig {
    pub fault_type: String,
    pub probability: f64,
    pub duration_ms: i32,
    pub target_operation: Option<String>,
}

// FoundationDB-style client-side transaction structures
#[derive(Debug, Clone)]
pub struct AtomicOperation {
    pub op_type: String,  // "set" or "delete"
    pub key: String,
    pub value: Option<String>,  // Only for "set" operations
    pub column_family: Option<String>,
}

#[derive(Debug)]
pub struct AtomicCommitRequest {
    pub read_version: u64,
    pub operations: Vec<AtomicOperation>,
    pub read_conflict_keys: Vec<String>,
    pub timeout_seconds: u64,
}

#[derive(Debug)]  
pub struct AtomicCommitResult {
    pub success: bool,
    pub error: String,
    pub error_code: Option<String>,
    pub committed_version: Option<u64>,
}

impl OpResult {
    pub fn success() -> Self {
        OpResult {
            success: true,
            error: String::new(),
            error_code: None,
        }
    }

    pub fn error(message: &str, code: Option<&str>) -> Self {
        OpResult {
            success: false,
            error: message.to_string(),
            error_code: code.map(|c| c.to_string()),
        }
    }

    pub fn from_result<T>(result: Result<T, &str>, error_code: Option<&str>) -> Self {
        match result {
            Ok(_) => Self::success(),
            Err(msg) => Self::error(msg, error_code),
        }
    }
}

impl TransactionalKvDatabase {
    pub fn new(db_path: &str, config: &Config, column_families: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        opts.set_write_buffer_size((config.rocksdb.write_buffer_size_mb * 1024 * 1024) as usize);
        opts.set_max_write_buffer_number(config.rocksdb.max_write_buffer_number as i32);
        opts.set_max_background_jobs(config.rocksdb.max_background_jobs as i32);
        
        if config.rocksdb.dynamic_level_bytes {
            opts.set_level_compaction_dynamic_level_bytes(true);
        }
        
        opts.set_bytes_per_sync(config.rocksdb.bytes_per_sync);
        opts.set_compression_per_level(&config.compression.get_compression_per_level());
        opts.set_target_file_size_base((config.compaction.target_file_size_base_mb * 1024 * 1024) as u64);
        opts.set_target_file_size_multiplier(config.compaction.target_file_size_multiplier as i32);
        opts.set_max_bytes_for_level_base((config.compaction.max_bytes_for_level_base_mb * 1024 * 1024) as u64);
        opts.set_max_bytes_for_level_multiplier(config.compaction.max_bytes_for_level_multiplier as f64);
        
        let mut table_opts = BlockBasedOptions::default();
        let cache = Cache::new_lru_cache((config.rocksdb.block_cache_size_mb * 1024 * 1024) as usize);
        table_opts.set_block_cache(&cache);
        table_opts.set_block_size((config.rocksdb.block_size_kb * 1024) as usize);
        
        if config.bloom_filter.enabled {
            table_opts.set_bloom_filter(config.bloom_filter.bits_per_key as f64, false);
        }
        
        table_opts.set_cache_index_and_filter_blocks(config.cache.cache_index_and_filter_blocks);
        table_opts.set_pin_l0_filter_and_index_blocks_in_cache(config.cache.pin_l0_filter_and_index_blocks_in_cache);
        opts.set_block_based_table_factory(&table_opts);
        let txn_db_opts = TransactionDBOptions::default();
        let db = TransactionDB::open(&opts, &txn_db_opts, db_path)?;
        let db = Arc::new(db);
        
        let mut cf_handles = HashMap::new();
        cf_handles.insert("default".to_string(), "default".to_string());
        for cf_name in column_families {
            cf_handles.insert(cf_name.to_string(), cf_name.to_string());
        }
        
        // Create write queue channel
        let (write_queue_tx, write_queue_rx) = mpsc::channel();
        
        // Start write worker thread
        let db_for_worker = db.clone();
        std::thread::spawn(move || {
            Self::write_worker(db_for_worker, write_queue_rx);
        });
        
        Ok(Self {
            db,
            cf_handles,
            _config: config.clone(),
            write_queue_tx,
            fault_injection: Arc::new(RwLock::new(None)),
            current_version: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        })
    }

    // FoundationDB-style client-side transaction methods
    
    /// Get current read version for transaction consistency
    pub fn get_read_version(&self) -> u64 {
        self.current_version.load(std::sync::atomic::Ordering::SeqCst)
    }
    
    /// Snapshot read at specific version (used by client for consistent reads)
    pub fn snapshot_read(&self, key: &str, _read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

        // Validate column family if specified
        if let Some(cf_name) = column_family {
            if !self.cf_handles.contains_key(cf_name) {
                return Err(format!("Column family '{}' not found", cf_name));
            }
        }

        // Create snapshot at specific version (simplified - in real FDB this would be more complex)
        let snapshot = self.db.snapshot();
        let mut read_opts = ReadOptions::default();
        read_opts.set_snapshot(&snapshot);
        
        match self.db.get_opt(key, &read_opts) {
            Ok(Some(value)) => {
                let value_str = String::from_utf8_lossy(&value).to_string();
                Ok(GetResult { value: value_str, found: true })
            }
            Ok(None) => Ok(GetResult { value: String::new(), found: false }),
            Err(e) => Err(format!("Snapshot read failed: {}", e))
        }
    }
    
    /// Atomic commit of client-buffered operations with conflict detection
    pub fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
        // Check for fault injection
        if let Some(error_code) = self.should_inject_fault("commit") {
            let error_msg = match error_code.as_str() {
                "TIMEOUT" => "Operation timeout",
                "CONFLICT" => "Transaction conflict",
                _ => "Fault injected",
            };
            return AtomicCommitResult {
                success: false,
                error: error_msg.to_string(),
                error_code: Some(error_code),
                committed_version: None,
            };
        }

        // Conflict detection: check if any read keys were modified since read_version
        let current_version = self.get_read_version();
        if request.read_version < current_version {
            // In a real implementation, we'd check if specific read keys were modified
            // For now, we'll do a simplified version check
            for read_key in &request.read_conflict_keys {
                // Simplified conflict check - in reality this would check key-specific versions
                if self.has_key_been_modified_since(read_key, request.read_version) {
                    return AtomicCommitResult {
                        success: false,
                        error: format!("Conflict detected on key: {}", read_key),
                        error_code: Some("CONFLICT".to_string()),
                        committed_version: None,
                    };
                }
            }
        }

        // Create atomic RocksDB transaction 
        let write_opts = WriteOptions::default();
        let mut txn_opts = TransactionOptions::default();
        txn_opts.set_snapshot(true);
        let rocksdb_txn = self.db.transaction_opt(&write_opts, &txn_opts);

        // Apply all operations atomically
        for operation in &request.operations {
            let result = match operation.op_type.as_str() {
                "set" => {
                    if let Some(value) = &operation.value {
                        rocksdb_txn.put(&operation.key, value)
                    } else {
                        return AtomicCommitResult {
                            success: false,
                            error: "Set operation missing value".to_string(),
                            error_code: Some("INVALID_OPERATION".to_string()),
                            committed_version: None,
                        };
                    }
                }
                "delete" => rocksdb_txn.delete(&operation.key),
                _ => {
                    return AtomicCommitResult {
                        success: false,
                        error: format!("Unknown operation type: {}", operation.op_type),
                        error_code: Some("INVALID_OPERATION".to_string()),
                        committed_version: None,
                    };
                }
            };

            if let Err(e) = result {
                return AtomicCommitResult {
                    success: false,
                    error: format!("Operation failed: {}", e),
                    error_code: Some("OPERATION_FAILED".to_string()),
                    committed_version: None,
                };
            }
        }

        // Commit transaction atomically
        match rocksdb_txn.commit() {
            Ok(_) => {
                // Increment global version counter
                let committed_version = self.current_version.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                
                AtomicCommitResult {
                    success: true,
                    error: String::new(),
                    error_code: None,
                    committed_version: Some(committed_version),
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                if error_msg.contains("Resource busy") || error_msg.contains("Deadlock") {
                    AtomicCommitResult {
                        success: false,
                        error: "Transaction conflict".to_string(),
                        error_code: Some("CONFLICT".to_string()),
                        committed_version: None,
                    }
                } else if error_msg.contains("TimedOut") {
                    AtomicCommitResult {
                        success: false,
                        error: "Transaction timed out".to_string(),
                        error_code: Some("TIMEOUT".to_string()),
                        committed_version: None,
                    }
                } else {
                    AtomicCommitResult {
                        success: false,
                        error: format!("Commit failed: {}", e),
                        error_code: Some("COMMIT_FAILED".to_string()),
                        committed_version: None,
                    }
                }
            }
        }
    }

    /// Simplified conflict detection (in reality this would be more sophisticated)
    fn has_key_been_modified_since(&self, _key: &str, _since_version: u64) -> bool {
        // Simplified implementation - always return false (no conflict)
        // In a real system, this would check key-specific version metadata
        false
    }

    // Non-transactional operations for backward compatibility
    pub fn get(&self, key: &str) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

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

    pub fn put(&self, key: &str, value: &str) -> OpResult {
        let (response_tx, response_rx) = mpsc::channel();
        let write_request = WriteRequest {
            operation: WriteOperation::Put {
                key: key.to_string(),
                value: value.to_string(),
            },
            response_tx,
        };

        // Send write request to the queue
        if let Err(_) = self.write_queue_tx.send(write_request) {
            return OpResult { success: false, error: "write queue channel closed".to_string(), error_code: None };
        }

        // Wait for response
        match response_rx.recv() {
            Ok(response) => response,
            Err(_) => OpResult { success: false, error: "response channel closed".to_string(), error_code: None },
        }
    }

    pub fn delete(&self, key: &str) -> OpResult {
        let (response_tx, response_rx) = mpsc::channel();
        let write_request = WriteRequest {
            operation: WriteOperation::Delete {
                key: key.to_string(),
            },
            response_tx,
        };

        // Send write request to the queue
        if let Err(_) = self.write_queue_tx.send(write_request) {
            return OpResult { success: false, error: "write queue channel closed".to_string(), error_code: None };
        }

        // Wait for response
        match response_rx.recv() {
            Ok(response) => response,
            Err(_) => OpResult { success: false, error: "response channel closed".to_string(), error_code: None },
        }
    }

    pub fn list_keys(&self, prefix: &str, limit: u32) -> Result<Vec<String>, String> {
        let mut keys = Vec::new();
        let iter = self.db.prefix_iterator(prefix);
        
        for (count, result) in iter.enumerate() {
            if count >= limit as usize {
                break;
            }
            
            match result {
                Ok((key, _)) => {
                    keys.push(String::from_utf8_lossy(&key).to_string());
                }
                Err(e) => {
                    error!("Failed to iterate key: {}", e);
                    return Err(format!("Failed to iterate keys: {}", e));
                }
            }
        }
        
        Ok(keys)
    }

    // Fault injection for testing
    pub fn set_fault_injection(&self, config: Option<FaultInjectionConfig>) -> OpResult {
        let mut fault_injection = self.fault_injection.write().unwrap();
        *fault_injection = config;
        OpResult::success()
    }

    pub fn clear_fault_injection(&self) {
        let mut fault_injection = self.fault_injection.write().unwrap();
        *fault_injection = None;
    }

    fn should_inject_fault(&self, operation: &str) -> Option<String> {
        let fault_injection = self.fault_injection.read().unwrap();
        if let Some(ref config) = *fault_injection {
            if let Some(ref target) = config.target_operation {
                if target != operation {
                    return None;
                }
            }
            
            let random: f64 = rand::random();
            if random < config.probability {
                if config.duration_ms > 0 {
                    std::thread::sleep(Duration::from_millis(config.duration_ms as u64));
                }
                return Some(config.fault_type.clone());
            }
        }
        None
    }

    fn write_worker(db: Arc<TransactionDB>, write_queue_rx: mpsc::Receiver<WriteRequest>) {
        while let Ok(request) = write_queue_rx.recv() {
            let result = match request.operation {
                WriteOperation::Put { key, value } => {
                    match db.put(&key, &value) {
                        Ok(_) => OpResult::success(),
                        Err(e) => OpResult::error(&format!("put failed: {}", e), Some("PUT_FAILED")),
                    }
                }
                WriteOperation::Delete { key } => {
                    match db.delete(&key) {
                        Ok(_) => OpResult::success(),
                        Err(e) => OpResult::error(&format!("delete failed: {}", e), Some("DELETE_FAILED")),
                    }
                }
            };

            let _ = request.response_tx.send(result);
        }
    }
}

// Add tests module at the end
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_foundationdb_style_transactions() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_db");
        let config = Config::default();
        
        let db = TransactionalKvDatabase::new(db_path.to_str().unwrap(), &config, &[]).unwrap();
        
        // Test get read version
        let read_version = db.get_read_version();
        assert!(read_version > 0);
        
        // Test snapshot read on non-existent key
        let result = db.snapshot_read("test_key", read_version, None);
        assert!(result.is_ok());
        assert!(!result.unwrap().found);
        
        // Test atomic commit with write operation
        let operations = vec![AtomicOperation {
            op_type: "set".to_string(),
            key: "test_key".to_string(),
            value: Some("test_value".to_string()),
            column_family: None,
        }];
        
        let commit_request = AtomicCommitRequest {
            read_version,
            operations,
            read_conflict_keys: vec![],
            timeout_seconds: 60,
        };
        
        let commit_result = db.atomic_commit(commit_request);
        assert!(commit_result.success);
        assert!(commit_result.committed_version.is_some());
        
        // Test snapshot read on existing key
        let new_read_version = db.get_read_version();
        let result = db.snapshot_read("test_key", new_read_version, None);
        assert!(result.is_ok());
        let get_result = result.unwrap();
        assert!(get_result.found);
        assert_eq!(get_result.value, "test_value");
    }
}