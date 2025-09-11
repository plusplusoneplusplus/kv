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
    pub value: Vec<u8>,
    pub found: bool,
}

#[derive(Debug, Clone)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct GetRangeResult {
    pub key_values: Vec<KeyValue>,
    pub success: bool,
    pub error: String,
}

#[derive(Debug)]
pub struct OpResult {
    pub success: bool,
    pub error: String,
    pub error_code: Option<String>,
}

#[derive(Debug)]
pub enum WriteOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
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
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,  // Only for "set" operations
    pub column_family: Option<String>,
}

#[derive(Debug)]
pub struct AtomicCommitRequest {
    pub read_version: u64,
    pub operations: Vec<AtomicOperation>,
    pub read_conflict_keys: Vec<Vec<u8>>,
    pub timeout_seconds: u64,
}

#[derive(Debug)]  
pub struct AtomicCommitResult {
    pub success: bool,
    pub error: String,
    pub error_code: Option<String>,
    pub committed_version: Option<u64>,
    pub generated_keys: Vec<Vec<u8>>,  // Keys generated for versionstamped operations
    pub generated_values: Vec<Vec<u8>>,  // Values generated for versionstamped operations
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
    pub fn snapshot_read(&self, key: &[u8], _read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
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
                Ok(GetResult { value, found: true })
            }
            Ok(None) => Ok(GetResult { value: Vec::new(), found: false }),
            Err(e) => Err(format!("Snapshot read failed: {}", e))
        }
    }

    /// Generate a versionstamp by appending commit version and batch order to a prefix
    fn generate_versionstamp(&self, prefix: &[u8], commit_version: u64, batch_order: u16) -> Vec<u8> {
        let mut versionstamped_data = prefix.to_vec();
        
        // Append 8-byte commit version + 2-byte batch order (10 bytes total, FoundationDB-compatible)
        versionstamped_data.extend_from_slice(&commit_version.to_be_bytes());
        versionstamped_data.extend_from_slice(&batch_order.to_be_bytes());
        
        versionstamped_data
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
                generated_keys: Vec::new(),
                generated_values: Vec::new(),
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
                        error: format!("Conflict detected on key: {:?}", read_key),
                        error_code: Some("CONFLICT".to_string()),
                        committed_version: None,
                        generated_keys: Vec::new(),
                        generated_values: Vec::new(),
                    };
                }
            }
        }

        // Create atomic RocksDB transaction 
        let write_opts = WriteOptions::default();
        let mut txn_opts = TransactionOptions::default();
        txn_opts.set_snapshot(true);
        let rocksdb_txn = self.db.transaction_opt(&write_opts, &txn_opts);

        // Pre-allocate the commit version for versionstamped operations
        let commit_version = self.current_version.load(std::sync::atomic::Ordering::SeqCst) + 1;
        let mut generated_keys = Vec::new();
        let mut generated_values = Vec::new();
        let mut batch_order: u16 = 0; // 2-byte counter for operations within transaction
        
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
                            generated_keys: Vec::new(),
                            generated_values: Vec::new(),
                        };
                    }
                }
                "delete" => rocksdb_txn.delete(&operation.key),
                "SET_VERSIONSTAMPED_KEY" => {
                    if let Some(value) = &operation.value {
                        // Generate a unique key by appending the commit version to the key prefix
                        let versionstamped_key = self.generate_versionstamp(&operation.key, commit_version, batch_order);
                        batch_order += 1; // Increment for next versionstamped operation
                        
                        // Store the generated key for returning to client
                        generated_keys.push(versionstamped_key.clone());
                        
                        // Apply the operation with the generated key
                        rocksdb_txn.put(&versionstamped_key, value)
                    } else {
                        return AtomicCommitResult {
                            success: false,
                            error: "Versionstamped key operation missing value".to_string(),
                            error_code: Some("INVALID_OPERATION".to_string()),
                            committed_version: None,
                            generated_keys: Vec::new(),
                            generated_values: Vec::new(),
                        };
                    }
                }
                "SET_VERSIONSTAMPED_VALUE" => {
                    if let Some(value_prefix) = &operation.value {
                        // Generate a unique value by appending the commit version to the value prefix
                        let versionstamped_value = self.generate_versionstamp(value_prefix, commit_version, batch_order);
                        batch_order += 1; // Increment for next versionstamped operation
                        
                        // Store the generated value for returning to client
                        generated_values.push(versionstamped_value.clone());
                        
                        // Apply the operation with the generated value
                        rocksdb_txn.put(&operation.key, &versionstamped_value)
                    } else {
                        return AtomicCommitResult {
                            success: false,
                            error: "Versionstamped value operation missing value prefix".to_string(),
                            error_code: Some("INVALID_OPERATION".to_string()),
                            committed_version: None,
                            generated_keys: Vec::new(),
                            generated_values: Vec::new(),
                        };
                    }
                }
                _ => {
                    return AtomicCommitResult {
                        success: false,
                        error: format!("Unknown operation type: {}", operation.op_type),
                        error_code: Some("INVALID_OPERATION".to_string()),
                        committed_version: None,
                        generated_keys: Vec::new(),
                        generated_values: Vec::new(),
                    };
                }
            };

            if let Err(e) = result {
                return AtomicCommitResult {
                    success: false,
                    error: format!("Operation failed: {}", e),
                    error_code: Some("OPERATION_FAILED".to_string()),
                    committed_version: None,
                    generated_keys: Vec::new(),
                    generated_values: Vec::new(),
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
                    generated_keys,
                    generated_values,
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
                        generated_keys: Vec::new(),
                        generated_values: Vec::new(),
                    }
                } else if error_msg.contains("TimedOut") {
                    AtomicCommitResult {
                        success: false,
                        error: "Transaction timed out".to_string(),
                        error_code: Some("TIMEOUT".to_string()),
                        committed_version: None,
                        generated_keys: Vec::new(),
                        generated_values: Vec::new(),
                    }
                } else {
                    AtomicCommitResult {
                        success: false,
                        error: format!("Commit failed: {}", e),
                        error_code: Some("COMMIT_FAILED".to_string()),
                        committed_version: None,
                        generated_keys: Vec::new(),
                        generated_values: Vec::new(),
                    }
                }
            }
        }
    }

    /// Simplified conflict detection (in reality this would be more sophisticated)
    fn has_key_been_modified_since(&self, _key: &[u8], _since_version: u64) -> bool {
        // Simplified implementation - always return false (no conflict)
        // In a real system, this would check key-specific version metadata
        false
    }

    // Non-transactional operations for backward compatibility
    pub fn get(&self, key: &[u8]) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        match txn.get(key) {
            Ok(Some(value)) => {
                Ok(GetResult {
                    value,
                    found: true,
                })
            }
            Ok(None) => {
                Ok(GetResult {
                    value: Vec::new(),
                    found: false,
                })
            }
            Err(e) => {
                error!("Failed to get value: {}", e);
                Err(format!("failed to get value: {}", e))
            }
        }
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> OpResult {
        let (response_tx, response_rx) = mpsc::channel();
        let write_request = WriteRequest {
            operation: WriteOperation::Put {
                key: key.to_vec(),
                value: value.to_vec(),
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

    pub fn delete(&self, key: &[u8]) -> OpResult {
        let (response_tx, response_rx) = mpsc::channel();
        let write_request = WriteRequest {
            operation: WriteOperation::Delete {
                key: key.to_vec(),
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

    pub fn list_keys(&self, prefix: &[u8], limit: u32) -> Result<Vec<Vec<u8>>, String> {
        let mut keys = Vec::new();
        let iter = self.db.prefix_iterator(prefix);
        
        for (count, result) in iter.enumerate() {
            if count >= limit as usize {
                break;
            }
            
            match result {
                Ok((key, _)) => {
                    keys.push(key.to_vec());
                }
                Err(e) => {
                    error!("Failed to iterate key: {}", e);
                    return Err(format!("Failed to iterate keys: {}", e));
                }
            }
        }
        
        Ok(keys)
    }

    pub fn get_range(&self, start_key: &str, end_key: Option<&str>, limit: Option<i32>, _column_family: Option<&str>) -> GetRangeResult {
        let mut key_values = Vec::new();
        let limit = limit.unwrap_or(1000).max(1) as usize;
        
        // Create a read-only transaction for consistency
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        // Use iterator to scan the range
        let iter = txn.iterator(rocksdb::IteratorMode::From(start_key.as_bytes(), rocksdb::Direction::Forward));
        
        for result in iter {
            if key_values.len() >= limit {
                break;
            }
            
            match result {
                Ok((key, value)) => {
                    // If this is a prefix scan (no end_key), ensure the key starts with the start_key
                    if end_key.is_none() && !key.starts_with(start_key.as_bytes()) {
                        break;
                    }
                    
                    // If end_key is specified, stop when we reach it
                    if let Some(end) = end_key {
                        if key.as_ref() >= end.as_bytes() {
                            break;
                        }
                    }
                    
                    key_values.push(KeyValue {
                        key: key.to_vec(),
                        value: value.to_vec(),
                    });
                }
                Err(e) => {
                    error!("Failed to iterate range: {}", e);
                    return GetRangeResult {
                        key_values: Vec::new(),
                        success: false,
                        error: format!("Failed to iterate range: {}", e),
                    };
                }
            }
        }
        
        GetRangeResult {
            key_values,
            success: true,
            error: String::new(),
        }
    }

    pub fn snapshot_get_range(&self, start_key: &[u8], end_key: Option<&[u8]>, _read_version: u64, limit: Option<i32>, _column_family: Option<&str>) -> GetRangeResult {
        let mut key_values = Vec::new();
        let limit = limit.unwrap_or(1000).max(1) as usize;
        
        // Create snapshot at specific version
        let snapshot = self.db.snapshot();
        let mut read_opts = ReadOptions::default();
        read_opts.set_snapshot(&snapshot);
        
        // Use iterator with snapshot for consistent range read
        let iter = self.db.iterator_opt(rocksdb::IteratorMode::From(start_key, rocksdb::Direction::Forward), read_opts);
        
        for result in iter {
            if key_values.len() >= limit {
                break;
            }
            
            match result {
                Ok((key, value)) => {
                    // If this is a prefix scan (no end_key), ensure the key starts with the start_key
                    if end_key.is_none() && !key.starts_with(start_key) {
                        break;
                    }
                    
                    // If end_key is specified, stop when we reach it
                    if let Some(end) = end_key {
                        if key.as_ref() >= end {
                            break;
                        }
                    }
                    
                    key_values.push(KeyValue {
                        key: key.to_vec(),
                        value: value.to_vec(),
                    });
                }
                Err(e) => {
                    error!("Failed to iterate snapshot range: {}", e);
                    return GetRangeResult {
                        key_values: Vec::new(),
                        success: false,
                        error: format!("Failed to iterate snapshot range: {}", e),
                    };
                }
            }
        }
        
        GetRangeResult {
            key_values,
            success: true,
            error: String::new(),
        }
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
        let result = db.snapshot_read(b"test_key", read_version, None);
        assert!(result.is_ok());
        assert!(!result.unwrap().found);
        
        // Test atomic commit with write operation
        let operations = vec![AtomicOperation {
            op_type: "set".to_string(),
            key: b"test_key".to_vec(),
            value: Some(b"test_value".to_vec()),
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
        let result = db.snapshot_read(b"test_key", new_read_version, None);
        assert!(result.is_ok());
        let get_result = result.unwrap();
        assert!(get_result.found);
        assert_eq!(get_result.value, b"test_value".to_vec());
    }

    #[test]
    fn test_range_operations() {
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_range_db");
        let config = Config::default();
        
        let db = TransactionalKvDatabase::new(db_path.to_str().unwrap(), &config, &[]).unwrap();
        
        // Insert some test data
        let put_result = db.put(b"key001", b"value1");
        assert!(put_result.success);
        let put_result = db.put(b"key002", b"value2");
        assert!(put_result.success);
        let put_result = db.put(b"key003", b"value3");
        assert!(put_result.success);
        let put_result = db.put(b"other_key", b"other_value");
        assert!(put_result.success);
        
        // Test get_range with prefix
        let range_result = db.get_range("key", None, Some(10), None);
        assert!(range_result.success);
        assert_eq!(range_result.key_values.len(), 3);
        
        // Test get_range with start and end key
        let range_result = db.get_range("key001", Some("key003"), Some(10), None);
        assert!(range_result.success);
        assert_eq!(range_result.key_values.len(), 2); // key001 and key002, but not key003 (exclusive end)
        
        // Test snapshot get_range
        let read_version = db.get_read_version();
        let snapshot_range_result = db.snapshot_get_range(b"key", None, read_version, Some(10), None);
        assert!(snapshot_range_result.success);
        assert_eq!(snapshot_range_result.key_values.len(), 3);
    }

    #[test]
    fn test_versionstamped_key_operations() {
        use tempfile::tempdir;
        
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_versionstamp_db");
        let config = Config::default();
        
        let db = TransactionalKvDatabase::new(db_path.to_str().unwrap(), &config, &[]).unwrap();
        let initial_read_version = db.get_read_version();
        
        // Test 1: Single versionstamped key operation
        let versionstamp_op = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: b"user_score_".to_vec(),
            value: Some(b"100".to_vec()),
            column_family: None,
        };
        
        let commit_request = AtomicCommitRequest {
            read_version: initial_read_version,
            operations: vec![versionstamp_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let commit_result = db.atomic_commit(commit_request);
        assert!(commit_result.success, "Versionstamp commit should succeed: {}", commit_result.error);
        assert!(commit_result.committed_version.is_some(), "Should have committed version");
        assert_eq!(commit_result.generated_keys.len(), 1, "Should have one generated key");
        
        let generated_key = &commit_result.generated_keys[0];
        assert!(generated_key.starts_with(b"user_score_"), "Generated key should start with prefix");
        assert_eq!(generated_key.len(), b"user_score_".len() + 10, "Generated key should be prefix + 10 bytes for version");
        
        // Test 2: Verify the versionstamped key was actually stored
        let new_read_version = db.get_read_version();
        let get_result = db.snapshot_read(generated_key, new_read_version, None);
        assert!(get_result.is_ok(), "Should be able to read generated key");
        let get_result = get_result.unwrap();
        assert!(get_result.found, "Generated key should be found in database");
        assert_eq!(get_result.value, b"100", "Value should match what was stored");
        
        // Test 3: Multiple versionstamped keys in same transaction
        let vs_op1 = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: b"event_".to_vec(),
            value: Some(b"login".to_vec()),
            column_family: None,
        };
        
        let vs_op2 = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: b"event_".to_vec(),
            value: Some(b"logout".to_vec()),
            column_family: None,
        };
        
        let regular_op = AtomicOperation {
            op_type: "set".to_string(),
            key: b"user_status".to_vec(),
            value: Some(b"active".to_vec()),
            column_family: None,
        };
        
        let multi_commit_request = AtomicCommitRequest {
            read_version: new_read_version,
            operations: vec![vs_op1, vs_op2, regular_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let multi_commit_result = db.atomic_commit(multi_commit_request);
        assert!(multi_commit_result.success, "Multi-operation commit should succeed: {}", multi_commit_result.error);
        assert_eq!(multi_commit_result.generated_keys.len(), 2, "Should have two generated keys");
        
        // Verify both versionstamped keys have the same commit version but different batch order
        let key1 = &multi_commit_result.generated_keys[0];
        let key2 = &multi_commit_result.generated_keys[1];
        
        // Extract version bytes (last 10 bytes) from each key
        let version1 = &key1[key1.len() - 10..];
        let version2 = &key2[key2.len() - 10..];
        
        // Extract commit version (first 8 bytes of version stamp)
        let commit_version1 = &version1[..8];
        let commit_version2 = &version2[..8];
        assert_eq!(commit_version1, commit_version2, "Keys from same transaction should have same commit version");
        
        // Extract batch order (last 2 bytes of version stamp) 
        let batch_order1 = &version1[8..];
        let batch_order2 = &version2[8..];
        assert_ne!(batch_order1, batch_order2, "Keys from same transaction should have different batch order for uniqueness");
        
        // Verify regular key was also stored
        let final_read_version = db.get_read_version();
        let regular_get_result = db.snapshot_read(b"user_status", final_read_version, None);
        assert!(regular_get_result.is_ok());
        let regular_get_result = regular_get_result.unwrap();
        assert!(regular_get_result.found);
        assert_eq!(regular_get_result.value, b"active");
        
        // Test 4: Versionstamped key without value should fail
        let invalid_vs_op = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: b"prefix_".to_vec(),
            value: None,
            column_family: None,
        };
        
        let invalid_commit_request = AtomicCommitRequest {
            read_version: final_read_version,
            operations: vec![invalid_vs_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let invalid_commit_result = db.atomic_commit(invalid_commit_request);
        assert!(!invalid_commit_result.success, "Versionstamp operation without value should fail");
        assert!(invalid_commit_result.error.contains("missing value"), "Error should mention missing value");
        assert_eq!(invalid_commit_result.error_code, Some("INVALID_OPERATION".to_string()));
        assert_eq!(invalid_commit_result.generated_keys.len(), 0, "Should have no generated keys on failure");
        
        // Test 5: Verify keys are unique across different transactions
        let vs_op3 = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: b"unique_test_".to_vec(),
            value: Some(b"value1".to_vec()),
            column_family: None,
        };
        
        let commit3 = AtomicCommitRequest {
            read_version: db.get_read_version(),
            operations: vec![vs_op3],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let result3 = db.atomic_commit(commit3);
        assert!(result3.success);
        
        let vs_op4 = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: b"unique_test_".to_vec(),
            value: Some(b"value2".to_vec()),
            column_family: None,
        };
        
        let commit4 = AtomicCommitRequest {
            read_version: db.get_read_version(),
            operations: vec![vs_op4],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let result4 = db.atomic_commit(commit4);
        assert!(result4.success);
        
        // Generated keys should be different
        let key3 = &result3.generated_keys[0];
        let key4 = &result4.generated_keys[0];
        assert_ne!(key3, key4, "Keys from different transactions should be unique");
        
        // Both should be retrievable with their respective values
        let final_version = db.get_read_version();
        let get3 = db.snapshot_read(key3, final_version, None).unwrap();
        let get4 = db.snapshot_read(key4, final_version, None).unwrap();
        
        assert!(get3.found && get4.found, "Both versionstamped keys should be found");
        assert_eq!(get3.value, b"value1");
        assert_eq!(get4.value, b"value2");
    }

    #[test]
    fn test_versionstamped_value_operations() {
        use tempfile::tempdir;
        
        let temp_dir = tempdir().unwrap();
        let db_path = temp_dir.path().join("test_versionstamp_value_db");
        let config = Config::default();
        
        let db = TransactionalKvDatabase::new(db_path.to_str().unwrap(), &config, &[]).unwrap();
        let initial_read_version = db.get_read_version();
        
        // Test 1: Single versionstamped value operation
        let versionstamp_op = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
            key: b"user_session".to_vec(),
            value: Some(b"session_".to_vec()),
            column_family: None,
        };
        
        let commit_request = AtomicCommitRequest {
            read_version: initial_read_version,
            operations: vec![versionstamp_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let commit_result = db.atomic_commit(commit_request);
        assert!(commit_result.success, "Versionstamp value commit should succeed: {}", commit_result.error);
        assert!(commit_result.committed_version.is_some(), "Should have committed version");
        assert_eq!(commit_result.generated_values.len(), 1, "Should have one generated value");
        assert_eq!(commit_result.generated_keys.len(), 0, "Should have no generated keys for value operation");
        
        let generated_value = &commit_result.generated_values[0];
        assert!(generated_value.starts_with(b"session_"), "Generated value should start with prefix");
        assert_eq!(generated_value.len(), b"session_".len() + 10, "Generated value should be prefix + 10 bytes for version");
        
        // Test 2: Verify the versionstamped value was actually stored
        let new_read_version = db.get_read_version();
        let get_result = db.snapshot_read(b"user_session", new_read_version, None);
        assert!(get_result.is_ok(), "Should be able to read key with versionstamped value");
        let get_result = get_result.unwrap();
        assert!(get_result.found, "Key with versionstamped value should be found in database");
        assert_eq!(get_result.value, *generated_value, "Value should match the generated versionstamped value");
        
        // Test 3: Multiple versionstamped values in same transaction
        let vs_op1 = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
            key: b"log_entry_1".to_vec(),
            value: Some(b"event_".to_vec()),
            column_family: None,
        };
        
        let vs_op2 = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
            key: b"log_entry_2".to_vec(),
            value: Some(b"event_".to_vec()),
            column_family: None,
        };
        
        let regular_op = AtomicOperation {
            op_type: "set".to_string(),
            key: b"regular_key".to_vec(),
            value: Some(b"regular_value".to_vec()),
            column_family: None,
        };
        
        let multi_commit_request = AtomicCommitRequest {
            read_version: db.get_read_version(),
            operations: vec![vs_op1, vs_op2, regular_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let multi_commit_result = db.atomic_commit(multi_commit_request);
        assert!(multi_commit_result.success, "Multi-operation commit should succeed: {}", multi_commit_result.error);
        assert_eq!(multi_commit_result.generated_values.len(), 2, "Should have two generated values");
        
        // Verify both versionstamped values have the same commit version but different batch order
        let value1 = &multi_commit_result.generated_values[0];
        let value2 = &multi_commit_result.generated_values[1];
        
        // Extract version bytes (last 10 bytes) from each value
        let version1 = &value1[value1.len() - 10..];
        let version2 = &value2[value2.len() - 10..];
        
        // Extract commit version (first 8 bytes of version stamp)
        let commit_version1 = &version1[..8];
        let commit_version2 = &version2[..8];
        assert_eq!(commit_version1, commit_version2, "Values from same transaction should have same commit version");
        
        // Extract batch order (last 2 bytes of version stamp) 
        let batch_order1 = &version1[8..];
        let batch_order2 = &version2[8..];
        assert_ne!(batch_order1, batch_order2, "Values from same transaction should have different batch order for uniqueness");
        
        // Verify regular key was also stored
        let final_read_version = db.get_read_version();
        let regular_result = db.snapshot_read(b"regular_key", final_read_version, None);
        assert!(regular_result.is_ok() && regular_result.unwrap().found, "Regular key should be stored");
        
        // Test 4: Error handling for missing value prefix
        let invalid_vs_op = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
            key: b"test_key".to_vec(),
            value: None,
            column_family: None,
        };
        
        let invalid_commit_request = AtomicCommitRequest {
            read_version: final_read_version,
            operations: vec![invalid_vs_op],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let invalid_commit_result = db.atomic_commit(invalid_commit_request);
        assert!(!invalid_commit_result.success, "Versionstamp value operation without value prefix should fail");
        assert!(invalid_commit_result.error.contains("missing value prefix"), "Error should mention missing value prefix");
        assert_eq!(invalid_commit_result.error_code, Some("INVALID_OPERATION".to_string()));
        assert_eq!(invalid_commit_result.generated_values.len(), 0, "Should have no generated values on failure");
        
        // Test 5: Verify values are unique across different transactions
        let vs_op3 = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
            key: b"unique_test_1".to_vec(),
            value: Some(b"prefix_".to_vec()),
            column_family: None,
        };
        
        let commit3 = AtomicCommitRequest {
            read_version: db.get_read_version(),
            operations: vec![vs_op3],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let result3 = db.atomic_commit(commit3);
        assert!(result3.success);
        
        let vs_op4 = AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
            key: b"unique_test_2".to_vec(),
            value: Some(b"prefix_".to_vec()),
            column_family: None,
        };
        
        let commit4 = AtomicCommitRequest {
            read_version: db.get_read_version(),
            operations: vec![vs_op4],
            read_conflict_keys: vec![],
            timeout_seconds: 30,
        };
        
        let result4 = db.atomic_commit(commit4);
        assert!(result4.success);
        
        // Generated values should be different
        let value3 = &result3.generated_values[0];
        let value4 = &result4.generated_values[0];
        assert_ne!(value3, value4, "Values from different transactions should be unique");
        
        // Both should be retrievable with their respective versionstamped values
        let final_version = db.get_read_version();
        let get3 = db.snapshot_read(b"unique_test_1", final_version, None).unwrap();
        let get4 = db.snapshot_read(b"unique_test_2", final_version, None).unwrap();
        
        assert!(get3.found && get4.found, "Both versionstamped values should be found");
        assert_eq!(get3.value, *value3, "First key should have its versionstamped value");
        assert_eq!(get4.value, *value4, "Second key should have its versionstamped value");
    }
}