use std::sync::Arc;
use std::time::Instant;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use std::net::TcpStream;
use parking_lot::Mutex;
use super::config::{log_transaction_event, log_operation_timing, log_error, log_network_operation, is_debug_enabled};
use super::error::{KvResult, KvError};
use super::future::KvFuture;
use crate::generated::kvstore::*;
use uuid::Uuid;

type ThriftClient = TransactionalKVSyncClient<TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>, TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>>;

pub struct Transaction {
    read_version: i64,
    client: Arc<Mutex<ThriftClient>>,
    operations: Vec<Operation>,
    read_conflict_keys: Vec<String>,
    committed: bool,
    aborted: bool,
    transaction_id: String,
}

impl Transaction {
    pub(crate) fn new(
        read_version: i64,
        client: Arc<Mutex<ThriftClient>>,
    ) -> Self {
        let transaction_id = Uuid::new_v4().to_string();
        if is_debug_enabled() {
            log_transaction_event(&format!("Transaction created with read_version: {}", read_version), Some(&transaction_id));
        }
        Self {
            read_version,
            client,
            operations: Vec::new(),
            read_conflict_keys: Vec::new(),
            committed: false,
            aborted: false,
            transaction_id,
        }
    }
    
    pub fn read_version(&self) -> i64 {
        self.read_version
    }
    
    /// Get a value by key as binary data (checks local writes first, then reads from database)
    pub fn get(&self, key: &[u8], column_family: Option<&str>) -> KvFuture<Option<Vec<u8>>> {
        if self.committed || self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already finished".to_string())) });
        }
        
        if is_debug_enabled() {
            // Use debug representation for binary data that might contain null bytes
            let key_debug = format!("{:?}", key);
            log_transaction_event(&format!("get key: {} (cf: {:?})", key_debug, column_family), Some(&self.transaction_id));
        }
        
        // First check if we have a local write for this key
        let cf_str = column_family.map(|s| s.to_string());
        
        if is_debug_enabled() {
            log_transaction_event(&format!("checking {} local operations for key {:?}", self.operations.len(), key), Some(&self.transaction_id));
        }
        
        for (i, op) in self.operations.iter().rev().enumerate() {
            if is_debug_enabled() {
                log_transaction_event(&format!("op {}: type={}, key={:?}, cf={:?}, key_matches={}, cf_matches={}", 
                    i, op.type_, op.key, op.column_family, 
                    op.key.as_slice() == key, 
                    op.column_family == cf_str), Some(&self.transaction_id));
            }
            
            if op.key.as_slice() == key && op.column_family == cf_str {
                match op.type_.as_str() {
                    "set" => {
                        if is_debug_enabled() {
                            log_transaction_event(&format!("found local set operation with value len {}", op.value.as_ref().map(|v| v.len()).unwrap_or(0)), Some(&self.transaction_id));
                        }
                        let value = op.value.clone();
                        return KvFuture::new(async move { 
                            if is_debug_enabled() {
                                println!("Returning local value immediately");
                            }
                            Ok(value) 
                        });
                    }
                    "delete" => {
                        if is_debug_enabled() {
                            log_transaction_event("found local delete operation", Some(&self.transaction_id));
                        }
                        return KvFuture::new(async { Ok(None) });
                    }
                    _ => {} // Continue checking other operations
                }
            }
        }
        
        if is_debug_enabled() {
            log_transaction_event("no local operation found, reading from database", Some(&self.transaction_id));
        }
        
        // No local write found, read from database
        let client = Arc::clone(&self.client);
        let request = GetRequest::new(
            key.to_vec(),
            column_family.map(|s| s.to_string()),
        );
        
        let tx_id = self.transaction_id.clone();
        KvFuture::new(async move {
            let start_time = Instant::now();
            let response = tokio::task::spawn_blocking(move || {
                client.lock().get(request)
            })
            .await
            .map_err(|e| {
                if is_debug_enabled() {
                    log_error(&format!("Transaction {} get task", tx_id), &format!("{}", e));
                }
                KvError::Unknown(format!("Task join error: {}", e))
            })?
            .map_err(|e| {
                if is_debug_enabled() {
                    log_error(&format!("Transaction {} get thrift", tx_id), &format!("{:?}", e));
                }
                KvError::from(e)
            })?;
            
            if let Some(error) = response.error {
                if is_debug_enabled() {
                    log_error(&format!("Transaction {} get", tx_id), &error);
                }
                return Err(KvError::ServerError(error.to_string()));
            }
            
            let operation_time = start_time.elapsed().as_millis() as u64;
            if is_debug_enabled() {
                log_operation_timing(&format!("Transaction {} get", tx_id), operation_time);
            }
            
            if response.found {
                if is_debug_enabled() {
                    log_network_operation(&format!("Transaction {} get found value (length: {})", tx_id, response.value.len()), None);
                }
                // Return raw bytes instead of converting to string
                Ok(Some(response.value))
            } else {
                if is_debug_enabled() {
                    log_network_operation(&format!("Transaction {} get key not found", tx_id), None);
                }
                Ok(None)
            }
        })
    }
    
    /// Set a key-value pair as binary data (buffered for atomic commit)
    pub fn set(&mut self, key: &[u8], value: &[u8], column_family: Option<&str>) -> KvResult<()> {
        if self.committed || self.aborted {
            return Err(KvError::TransactionNotFound("Transaction already finished".to_string()));
        }
        
        if is_debug_enabled() {
            // Use debug representation for binary data that might contain null bytes
            let key_debug = format!("{:?}", key);
            let value_len = value.len();
            log_transaction_event(&format!("set key: {} (cf: {:?}, value_len: {})", key_debug, column_family, value_len), Some(&self.transaction_id));
        }
        
        let operation = Operation::new(
            "set".to_string(),
            key.to_vec(),
            Some(value.to_vec()),
            column_family.map(|s| s.to_string()),
        );
        
        self.operations.push(operation);
        Ok(())
    }
    
    /// Delete a key using binary data (buffered for atomic commit)
    pub fn delete(&mut self, key: &[u8], column_family: Option<&str>) -> KvResult<()> {
        if self.committed || self.aborted {
            return Err(KvError::TransactionNotFound("Transaction already finished".to_string()));
        }
        
        if is_debug_enabled() {
            // Use debug representation for binary data that might contain null bytes
            let key_debug = format!("{:?}", key);
            log_transaction_event(&format!("delete key: {} (cf: {:?})", key_debug, column_family), Some(&self.transaction_id));
        }
        
        let operation = Operation::new(
            "delete".to_string(),
            key.to_vec(),
            None,
            column_family.map(|s| s.to_string()),
        );
        
        self.operations.push(operation);
        Ok(())
    }
    
    /// Get a range of key-value pairs as binary data
    pub fn get_range(&self, start_key: &[u8], end_key: Option<&[u8]>, limit: Option<u32>, column_family: Option<&str>) -> KvFuture<Vec<(Vec<u8>, Vec<u8>)>> {
        if self.committed || self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already finished".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = GetRangeRequest::new(
            start_key.to_vec(),
            end_key.map(|k| k.to_vec()),
            limit.map(|l| l as i32),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = tokio::task::spawn_blocking(move || {
                client.lock().get_range(request)
            })
            .await
            .map_err(|e| KvError::Unknown(format!("Task join error: {}", e)))?
            .map_err(KvError::from)?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                return Err(KvError::ServerError(error_msg));
            }
            
            let result = response.key_values
                .into_iter()
                .map(|kv| (kv.key, kv.value))
                .collect();
            
            Ok(result)
        })
    }
    
    /// Add a read conflict for the given key (buffered)
    pub fn add_read_conflict(&mut self, key: &str, _column_family: Option<&str>) -> KvResult<()> {
        if self.committed || self.aborted {
            return Err(KvError::TransactionNotFound("Transaction already finished".to_string()));
        }
        
        // Add to read conflict keys list
        self.read_conflict_keys.push(key.to_string());
        Ok(())
    }
    
    /// Set a versionstamped key (buffered for atomic commit)
    /// Note: The actual key will be generated during commit
    pub fn set_versionstamped_key(&mut self, key_prefix: &str, value: &str, column_family: Option<&str>) -> KvResult<()> {
        if self.committed || self.aborted {
            return Err(KvError::TransactionNotFound("Transaction already finished".to_string()));
        }
        
        let operation = Operation::new(
            "SET_VERSIONSTAMPED_KEY".to_string(),
            key_prefix.as_bytes().to_vec(),
            Some(value.as_bytes().to_vec()),
            column_family.map(|s| s.to_string()),
        );
        
        self.operations.push(operation);
        Ok(())
    }
    
    /// Commit the transaction using atomic commit
    pub fn commit(mut self) -> KvFuture<()> {
        if self.committed {
            return KvFuture::new(async { Ok(()) });
        }
        
        if self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already aborted".to_string())) });
        }
        
        if is_debug_enabled() {
            log_transaction_event(&format!("committing with {} operations, {} read conflicts", self.operations.len(), self.read_conflict_keys.len()), Some(&self.transaction_id));
        }
        
        let client = Arc::clone(&self.client);
        let read_conflict_keys_binary: Vec<Vec<u8>> = self.read_conflict_keys.iter()
            .map(|k| k.as_bytes().to_vec())
            .collect();
        let request = AtomicCommitRequest::new(
            self.read_version,
            self.operations.clone(),
            read_conflict_keys_binary,
            None, // timeout_seconds
        );
        
        self.committed = true;
        let tx_id = self.transaction_id.clone();
        
        KvFuture::new(async move {
            let start_time = Instant::now();
            let response = tokio::task::spawn_blocking(move || {
                client.lock().atomic_commit(request)
            })
            .await
            .map_err(|e| {
                if is_debug_enabled() {
                    log_error(&format!("Transaction {} commit task", tx_id), &format!("{}", e));
                }
                KvError::Unknown(format!("Task join error: {}", e))
            })?
            .map_err(|e| {
                if is_debug_enabled() {
                    log_error(&format!("Transaction {} commit thrift", tx_id), &format!("{:?}", e));
                }
                KvError::from(e)
            })?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                if is_debug_enabled() {
                    log_error(&format!("Transaction {} commit failed", tx_id), &error_msg);
                }
                if let Some(error_code) = response.error_code {
                    if error_code == "CONFLICT" {
                        return Err(KvError::TransactionConflict(error_msg));
                    }
                }
                return Err(KvError::ServerError(error_msg));
            }
            
            let operation_time = start_time.elapsed().as_millis() as u64;
            if is_debug_enabled() {
                log_transaction_event("committed successfully", Some(&tx_id));
                log_operation_timing(&format!("Transaction {} commit", tx_id), operation_time);
            }
            
            Ok(())
        })
    }
    
    /// Abort the transaction (no server call needed, just local cleanup)
    pub fn abort(mut self) -> KvFuture<()> {
        if self.aborted {
            return KvFuture::new(async { Ok(()) });
        }
        
        if self.committed {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already committed".to_string())) });
        }
        
        if is_debug_enabled() {
            log_transaction_event("aborted", Some(&self.transaction_id));
        }
        self.aborted = true;
        
        // No server call needed for abort since operations are buffered locally
        KvFuture::new(async { Ok(()) })
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed && !self.aborted {
            // Auto-abort on drop (no server call needed since operations are buffered locally)
            self.aborted = true;
        }
    }
}

/// Read-only transaction for snapshot operations
pub struct ReadTransaction {
    read_version: i64,
    client: Arc<Mutex<ThriftClient>>,
}

impl ReadTransaction {
    pub(crate) fn new(
        read_version: i64,
        client: Arc<Mutex<ThriftClient>>,
    ) -> Self {
        Self {
            read_version,
            client,
        }
    }
    
    pub fn read_version(&self) -> i64 {
        self.read_version
    }
    
    /// Set the read version for this transaction
    pub async fn set_read_version(&mut self, version: i64) -> KvResult<()> {
        let request = SetReadVersionRequest::new(version);
        let client = Arc::clone(&self.client);
        
        let response = tokio::task::spawn_blocking(move || {
            client.lock().set_read_version(request)
        })
        .await
        .map_err(|e| KvError::Unknown(format!("Task join error: {}", e)))?
        .map_err(KvError::from)?;
        
        if !response.success {
            let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
            return Err(KvError::ServerError(error_msg));
        }
        
        self.read_version = version;
        Ok(())
    }
    
    /// Get a value by key at the snapshot version
    pub fn snapshot_get(&self, key: &[u8], column_family: Option<&str>) -> KvFuture<Option<Vec<u8>>> {
        let read_version = self.read_version;
        let client = Arc::clone(&self.client);
        let request = SnapshotGetRequest::new(
            key.to_vec(),
            read_version,
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = tokio::task::spawn_blocking(move || {
                client.lock().snapshot_get(request)
            })
            .await
            .map_err(|e| KvError::Unknown(format!("Task join error: {}", e)))?
            .map_err(KvError::from)?;
            
            if let Some(error) = response.error {
                return Err(KvError::ServerError(error));
            }
            
            if response.found {
                Ok(Some(response.value))
            } else {
                Ok(None)
            }
        })
    }
    
    /// Get a range of key-value pairs at the snapshot version
    pub fn snapshot_get_range(&self, start_key: &[u8], end_key: Option<&[u8]>, limit: Option<u32>, column_family: Option<&str>) -> KvFuture<Vec<(Vec<u8>, Vec<u8>)>> {
        let read_version = self.read_version;
        let client = Arc::clone(&self.client);
        let request = SnapshotGetRangeRequest::new(
            start_key.to_vec(),
            end_key.map(|k| k.to_vec()),
            read_version,
            limit.map(|l| l as i32),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = tokio::task::spawn_blocking(move || {
                client.lock().snapshot_get_range(request)
            })
            .await
            .map_err(|e| KvError::Unknown(format!("Task join error: {}", e)))?
            .map_err(KvError::from)?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                return Err(KvError::ServerError(error_msg));
            }
            
            let result = response.key_values
                .into_iter()
                .map(|kv| (kv.key, kv.value))
                .collect();
            
            Ok(result)
        })
    }
}