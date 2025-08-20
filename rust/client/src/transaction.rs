use std::sync::Arc;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use std::net::TcpStream;
use parking_lot::Mutex;
use crate::error::{KvResult, KvError};
use crate::future::KvFuture;
use crate::kvstore::*;

pub struct Transaction {
    transaction_id: String,
    client: Arc<Mutex<TransactionalKVSyncClient<TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>, TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>>>>,
    committed: bool,
    aborted: bool,
}

impl Transaction {
    pub(crate) fn new(
        transaction_id: String,
        client: Arc<Mutex<TransactionalKVSyncClient<TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>, TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>>>>,
    ) -> Self {
        Self {
            transaction_id,
            client,
            committed: false,
            aborted: false,
        }
    }
    
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }
    
    /// Get a value by key
    pub fn get(&self, key: &str, column_family: Option<&str>) -> KvFuture<Option<String>> {
        if self.committed || self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already finished".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = GetRequest::new(
            self.transaction_id.clone(),
            key.to_string(),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = client.lock().get(request)
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
    
    /// Set a key-value pair
    pub fn set(&self, key: &str, value: &str, column_family: Option<&str>) -> KvFuture<()> {
        if self.committed || self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already finished".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = SetRequest::new(
            self.transaction_id.clone(),
            key.to_string(),
            value.to_string(),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = client.lock().set_key(request)
                .map_err(KvError::from)?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                if let Some(error_code) = response.error_code {
                    if error_code == "CONFLICT" {
                        return Err(KvError::TransactionConflict(error_msg));
                    }
                }
                return Err(KvError::ServerError(error_msg));
            }
            
            Ok(())
        })
    }
    
    /// Delete a key
    pub fn delete(&self, key: &str, column_family: Option<&str>) -> KvFuture<()> {
        if self.committed || self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already finished".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = DeleteRequest::new(
            self.transaction_id.clone(),
            key.to_string(),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = client.lock().delete_key(request)
                .map_err(KvError::from)?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                if let Some(error_code) = response.error_code {
                    if error_code == "CONFLICT" {
                        return Err(KvError::TransactionConflict(error_msg));
                    }
                }
                return Err(KvError::ServerError(error_msg));
            }
            
            Ok(())
        })
    }
    
    /// Get a range of key-value pairs
    pub fn get_range(&self, start_key: &str, end_key: Option<&str>, limit: Option<u32>, column_family: Option<&str>) -> KvFuture<Vec<(String, String)>> {
        if self.committed || self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already finished".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = GetRangeRequest::new(
            self.transaction_id.clone(),
            start_key.to_string(),
            end_key.map(|s| s.to_string()),
            limit.map(|l| l as i32),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = client.lock().get_range(request)
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
    
    /// Add a read conflict for the given key
    pub fn add_read_conflict(&self, key: &str, column_family: Option<&str>) -> KvFuture<()> {
        if self.committed || self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already finished".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = AddReadConflictRequest::new(
            self.transaction_id.clone(),
            key.to_string(),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = client.lock().add_read_conflict(request)
                .map_err(KvError::from)?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                return Err(KvError::ServerError(error_msg));
            }
            
            Ok(())
        })
    }
    
    /// Set a versionstamped key
    pub fn set_versionstamped_key(&self, key_prefix: &str, value: &str, column_family: Option<&str>) -> KvFuture<String> {
        if self.committed || self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already finished".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = SetVersionstampedKeyRequest::new(
            self.transaction_id.clone(),
            key_prefix.to_string(),
            value.to_string(),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = client.lock().set_versionstamped_key(request)
                .map_err(KvError::from)?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                return Err(KvError::ServerError(error_msg));
            }
            
            Ok(response.generated_key)
        })
    }
    
    /// Commit the transaction
    pub fn commit(mut self) -> KvFuture<()> {
        if self.committed {
            return KvFuture::new(async { Ok(()) });
        }
        
        if self.aborted {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already aborted".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = CommitTransactionRequest::new(self.transaction_id.clone());
        
        self.committed = true;
        
        KvFuture::new(async move {
            let response = client.lock().commit_transaction(request)
                .map_err(KvError::from)?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                if let Some(error_code) = response.error_code {
                    if error_code == "CONFLICT" {
                        return Err(KvError::TransactionConflict(error_msg));
                    }
                }
                return Err(KvError::ServerError(error_msg));
            }
            
            Ok(())
        })
    }
    
    /// Abort the transaction
    pub fn abort(mut self) -> KvFuture<()> {
        if self.aborted {
            return KvFuture::new(async { Ok(()) });
        }
        
        if self.committed {
            return KvFuture::new(async { Err(KvError::TransactionNotFound("Transaction already committed".to_string())) });
        }
        
        let client = Arc::clone(&self.client);
        let request = AbortTransactionRequest::new(self.transaction_id.clone());
        
        self.aborted = true;
        
        KvFuture::new(async move {
            let response = client.lock().abort_transaction(request)
                .map_err(KvError::from)?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                return Err(KvError::ServerError(error_msg));
            }
            
            Ok(())
        })
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.committed && !self.aborted {
            // Auto-abort on drop to clean up resources
            let client = Arc::clone(&self.client);
            let request = AbortTransactionRequest::new(self.transaction_id.clone());
            let _ = client.lock().abort_transaction(request);
        }
    }
}

/// Read-only transaction for snapshot operations
pub struct ReadTransaction {
    transaction_id: String,
    client: Arc<Mutex<TransactionalKVSyncClient<TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>, TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>>>>,
    read_version: Option<i64>,
}

impl ReadTransaction {
    pub(crate) fn new(
        transaction_id: String,
        client: Arc<Mutex<TransactionalKVSyncClient<TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>, TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>>>>,
    ) -> Self {
        Self {
            transaction_id,
            client,
            read_version: None,
        }
    }
    
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }
    
    /// Set the read version for this transaction
    pub async fn set_read_version(&mut self, version: i64) -> KvResult<()> {
        let request = SetReadVersionRequest::new(
            self.transaction_id.clone(),
            version,
        );
        
        let response = self.client.lock().set_read_version(request)
            .map_err(KvError::from)?;
        
        if !response.success {
            let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
            return Err(KvError::ServerError(error_msg));
        }
        
        self.read_version = Some(version);
        Ok(())
    }
    
    /// Get a value by key at the snapshot version
    pub fn snapshot_get(&self, key: &str, column_family: Option<&str>) -> KvFuture<Option<String>> {
        let read_version = self.read_version.unwrap_or(0);
        let client = Arc::clone(&self.client);
        let request = SnapshotGetRequest::new(
            self.transaction_id.clone(),
            key.to_string(),
            read_version,
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = client.lock().snapshot_get(request)
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
    pub fn snapshot_get_range(&self, start_key: &str, end_key: Option<&str>, limit: Option<u32>, column_family: Option<&str>) -> KvFuture<Vec<(String, String)>> {
        let read_version = self.read_version.unwrap_or(0);
        let client = Arc::clone(&self.client);
        let request = SnapshotGetRangeRequest::new(
            self.transaction_id.clone(),
            start_key.to_string(),
            end_key.map(|s| s.to_string()),
            read_version,
            limit.map(|l| l as i32),
            column_family.map(|s| s.to_string()),
        );
        
        KvFuture::new(async move {
            let response = client.lock().snapshot_get_range(request)
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