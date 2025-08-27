use std::net::TcpStream;
use std::sync::Arc;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use parking_lot::Mutex;
use crate::error::{KvResult, KvError};
use crate::transaction::{Transaction, ReadTransaction};
use crate::future::KvFuture;

// Use the Thrift client types
use crate::kvstore::*;

pub struct KvStoreClient {
    client: Arc<Mutex<TransactionalKVSyncClient<TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>, TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>>>>,
}

impl KvStoreClient {
    /// Connect to the KV store server
    pub fn connect(address: &str) -> KvResult<Self> {
        let stream = TcpStream::connect(address)
            .map_err(|e| KvError::NetworkError(format!("Failed to connect to {}: {}", address, e)))?;
        
        // Set up transports
        let read_transport = TBufferedReadTransport::new(stream.try_clone()
            .map_err(|e| KvError::TransportError(format!("Failed to clone stream: {}", e)))?);
        let write_transport = TBufferedWriteTransport::new(stream);
        
        // Set up protocols
        let input_protocol = TBinaryInputProtocol::new(read_transport, true);
        let output_protocol = TBinaryOutputProtocol::new(write_transport, true);
        
        // Create client
        let client = TransactionalKVSyncClient::new(input_protocol, output_protocol);
        
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }
    
    /// Begin a new transaction
    pub fn begin_transaction(&self, _column_families: Option<Vec<String>>, _timeout_seconds: Option<u64>) -> KvFuture<Transaction> {
        let client = Arc::clone(&self.client);
        let request = GetReadVersionRequest::new();
        
        KvFuture::new(async move {
            let client_for_spawn = Arc::clone(&client);
            let response = tokio::task::spawn_blocking(move || {
                client_for_spawn.lock().get_read_version(request)
            })
            .await
            .map_err(|e| KvError::Unknown(format!("Task join error: {}", e)))?
            .map_err(KvError::from)?;
            
            if !response.success {
                return Err(KvError::ServerError(
                    response.error.unwrap_or_else(|| "Unknown error".to_string())
                ));
            }
            
            // Use the read version for the transaction
            Ok(Transaction::new(response.read_version, client))
        })
    }
    
    /// Begin a new read-only transaction (snapshot)
    pub fn begin_read_transaction(&self, read_version: Option<i64>) -> KvFuture<ReadTransaction> {
        let client = Arc::clone(&self.client);
        
        KvFuture::new(async move {
            let request = GetReadVersionRequest::new();
            let client_for_spawn = Arc::clone(&client);
            let response = tokio::task::spawn_blocking(move || {
                client_for_spawn.lock().get_read_version(request)
            })
            .await
            .map_err(|e| KvError::Unknown(format!("Task join error: {}", e)))?
            .map_err(KvError::from)?;
            
            if !response.success {
                return Err(KvError::ServerError(
                    response.error.unwrap_or_else(|| "Unknown error".to_string())
                ));
            }
            
            let version = read_version.unwrap_or(response.read_version);
            let read_tx = ReadTransaction::new(version, client);
            
            Ok(read_tx)
        })
    }
    
    /// Health check - ping the server
    pub fn ping(&self, message: Option<String>) -> KvFuture<String> {
        let client = Arc::clone(&self.client);
        let request = PingRequest::new(
            message,
            Some(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64),
        );
        
        KvFuture::new(async move {
            let response = tokio::task::spawn_blocking(move || {
                client.lock().ping(request)
            })
            .await
            .map_err(|e| KvError::Unknown(format!("Task join error: {}", e)))?
            .map_err(KvError::from)?;
            
            Ok(response.message)
        })
    }
    
    /// Create a new client with connection pooling (for high-throughput scenarios)
    pub fn connect_with_pool(address: &str, pool_size: usize) -> KvResult<ClientPool> {
        let mut clients = Vec::with_capacity(pool_size);
        
        for _ in 0..pool_size {
            clients.push(Self::connect(address)?);
        }
        
        Ok(ClientPool::new(clients))
    }
}

impl Clone for KvStoreClient {
    fn clone(&self) -> Self {
        Self {
            client: Arc::clone(&self.client),
        }
    }
}

/// Connection pool for high-throughput applications
pub struct ClientPool {
    clients: Vec<KvStoreClient>,
    next_idx: std::sync::atomic::AtomicUsize,
}

impl ClientPool {
    fn new(clients: Vec<KvStoreClient>) -> Self {
        Self {
            clients,
            next_idx: std::sync::atomic::AtomicUsize::new(0),
        }
    }
    
    /// Get a client from the pool using round-robin selection
    pub fn get_client(&self) -> &KvStoreClient {
        let idx = self.next_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % self.clients.len();
        &self.clients[idx]
    }
    
    /// Begin a transaction using a pooled client
    pub fn begin_transaction(&self, column_families: Option<Vec<String>>, timeout_seconds: Option<u64>) -> KvFuture<Transaction> {
        self.get_client().begin_transaction(column_families, timeout_seconds)
    }
    
    /// Begin a read transaction using a pooled client
    pub fn begin_read_transaction(&self, read_version: Option<i64>) -> KvFuture<ReadTransaction> {
        self.get_client().begin_read_transaction(read_version)
    }
}