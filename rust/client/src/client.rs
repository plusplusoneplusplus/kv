use std::net::TcpStream;
use std::sync::Arc;
use std::time::Instant;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use parking_lot::Mutex;
use crate::config::{ClientConfig, log_connection_event, log_transaction_event, log_operation_timing, log_error, log_network_operation, init_debug_logging};
use crate::error::{KvResult, KvError};
use crate::transaction::{Transaction, ReadTransaction};
use crate::future::KvFuture;

// Use the Thrift client types
use crate::kvstore::*;

pub struct KvStoreClient {
    client: Arc<Mutex<TransactionalKVSyncClient<TBinaryInputProtocol<TBufferedReadTransport<TcpStream>>, TBinaryOutputProtocol<TBufferedWriteTransport<TcpStream>>>>>,
    config: ClientConfig,
    server_address: String,
}

impl KvStoreClient {
    /// Connect to the KV store server with default configuration
    pub fn connect(address: &str) -> KvResult<Self> {
        Self::connect_with_config(address, ClientConfig::default())
    }

    /// Connect to the KV store server with custom configuration
    pub fn connect_with_config(address: &str, config: ClientConfig) -> KvResult<Self> {
        if config.debug_mode {
            init_debug_logging();
            log_connection_event("Attempting connection", address);
        }

        let start_time = Instant::now();
        let stream = TcpStream::connect(address)
            .map_err(|e| {
                let error_msg = format!("Failed to connect to {}: {}", address, e);
                if config.debug_mode {
                    log_error("TCP connection", &error_msg);
                }
                KvError::NetworkError(error_msg)
            })?;
        
        // Set up transports
        let read_transport = TBufferedReadTransport::new(stream.try_clone()
            .map_err(|e| {
                let error_msg = format!("Failed to clone stream: {}", e);
                if config.debug_mode {
                    log_error("Stream cloning", &error_msg);
                }
                KvError::TransportError(error_msg)
            })?);
        let write_transport = TBufferedWriteTransport::new(stream);
        
        // Set up protocols
        let input_protocol = TBinaryInputProtocol::new(read_transport, true);
        let output_protocol = TBinaryOutputProtocol::new(write_transport, true);
        
        // Create client
        let client = TransactionalKVSyncClient::new(input_protocol, output_protocol);
        
        let connection_time = start_time.elapsed().as_millis() as u64;
        if config.debug_mode {
            log_connection_event("Connection established", address);
            log_operation_timing("connect", connection_time);
        }

        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            config,
            server_address: address.to_string(),
        })
    }
    
    /// Begin a new transaction
    pub fn begin_transaction(&self, _column_families: Option<Vec<String>>, _timeout_seconds: Option<u64>) -> KvFuture<Transaction> {
        if self.config.debug_mode {
            log_transaction_event("Starting transaction", None);
            log_network_operation("get_read_version request", None);
        }
        let client = Arc::clone(&self.client);
        let request = GetReadVersionRequest::new();
        
        let debug_mode = self.config.debug_mode;
        KvFuture::new(async move {
            let start_time = Instant::now();
            let client_for_spawn = Arc::clone(&client);
            let response = tokio::task::spawn_blocking(move || {
                client_for_spawn.lock().get_read_version(request)
            })
            .await
            .map_err(|e| {
                let error_msg = format!("Task join error: {}", e);
                if debug_mode {
                    log_error("get_read_version task", &error_msg);
                }
                KvError::Unknown(error_msg)
            })?
            .map_err(|e| {
                if debug_mode {
                    log_error("get_read_version thrift", &format!("{:?}", e));
                }
                KvError::from(e)
            })?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                if debug_mode {
                    log_error("begin_transaction", &error_msg);
                }
                return Err(KvError::ServerError(error_msg));
            }
            
            let operation_time = start_time.elapsed().as_millis() as u64;
            if debug_mode {
                log_transaction_event(&format!("Transaction started with read_version: {}", response.read_version), None);
                log_operation_timing("begin_transaction", operation_time);
            }
            
            // Use the read version for the transaction
            Ok(Transaction::new(response.read_version, client))
        })
    }
    
    /// Begin a new read-only transaction (snapshot)
    pub fn begin_read_transaction(&self, read_version: Option<i64>) -> KvFuture<ReadTransaction> {
        if self.config.debug_mode {
            log_transaction_event(&format!("Starting read transaction with version: {:?}", read_version), None);
        }
        let client = Arc::clone(&self.client);
        let debug_mode = self.config.debug_mode;
        
        KvFuture::new(async move {
            let start_time = Instant::now();
            let request = GetReadVersionRequest::new();
            let client_for_spawn = Arc::clone(&client);
            let response = tokio::task::spawn_blocking(move || {
                client_for_spawn.lock().get_read_version(request)
            })
            .await
            .map_err(|e| {
                let error_msg = format!("Task join error: {}", e);
                if debug_mode {
                    log_error("begin_read_transaction task", &error_msg);
                }
                KvError::Unknown(error_msg)
            })?
            .map_err(|e| {
                if debug_mode {
                    log_error("begin_read_transaction thrift", &format!("{:?}", e));
                }
                KvError::from(e)
            })?;
            
            if !response.success {
                let error_msg = response.error.unwrap_or_else(|| "Unknown error".to_string());
                if debug_mode {
                    log_error("begin_read_transaction", &error_msg);
                }
                return Err(KvError::ServerError(error_msg));
            }
            
            let version = read_version.unwrap_or(response.read_version);
            let operation_time = start_time.elapsed().as_millis() as u64;
            if debug_mode {
                log_transaction_event(&format!("Read transaction started with version: {}", version), None);
                log_operation_timing("begin_read_transaction", operation_time);
            }
            let read_tx = ReadTransaction::new(version, client);
            
            Ok(read_tx)
        })
    }
    
    /// Health check - ping the server
    pub fn ping(&self, message: Option<String>) -> KvFuture<String> {
        if self.config.debug_mode {
            log_network_operation(&format!("ping: {}", message.as_deref().unwrap_or("(empty)")), None);
        }
        let client = Arc::clone(&self.client);
        let debug_mode = self.config.debug_mode;
        let request = PingRequest::new(
            message,
            Some(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64),
        );
        
        KvFuture::new(async move {
            let start_time = Instant::now();
            let response = tokio::task::spawn_blocking(move || {
                client.lock().ping(request)
            })
            .await
            .map_err(|e| {
                let error_msg = format!("Task join error: {}", e);
                if debug_mode {
                    log_error("ping task", &error_msg);
                }
                KvError::Unknown(error_msg)
            })?
            .map_err(|e| {
                if debug_mode {
                    log_error("ping thrift", &format!("{:?}", e));
                }
                KvError::from(e)
            })?;
            
            let operation_time = start_time.elapsed().as_millis() as u64;
            if debug_mode {
                log_operation_timing("ping", operation_time);
                log_network_operation(&format!("ping response: {}", response.message), None);
            }
            
            Ok(response.message)
        })
    }
    
    /// Create a new client with connection pooling (for high-throughput scenarios)
    pub fn connect_with_pool(address: &str, pool_size: usize) -> KvResult<ClientPool> {
        Self::connect_with_pool_and_config(address, pool_size, ClientConfig::default())
    }

    /// Create a new client with connection pooling and custom configuration
    pub fn connect_with_pool_and_config(address: &str, pool_size: usize, config: ClientConfig) -> KvResult<ClientPool> {
        if config.debug_mode {
            log_connection_event(&format!("Creating connection pool (size: {})", pool_size), address);
        }
        let mut clients = Vec::with_capacity(pool_size);
        
        for i in 0..pool_size {
            if config.debug_mode {
                log_connection_event(&format!("Creating pool connection {}/{}", i + 1, pool_size), address);
            }
            clients.push(Self::connect_with_config(address, config.clone())?);
        }
        
        Ok(ClientPool::new(clients))
    }
}

impl Clone for KvStoreClient {
    fn clone(&self) -> Self {
        if self.config.debug_mode {
            log_connection_event("Cloning client connection", &self.server_address);
        }
        Self {
            client: Arc::clone(&self.client),
            config: self.config.clone(),
            server_address: self.server_address.clone(),
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