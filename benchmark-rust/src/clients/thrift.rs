use async_trait::async_trait;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TTcpChannel, TIoChannel};

use crate::{BenchmarkConfig, BenchmarkResult};
use super::{ClientFactory, KvOperations};

use std::collections::HashMap;

// Use the generated Thrift code re-exported by the server crate's library.
use rocksdb_server::lib::kvstore as thrift_kv;
use rocksdb_server::lib::kvstore::TTransactionalKVSyncClient; // bring client trait methods into scope

use tokio::sync::oneshot;

enum Rpc {
    // FoundationDB-style client-side transactions
    GetReadVersion { tx: oneshot::Sender<Result<thrift_kv::GetReadVersionResponse, thrift::Error>> },
    SnapshotRead { key: String, read_version: i64, tx: oneshot::Sender<Result<thrift_kv::SnapshotReadResponse, thrift::Error>> },
    AtomicCommit { operations: Vec<thrift_kv::Operation>, read_version: i64, read_conflicts: Vec<Vec<u8>>, tx: oneshot::Sender<Result<thrift_kv::AtomicCommitResponse, thrift::Error>> },
    
    // Non-transactional operations 
    #[allow(dead_code)]
    Put { key: String, value: String, tx: oneshot::Sender<Result<thrift_kv::SetResponse, thrift::Error>> },
    #[allow(dead_code)]
    Get { key: String, tx: oneshot::Sender<Result<thrift_kv::GetResponse, thrift::Error>> },
    
    Ping { message: String, timestamp: i64, tx: oneshot::Sender<Result<thrift_kv::PingResponse, thrift::Error>> },
}

// FoundationDB-style client-side transaction
struct ClientTransaction {
    read_version: i64,
    pending_writes: HashMap<String, String>,  // key -> value
    pending_deletes: std::collections::HashSet<String>,  // deleted keys
    read_conflict_keys: std::collections::HashSet<String>,  // track reads for conflict detection
    client_tx: mpsc::Sender<Rpc>,
}

impl ClientTransaction {
    fn new(read_version: i64, client_tx: mpsc::Sender<Rpc>) -> Self {
        ClientTransaction {
            read_version,
            pending_writes: HashMap::new(),
            pending_deletes: std::collections::HashSet::new(),
            read_conflict_keys: std::collections::HashSet::new(),
            client_tx,
        }
    }
    
    async fn get(&mut self, key: &str) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        // Track this key for conflict detection
        self.read_conflict_keys.insert(key.to_string());
        
        // Check client-side buffers first (read-your-writes)
        if self.pending_deletes.contains(key) {
            return Ok(None);
        }
        
        if let Some(value) = self.pending_writes.get(key) {
            return Ok(Some(value.clone()));
        }
        
        // Fall back to server snapshot read
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.client_tx.send(Rpc::SnapshotRead {
            key: key.to_string(),
            read_version: self.read_version,
            tx: resp_tx,
        });
        
        match resp_rx.await? {
            Ok(response) => {
                if response.found {
                    Ok(Some(String::from_utf8_lossy(&response.value).to_string()))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(Box::new(e))
        }
    }
    
    fn set(&mut self, key: String, value: String) {
        // Pure client-side operation - no network I/O!
        self.pending_writes.insert(key.clone(), value);
        self.pending_deletes.remove(&key);  // Remove from deletes if previously deleted
    }
    
    #[allow(dead_code)]
    fn delete(&mut self, key: String) {
        // Pure client-side operation - no network I/O!
        self.pending_deletes.insert(key.clone());
        self.pending_writes.remove(&key);  // Remove from writes if previously written
    }
    
    async fn commit(self) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        // Convert client operations to thrift operations
        let mut operations = Vec::new();
        
        for (key, value) in self.pending_writes {
            operations.push(thrift_kv::Operation::new("set".to_string(), key.into_bytes(), Some(value.into_bytes()), None));
        }
        
        for key in self.pending_deletes {
            operations.push(thrift_kv::Operation::new("delete".to_string(), key.into_bytes(), None, None));
        }
        
        // Send entire transaction to server in one atomic operation
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.client_tx.send(Rpc::AtomicCommit {
            operations,
            read_version: self.read_version,
            read_conflicts: self.read_conflict_keys.into_iter().map(|k| k.into_bytes()).collect(),
            tx: resp_tx,
        });
        
        match resp_rx.await? {
            Ok(response) => Ok(response.success),
            Err(e) => Err(Box::new(e))
        }
    }
}

pub struct ThriftClient {
    tx: mpsc::Sender<Rpc>,
    config: BenchmarkConfig,
}

impl ThriftClient {
    async fn get_read_version(&self) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.tx.send(Rpc::GetReadVersion { tx: resp_tx });
        
        let result = tokio::time::timeout(self.config.timeout, resp_rx).await?;
        match result? {
            Ok(response) => {
                if response.success {
                    Ok(response.read_version)
                } else {
                    Err(format!("Failed to get read version: {:?}", response.error).into())
                }
            }
            Err(e) => Err(Box::new(e))
        }
    }
}

pub struct ThriftClientFactory;

#[async_trait]
impl ClientFactory for ThriftClientFactory {
    async fn create_client(&self, server_addr: &str, config: &BenchmarkConfig)
        -> anyhow::Result<Arc<dyn KvOperations>>
    {
        // Spawn a dedicated blocking thread to own the Thrift client and process requests
        let (tx, rx) = mpsc::channel::<Rpc>();
        let addr = server_addr.to_string();
        let op_timeout_secs = config.timeout.as_secs() as i64;
        thread::spawn(move || {
            // Build client once and reuse it
            let mut channel = TTcpChannel::new();
            if let Err(e) = channel.open(&addr) { 
                eprintln!("Thrift client failed to connect to {}: {}", addr, e);
                // Drain requests with errors so senders don't hang
                while let Ok(rpc) = rx.recv() {
                    match rpc {
                        Rpc::Put { tx, .. } => { let _ = tx.send(Err(thrift::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "connect failed")))); }
                        Rpc::Get { tx, .. } => { let _ = tx.send(Err(thrift::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "connect failed")))); }
                        Rpc::GetReadVersion { tx, .. } => { let _ = tx.send(Err(thrift::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "connect failed")))); }
                        Rpc::SnapshotRead { tx, .. } => { let _ = tx.send(Err(thrift::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "connect failed")))); }
                        Rpc::AtomicCommit { tx, .. } => { let _ = tx.send(Err(thrift::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "connect failed")))); }
                        Rpc::Ping { tx, .. } => { let _ = tx.send(Err(thrift::Error::from(std::io::Error::new(std::io::ErrorKind::Other, "connect failed")))); }
                    }
                }
                return;
            }

            let (i_chan, o_chan) = channel.split().expect("split tcp channel");
            let i_tr = TBufferedReadTransport::new(i_chan);
            let o_tr = TBufferedWriteTransport::new(o_chan);
            let i_prot = TBinaryInputProtocol::new(i_tr, true);
            let o_prot = TBinaryOutputProtocol::new(o_tr, true);
            let mut client = thrift_kv::TransactionalKVSyncClient::new(i_prot, o_prot);

            while let Ok(rpc) = rx.recv() {
                match rpc {
                    // New FoundationDB-style RPCs
                    Rpc::GetReadVersion { tx } => {
                        let res = client.get_read_version(thrift_kv::GetReadVersionRequest::new());
                        let _ = tx.send(res);
                    }
                    Rpc::SnapshotRead { key, read_version, tx } => {
                        let res = client.snapshot_read(thrift_kv::SnapshotReadRequest::new(key.into_bytes(), read_version, None));
                        let _ = tx.send(res);
                    }
                    Rpc::AtomicCommit { operations, read_version, read_conflicts, tx } => {
                        let res = client.atomic_commit(thrift_kv::AtomicCommitRequest::new(
                            read_version,
                            operations,
                            read_conflicts,
                            Some(op_timeout_secs)
                        ));
                        let _ = tx.send(res);
                    }
                    
                    // Non-transactional operations
                    Rpc::Put { key, value, tx } => {
                        let res = client.set_key(thrift_kv::SetRequest { key: key.into_bytes(), value: value.into_bytes(), column_family: None });
                        let _ = tx.send(res);
                    }
                    Rpc::Get { key, tx } => {
                        let res = client.get(thrift_kv::GetRequest { key: key.into_bytes(), column_family: None });
                        let _ = tx.send(res);
                    }
                    Rpc::Ping { message, timestamp, tx } => {
                        let res = client.ping(thrift_kv::PingRequest { message: Some(message), timestamp: Some(timestamp) });
                        let _ = tx.send(res);
                    }
                }
            }
        });

        Ok(Arc::new(ThriftClient { tx, config: config.clone() }))
    }
}

#[async_trait]
impl KvOperations for ThriftClient {
    async fn put(&self, key: &str, value: &str) -> BenchmarkResult {
        let start = Instant::now();
        
        // FoundationDB-style client-side transaction
        let read_version = match self.get_read_version().await {
            Ok(version) => version,
            Err(e) => {
                return BenchmarkResult {
                    operation: "write".to_string(),
                    latency: start.elapsed(),
                    success: false,
                    error: Some(format!("Failed to get read version: {}", e)),
                };
            }
        };
        
        let mut transaction = ClientTransaction::new(read_version, self.tx.clone());
        transaction.set(key.to_string(), value.to_string());
        
        let commit_result = transaction.commit().await;
        let latency = start.elapsed();
        
        match commit_result {
            Ok(success) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success,
                error: if success { None } else { Some("Commit failed".to_string()) },
            },
            Err(e) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: false,
                error: Some(e.to_string()),
            },
        }
    }

    async fn get(&self, key: &str) -> BenchmarkResult {
        let start = Instant::now();
        
        // FoundationDB-style client-side transaction
        let read_version = match self.get_read_version().await {
            Ok(version) => version,
            Err(e) => {
                return BenchmarkResult {
                    operation: "read".to_string(),
                    latency: start.elapsed(),
                    success: false,
                    error: Some(format!("Failed to get read version: {}", e)),
                };
            }
        };
        
        let mut transaction = ClientTransaction::new(read_version, self.tx.clone());
        let get_result = transaction.get(key).await;
        let latency = start.elapsed();
        
        match get_result {
            Ok(Some(_value)) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: true,
                error: None,
            },
            Ok(None) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: false, // For benchmark purposes, not found = failed
                error: None,
            },
            Err(e) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: false,
                error: Some(e.to_string()),
            },
        }
    }

    async fn ping(&self, worker_id: usize) -> BenchmarkResult {
        let start = Instant::now();
        let message = format!("ping-{}", worker_id);
        let expected_message = message.clone();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.tx.send(Rpc::Ping { message, timestamp, tx: resp_tx });
        let result = tokio::time::timeout(self.config.timeout, resp_rx).await;
        let latency = start.elapsed();

        match result {
            Ok(Ok(Ok(resp))) => {
                let success = resp.message == expected_message && resp.timestamp == timestamp;
                BenchmarkResult {
                    operation: "ping".to_string(),
                    latency,
                    success,
                    error: if success { None } else { Some("ping response validation failed".to_string()) },
                }
            }
            Ok(Ok(Err(e))) => BenchmarkResult {
                operation: "ping".to_string(),
                latency,
                success: false,
                error: Some(e.to_string()),
            },
            Ok(Err(recv_err)) => BenchmarkResult {
                operation: "ping".to_string(),
                latency,
                success: false,
                error: Some(format!("channel closed: {}", recv_err)),
            },
            Err(_) => BenchmarkResult {
                operation: "ping".to_string(),
                latency,
                success: false,
                error: Some("timeout".to_string()),
            },
        }
    }
}
