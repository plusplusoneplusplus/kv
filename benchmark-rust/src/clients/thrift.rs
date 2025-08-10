use async_trait::async_trait;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport, TTcpChannel, TIoChannel};

use crate::{BenchmarkConfig, BenchmarkResult};
use super::{ClientFactory, KvOperations};

// Use the generated Thrift code re-exported by the server crate's library.
use rocksdb_server::kvstore as thrift_kv;
use rocksdb_server::kvstore::TKVStoreSyncClient;

use tokio::sync::oneshot;

enum Rpc {
    Put { key: String, value: String, tx: oneshot::Sender<Result<thrift_kv::PutResponse, thrift::Error>> },
    Get { key: String, tx: oneshot::Sender<Result<thrift_kv::GetResponse, thrift::Error>> },
    Ping { message: String, timestamp: i64, tx: oneshot::Sender<Result<thrift_kv::PingResponse, thrift::Error>> },
}

pub struct ThriftClient {
    tx: mpsc::Sender<Rpc>,
    config: BenchmarkConfig,
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
            let mut client = thrift_kv::KVStoreSyncClient::new(i_prot, o_prot);

            while let Ok(rpc) = rx.recv() {
                match rpc {
                    Rpc::Put { key, value, tx } => {
                        let res = client.put(thrift_kv::PutRequest { key, value });
                        let _ = tx.send(res);
                    }
                    Rpc::Get { key, tx } => {
                        let res = client.get(thrift_kv::GetRequest { key });
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
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.tx.send(Rpc::Put { key: key.to_string(), value: value.to_string(), tx: resp_tx });
        let result = tokio::time::timeout(self.config.timeout, resp_rx).await;
        let latency = start.elapsed();

        match result {
            Ok(Ok(Ok(resp))) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: resp.success,
                error: resp.error,
            },
            Ok(Ok(Err(e))) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: false,
                error: Some(e.to_string()),
            },
            Ok(Err(recv_err)) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: false,
                error: Some(format!("channel closed: {}", recv_err)),
            },
            Err(_) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: false,
                error: Some("timeout".to_string()),
            },
        }
    }

    async fn get(&self, key: &str) -> BenchmarkResult {
        let start = Instant::now();
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self.tx.send(Rpc::Get { key: key.to_string(), tx: resp_tx });
        let result = tokio::time::timeout(self.config.timeout, resp_rx).await;
        let latency = start.elapsed();

        match result {
            Ok(Ok(Ok(resp))) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: resp.found,
                error: None,
            },
            Ok(Ok(Err(e))) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: false,
                error: Some(e.to_string()),
            },
            Ok(Err(recv_err)) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: false,
                error: Some(format!("channel closed: {}", recv_err)),
            },
            Err(_) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: false,
                error: Some("timeout".to_string()),
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