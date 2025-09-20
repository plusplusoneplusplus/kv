use async_trait::async_trait;
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use tonic::transport::Channel;
use tonic::Request;

use super::{ClientFactory, KvOperations};
use crate::{BenchmarkConfig, BenchmarkResult};

pub mod kvstore {
    tonic::include_proto!("kvstore");
}

use kvstore::kv_store_client::KvStoreClient;
use kvstore::{GetRequest, PingRequest, PutRequest};

pub struct GrpcClient {
    client: KvStoreClient<Channel>,
    config: BenchmarkConfig,
}

pub struct GrpcClientFactory;

#[async_trait]
impl ClientFactory for GrpcClientFactory {
    async fn create_client(
        &self,
        server_addr: &str,
        config: &BenchmarkConfig,
    ) -> anyhow::Result<Arc<dyn KvOperations>> {
        let channel = Channel::from_shared(format!("http://{}", server_addr))?
            .connect()
            .await?;

        let client = KvStoreClient::new(channel);

        Ok(Arc::new(GrpcClient {
            client,
            config: config.clone(),
        }))
    }
}

#[async_trait]
impl KvOperations for GrpcClient {
    async fn put(&self, key: &str, value: &str) -> BenchmarkResult {
        let start = Instant::now();

        let request = Request::new(PutRequest {
            key: key.to_string(),
            value: value.to_string(),
        });

        let result =
            tokio::time::timeout(self.config.timeout, self.client.clone().put(request)).await;

        let latency = start.elapsed();

        match result {
            Ok(Ok(_)) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: true,
                error: None,
            },
            Ok(Err(e)) => BenchmarkResult {
                operation: "write".to_string(),
                latency,
                success: false,
                error: Some(e.to_string()),
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

        let request = Request::new(GetRequest {
            key: key.to_string(),
        });

        let result =
            tokio::time::timeout(self.config.timeout, self.client.clone().get(request)).await;

        let latency = start.elapsed();

        match result {
            Ok(Ok(_)) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: true,
                error: None,
            },
            Ok(Err(e)) => BenchmarkResult {
                operation: "read".to_string(),
                latency,
                success: false,
                error: Some(e.to_string()),
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

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        let message = format!("ping-{}", worker_id);

        let request = Request::new(PingRequest {
            message: message.clone(),
            timestamp,
        });

        let result =
            tokio::time::timeout(self.config.timeout, self.client.clone().ping(request)).await;

        let latency = start.elapsed();

        match result {
            Ok(Ok(response)) => {
                let resp = response.into_inner();
                let success = resp.message == message && resp.timestamp == timestamp;
                BenchmarkResult {
                    operation: "ping".to_string(),
                    latency,
                    success,
                    error: if success {
                        None
                    } else {
                        Some("ping response validation failed".to_string())
                    },
                }
            }
            Ok(Err(e)) => BenchmarkResult {
                operation: "ping".to_string(),
                latency,
                success: false,
                error: Some(e.to_string()),
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
