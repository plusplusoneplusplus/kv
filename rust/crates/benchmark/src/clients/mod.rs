use async_trait::async_trait;
use std::sync::Arc;

use crate::{BenchmarkConfig, BenchmarkResult};

pub mod grpc;
pub mod thrift;
pub mod raw;

#[async_trait]
pub trait KvOperations: Send + Sync {
    async fn put(&self, key: &str, value: &str) -> BenchmarkResult;
    async fn get(&self, key: &str) -> BenchmarkResult;
    async fn ping(&self, worker_id: usize) -> BenchmarkResult;
}

#[async_trait]
pub trait ClientFactory: Send + Sync {
    async fn create_client(&self, server_addr: &str, config: &BenchmarkConfig) 
        -> anyhow::Result<Arc<dyn KvOperations>>;
}