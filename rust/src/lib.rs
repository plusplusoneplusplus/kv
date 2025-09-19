// Library exports for testing and external use

pub mod lib {
    pub mod config;
    pub mod db;
    pub mod db_trait;
    pub mod service_core;
    pub mod database_factory;
    pub mod service;
    pub mod proto;
    pub mod kv_operations;
    pub mod thrift_adapter;
}

pub mod generated {
    pub mod kvstore;
}

pub mod client;

// Re-export commonly used types for convenience
pub use lib::config::{Config, DeploymentConfig, DeploymentMode};
pub use lib::db::{TransactionalKvDatabase, OpResult, GetResult, AtomicOperation, AtomicCommitRequest, AtomicCommitResult};
pub use lib::db_trait::KvDatabase;
pub use lib::service_core::KvServiceCore;
pub use lib::database_factory::DatabaseFactory;
pub use generated::kvstore::*;

// Re-export client types for convenience
pub use client::{KvStoreClient, ClientConfig, KvError, KvResult, Transaction, ReadTransaction, KvFuture, KvFuturePtr};
