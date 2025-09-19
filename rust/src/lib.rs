// Library exports for testing and external use

pub mod lib {
    pub mod config;
    pub mod db;
    pub mod db_trait;
    pub mod service_core;
    pub mod service;
    pub mod proto;
    pub mod thrift_handler;
}

pub mod generated {
    pub mod kvstore;
}

pub mod client;

// Re-export commonly used types for convenience
pub use lib::config::Config;
pub use lib::db::{TransactionalKvDatabase, OpResult, GetResult, AtomicOperation, AtomicCommitRequest, AtomicCommitResult};
pub use lib::db_trait::KvDatabase;
pub use lib::service_core::KvServiceCore;
pub use generated::kvstore::*;

// Re-export client types for convenience
pub use client::{KvStoreClient, ClientConfig, KvError, KvResult, Transaction, ReadTransaction, KvFuture, KvFuturePtr};
