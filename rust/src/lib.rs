// Library exports for testing and external use

pub mod lib {
    pub mod service_core;
    pub mod service;
    pub mod proto;
    pub mod thrift_adapter;
    pub mod replication;
    pub mod operations;
    pub mod cluster;
    pub mod config;
}

pub mod generated {
    pub mod kvstore;
}

pub mod client;

// Re-export commonly used types for convenience
// Note: Use specific imports to avoid conflicts with generated Thrift types
pub use kv_storage_api::{KvDatabase, GetResult, OpResult, GetRangeResult, FaultInjectionConfig, WriteOperation, AtomicOperation};
pub use kv_storage_rocksdb::{Config, TransactionalKvDatabase, DatabaseFactory};
pub use lib::service_core::KvServiceCore;
pub use lib::operations::{KvOperation, OperationResult, OperationType, DatabaseOperation};
pub use lib::replication::{RoutingManager, RoutingError, RoutingResult};

// Re-export generated Thrift types with prefix to avoid conflicts
pub use generated::kvstore::{
    // Thrift service types
    TransactionalKVSyncHandler, TTransactionalKVSyncClient,
    // Thrift request/response types
    GetRequest, GetResponse, SetRequest, SetResponse, DeleteRequest, DeleteResponse,
    GetRangeRequest, GetRangeResponse, SnapshotGetRequest, SnapshotGetResponse,
    SnapshotGetRangeRequest, SnapshotGetRangeResponse,
    AtomicCommitResponse, FaultInjectionRequest, FaultInjectionResponse,
    // Use qualified names for conflicting types
    KeyValue as ThriftKeyValue, AtomicCommitRequest as ThriftAtomicCommitRequest
};

// Re-export client types for convenience
pub use client::{KvStoreClient, ClientConfig, KvError, KvResult, Transaction, ReadTransaction, KvFuture, KvFuturePtr};
