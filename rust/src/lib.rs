// Library exports for testing and external use

pub mod lib {
    pub mod config;
    pub mod db;
    pub mod kvstore;
    pub mod service;
    pub mod proto;
}

// Re-export commonly used types for convenience
pub use lib::config::Config;
pub use lib::db::{TransactionalKvDatabase, OpResult, GetResult, AtomicOperation, AtomicCommitRequest, AtomicCommitResult};
pub use lib::kvstore::*;
