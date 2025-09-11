// Library exports for testing and external use

pub mod lib {
    pub mod config;
    pub mod db;
    pub mod service;
    pub mod proto;
}

pub mod generated {
    pub mod kvstore;
}

// Re-export commonly used types for convenience
pub use lib::config::Config;
pub use lib::db::{TransactionalKvDatabase, OpResult, GetResult, AtomicOperation, AtomicCommitRequest, AtomicCommitResult};
pub use generated::kvstore::*;
