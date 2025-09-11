pub mod client;
pub mod config;
pub mod error;
pub mod transaction;
pub mod future;

// Re-export server's generated types
pub use crate::generated;

#[cfg(feature = "ffi")]
pub mod ffi;

pub use client::KvStoreClient;
pub use config::ClientConfig;
pub use error::{KvError, KvResult};
pub use transaction::{Transaction, ReadTransaction};
pub use future::{KvFuture, KvFuturePtr};

// Re-export Thrift types for convenience
pub use generated::kvstore::*;