use std::sync::Arc;
use crate::lib::db_trait::KvDatabase;
use crate::lib::db::{AtomicCommitRequest, AtomicCommitResult, GetResult, OpResult, GetRangeResult};

/// Core business logic for KV operations, protocol-agnostic.
/// This contains all the business logic that can be shared between
/// different protocol handlers (Thrift, gRPC, etc.)
pub struct KvServiceCore {
    database: Arc<dyn KvDatabase>,
}

impl KvServiceCore {
    pub fn new(database: Arc<dyn KvDatabase>) -> Self {
        Self { database }
    }

    /// Get a value by key
    pub async fn get(&self, key: &[u8], column_family: Option<&str>) -> Result<GetResult, String> {
        self.database.get(key, column_family).await
    }

    /// Put a key-value pair
    pub async fn put(&self, key: &[u8], value: &[u8], column_family: Option<&str>) -> OpResult {
        self.database.put(key, value, column_family).await
    }

    /// Delete a key
    pub async fn delete(&self, key: &[u8], column_family: Option<&str>) -> OpResult {
        self.database.delete(key, column_family).await
    }

    /// List keys with a given prefix
    pub async fn list_keys(&self, prefix: &[u8], limit: u32, column_family: Option<&str>) -> Result<Vec<Vec<u8>>, String> {
        self.database.list_keys(prefix, limit, column_family).await
    }

    /// Get a range of key-value pairs
    pub async fn get_range(
        &self,
        start_key: &[u8],
        end_key: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
        column_family: Option<&str>,
    ) -> Result<GetRangeResult, String> {
        self.database.get_range(start_key, end_key, limit, reverse, column_family).await
    }

    /// Perform atomic commit of multiple operations
    pub async fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
        self.database.atomic_commit(request).await
    }

    /// Get the current read version for snapshot reads
    pub async fn get_read_version(&self) -> u64 {
        self.database.get_read_version().await
    }

    /// Read a key at a specific version (snapshot read)
    pub async fn snapshot_read(&self, key: &[u8], read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
        self.database.snapshot_read(key, read_version, column_family).await
    }

    /// Get a range of key-value pairs at a specific version
    pub async fn snapshot_get_range(
        &self,
        start_key: &[u8],
        end_key: Option<&[u8]>,
        limit: Option<usize>,
        reverse: bool,
        read_version: u64,
        column_family: Option<&str>,
    ) -> Result<GetRangeResult, String> {
        self.database.snapshot_get_range(start_key, end_key, limit, reverse, read_version, column_family).await
    }
}