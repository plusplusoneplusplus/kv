use async_trait::async_trait;
use crate::lib::db::{GetResult, OpResult, AtomicCommitRequest, AtomicCommitResult, GetRangeResult, FaultInjectionConfig};

/// Abstract interface for key-value database operations.
/// This trait allows both standalone and replicated implementations
/// to share the same core business logic.
#[async_trait]
pub trait KvDatabase: Send + Sync {
    /// Get a value by key from the specified column family
    async fn get(&self, key: &[u8], column_family: Option<&str>) -> Result<GetResult, String>;

    /// Put a key-value pair into the specified column family
    async fn put(&self, key: &[u8], value: &[u8], column_family: Option<&str>) -> OpResult;

    /// Delete a key from the specified column family
    async fn delete(&self, key: &[u8], column_family: Option<&str>) -> OpResult;

    /// List keys with a given prefix
    async fn list_keys(&self, prefix: &[u8], limit: u32, column_family: Option<&str>) -> Result<Vec<Vec<u8>>, String>;

    /// Get a range of key-value pairs
    async fn get_range(
        &self,
        begin_key: &[u8],
        end_key: &[u8],
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        limit: Option<i32>,
        column_family: Option<&str>,
    ) -> Result<GetRangeResult, String>;

    /// Perform atomic commit of multiple operations
    async fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult;

    /// Get the current read version for snapshot reads
    async fn get_read_version(&self) -> u64;

    /// Read a key at a specific version (snapshot read)
    async fn snapshot_read(&self, key: &[u8], read_version: u64, column_family: Option<&str>) -> Result<GetResult, String>;

    /// Get a range of key-value pairs at a specific version
    async fn snapshot_get_range(
        &self,
        begin_key: &[u8],
        end_key: &[u8],
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        read_version: u64,
        limit: Option<i32>,
        column_family: Option<&str>,
    ) -> Result<GetRangeResult, String>;

    /// Set fault injection configuration for testing
    async fn set_fault_injection(&self, config: Option<FaultInjectionConfig>) -> OpResult;
}