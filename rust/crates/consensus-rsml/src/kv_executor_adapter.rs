//! KvStoreExecutor adapter for RSML integration
//!
//! This module provides an adapter that bridges the main rocksdb_server's KvStoreExecutor
//! to the consensus-rsml's KvExecutor trait, enabling RSML consensus to drive the KV store.

use std::sync::Arc;
use async_trait::async_trait;
use crate::execution::KvExecutor;

/// Adapter that bridges rocksdb_server::KvStoreExecutor to consensus-rsml::KvExecutor
///
/// This adapter allows RSML's ExecutionNotifier to work with the main KV store executor
/// without creating a circular dependency between crates.
///
/// # Type Safety
///
/// The adapter uses dynamic dispatch to avoid direct type dependencies on rocksdb_server.
/// The actual KvStoreExecutor is wrapped in a trait object that implements the standard
/// apply_operation interface.
pub struct KvStoreExecutorAdapter {
    /// Inner executor trait object
    ///
    /// This is typically rocksdb_server::lib::replication::KvStoreExecutor wrapped
    /// in the ExecutorTrait wrapper, but the adapter doesn't need to know the concrete type.
    inner: Arc<dyn ExecutorTrait>,
}

/// Trait that abstracts the actual KvStoreExecutor operations
///
/// This trait matches the interface of rocksdb_server::lib::replication::KvStoreExecutor
/// but can be implemented independently to avoid circular dependencies.
#[async_trait]
pub trait ExecutorTrait: Send + Sync {
    /// Apply a serialized KvOperation and return a serialized OperationResult
    ///
    /// # Arguments
    /// * `sequence` - The sequence number for ordering
    /// * `operation_bytes` - Serialized KvOperation (via bincode)
    ///
    /// # Returns
    /// Serialized OperationResult on success, or error message on failure
    async fn apply_serialized_operation(
        &self,
        sequence: u64,
        operation_bytes: Vec<u8>,
    ) -> Result<Vec<u8>, String>;
}

impl KvStoreExecutorAdapter {
    /// Create a new adapter wrapping the given executor
    pub fn new(executor: Arc<dyn ExecutorTrait>) -> Self {
        Self { inner: executor }
    }
}

#[async_trait]
impl KvExecutor for KvStoreExecutorAdapter {
    async fn apply_operation(&self, sequence: u64, operation_bytes: Vec<u8>) -> Result<Vec<u8>, String> {
        // Delegate to the inner executor
        self.inner.apply_serialized_operation(sequence, operation_bytes).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock executor for testing the adapter
    struct MockExecutor {
        should_fail: bool,
    }

    #[async_trait]
    impl ExecutorTrait for MockExecutor {
        async fn apply_serialized_operation(
            &self,
            sequence: u64,
            operation_bytes: Vec<u8>,
        ) -> Result<Vec<u8>, String> {
            if self.should_fail {
                return Err("Mock failure".to_string());
            }

            // Echo back the sequence and data length
            let result = format!("Applied seq {} with {} bytes", sequence, operation_bytes.len());
            Ok(result.into_bytes())
        }
    }

    #[tokio::test]
    async fn test_adapter_delegates_to_executor() {
        let executor = Arc::new(MockExecutor { should_fail: false });
        let adapter = KvStoreExecutorAdapter::new(executor);

        let operation = b"SET key value".to_vec();
        let result = adapter.apply_operation(1, operation).await;

        assert!(result.is_ok());
        let result_str = String::from_utf8(result.unwrap()).unwrap();
        assert!(result_str.contains("Applied seq 1"));
        assert!(result_str.contains("13 bytes"));
    }

    #[tokio::test]
    async fn test_adapter_propagates_errors() {
        let executor = Arc::new(MockExecutor { should_fail: true });
        let adapter = KvStoreExecutorAdapter::new(executor);

        let operation = b"SET key value".to_vec();
        let result = adapter.apply_operation(1, operation).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Mock failure");
    }
}