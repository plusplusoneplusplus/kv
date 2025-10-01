//! RSML execution notifier for bridging RSML learner to KvStoreExecutor
//!
//! This module provides the KvExecutionNotifier that implements RSML's ExecutionNotifier trait.
//! It deserializes committed operations, applies them via KvStoreExecutor, and caches results.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use async_trait::async_trait;
use dashmap::DashMap;
use tracing::{debug, error};
use rsml::learner::notification::ExecutionNotifier;
use rsml::learner::error::LearnerResult;

/// Trait for a KV store executor that can apply operations
///
/// This trait abstracts the actual KvStoreExecutor to allow this crate
/// to avoid depending on the main rocksdb_server crate, while still
/// defining the contract needed for execution.
#[async_trait]
pub trait KvExecutor: Send + Sync {
    /// Apply an operation at the given sequence number
    ///
    /// # Arguments
    /// * `sequence` - The sequence number for this operation (for ordering)
    /// * `operation_bytes` - Serialized KvOperation (via bincode)
    ///
    /// # Returns
    /// Serialized OperationResult on success, or error message on failure
    async fn apply_operation(&self, sequence: u64, operation_bytes: Vec<u8>) -> Result<Vec<u8>, String>;
}

/// RSML execution notifier that bridges committed RSML values to KvStoreExecutor
///
/// This component:
/// - Receives committed values from RSML learner via notify_commit
/// - The payload contains serialized KvOperation (via bincode)
/// - Calls KvExecutor.apply_operation(seq, operation_bytes) which will:
///   - Deserialize to KvOperation
///   - Apply to the KV store
///   - Serialize OperationResult back
/// - Caches the serialized result for later retrieval
/// - Tracks applied_sequence for readiness checks
///
/// # Integration with KvStoreExecutor
///
/// To use with the actual KvStoreExecutor from the main crate:
///
/// ```ignore
/// use rocksdb_server::lib::replication::executor::KvStoreExecutor;
/// use rocksdb_server::lib::operations::{KvOperation, OperationResult};
///
/// struct KvStoreExecutorAdapter {
///     executor: Arc<KvStoreExecutor>,
/// }
///
/// #[async_trait]
/// impl KvExecutor for KvStoreExecutorAdapter {
///     async fn apply_operation(&self, sequence: u64, operation_bytes: Vec<u8>)
///         -> Result<Vec<u8>, String> {
///         // Deserialize KvOperation
///         let operation: KvOperation = bincode::deserialize(&operation_bytes)
///             .map_err(|e| format!("Failed to deserialize KvOperation: {}", e))?;
///
///         // Apply via actual KvStoreExecutor
///         let result = self.executor.apply_operation(sequence, operation).await
///             .map_err(|e| format!("Execution failed: {}", e))?;
///
///         // Serialize OperationResult
///         bincode::serialize(&result)
///             .map_err(|e| format!("Failed to serialize OperationResult: {}", e))
///     }
/// }
/// ```
#[derive(Debug, Clone)]
pub struct KvExecutionNotifier<E: KvExecutor> {
    /// The KV store executor that applies operations
    executor: Arc<E>,

    /// Last applied sequence number
    applied_sequence: Arc<AtomicU64>,

    /// Result cache: maps sequence number to serialized OperationResult
    result_cache: Arc<DashMap<u64, Vec<u8>>>,

    /// Maximum cache size (LRU eviction when exceeded)
    max_cache_size: usize,
}

impl<E: KvExecutor> KvExecutionNotifier<E> {
    /// Create a new KV execution notifier
    ///
    /// # Arguments
    /// * `executor` - The KV store executor that applies operations
    /// * `max_cache_size` - Maximum number of results to cache (default 10000)
    pub fn new(executor: Arc<E>, max_cache_size: usize) -> Self {
        Self {
            executor,
            applied_sequence: Arc::new(AtomicU64::new(0)),
            result_cache: Arc::new(DashMap::new()),
            max_cache_size,
        }
    }

    /// Get the last applied sequence number
    pub fn get_applied_sequence(&self) -> u64 {
        self.applied_sequence.load(Ordering::SeqCst)
    }

    /// Get a cached result by sequence number
    pub fn get_result(&self, sequence: u64) -> Option<Vec<u8>> {
        self.result_cache.get(&sequence).map(|entry| entry.value().clone())
    }

    /// Check if a sequence has been applied
    pub fn is_applied(&self, sequence: u64) -> bool {
        self.get_applied_sequence() >= sequence
    }

    /// Evict old entries if cache exceeds max size
    fn evict_old_entries(&self) {
        let current_size = self.result_cache.len();
        if current_size <= self.max_cache_size {
            return;
        }

        let evict_count = current_size - self.max_cache_size;
        let applied = self.get_applied_sequence();

        // Evict oldest entries (lowest sequence numbers)
        let evict_threshold = applied.saturating_sub(self.max_cache_size as u64);

        let to_remove: Vec<u64> = self.result_cache
            .iter()
            .filter_map(|entry| {
                let seq = *entry.key();
                if seq <= evict_threshold {
                    Some(seq)
                } else {
                    None
                }
            })
            .take(evict_count)
            .collect();

        let evicted_count = to_remove.len();
        for seq in to_remove {
            self.result_cache.remove(&seq);
        }

        if evicted_count > 0 {
            debug!("Evicted {} old result cache entries", evicted_count);
        }
    }
}

#[async_trait]
impl<E: KvExecutor + 'static> ExecutionNotifier for KvExecutionNotifier<E> {
    async fn notify_commit(
        &mut self,
        sequence: u64,
        value: rsml::messages::CommittedValue,
    ) -> LearnerResult<()> {
        debug!("KvExecutionNotifier: Applying committed operation at sequence {}", sequence);

        // The payload contains serialized KvOperation
        // The executor will deserialize, apply, and serialize the result
        let operation_bytes = value.value;

        // Apply the operation via executor (which handles deserialization and serialization)
        match self.executor.apply_operation(sequence, operation_bytes).await {
            Ok(result_bytes) => {
                // Cache the serialized result
                self.result_cache.insert(sequence, result_bytes);

                // Update applied sequence
                self.applied_sequence.store(sequence, Ordering::SeqCst);

                // Evict old entries if needed
                self.evict_old_entries();

                debug!("Successfully applied operation at sequence {}", sequence);
                Ok(())
            }
            Err(e) => {
                error!("Failed to apply operation at sequence {}: {}", sequence, e);
                Err(rsml::learner::error::LearnerError::InvalidConfiguration {
                    reason: format!("KvExecutor failed for sequence {}: {}", sequence, e),
                })
            }
        }
    }

    async fn notify_commit_batch(
        &mut self,
        commits: Vec<(u64, rsml::messages::CommittedValue)>,
    ) -> LearnerResult<()> {
        debug!("KvExecutionNotifier: Applying batch of {} commits", commits.len());

        for (sequence, value) in commits {
            self.notify_commit(sequence, value).await?;
        }

        Ok(())
    }

    fn next_expected_sequence(&self) -> u64 {
        self.applied_sequence.load(Ordering::SeqCst) + 1
    }

    fn is_ready(&self) -> bool {
        // Always ready to accept commits
        true
    }

    async fn wait_until_ready(&self) -> LearnerResult<()> {
        // No-op: we're always ready
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Mock KV executor for testing that handles serialization/deserialization
    #[derive(Debug)]
    struct MockKvExecutor {
        operations: Mutex<Vec<(u64, Vec<u8>)>>,
        should_fail: bool,
    }

    impl MockKvExecutor {
        fn new() -> Self {
            Self {
                operations: Mutex::new(Vec::new()),
                should_fail: false,
            }
        }

        fn new_failing() -> Self {
            Self {
                operations: Mutex::new(Vec::new()),
                should_fail: true,
            }
        }

        fn get_operations(&self) -> Vec<(u64, Vec<u8>)> {
            self.operations.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl KvExecutor for MockKvExecutor {
        async fn apply_operation(&self, sequence: u64, operation_bytes: Vec<u8>)
            -> Result<Vec<u8>, String> {
            if self.should_fail {
                return Err("Mock executor failure".to_string());
            }

            // Record the operation
            self.operations.lock().unwrap().push((sequence, operation_bytes.clone()));

            // Return a mock result
            let result = format!("Result for seq {}", sequence);
            Ok(result.into_bytes())
        }
    }

    fn create_committed_value(data: Vec<u8>) -> rsml::messages::CommittedValue {
        use rsml::prelude::*;
        use rsml::messages::{CommitMetadata, CommitSource};

        rsml::messages::CommittedValue {
            slot: 1,
            value: data,
            view: View::new(1, ReplicaId::new(0)),
            commit_time: std::time::SystemTime::now(),
            configuration_id: ConfigurationId::new(1),
            proposer_id: ReplicaId::new(0),
            sequence_number: 1,
            metadata: CommitMetadata {
                source: CommitSource::Broadcast,
                quorum_acceptors: None,
                leader_id: None,
                detection_latency: std::time::Duration::from_secs(0),
                value_hash: 0,
            },
        }
    }

    #[tokio::test]
    async fn test_notifier_creation() {
        let executor = Arc::new(MockKvExecutor::new());
        let notifier = KvExecutionNotifier::new(executor, 100);

        assert_eq!(notifier.get_applied_sequence(), 0);
        assert_eq!(notifier.next_expected_sequence(), 1);
        assert!(notifier.is_ready());
    }

    #[tokio::test]
    async fn test_notify_commit_applies_operation() {
        let executor = Arc::new(MockKvExecutor::new());
        let mut notifier = KvExecutionNotifier::new(executor.clone(), 100);

        let operation = b"SET key value".to_vec();
        let committed_value = create_committed_value(operation.clone());

        let result = notifier.notify_commit(1, committed_value).await;
        assert!(result.is_ok());

        // Verify the operation was applied
        let ops = executor.get_operations();
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].0, 1); // sequence
        assert_eq!(ops[0].1, operation);

        // Verify applied sequence updated
        assert_eq!(notifier.get_applied_sequence(), 1);
        assert_eq!(notifier.next_expected_sequence(), 2);
        assert!(notifier.is_applied(1));
        assert!(!notifier.is_applied(2));
    }

    #[tokio::test]
    async fn test_result_caching() {
        let executor = Arc::new(MockKvExecutor::new());
        let mut notifier = KvExecutionNotifier::new(executor, 100);

        let operation = b"GET key".to_vec();
        let committed_value = create_committed_value(operation);

        notifier.notify_commit(1, committed_value).await.unwrap();

        // Retrieve cached result
        let result = notifier.get_result(1);
        assert!(result.is_some());
        assert_eq!(result.unwrap(), b"Result for seq 1");

        // Non-existent result
        assert!(notifier.get_result(999).is_none());
    }

    #[tokio::test]
    async fn test_batch_commit() {
        let executor = Arc::new(MockKvExecutor::new());
        let mut notifier = KvExecutionNotifier::new(executor.clone(), 100);

        let batch = vec![
            (1, create_committed_value(b"op1".to_vec())),
            (2, create_committed_value(b"op2".to_vec())),
            (3, create_committed_value(b"op3".to_vec())),
        ];

        let result = notifier.notify_commit_batch(batch).await;
        assert!(result.is_ok());

        // Verify all operations applied
        let ops = executor.get_operations();
        assert_eq!(ops.len(), 3);
        assert_eq!(notifier.get_applied_sequence(), 3);

        // Verify all results cached
        assert!(notifier.get_result(1).is_some());
        assert!(notifier.get_result(2).is_some());
        assert!(notifier.get_result(3).is_some());
    }

    #[tokio::test]
    async fn test_executor_failure_propagates() {
        let executor = Arc::new(MockKvExecutor::new_failing());
        let mut notifier = KvExecutionNotifier::new(executor, 100);

        let operation = b"SET key value".to_vec();
        let committed_value = create_committed_value(operation);

        let result = notifier.notify_commit(1, committed_value).await;
        assert!(result.is_err());

        // Sequence should not advance on failure
        assert_eq!(notifier.get_applied_sequence(), 0);
        assert!(notifier.get_result(1).is_none());
    }

    #[tokio::test]
    async fn test_cache_eviction() {
        let executor = Arc::new(MockKvExecutor::new());
        let mut notifier = KvExecutionNotifier::new(executor, 10); // Small cache

        // Apply more than max_cache_size operations
        for seq in 1..=20u64 {
            let operation = format!("op{}", seq).into_bytes();
            let committed_value = create_committed_value(operation);
            notifier.notify_commit(seq, committed_value).await.unwrap();
        }

        assert_eq!(notifier.get_applied_sequence(), 20);

        // Cache should not exceed max size
        assert!(notifier.result_cache.len() <= 10);

        // Recent results should still be cached
        assert!(notifier.get_result(20).is_some());
        assert!(notifier.get_result(19).is_some());

        // Old results should be evicted
        let old_cached = (1..=10).filter(|&seq| notifier.get_result(seq).is_some()).count();
        assert!(old_cached < 10, "Old entries should be mostly evicted");
    }

    #[tokio::test]
    async fn test_is_applied_check() {
        let executor = Arc::new(MockKvExecutor::new());
        let mut notifier = KvExecutionNotifier::new(executor, 100);

        assert!(!notifier.is_applied(1));
        assert!(!notifier.is_applied(5));

        // Apply sequence 1
        let committed_value = create_committed_value(b"op1".to_vec());
        notifier.notify_commit(1, committed_value).await.unwrap();

        assert!(notifier.is_applied(1));
        assert!(!notifier.is_applied(2));

        // Apply up to sequence 5
        for seq in 2..=5 {
            let committed_value = create_committed_value(format!("op{}", seq).into_bytes());
            notifier.notify_commit(seq, committed_value).await.unwrap();
        }

        assert!(notifier.is_applied(5));
        assert!(notifier.is_applied(3)); // All sequences <= 5 are applied
        assert!(!notifier.is_applied(6));
    }

    #[tokio::test]
    async fn test_wait_until_ready() {
        let executor = Arc::new(MockKvExecutor::new());
        let notifier = KvExecutionNotifier::new(executor, 100);

        // Should always succeed immediately
        let result = notifier.wait_until_ready().await;
        assert!(result.is_ok());
    }
}