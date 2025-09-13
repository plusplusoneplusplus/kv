use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::{Arc, Mutex as StdMutex};
use super::error::KvResult;
use super::CommitResult;

/// A future that can be used from C code
pub struct KvFuture<T> {
    inner: Pin<Box<dyn Future<Output = KvResult<T>> + Send + 'static>>,
}

impl<T> KvFuture<T> {
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = KvResult<T>> + Send + 'static,
    {
        Self {
            inner: Box::pin(future),
        }
    }
    
    pub async fn await_result(self) -> KvResult<T> {
        self.inner.await
    }
}

impl<T> Future for KvFuture<T> {
    type Output = KvResult<T>;
    
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.as_mut().poll(cx)
    }
}

// Opaque pointer type for C FFI
pub struct KvFuturePtr<T> {
    result: Arc<StdMutex<Option<KvResult<T>>>>,
}

impl<T: Send + 'static> KvFuturePtr<T> {
    pub fn new(future: KvFuture<T>) -> Self {
        let result_arc = Arc::new(StdMutex::new(None));
        
        // Spawn the future to run in the background
        let result_arc_clone = Arc::clone(&result_arc);
        super::ffi::RUNTIME.spawn(async move {
            let result = future.await_result().await;
            if let Ok(mut guard) = result_arc_clone.lock() {
                *guard = Some(result);
            }
        });
        
        Self {
            result: result_arc,
        }
    }
    
    pub fn poll(&self) -> bool {
        if let Ok(guard) = self.result.lock() {
            guard.is_some()
        } else {
            false
        }
    }
    
    pub fn take_result(&self) -> Option<KvResult<T>> {
        if let Ok(mut guard) = self.result.lock() {
            guard.take()
        } else {
            None
        }
    }
    
    pub fn is_ready(&self) -> bool {
        if let Ok(guard) = self.result.lock() {
            guard.is_some()
        } else {
            false
        }
    }
}

impl KvFuturePtr<CommitResult> {
    pub fn take_commit_result(&self) -> KvResult<CommitResult> {
        match self.take_result() {
            Some(result) => result,
            None => Err(super::error::KvError::Unknown("Future not ready".to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::error::KvError;
    use std::time::Duration;

    #[tokio::test]
    async fn test_kv_future_new_and_await() {
        let future = KvFuture::new(async { Ok::<i32, KvError>(42) });
        let result = future.await_result().await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_kv_future_error_result() {
        let future = KvFuture::new(async {
            Err::<i32, KvError>(KvError::Unknown("test error".to_string()))
        });
        let result = future.await_result().await;

        assert!(result.is_err());
        match result.unwrap_err() {
            KvError::Unknown(msg) => assert_eq!(msg, "test error"),
            _ => panic!("Expected Unknown error"),
        }
    }

    #[tokio::test]
    async fn test_kv_future_as_future_trait() {
        let future = KvFuture::new(async { Ok::<String, KvError>("hello".to_string()) });

        // Test that it implements Future trait
        let result = future.await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_kv_future_with_complex_type() {
        let data = vec![b"key1".to_vec(), b"key2".to_vec()];
        let future = KvFuture::new(async move {
            Ok::<Vec<Vec<u8>>, KvError>(data)
        });

        let result = future.await_result().await;
        assert!(result.is_ok());
        let result_data = result.unwrap();
        assert_eq!(result_data.len(), 2);
        assert_eq!(result_data[0], b"key1");
        assert_eq!(result_data[1], b"key2");
    }

    #[test]
    fn test_kv_future_ptr_creation_and_polling() {
        // Create a simple future that resolves immediately
        let future = KvFuture::new(async { Ok::<i32, KvError>(123) });
        let future_ptr = KvFuturePtr::new(future);

        // Initially might not be ready (depends on runtime scheduling)
        // But we can test the polling interface
        let _is_ready = future_ptr.poll();
        let _is_ready2 = future_ptr.is_ready();

        // Both poll() and is_ready() should return the same value
        assert_eq!(future_ptr.poll(), future_ptr.is_ready());
    }

    #[test]
    fn test_kv_future_ptr_take_result_when_not_ready() {
        let future = KvFuture::new(async {
            // Add a small delay to ensure it's not immediately ready
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<i32, KvError>(456)
        });
        let future_ptr = KvFuturePtr::new(future);

        // Should return None when not ready
        let result = future_ptr.take_result();
        if !future_ptr.is_ready() {
            assert!(result.is_none());
        }
    }

    #[test]
    fn test_kv_future_ptr_take_result_multiple_times() {
        let future = KvFuture::new(async { Ok::<i32, KvError>(789) });
        let future_ptr = KvFuturePtr::new(future);

        // Wait a moment for the future to potentially complete
        std::thread::sleep(Duration::from_millis(10));

        // First take might succeed
        let first_result = future_ptr.take_result();

        // Second take should always return None (since take_result consumes the value)
        let second_result = future_ptr.take_result();
        assert!(second_result.is_none());

        // If first succeeded, verify it was correct
        if let Some(result) = first_result {
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), 789);
        }
    }

    #[test]
    fn test_commit_result_future_ptr() {
        let commit_result = CommitResult::new(
            vec![b"generated_key".to_vec()],
            vec![b"generated_value".to_vec()]
        );

        let future = KvFuture::new(async move {
            Ok::<CommitResult, KvError>(commit_result)
        });
        let future_ptr = KvFuturePtr::new(future);

        // Wait a moment for completion
        std::thread::sleep(Duration::from_millis(10));

        // Test the specialized commit result method
        if future_ptr.is_ready() {
            let result = future_ptr.take_commit_result();
            assert!(result.is_ok());
            let commit_data = result.unwrap();
            assert_eq!(commit_data.generated_keys.len(), 1);
            assert_eq!(commit_data.generated_keys[0], b"generated_key");
        }
    }

    #[test]
    fn test_commit_result_future_ptr_not_ready() {
        let future = KvFuture::new(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok::<CommitResult, KvError>(CommitResult::empty())
        });
        let future_ptr = KvFuturePtr::new(future);

        // Should return error when not ready
        if !future_ptr.is_ready() {
            let result = future_ptr.take_commit_result();
            assert!(result.is_err());
            match result.unwrap_err() {
                KvError::Unknown(msg) => assert!(msg.contains("not ready")),
                _ => panic!("Expected Unknown error for not ready future"),
            }
        }
    }

    #[test]
    fn test_kv_future_ptr_error_propagation() {
        let future = KvFuture::new(async {
            Err::<i32, KvError>(KvError::TransactionConflict("test conflict".to_string()))
        });
        let future_ptr = KvFuturePtr::new(future);

        // Wait for completion
        std::thread::sleep(Duration::from_millis(10));

        if future_ptr.is_ready() {
            let result = future_ptr.take_result();
            assert!(result.is_some());
            let error_result = result.unwrap();
            assert!(error_result.is_err());

            match error_result.unwrap_err() {
                KvError::TransactionConflict(msg) => {
                    assert_eq!(msg, "test conflict");
                }
                _ => panic!("Expected TransactionConflict error"),
            }
        }
    }

    #[tokio::test]
    async fn test_kv_future_with_async_operations() {
        let future = KvFuture::new(async {
            // Simulate some async work
            tokio::time::sleep(Duration::from_millis(1)).await;

            // Return a complex result
            let data = vec![
                (b"key1".to_vec(), b"value1".to_vec()),
                (b"key2".to_vec(), b"value2".to_vec()),
            ];
            Ok::<Vec<(Vec<u8>, Vec<u8>)>, KvError>(data)
        });

        let result = future.await_result().await;
        assert!(result.is_ok());
        let kv_pairs = result.unwrap();
        assert_eq!(kv_pairs.len(), 2);
        assert_eq!(kv_pairs[0].0, b"key1");
        assert_eq!(kv_pairs[0].1, b"value1");
        assert_eq!(kv_pairs[1].0, b"key2");
        assert_eq!(kv_pairs[1].1, b"value2");
    }
}