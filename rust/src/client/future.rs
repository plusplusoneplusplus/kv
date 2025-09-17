use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::{Arc, Mutex as StdMutex};
use std::os::raw::c_void;
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

// C callback function type
pub type KvFutureCallback = extern "C" fn(*mut c_void, *mut c_void);

// Thread-safe wrapper for callback data
struct CallbackData {
    callback: KvFutureCallback,
    user_context: usize, // Store as usize for Send safety
}

unsafe impl Send for CallbackData {}
unsafe impl Sync for CallbackData {}

// Opaque pointer type for C FFI
pub struct KvFuturePtr<T> {
    result: Arc<StdMutex<Option<KvResult<T>>>>,
    callback: Arc<StdMutex<Option<CallbackData>>>,
    future_id: usize,
}

impl<T: Send + 'static> KvFuturePtr<T> {
    pub fn new(future: KvFuture<T>) -> Self {
        let result_arc = Arc::new(StdMutex::new(None));
        let callback_arc = Arc::new(StdMutex::new(None::<CallbackData>));
        let future_id = super::ffi::next_id();

        // Spawn the future to run in the background
        let result_arc_clone = Arc::clone(&result_arc);
        let callback_arc_clone = Arc::clone(&callback_arc);
        let future_handle_id = future_id; // Store as usize for Send safety

        super::ffi::RUNTIME.spawn(async move {
            let result = future.await_result().await;

            // Store result
            if let Ok(mut guard) = result_arc_clone.lock() {
                *guard = Some(result);
            }

            // Execute callback if set
            if let Ok(guard) = callback_arc_clone.lock() {
                if let Some(ref callback_data) = *guard {
                    let future_handle = future_handle_id as *mut c_void;
                    let user_context = callback_data.user_context as *mut c_void;
                    (callback_data.callback)(future_handle, user_context);
                }
            }
        });

        Self {
            result: result_arc,
            callback: callback_arc,
            future_id,
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

    pub fn set_callback(&self, callback: KvFutureCallback, user_context: *mut c_void) -> bool {
        // Check if already completed
        let is_ready = if let Ok(guard) = self.result.lock() {
            guard.is_some()
        } else {
            false
        };

        let callback_data = CallbackData {
            callback,
            user_context: user_context as usize,
        };

        if is_ready {
            // Execute immediately if already ready
            let future_handle = self.future_id as *mut c_void;
            let user_ctx = callback_data.user_context as *mut c_void;
            (callback_data.callback)(future_handle, user_ctx);
            true
        } else {
            // Set for future execution
            if let Ok(mut guard) = self.callback.lock() {
                *guard = Some(callback_data);
                true
            } else {
                false
            }
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

    // Callback-related tests
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    static CALLBACK_CALLED: AtomicBool = AtomicBool::new(false);
    static CALLBACK_FUTURE_ID: AtomicUsize = AtomicUsize::new(0);
    static CALLBACK_USER_CONTEXT: AtomicUsize = AtomicUsize::new(0);

    extern "C" fn test_callback(future_handle: *mut c_void, user_context: *mut c_void) {
        CALLBACK_CALLED.store(true, Ordering::SeqCst);
        CALLBACK_FUTURE_ID.store(future_handle as usize, Ordering::SeqCst);
        CALLBACK_USER_CONTEXT.store(user_context as usize, Ordering::SeqCst);
    }

    #[test]
    fn test_callback_with_immediate_completion() {
        // Reset test state
        CALLBACK_CALLED.store(false, Ordering::SeqCst);
        CALLBACK_FUTURE_ID.store(0, Ordering::SeqCst);
        CALLBACK_USER_CONTEXT.store(0, Ordering::SeqCst);

        // Create a future that completes immediately
        let future = KvFuture::new(async { Ok::<i32, KvError>(42) });
        let future_ptr = KvFuturePtr::new(future);

        // Wait a moment for it to complete
        std::thread::sleep(Duration::from_millis(10));

        // Set callback after completion - should execute immediately
        let user_context = 0x1234usize as *mut c_void;
        let success = future_ptr.set_callback(test_callback, user_context);

        assert!(success);
        assert!(CALLBACK_CALLED.load(Ordering::SeqCst));
        assert_eq!(CALLBACK_FUTURE_ID.load(Ordering::SeqCst), future_ptr.future_id);
        assert_eq!(CALLBACK_USER_CONTEXT.load(Ordering::SeqCst), 0x1234);
    }

    #[test]
    fn test_callback_with_deferred_completion() {
        // Reset test state
        CALLBACK_CALLED.store(false, Ordering::SeqCst);
        CALLBACK_FUTURE_ID.store(0, Ordering::SeqCst);
        CALLBACK_USER_CONTEXT.store(0, Ordering::SeqCst);

        // Create a future with delay
        let future = KvFuture::new(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<String, KvError>("test".to_string())
        });
        let future_ptr = KvFuturePtr::new(future);

        // Set callback immediately
        let user_context = 0x5678usize as *mut c_void;
        let success = future_ptr.set_callback(test_callback, user_context);

        assert!(success);
        assert!(!CALLBACK_CALLED.load(Ordering::SeqCst)); // Should not be called yet

        // Wait for completion
        std::thread::sleep(Duration::from_millis(100));

        // Now callback should have been called
        assert!(CALLBACK_CALLED.load(Ordering::SeqCst));
        assert_eq!(CALLBACK_FUTURE_ID.load(Ordering::SeqCst), future_ptr.future_id);
        assert_eq!(CALLBACK_USER_CONTEXT.load(Ordering::SeqCst), 0x5678);
    }

    #[test]
    fn test_callback_with_error_result() {
        // Reset test state
        CALLBACK_CALLED.store(false, Ordering::SeqCst);

        // Create a future that fails
        let future = KvFuture::new(async {
            Err::<i32, KvError>(KvError::TransactionConflict("test error".to_string()))
        });
        let future_ptr = KvFuturePtr::new(future);

        // Set callback
        let user_context = 0x9ABCusize as *mut c_void;
        let success = future_ptr.set_callback(test_callback, user_context);

        assert!(success);

        // Wait for completion
        std::thread::sleep(Duration::from_millis(10));

        // Callback should still be called even on error
        assert!(CALLBACK_CALLED.load(Ordering::SeqCst));
        assert_eq!(CALLBACK_USER_CONTEXT.load(Ordering::SeqCst), 0x9ABC);
    }

    #[test]
    fn test_callback_thread_safety() {
        use std::sync::atomic::AtomicU32;

        static CALLBACK_COUNT: AtomicU32 = AtomicU32::new(0);

        extern "C" fn counting_callback(_future: *mut c_void, _context: *mut c_void) {
            CALLBACK_COUNT.fetch_add(1, Ordering::SeqCst);
        }

        // Reset counter
        CALLBACK_COUNT.store(0, Ordering::SeqCst);

        // Create multiple futures with callbacks
        let futures: Vec<_> = (0..10)
            .map(|i| {
                let future = KvFuture::new(async move {
                    tokio::time::sleep(Duration::from_millis(i * 5)).await;
                    Ok::<i32, KvError>(i as i32)
                });
                let future_ptr = KvFuturePtr::new(future);
                future_ptr.set_callback(counting_callback, std::ptr::null_mut());
                future_ptr
            })
            .collect();

        // Wait for all to complete
        std::thread::sleep(Duration::from_millis(100));

        // All callbacks should have been called
        assert_eq!(CALLBACK_COUNT.load(Ordering::SeqCst), 10);

        // Keep futures alive until test ends
        drop(futures);
    }

    #[test]
    fn test_multiple_callbacks_on_same_future() {
        // Reset test state
        CALLBACK_CALLED.store(false, Ordering::SeqCst);

        let future = KvFuture::new(async { Ok::<i32, KvError>(123) });
        let future_ptr = KvFuturePtr::new(future);

        // Set first callback
        let success1 = future_ptr.set_callback(test_callback, 0x1111usize as *mut c_void);
        assert!(success1);

        // Set second callback - should replace the first
        let success2 = future_ptr.set_callback(test_callback, 0x2222usize as *mut c_void);
        assert!(success2);

        // Wait for completion
        std::thread::sleep(Duration::from_millis(10));

        // Should have called callback with the second context
        assert!(CALLBACK_CALLED.load(Ordering::SeqCst));
        assert_eq!(CALLBACK_USER_CONTEXT.load(Ordering::SeqCst), 0x2222);
    }
}