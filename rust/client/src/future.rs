use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use parking_lot::Mutex;
use std::sync::{Arc, Mutex as StdMutex};
use crate::error::KvResult;

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
    inner: Arc<Mutex<Option<KvFuture<T>>>>,
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
            inner: Arc::new(Mutex::new(None)), // No longer needed
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