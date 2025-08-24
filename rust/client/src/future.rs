use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use parking_lot::Mutex;
use std::sync::Arc;
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
    result: Arc<Mutex<Option<KvResult<T>>>>,
}

impl<T> KvFuturePtr<T> {
    pub fn new(future: KvFuture<T>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(future))),
            result: Arc::new(Mutex::new(None)),
        }
    }
    
    pub fn poll(&self) -> bool {
        let mut inner_guard = self.inner.lock();
        if let Some(mut future) = inner_guard.take() {
            // Create a no-op waker for polling
            let waker = futures::task::noop_waker();
            let mut context = Context::from_waker(&waker);
            
            match Pin::new(&mut future).poll(&mut context) {
                Poll::Ready(result) => {
                    *self.result.lock() = Some(result);
                    true
                }
                Poll::Pending => {
                    *inner_guard = Some(future);
                    false
                }
            }
        } else {
            // Already completed
            true
        }
    }
    
    pub fn take_result(&self) -> Option<KvResult<T>> {
        self.result.lock().take()
    }
    
    pub fn is_ready(&self) -> bool {
        self.result.lock().is_some()
    }
}