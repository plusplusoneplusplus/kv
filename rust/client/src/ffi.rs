use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::ptr;
use std::sync::Arc;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use crate::{KvStoreClient, Transaction, ReadTransaction, KvError};
use crate::error::KvErrorCode;
use crate::future::KvFuturePtr;

// Global runtime for async operations
#[allow(dead_code)]
static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Runtime::new().expect("Failed to create tokio runtime")
});

// Global storage for objects to prevent them from being dropped
static CLIENTS: Lazy<Mutex<HashMap<usize, Arc<KvStoreClient>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static TRANSACTIONS: Lazy<Mutex<HashMap<usize, Arc<Transaction>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static READ_TRANSACTIONS: Lazy<Mutex<HashMap<usize, Arc<ReadTransaction>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static FUTURES: Lazy<Mutex<HashMap<usize, Box<dyn std::any::Any + Send>>>> = Lazy::new(|| Mutex::new(HashMap::new()));

static NEXT_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

fn next_id() -> usize {
    NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

// Opaque handle types for C
pub type KvClientHandle = *mut c_void;
pub type KvTransactionHandle = *mut c_void;
pub type KvReadTransactionHandle = *mut c_void;
pub type KvFutureHandle = *mut c_void;

// Result structure for C
#[repr(C)]
pub struct KvResult {
    pub success: c_int,
    pub error_code: c_int,
    pub error_message: *mut c_char,
}

impl KvResult {
    fn success() -> Self {
        Self {
            success: 1,
            error_code: KvErrorCode::Success as c_int,
            error_message: ptr::null_mut(),
        }
    }
    
    fn error(err: &KvError) -> Self {
        let error_code = KvErrorCode::from(err) as c_int;
        let error_message = CString::new(err.to_string())
            .unwrap_or_else(|_| CString::new("Invalid error message").unwrap())
            .into_raw();
        
        Self {
            success: 0,
            error_code,
            error_message,
        }
    }
}

// Key-value pair for C
#[repr(C)]
pub struct KvPair {
    pub key: *mut c_char,
    pub value: *mut c_char,
}

// Array of key-value pairs for range operations
#[repr(C)]
pub struct KvPairArray {
    pub pairs: *mut KvPair,
    pub count: usize,
}

/// Initialize the KV client library
#[no_mangle]
pub extern "C" fn kv_init() -> c_int {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt::try_init();
    0  // Return 0 for success (C convention)
}

/// Create a new client connection
#[no_mangle]
pub extern "C" fn kv_client_create(address: *const c_char) -> KvClientHandle {
    if address.is_null() {
        return ptr::null_mut();
    }
    
    let address_str = unsafe {
        match CStr::from_ptr(address).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    match KvStoreClient::connect(address_str) {
        Ok(client) => {
            let id = next_id();
            CLIENTS.lock().insert(id, Arc::new(client));
            id as KvClientHandle
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Destroy a client connection
#[no_mangle]
pub extern "C" fn kv_client_destroy(client: KvClientHandle) {
    if !client.is_null() {
        let id = client as usize;
        CLIENTS.lock().remove(&id);
    }
}

/// Begin a new transaction
#[no_mangle]
pub extern "C" fn kv_transaction_begin(
    client: KvClientHandle,
    timeout_seconds: c_int,
) -> KvFutureHandle {
    if client.is_null() {
        return ptr::null_mut();
    }
    
    let id = client as usize;
    let client_arc = match CLIENTS.lock().get(&id).cloned() {
        Some(c) => c,
        None => return ptr::null_mut(),
    };
    
    let timeout = if timeout_seconds > 0 {
        Some(timeout_seconds as u64)
    } else {
        None
    };
    
    let future = client_arc.begin_transaction(None, timeout);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Begin a read-only transaction
#[no_mangle]
pub extern "C" fn kv_read_transaction_begin(
    client: KvClientHandle,
    read_version: i64,
) -> KvFutureHandle {
    if client.is_null() {
        return ptr::null_mut();
    }
    
    let id = client as usize;
    let client_arc = match CLIENTS.lock().get(&id).cloned() {
        Some(c) => c,
        None => return ptr::null_mut(),
    };
    
    let version = if read_version >= 0 {
        Some(read_version)
    } else {
        None
    };
    
    let future = client_arc.begin_read_transaction(version);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Poll a future to check if it's ready
#[no_mangle]
pub extern "C" fn kv_future_poll(future: KvFutureHandle) -> c_int {
    if future.is_null() {
        return -1;
    }
    
    let future_id = future as usize;
    let futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.get(&future_id) {
        // Try to downcast to different future types
        if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Transaction>>() {
            if future_ptr.poll() { 1 } else { 0 }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<ReadTransaction>>() {
            if future_ptr.poll() { 1 } else { 0 }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<()>>() {
            if future_ptr.poll() { 1 } else { 0 }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Option<String>>>() {
            if future_ptr.poll() { 1 } else { 0 }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Vec<(String, String)>>>() {
            if future_ptr.poll() { 1 } else { 0 }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<String>>() {
            if future_ptr.poll() { 1 } else { 0 }
        } else {
            -1
        }
    } else {
        -1
    }
}

/// Get the result of a transaction begin future
#[no_mangle]
pub extern "C" fn kv_future_get_transaction(future: KvFutureHandle) -> KvTransactionHandle {
    if future.is_null() {
        return ptr::null_mut();
    }
    
    let future_id = future as usize;
    let mut futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.remove(&future_id) {
        if let Ok(future_ptr) = boxed_future.downcast::<KvFuturePtr<Transaction>>() {
            if let Some(result) = future_ptr.take_result() {
                match result {
                    Ok(transaction) => {
                        let tx_id = next_id();
                        TRANSACTIONS.lock().insert(tx_id, Arc::new(transaction));
                        return tx_id as KvTransactionHandle;
                    }
                    Err(_) => return ptr::null_mut(),
                }
            }
        }
    }
    
    ptr::null_mut()
}

/// Get the result of a read transaction begin future
#[no_mangle]
pub extern "C" fn kv_future_get_read_transaction(future: KvFutureHandle) -> KvReadTransactionHandle {
    if future.is_null() {
        return ptr::null_mut();
    }
    
    let future_id = future as usize;
    let mut futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.remove(&future_id) {
        if let Ok(future_ptr) = boxed_future.downcast::<KvFuturePtr<ReadTransaction>>() {
            if let Some(result) = future_ptr.take_result() {
                match result {
                    Ok(read_transaction) => {
                        let tx_id = next_id();
                        READ_TRANSACTIONS.lock().insert(tx_id, Arc::new(read_transaction));
                        return tx_id as KvReadTransactionHandle;
                    }
                    Err(_) => return ptr::null_mut(),
                }
            }
        }
    }
    
    ptr::null_mut()
}

/// Get a value from a transaction
#[no_mangle]
pub extern "C" fn kv_transaction_get(
    transaction: KvTransactionHandle,
    key: *const c_char,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() || key.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let key_str = unsafe {
        match CStr::from_ptr(key).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    let cf_str = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s),
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    let future = tx_arc.get(key_str, cf_str);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Set a key-value pair in a transaction
#[no_mangle]
pub extern "C" fn kv_transaction_set(
    transaction: KvTransactionHandle,
    key: *const c_char,
    value: *const c_char,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() || key.is_null() || value.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let key_str = unsafe {
        match CStr::from_ptr(key).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    let value_str = unsafe {
        match CStr::from_ptr(value).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    let cf_str = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s),
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    let future = tx_arc.set(key_str, value_str, cf_str);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Commit a transaction
#[no_mangle]
pub extern "C" fn kv_transaction_commit(transaction: KvTransactionHandle) -> KvFutureHandle {
    if transaction.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx = match TRANSACTIONS.lock().remove(&tx_id) {
        Some(tx) => match Arc::try_unwrap(tx) {
            Ok(tx) => tx,
            Err(tx_arc) => {
                // Put it back if we can't unwrap
                TRANSACTIONS.lock().insert(tx_id, tx_arc);
                return ptr::null_mut();
            }
        },
        None => return ptr::null_mut(),
    };
    
    let future = tx.commit();
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Get the result of a void future (commit, set, delete, etc.)
#[no_mangle]
pub extern "C" fn kv_future_get_void_result(future: KvFutureHandle) -> KvResult {
    if future.is_null() {
        return KvResult::error(&KvError::Unknown("Null future handle".to_string()));
    }
    
    let future_id = future as usize;
    let mut futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.remove(&future_id) {
        if let Ok(future_ptr) = boxed_future.downcast::<KvFuturePtr<()>>() {
            if let Some(result) = future_ptr.take_result() {
                return match result {
                    Ok(()) => KvResult::success(),
                    Err(err) => KvResult::error(&err),
                };
            }
        }
    }
    
    KvResult::error(&KvError::Unknown("Future not ready or invalid type".to_string()))
}

/// Get the result of a string future (get operation)
#[no_mangle]
pub extern "C" fn kv_future_get_string_result(future: KvFutureHandle, value: *mut *mut c_char) -> KvResult {
    if future.is_null() || value.is_null() {
        return KvResult::error(&KvError::Unknown("Null pointer".to_string()));
    }
    
    let future_id = future as usize;
    let mut futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.remove(&future_id) {
        // Try Option<String> first
        match boxed_future.downcast::<KvFuturePtr<Option<String>>>() {
            Ok(future_ptr) => {
                if let Some(result) = future_ptr.take_result() {
                    return match result {
                        Ok(Some(val)) => {
                            match CString::new(val) {
                                Ok(c_string) => {
                                    unsafe { *value = c_string.into_raw(); }
                                    KvResult::success()
                                }
                                Err(_) => KvResult::error(&KvError::Unknown("Invalid string value".to_string())),
                            }
                        }
                        Ok(None) => {
                            unsafe { *value = ptr::null_mut(); }
                            KvResult::success()
                        }
                        Err(err) => KvResult::error(&err),
                    };
                }
            }
            Err(boxed_future) => {
                // Try String type
                if let Ok(future_ptr) = boxed_future.downcast::<KvFuturePtr<String>>() {
                    if let Some(result) = future_ptr.take_result() {
                        return match result {
                            Ok(val) => {
                                match CString::new(val) {
                                    Ok(c_string) => {
                                        unsafe { *value = c_string.into_raw(); }
                                        KvResult::success()
                                    }
                                    Err(_) => KvResult::error(&KvError::Unknown("Invalid string value".to_string())),
                                }
                            }
                            Err(err) => KvResult::error(&err),
                        };
                    }
                }
            }
        }
    }
    
    KvResult::error(&KvError::Unknown("Future not ready or invalid type".to_string()))
}

/// Free a string returned by the library
#[no_mangle]
pub extern "C" fn kv_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe {
            let _ = CString::from_raw(s);
        }
    }
}

/// Free a KvResult error message
#[no_mangle]
pub extern "C" fn kv_result_free(result: *mut KvResult) {
    if !result.is_null() {
        unsafe {
            let result_ref = &mut *result;
            if !result_ref.error_message.is_null() {
                let _ = CString::from_raw(result_ref.error_message);
                result_ref.error_message = ptr::null_mut();
            }
        }
    }
}

/// Get a value from a read transaction
#[no_mangle]
pub extern "C" fn kv_read_transaction_get(
    transaction: KvReadTransactionHandle,
    key: *const c_char,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() || key.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match READ_TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let key_str = unsafe {
        match CStr::from_ptr(key).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    let cf_str = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s),
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    let future = tx_arc.snapshot_get(key_str, cf_str);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Get a range of key-value pairs from a read transaction
#[no_mangle]
pub extern "C" fn kv_read_transaction_get_range(
    transaction: KvReadTransactionHandle,
    start_key: *const c_char,
    end_key: *const c_char,
    limit: c_int,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() || start_key.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match READ_TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let start_key_str = unsafe {
        match CStr::from_ptr(start_key).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    let end_key_str = if end_key.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(end_key).to_str() {
                Ok(s) => Some(s),
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    let cf_str = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s),
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    let limit_val = if limit > 0 { Some(limit as usize) } else { None };
    
    let future = tx_arc.snapshot_get_range(start_key_str, end_key_str, limit_val.map(|l| l as u32), cf_str);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Destroy a read transaction
#[no_mangle]
pub extern "C" fn kv_read_transaction_destroy(transaction: KvReadTransactionHandle) {
    if !transaction.is_null() {
        let tx_id = transaction as usize;
        READ_TRANSACTIONS.lock().remove(&tx_id);
    }
}

/// Ping the server (health check)
#[no_mangle]
pub extern "C" fn kv_client_ping(
    client: KvClientHandle,
    message: *const c_char,
) -> KvFutureHandle {
    if client.is_null() {
        return ptr::null_mut();
    }
    
    let id = client as usize;
    let client_arc = match CLIENTS.lock().get(&id).cloned() {
        Some(c) => c,
        None => return ptr::null_mut(),
    };
    
    let message_str = if message.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(message).to_str() {
                Ok(s) => Some(s.to_string()),
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    let future = client_arc.ping(message_str);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Abort a transaction
#[no_mangle]
pub extern "C" fn kv_transaction_abort(transaction: KvTransactionHandle) -> KvFutureHandle {
    if transaction.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx = match TRANSACTIONS.lock().remove(&tx_id) {
        Some(tx) => match Arc::try_unwrap(tx) {
            Ok(tx) => tx,
            Err(tx_arc) => {
                // Put it back if we can't unwrap
                TRANSACTIONS.lock().insert(tx_id, tx_arc);
                return ptr::null_mut();
            }
        },
        None => return ptr::null_mut(),
    };
    
    let future = tx.abort();
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Cleanup and shutdown the library
#[no_mangle]
pub extern "C" fn kv_shutdown() {
    CLIENTS.lock().clear();
    TRANSACTIONS.lock().clear();
    READ_TRANSACTIONS.lock().clear();
    FUTURES.lock().clear();
}