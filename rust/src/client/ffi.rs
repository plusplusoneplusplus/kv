use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use libc::{size_t};
use std::ptr;
use std::sync::Arc;
use std::slice;
use std::mem::size_of;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::collections::HashMap;
use super::{KvStoreClient, Transaction, ReadTransaction, KvError, KvFuture, ClientConfig, CommitResult};
use super::error::KvErrorCode;
use super::future::KvFuturePtr;

// Function return code constants (matching kvstore_client.h)
const KV_FUNCTION_SUCCESS: c_int = 1;
const KV_FUNCTION_FAILURE: c_int = 0;
const KV_FUNCTION_ERROR: c_int = -1;

// Global runtime for async operations
#[allow(dead_code)]
pub static RUNTIME: Lazy<tokio::runtime::Runtime> = Lazy::new(|| {
    tokio::runtime::Runtime::new().expect("Failed to create tokio runtime")
});

// Global storage for objects to prevent them from being dropped
static CLIENTS: Lazy<Mutex<HashMap<usize, Arc<KvStoreClient>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static TRANSACTIONS: Lazy<Mutex<HashMap<usize, Arc<Mutex<Transaction>>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static READ_TRANSACTIONS: Lazy<Mutex<HashMap<usize, Arc<ReadTransaction>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static FUTURES: Lazy<Mutex<HashMap<usize, Box<dyn std::any::Any + Send>>>> = Lazy::new(|| Mutex::new(HashMap::new()));
static CONFIGS: Lazy<Mutex<HashMap<usize, ClientConfig>>> = Lazy::new(|| Mutex::new(HashMap::new()));

static NEXT_ID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(1);

pub fn next_id() -> usize {
    NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

// Opaque handle types for C
pub type KvClientHandle = *mut c_void;
pub type KvTransactionHandle = *mut c_void;
pub type KvReadTransactionHandle = *mut c_void;
pub type KvFutureHandle = *mut c_void;
pub type KvConfigHandle = *mut c_void;

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

// Binary data structure (following FoundationDB pattern)
#[repr(C)]
pub struct KvBinaryData {
    pub data: *mut u8,
    pub length: c_int,
}

// Key-value pair with binary support
#[repr(C)]
pub struct KvPair {
    pub key: KvBinaryData,
    pub value: KvBinaryData,
}

// Array of key-value pairs for range operations
#[repr(C)]
pub struct KvPairArray {
    pub pairs: *mut KvPair,
    pub count: usize,
}

// Array of binary data for generated keys/values
#[repr(C)]
pub struct KvBinaryDataArray {
    pub data: *mut KvBinaryData,
    pub count: usize,
}

// Commit result containing generated keys and values
#[repr(C)]
pub struct KvCommitResult {
    pub success: c_int,
    pub error_code: c_int,
    pub error_message: *mut c_char,
    pub generated_keys: KvBinaryDataArray,
    pub generated_values: KvBinaryDataArray,
}

/// Initialize the KV client library
#[no_mangle]
pub extern "C" fn kv_init() -> c_int {
    // Initialize tracing for debugging
    let _ = tracing_subscriber::fmt()
        .with_ansi(false)  // Disable ANSI color codes
        .try_init();
    0  // Return 0 for success (C convention)
}

/// Create a new client connection with default configuration
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

/// Create a new client configuration with default values
#[no_mangle]
pub extern "C" fn kv_config_create() -> KvConfigHandle {
    let config = ClientConfig::default();
    let id = next_id();
    CONFIGS.lock().insert(id, config);
    id as KvConfigHandle
}

/// Create a new client configuration with debug enabled
#[no_mangle]
pub extern "C" fn kv_config_create_with_debug() -> KvConfigHandle {
    let config = ClientConfig::with_debug();
    let id = next_id();
    CONFIGS.lock().insert(id, config);
    id as KvConfigHandle
}

/// Enable debug mode on a configuration
#[no_mangle]
pub extern "C" fn kv_config_enable_debug(config: KvConfigHandle) -> c_int {
    if config.is_null() {
        return KV_FUNCTION_ERROR;
    }
    
    let id = config as usize;
    let mut configs = CONFIGS.lock();
    
    if let Some(cfg) = configs.get_mut(&id) {
        *cfg = cfg.clone().enable_debug();
        KV_FUNCTION_SUCCESS
    } else {
        KV_FUNCTION_ERROR
    }
}

/// Set connection timeout on a configuration
#[no_mangle]
pub extern "C" fn kv_config_set_connection_timeout(config: KvConfigHandle, timeout_seconds: u64) -> c_int {
    if config.is_null() {
        return KV_FUNCTION_ERROR;
    }
    
    let id = config as usize;
    let mut configs = CONFIGS.lock();
    
    if let Some(cfg) = configs.get_mut(&id) {
        *cfg = cfg.clone().with_connection_timeout(timeout_seconds);
        KV_FUNCTION_SUCCESS
    } else {
        KV_FUNCTION_ERROR
    }
}

/// Set request timeout on a configuration
#[no_mangle]
pub extern "C" fn kv_config_set_request_timeout(config: KvConfigHandle, timeout_seconds: u64) -> c_int {
    if config.is_null() {
        return KV_FUNCTION_ERROR;
    }
    
    let id = config as usize;
    let mut configs = CONFIGS.lock();
    
    if let Some(cfg) = configs.get_mut(&id) {
        *cfg = cfg.clone().with_request_timeout(timeout_seconds);
        KV_FUNCTION_SUCCESS
    } else {
        KV_FUNCTION_ERROR
    }
}

/// Set maximum retries on a configuration
#[no_mangle]
pub extern "C" fn kv_config_set_max_retries(config: KvConfigHandle, retries: u32) -> c_int {
    if config.is_null() {
        return KV_FUNCTION_ERROR;
    }
    
    let id = config as usize;
    let mut configs = CONFIGS.lock();
    
    if let Some(cfg) = configs.get_mut(&id) {
        *cfg = cfg.clone().with_max_retries(retries);
        KV_FUNCTION_SUCCESS
    } else {
        KV_FUNCTION_ERROR
    }
}

/// Create a new client connection with custom configuration
#[no_mangle]
pub extern "C" fn kv_client_create_with_config(address: *const c_char, config: KvConfigHandle) -> KvClientHandle {
    if address.is_null() || config.is_null() {
        return ptr::null_mut();
    }
    
    let address_str = unsafe {
        match CStr::from_ptr(address).to_str() {
            Ok(s) => s,
            Err(_) => return ptr::null_mut(),
        }
    };
    
    let config_id = config as usize;
    let client_config = match CONFIGS.lock().get(&config_id).cloned() {
        Some(cfg) => cfg,
        None => return ptr::null_mut(),
    };
    
    match KvStoreClient::connect_with_config(address_str, client_config) {
        Ok(client) => {
            let id = next_id();
            CLIENTS.lock().insert(id, Arc::new(client));
            id as KvClientHandle
        }
        Err(_) => ptr::null_mut(),
    }
}

/// Destroy a configuration handle
#[no_mangle]
pub extern "C" fn kv_config_destroy(config: KvConfigHandle) {
    if !config.is_null() {
        let id = config as usize;
        CONFIGS.lock().remove(&id);
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
        return KV_FUNCTION_ERROR;
    }
    
    let future_id = future as usize;
    let futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.get(&future_id) {
        // Try to downcast to different future types
        if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Transaction>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<ReadTransaction>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<()>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Option<Vec<u8>>>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Option<String>>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Vec<(Vec<u8>, Vec<u8>)>>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Vec<(String, String)>>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<String>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<CommitResult>>() {
            if future_ptr.poll() { KV_FUNCTION_SUCCESS } else { KV_FUNCTION_FAILURE }
        } else {
            KV_FUNCTION_ERROR
        }
    } else {
        KV_FUNCTION_ERROR
    }
}

/// Set a callback to be called when a future completes
#[no_mangle]
pub extern "C" fn kv_future_set_callback(
    future: KvFutureHandle,
    callback: extern "C" fn(KvFutureHandle, *mut c_void),
    user_context: *mut c_void,
) -> c_int {
    if future.is_null() {
        return KV_FUNCTION_ERROR;
    }

    let future_id = future as usize;
    let futures = FUTURES.lock();

    if let Some(boxed_future) = futures.get(&future_id) {
        // Try to downcast to different future types and set callback
        if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Transaction>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<ReadTransaction>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<()>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Option<Vec<u8>>>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Option<String>>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Vec<(Vec<u8>, Vec<u8>)>>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<Vec<(String, String)>>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<String>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        } else if let Some(future_ptr) = boxed_future.downcast_ref::<KvFuturePtr<CommitResult>>() {
            if future_ptr.set_callback(callback, user_context) {
                return KV_FUNCTION_SUCCESS;
            }
        }
    }

    KV_FUNCTION_ERROR
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
                        TRANSACTIONS.lock().insert(tx_id, Arc::new(Mutex::new(transaction)));
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

/// Get a value from a transaction (FoundationDB-style binary interface)
#[no_mangle]
pub extern "C" fn kv_transaction_get(
    transaction: KvTransactionHandle,
    key_data: *const u8,
    key_length: c_int,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() || key_data.is_null() || key_length < 0 {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let key_bytes = unsafe {
        slice::from_raw_parts(key_data, key_length as usize).to_vec() // Copy the data
    };
    
    let cf_str = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s.to_string()), // Copy the string
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    // Create the future outside of any locks
    let future = {
        let tx_guard = tx_arc.lock();
        tx_guard.get(&key_bytes, cf_str.as_deref())
    }; // Lock is released here
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Set a key-value pair in a transaction (FoundationDB-style binary interface)
#[no_mangle]
pub extern "C" fn kv_transaction_set(
    transaction: KvTransactionHandle,
    key_data: *const u8,
    key_length: c_int,
    value_data: *const u8,
    value_length: c_int,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() || key_data.is_null() || key_length < 0 || value_length < 0 {
        return ptr::null_mut();
    }
    
    // Allow NULL value_data only if value_length is 0 (empty value)
    if value_data.is_null() && value_length != 0 {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let key_bytes = unsafe {
        slice::from_raw_parts(key_data, key_length as usize).to_vec() // Copy the data
    };
    
    let value_bytes = if value_data.is_null() {
        Vec::new() // Empty value
    } else {
        unsafe {
            slice::from_raw_parts(value_data, value_length as usize).to_vec() // Copy the data
        }
    };
    
    let cf_str = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s.to_string()), // Copy the string
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    // Use the binary set method
    let result = {
        let mut tx_guard = tx_arc.lock();
        tx_guard.set(&key_bytes, &value_bytes, cf_str.as_deref())
    }; // Lock is released here
    let future = KvFuture::new(async move { result });
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Delete a key in a transaction (FoundationDB-style binary interface)
#[no_mangle]
pub extern "C" fn kv_transaction_delete(
    transaction: KvTransactionHandle,
    key_data: *const u8,
    key_length: c_int,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() || key_data.is_null() || key_length < 0 {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let key_bytes = unsafe {
        slice::from_raw_parts(key_data, key_length as usize).to_vec() // Copy the data
    };
    
    let cf_str = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s.to_string()), // Copy the string
                Err(_) => return ptr::null_mut(),
            }
        }
    };
    
    // Use the binary delete method
    let result = {
        let mut tx_guard = tx_arc.lock();
        tx_guard.delete(&key_bytes, cf_str.as_deref())
    }; // Lock is released here
    let future = KvFuture::new(async move { result });
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
        Some(tx_arc) => match Arc::try_unwrap(tx_arc) {
            Ok(tx_mutex) => match tx_mutex.into_inner() {
                tx => tx,
            },
            Err(tx_arc) => {
                // Put it back if we can't unwrap the Arc
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

/// Commit a transaction and return generated keys and values
#[no_mangle]
pub extern "C" fn kv_transaction_commit_with_results(transaction: KvTransactionHandle) -> KvFutureHandle {
    if transaction.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx = match TRANSACTIONS.lock().remove(&tx_id) {
        Some(tx_arc) => match Arc::try_unwrap(tx_arc) {
            Ok(tx_mutex) => match tx_mutex.into_inner() {
                tx => tx,
            },
            Err(tx_arc) => {
                // Put it back if we can't unwrap the Arc
                TRANSACTIONS.lock().insert(tx_id, tx_arc);
                return ptr::null_mut();
            }
        },
        None => return ptr::null_mut(),
    };
    
    let future = tx.commit_with_results();
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

/// Get the result of a value future (get operation) - returns binary data
#[no_mangle]
pub extern "C" fn kv_future_get_value_result(future: KvFutureHandle, value: *mut KvBinaryData) -> KvResult {
    if future.is_null() || value.is_null() {
        return KvResult::error(&KvError::Unknown("Null pointer".to_string()));
    }
    
    let future_id = future as usize;
    let mut futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.remove(&future_id) {
        // Try Option<Vec<u8>> first (for binary operations)
        match boxed_future.downcast::<KvFuturePtr<Option<Vec<u8>>>>() {
            Ok(future_ptr) => {
                if let Some(result) = future_ptr.take_result() {
                    return match result {
                        Ok(Some(bytes)) => {
                            let len = bytes.len() as c_int;
                            let data = unsafe {
                                let ptr = std::alloc::alloc(std::alloc::Layout::array::<u8>(bytes.len()).unwrap());
                                if ptr.is_null() {
                                    return KvResult::error(&KvError::Unknown("Memory allocation failed".to_string()));
                                }
                                std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
                                ptr
                            };
                            
                            unsafe {
                                (*value).data = data;
                                (*value).length = len;
                            }
                            KvResult::success()
                        }
                        Ok(None) => {
                            unsafe {
                                (*value).data = ptr::null_mut();
                                (*value).length = 0;
                            }
                            KvResult::success()
                        }
                        Err(err) => KvResult::error(&err),
                    };
                }
            }
            Err(boxed_future) => {
                // Try Option<String> next
                match boxed_future.downcast::<KvFuturePtr<Option<String>>>() {
                    Ok(future_ptr) => {
                        if let Some(result) = future_ptr.take_result() {
                            return match result {
                                Ok(Some(val)) => {
                                    let bytes = val.into_bytes();
                                    let len = bytes.len() as c_int;
                                    let data = unsafe {
                                        let ptr = std::alloc::alloc(std::alloc::Layout::array::<u8>(bytes.len()).unwrap());
                                        if ptr.is_null() {
                                            return KvResult::error(&KvError::Unknown("Memory allocation failed".to_string()));
                                        }
                                        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
                                        ptr
                                    };
                                    
                                    unsafe {
                                        (*value).data = data;
                                        (*value).length = len;
                                    }
                                    KvResult::success()
                                }
                                Ok(None) => {
                                    unsafe {
                                        (*value).data = ptr::null_mut();
                                        (*value).length = 0;
                                    }
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
                                        let bytes = val.into_bytes();
                                        let len = bytes.len() as c_int;
                                        let data = unsafe {
                                            let ptr = std::alloc::alloc(std::alloc::Layout::array::<u8>(bytes.len()).unwrap());
                                            if ptr.is_null() {
                                                return KvResult::error(&KvError::Unknown("Memory allocation failed".to_string()));
                                            }
                                            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
                                            ptr
                                        };
                                        
                                        unsafe {
                                            (*value).data = data;
                                            (*value).length = len;
                                        }
                                        KvResult::success()
                                    }
                                    Err(err) => KvResult::error(&err),
                                };
                            }
                        }
                    }
                }
            }
        }
    }
    
    KvResult::error(&KvError::Unknown("Future not ready or invalid type".to_string()))
}

/// Free binary data allocated by the library
#[no_mangle]
pub extern "C" fn kv_binary_free(data: *mut KvBinaryData) {
    if !data.is_null() {
        unsafe {
            let data_ref = &mut *data;
            if !data_ref.data.is_null() && data_ref.length > 0 {
                std::alloc::dealloc(
                    data_ref.data,
                    std::alloc::Layout::array::<u8>(data_ref.length as usize).unwrap()
                );
                data_ref.data = ptr::null_mut();
                data_ref.length = 0;
            }
        }
    }
}

/// Create binary data from null-terminated string
#[no_mangle]
pub extern "C" fn kv_binary_from_string(s: *const c_char) -> KvBinaryData {
    if s.is_null() {
        return KvBinaryData {
            data: ptr::null_mut(),
            length: 0,
        };
    }
    
    unsafe {
        let c_str = CStr::from_ptr(s);
        let bytes = c_str.to_bytes();
        let len = bytes.len() as c_int;
        
        if len == 0 {
            return KvBinaryData {
                data: ptr::null_mut(),
                length: 0,
            };
        }
        
        let data = std::alloc::alloc(std::alloc::Layout::array::<u8>(bytes.len()).unwrap());
        if data.is_null() {
            return KvBinaryData {
                data: ptr::null_mut(),
                length: 0,
            };
        }
        
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), data, bytes.len());
        
        KvBinaryData {
            data,
            length: len,
        }
    }
}

/// Create copy of binary data
#[no_mangle]
pub extern "C" fn kv_binary_copy(data: *const u8, length: c_int) -> KvBinaryData {
    if data.is_null() || length <= 0 {
        return KvBinaryData {
            data: ptr::null_mut(),
            length: 0,
        };
    }
    
    unsafe {
        let new_data = std::alloc::alloc(std::alloc::Layout::array::<u8>(length as usize).unwrap());
        if new_data.is_null() {
            return KvBinaryData {
                data: ptr::null_mut(),
                length: 0,
            };
        }
        
        std::ptr::copy_nonoverlapping(data, new_data, length as usize);
        
        KvBinaryData {
            data: new_data,
            length,
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

/// Get a value from a read transaction (FoundationDB-style binary interface)
#[no_mangle]
pub extern "C" fn kv_read_transaction_get(
    transaction: KvReadTransactionHandle,
    key_data: *const u8,
    key_length: c_int,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() || key_data.is_null() || key_length < 0 {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match READ_TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let key_bytes = unsafe {
        slice::from_raw_parts(key_data, key_length as usize)
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
    
    let future = tx_arc.snapshot_get(key_bytes, cf_str);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Get a range of key-value pairs from a read transaction (FoundationDB-style binary interface)
#[no_mangle]
pub extern "C" fn kv_read_transaction_get_range(
    transaction: KvReadTransactionHandle,
    start_key_data: *const u8,
    start_key_length: c_int,
    end_key_data: *const u8,
    end_key_length: c_int,
    begin_offset: c_int,
    begin_or_equal: c_int,  // 1 for true, 0 for false
    end_offset: c_int,
    end_or_equal: c_int,    // 1 for true, 0 for false
    limit: c_int,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match READ_TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let start_key_opt = if start_key_data.is_null() || start_key_length <= 0 {
        None
    } else {
        let start_key_bytes = unsafe {
            slice::from_raw_parts(start_key_data, start_key_length as usize)
        };
        if start_key_bytes.is_empty() { None } else { Some(start_key_bytes) }
    };
    
    let end_key_opt = if end_key_data.is_null() || end_key_length <= 0 {
        None
    } else {
        Some(unsafe {
            slice::from_raw_parts(end_key_data, end_key_length as usize)
        })
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
    
    let limit_val = if limit > 0 { Some(limit as u32) } else { None };
    
    let future = tx_arc.snapshot_get_range(
        start_key_opt, 
        end_key_opt, 
        Some(begin_offset), 
        Some(begin_or_equal != 0), 
        Some(end_offset), 
        Some(end_or_equal != 0), 
        limit_val, 
        cf_str
    );
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
    message_data: *const u8,
    message_length: c_int,
) -> KvFutureHandle {
    if client.is_null() {
        return ptr::null_mut();
    }
    
    let id = client as usize;
    let client_arc = match CLIENTS.lock().get(&id).cloned() {
        Some(c) => c,
        None => return ptr::null_mut(),
    };
    
    let message_bytes = if message_data.is_null() || message_length <= 0 {
        None
    } else {
        unsafe {
            let bytes = slice::from_raw_parts(message_data, message_length as usize);
            Some(bytes.to_vec())
        }
    };
    
    let future = client_arc.ping(message_bytes);
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Get a range of key-value pairs from a transaction (FoundationDB-style binary interface)
#[no_mangle]
pub extern "C" fn kv_transaction_get_range(
    transaction: KvTransactionHandle,
    start_key_data: *const u8,
    start_key_length: c_int,
    end_key_data: *const u8,
    end_key_length: c_int,
    begin_offset: c_int,
    begin_or_equal: c_int,  // 1 for true, 0 for false
    end_offset: c_int,
    end_or_equal: c_int,    // 1 for true, 0 for false
    limit: c_int,
    column_family: *const c_char,
) -> KvFutureHandle {
    if transaction.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return ptr::null_mut(),
    };
    
    let start_key_opt = if start_key_data.is_null() || start_key_length <= 0 {
        None
    } else {
        let start_key_bytes = unsafe {
            slice::from_raw_parts(start_key_data, start_key_length as usize)
        };
        if start_key_bytes.is_empty() { None } else { Some(start_key_bytes) }
    };
    
    let end_key_opt = if end_key_data.is_null() || end_key_length <= 0 {
        None
    } else {
        Some(unsafe {
            slice::from_raw_parts(end_key_data, end_key_length as usize)
        })
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
    
    let limit_val = if limit > 0 { Some(limit as u32) } else { None };
    
    let future = tx_arc.lock().get_range(
        start_key_opt, 
        end_key_opt, 
        Some(begin_offset), 
        Some(begin_or_equal != 0), 
        Some(end_offset), 
        Some(end_or_equal != 0), 
        limit_val, 
        cf_str
    );
    let future_ptr = KvFuturePtr::new(future);
    
    let future_id = next_id();
    FUTURES.lock().insert(future_id, Box::new(future_ptr));
    
    future_id as KvFutureHandle
}

/// Get the result of a key-value array future (range operations)
#[no_mangle]
pub extern "C" fn kv_future_get_kv_array_result(future: KvFutureHandle, pairs: *mut KvPairArray) -> KvResult {
    if future.is_null() || pairs.is_null() {
        return KvResult::error(&KvError::Unknown("Null pointer".to_string()));
    }
    
    let future_id = future as usize;
    let mut futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.remove(&future_id) {
        // Try binary format first
        match boxed_future.downcast::<KvFuturePtr<Vec<(Vec<u8>, Vec<u8>)>>>() {
            Ok(future_ptr) => {
            if let Some(result) = future_ptr.take_result() {
                return match result {
                    Ok(kv_vec) => {
                        let count = kv_vec.len();
                        if count == 0 {
                            unsafe {
                                (*pairs).pairs = ptr::null_mut();
                                (*pairs).count = 0;
                            }
                            return KvResult::success();
                        }
                        
                        // Allocate C array
                        let c_pairs = unsafe {
                            std::alloc::alloc(std::alloc::Layout::array::<KvPair>(count).unwrap()) as *mut KvPair
                        };
                        
                        if c_pairs.is_null() {
                            return KvResult::error(&KvError::Unknown("Memory allocation failed".to_string()));
                        }
                        
                        // Fill the array with binary data
                        for (i, (key_bytes, value_bytes)) in kv_vec.into_iter().enumerate() {
                            // Allocate and copy key data
                            let key_len = key_bytes.len();
                            let key_ptr = unsafe {
                                let ptr = std::alloc::alloc(std::alloc::Layout::array::<u8>(key_len).unwrap());
                                if ptr.is_null() {
                                    return KvResult::error(&KvError::Unknown("Memory allocation failed".to_string()));
                                }
                                std::ptr::copy_nonoverlapping(key_bytes.as_ptr(), ptr, key_len);
                                ptr
                            };
                            
                            // Allocate and copy value data
                            let value_len = value_bytes.len();
                            let value_ptr = unsafe {
                                let ptr = std::alloc::alloc(std::alloc::Layout::array::<u8>(value_len).unwrap());
                                if ptr.is_null() {
                                    return KvResult::error(&KvError::Unknown("Memory allocation failed".to_string()));
                                }
                                std::ptr::copy_nonoverlapping(value_bytes.as_ptr(), ptr, value_len);
                                ptr
                            };
                            
                            unsafe {
                                (*c_pairs.add(i)).key = KvBinaryData {
                                    data: key_ptr,
                                    length: key_len as c_int,
                                };
                                (*c_pairs.add(i)).value = KvBinaryData {
                                    data: value_ptr,
                                    length: value_len as c_int,
                                };
                            }
                        }
                        
                        unsafe {
                            (*pairs).pairs = c_pairs;
                            (*pairs).count = count;
                        }
                        KvResult::success()
                    }
                    Err(err) => KvResult::error(&err),
                };
            }
            }
            Err(boxed_future) => {
                // Try string format for backward compatibility
                if let Ok(future_ptr) = boxed_future.downcast::<KvFuturePtr<Vec<(String, String)>>>() {
            if let Some(result) = future_ptr.take_result() {
                return match result {
                    Ok(kv_vec) => {
                        let count = kv_vec.len();
                        if count == 0 {
                            unsafe {
                                (*pairs).pairs = ptr::null_mut();
                                (*pairs).count = 0;
                            }
                            return KvResult::success();
                        }
                        
                        // Allocate C array
                        let c_pairs = unsafe {
                            std::alloc::alloc(std::alloc::Layout::array::<KvPair>(count).unwrap()) as *mut KvPair
                        };
                        
                        if c_pairs.is_null() {
                            return KvResult::error(&KvError::Unknown("Memory allocation failed".to_string()));
                        }
                        
                        // Fill the array with binary data
                        for (i, (key, value)) in kv_vec.into_iter().enumerate() {
                            // Allocate and copy key data
                            let key_bytes = key.into_bytes();
                            let key_len = key_bytes.len() as c_int;
                            let key_data = if key_len > 0 {
                                unsafe {
                                    let ptr = std::alloc::alloc(std::alloc::Layout::array::<u8>(key_bytes.len()).unwrap());
                                    if ptr.is_null() {
                                        // Clean up previously allocated pairs
                                        for j in 0..i {
                                            if !(*c_pairs.add(j)).key.data.is_null() && (*c_pairs.add(j)).key.length > 0 {
                                                std::alloc::dealloc(
                                                    (*c_pairs.add(j)).key.data,
                                                    std::alloc::Layout::array::<u8>((*c_pairs.add(j)).key.length as usize).unwrap()
                                                );
                                            }
                                            if !(*c_pairs.add(j)).value.data.is_null() && (*c_pairs.add(j)).value.length > 0 {
                                                std::alloc::dealloc(
                                                    (*c_pairs.add(j)).value.data,
                                                    std::alloc::Layout::array::<u8>((*c_pairs.add(j)).value.length as usize).unwrap()
                                                );
                                            }
                                        }
                                        std::alloc::dealloc(c_pairs as *mut u8, std::alloc::Layout::array::<KvPair>(count).unwrap());
                                        return KvResult::error(&KvError::Unknown("Memory allocation failed for key".to_string()));
                                    }
                                    std::ptr::copy_nonoverlapping(key_bytes.as_ptr(), ptr, key_bytes.len());
                                    ptr
                                }
                            } else {
                                ptr::null_mut()
                            };
                            
                            // Allocate and copy value data
                            let value_bytes = value.into_bytes();
                            let value_len = value_bytes.len() as c_int;
                            let value_data = if value_len > 0 {
                                unsafe {
                                    let ptr = std::alloc::alloc(std::alloc::Layout::array::<u8>(value_bytes.len()).unwrap());
                                    if ptr.is_null() {
                                        // Clean up key data we just allocated
                                        if !key_data.is_null() {
                                            std::alloc::dealloc(key_data, std::alloc::Layout::array::<u8>(key_bytes.len()).unwrap());
                                        }
                                        // Clean up previously allocated pairs
                                        for j in 0..i {
                                            if !(*c_pairs.add(j)).key.data.is_null() && (*c_pairs.add(j)).key.length > 0 {
                                                std::alloc::dealloc(
                                                    (*c_pairs.add(j)).key.data,
                                                    std::alloc::Layout::array::<u8>((*c_pairs.add(j)).key.length as usize).unwrap()
                                                );
                                            }
                                            if !(*c_pairs.add(j)).value.data.is_null() && (*c_pairs.add(j)).value.length > 0 {
                                                std::alloc::dealloc(
                                                    (*c_pairs.add(j)).value.data,
                                                    std::alloc::Layout::array::<u8>((*c_pairs.add(j)).value.length as usize).unwrap()
                                                );
                                            }
                                        }
                                        std::alloc::dealloc(c_pairs as *mut u8, std::alloc::Layout::array::<KvPair>(count).unwrap());
                                        return KvResult::error(&KvError::Unknown("Memory allocation failed for value".to_string()));
                                    }
                                    std::ptr::copy_nonoverlapping(value_bytes.as_ptr(), ptr, value_bytes.len());
                                    ptr
                                }
                            } else {
                                ptr::null_mut()
                            };
                            
                            unsafe {
                                (*c_pairs.add(i)).key = KvBinaryData {
                                    data: key_data,
                                    length: key_len,
                                };
                                (*c_pairs.add(i)).value = KvBinaryData {
                                    data: value_data,
                                    length: value_len,
                                };
                            }
                        }
                        
                        unsafe {
                            (*pairs).pairs = c_pairs;
                            (*pairs).count = count;
                        }
                        
                        KvResult::success()
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

/// Free a KvPairArray returned by the library
#[no_mangle]
pub extern "C" fn kv_pair_array_free(pairs: *mut KvPairArray) {
    if !pairs.is_null() {
        unsafe {
            let pairs_ref = &mut *pairs;
            if !pairs_ref.pairs.is_null() && pairs_ref.count > 0 {
                for i in 0..pairs_ref.count {
                    let pair = pairs_ref.pairs.add(i);
                    // Free key data
                    if !(*pair).key.data.is_null() && (*pair).key.length > 0 {
                        std::alloc::dealloc(
                            (*pair).key.data,
                            std::alloc::Layout::array::<u8>((*pair).key.length as usize).unwrap()
                        );
                    }
                    // Free value data
                    if !(*pair).value.data.is_null() && (*pair).value.length > 0 {
                        std::alloc::dealloc(
                            (*pair).value.data,
                            std::alloc::Layout::array::<u8>((*pair).value.length as usize).unwrap()
                        );
                    }
                }
                std::alloc::dealloc(
                    pairs_ref.pairs as *mut u8,
                    std::alloc::Layout::array::<KvPair>(pairs_ref.count).unwrap()
                );
                pairs_ref.pairs = ptr::null_mut();
                pairs_ref.count = 0;
            }
        }
    }
}

/// Set a versionstamped key in a transaction (key prefix will be appended with version)
#[no_mangle]
pub extern "C" fn kv_transaction_set_versionstamped_key(
    transaction: KvTransactionHandle,
    key_prefix_data: *const u8,
    key_prefix_length: c_int,
    value_data: *const u8,
    value_length: c_int,
    column_family: *const c_char,
) -> c_int {
    if transaction.is_null() || key_prefix_data.is_null() || key_prefix_length < 0 || value_length < 0 {
        return KV_FUNCTION_FAILURE; // Error
    }
    
    // Allow NULL value_data only if value_length is 0 (empty value)
    if value_data.is_null() && value_length != 0 {
        return KV_FUNCTION_FAILURE; // Error
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return KV_FUNCTION_FAILURE, // Error
    };
    
    let key_prefix_bytes = unsafe {
        slice::from_raw_parts(key_prefix_data, key_prefix_length as usize)
    };
    
    let value_bytes = if value_data.is_null() {
        &[]
    } else {
        unsafe {
            slice::from_raw_parts(value_data, value_length as usize)
        }
    };
    
    let cf_name = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s),
                Err(_) => return KV_FUNCTION_FAILURE, // Error
            }
        }
    };
    
    let result = tx_arc.lock().set_versionstamped_key(key_prefix_bytes, value_bytes, cf_name);
    match result {
        Ok(_) => KV_FUNCTION_SUCCESS, // Success
        Err(_) => KV_FUNCTION_FAILURE, // Error
    }
}

/// Set a versionstamped value in a transaction (value prefix will be appended with version)
#[no_mangle]
pub extern "C" fn kv_transaction_set_versionstamped_value(
    transaction: KvTransactionHandle,
    key_data: *const u8,
    key_length: c_int,
    value_prefix_data: *const u8,
    value_prefix_length: c_int,
    column_family: *const c_char,
) -> c_int {
    if transaction.is_null() || key_data.is_null() || key_length < 0 || value_prefix_length < 0 {
        return KV_FUNCTION_FAILURE; // Error
    }
    
    // Allow NULL value_prefix_data only if value_prefix_length is 0 (empty prefix)
    if value_prefix_data.is_null() && value_prefix_length != 0 {
        return KV_FUNCTION_FAILURE; // Error
    }
    
    let tx_id = transaction as usize;
    let tx_arc = match TRANSACTIONS.lock().get(&tx_id).cloned() {
        Some(tx) => tx,
        None => return KV_FUNCTION_FAILURE, // Error
    };
    
    let key_bytes = unsafe {
        slice::from_raw_parts(key_data, key_length as usize)
    };
    
    let value_prefix_bytes = if value_prefix_data.is_null() {
        &[]
    } else {
        unsafe {
            slice::from_raw_parts(value_prefix_data, value_prefix_length as usize)
        }
    };
    
    
    let cf_name = if column_family.is_null() {
        None
    } else {
        unsafe {
            match CStr::from_ptr(column_family).to_str() {
                Ok(s) => Some(s),
                Err(_) => return KV_FUNCTION_FAILURE, // Error
            }
        }
    };
    
    let result = tx_arc.lock().set_versionstamped_value(key_bytes, value_prefix_bytes, cf_name);
    match result {
        Ok(_) => KV_FUNCTION_SUCCESS, // Success
        Err(_) => KV_FUNCTION_FAILURE, // Error
    }
}

/// Get the result of a commit future that returns generated keys and values
#[no_mangle]
pub extern "C" fn kv_future_get_commit_result(future: KvFutureHandle) -> KvCommitResult {
    if future.is_null() {
        return KvCommitResult {
            success: 0,
            error_code: KvErrorCode::Unknown as c_int,
            error_message: std::ptr::null_mut(),
            generated_keys: KvBinaryDataArray { data: std::ptr::null_mut(), count: 0 },
            generated_values: KvBinaryDataArray { data: std::ptr::null_mut(), count: 0 },
        };
    }
    
    let future_id = future as usize;
    let mut futures = FUTURES.lock();
    
    if let Some(boxed_future) = futures.remove(&future_id) {
        if let Ok(future_ptr) = boxed_future.downcast::<KvFuturePtr<CommitResult>>() {
            match future_ptr.take_commit_result() {
                Ok(commit_result) => {
                    let generated_keys = create_binary_data_array(commit_result.generated_keys);
                    let generated_values = create_binary_data_array(commit_result.generated_values);
                    
                    return KvCommitResult {
                        success: 1,
                        error_code: 0,
                        error_message: std::ptr::null_mut(),
                        generated_keys,
                        generated_values,
                    };
                },
                Err(e) => {
                    let error_code = KvErrorCode::from(&e) as c_int;
                    let error_message = CString::new(format!("{:?}", e))
                        .unwrap_or_else(|_| CString::new("Invalid error message").unwrap())
                        .into_raw();
                        
                    return KvCommitResult {
                        success: 0,
                        error_code,
                        error_message,
                        generated_keys: KvBinaryDataArray { data: std::ptr::null_mut(), count: 0 },
                        generated_values: KvBinaryDataArray { data: std::ptr::null_mut(), count: 0 },
                    };
                },
            }
        }
    }
    
    // Fallback error case
    let error_message = CString::new("Future not ready or invalid type")
        .unwrap_or_else(|_| CString::new("Invalid error message").unwrap())
        .into_raw();
    KvCommitResult {
        success: 0,
        error_code: KvErrorCode::Unknown as c_int,
        error_message,
        generated_keys: KvBinaryDataArray { data: std::ptr::null_mut(), count: 0 },
        generated_values: KvBinaryDataArray { data: std::ptr::null_mut(), count: 0 },
    }
}

/// Helper function to create KvBinaryDataArray from Vec<Vec<u8>>
fn create_binary_data_array(data: Vec<Vec<u8>>) -> KvBinaryDataArray {
    if data.is_empty() {
        return KvBinaryDataArray { data: std::ptr::null_mut(), count: 0 };
    }
    
    let count = data.len();
    let array = unsafe {
        let ptr = libc::malloc(count * size_of::<KvBinaryData>()) as *mut KvBinaryData;
        if ptr.is_null() {
            return KvBinaryDataArray { data: std::ptr::null_mut(), count: 0 };
        }
        
        for (i, item) in data.into_iter().enumerate() {
            let binary_data = create_binary_data(item);
            std::ptr::write(ptr.add(i), binary_data);
        }
        
        ptr
    };
    
    KvBinaryDataArray { data: array, count }
}

/// Helper function to create KvBinaryData from Vec<u8>
fn create_binary_data(data: Vec<u8>) -> KvBinaryData {
    if data.is_empty() {
        return KvBinaryData { data: std::ptr::null_mut(), length: 0 };
    }
    
    let length = data.len() as c_int;
    let ptr = unsafe {
        let ptr = libc::malloc(length as size_t) as *mut u8;
        if !ptr.is_null() {
            std::ptr::copy_nonoverlapping(data.as_ptr(), ptr, data.len());
        }
        ptr
    };
    
    KvBinaryData { data: ptr, length }
}

/// Free binary data array
#[no_mangle]
pub extern "C" fn kv_binary_data_array_free(array: *mut KvBinaryDataArray) {
    if array.is_null() {
        return;
    }
    
    unsafe {
        let array_ref = &mut *array;
        if !array_ref.data.is_null() {
            for i in 0..array_ref.count {
                let binary_data = &mut *array_ref.data.add(i);
                if !binary_data.data.is_null() {
                    libc::free(binary_data.data as *mut libc::c_void);
                    binary_data.data = std::ptr::null_mut();
                }
                binary_data.length = 0;
            }
            libc::free(array_ref.data as *mut libc::c_void);
            array_ref.data = std::ptr::null_mut();
        }
        array_ref.count = 0;
    }
}

/// Free commit result structure
#[no_mangle]
pub extern "C" fn kv_commit_result_free(result: *mut KvCommitResult) {
    if result.is_null() {
        return;
    }
    
    unsafe {
        let result_ref = &mut *result;
        
        // Free error message if present
        if !result_ref.error_message.is_null() {
            let _ = CString::from_raw(result_ref.error_message);
            result_ref.error_message = std::ptr::null_mut();
        }
        
        // Free generated keys array
        kv_binary_data_array_free(&mut result_ref.generated_keys as *mut KvBinaryDataArray);
        
        // Free generated values array
        kv_binary_data_array_free(&mut result_ref.generated_values as *mut KvBinaryDataArray);
        
        result_ref.success = 0;
        result_ref.error_code = 0;
    }
}

/// Abort a transaction
#[no_mangle]
pub extern "C" fn kv_transaction_abort(transaction: KvTransactionHandle) -> KvFutureHandle {
    if transaction.is_null() {
        return ptr::null_mut();
    }
    
    let tx_id = transaction as usize;
    let tx = match TRANSACTIONS.lock().remove(&tx_id) {
        Some(tx_arc) => match Arc::try_unwrap(tx_arc) {
            Ok(tx_mutex) => match tx_mutex.into_inner() {
                tx => tx,
            },
            Err(tx_arc) => {
                // Put it back if we can't unwrap the Arc
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
    CONFIGS.lock().clear();
}