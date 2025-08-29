use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{debug, info, trace, warn};

/// Configuration for KV store client debugging and logging
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// Enable verbose debug logging
    pub debug_mode: bool,
    /// Connection timeout in seconds
    pub connection_timeout: u64,
    /// Request timeout in seconds
    pub request_timeout: u64,
    /// Maximum number of retries for failed requests
    pub max_retries: u32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            debug_mode: false,
            connection_timeout: 30,
            request_timeout: 10,
            max_retries: 3,
        }
    }
}

impl ClientConfig {
    /// Create a new configuration with debugging enabled
    pub fn with_debug() -> Self {
        Self {
            debug_mode: true,
            ..Default::default()
        }
    }

    /// Enable debug mode
    pub fn enable_debug(mut self) -> Self {
        self.debug_mode = true;
        self
    }

    /// Set connection timeout
    pub fn with_connection_timeout(mut self, timeout: u64) -> Self {
        self.connection_timeout = timeout;
        self
    }

    /// Set request timeout
    pub fn with_request_timeout(mut self, timeout: u64) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set maximum retries
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }
}

/// Global debug state for the client
static DEBUG_ENABLED: AtomicBool = AtomicBool::new(false);

/// Initialize debugging for the client
pub fn init_debug_logging() {
    if DEBUG_ENABLED.load(Ordering::Relaxed) {
        return;
    }

    // Initialize tracing subscriber for debug output
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .init();

    DEBUG_ENABLED.store(true, Ordering::Relaxed);
    info!("KV Store client debug logging initialized");
}

/// Check if debug mode is enabled
pub fn is_debug_enabled() -> bool {
    DEBUG_ENABLED.load(Ordering::Relaxed)
}

/// Log debug information if debug mode is enabled
pub fn debug_log(msg: &str) {
    if is_debug_enabled() {
        debug!("{}", msg);
    }
}

/// Log trace information if debug mode is enabled
pub fn trace_log(msg: &str) {
    if is_debug_enabled() {
        trace!("{}", msg);
    }
}

/// Log connection events
pub fn log_connection_event(event: &str, address: &str) {
    if is_debug_enabled() {
        info!("Connection event: {} - Address: {}", event, address);
    }
}

/// Log transaction events
pub fn log_transaction_event(event: &str, tx_id: Option<&str>) {
    if is_debug_enabled() {
        match tx_id {
            Some(id) => info!("Transaction event: {} - TX ID: {}", event, id),
            None => info!("Transaction event: {}", event),
        }
    }
}

/// Log operation timing
pub fn log_operation_timing(operation: &str, duration_ms: u64) {
    if is_debug_enabled() {
        info!("Operation '{}' completed in {}ms", operation, duration_ms);
    }
}

/// Log errors with context
pub fn log_error(context: &str, error: &str) {
    if is_debug_enabled() {
        warn!("Error in {}: {}", context, error);
    }
}

/// Log network operations
pub fn log_network_operation(operation: &str, bytes: Option<usize>) {
    if is_debug_enabled() {
        match bytes {
            Some(size) => debug!("Network operation: {} ({} bytes)", operation, size),
            None => debug!("Network operation: {}", operation),
        }
    }
}