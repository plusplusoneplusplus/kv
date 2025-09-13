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
    if tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .try_init()
        .is_ok() {
        DEBUG_ENABLED.store(true, Ordering::Relaxed);
        info!("KV Store client debug logging initialized");
    } else {
        // Tracing was already initialized, just mark debug as enabled
        DEBUG_ENABLED.store(true, Ordering::Relaxed);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_client_config_default() {
        let config = ClientConfig::default();
        assert!(!config.debug_mode);
        assert_eq!(config.connection_timeout, 30);
        assert_eq!(config.request_timeout, 10);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_client_config_with_debug() {
        let config = ClientConfig::with_debug();
        assert!(config.debug_mode);
        assert_eq!(config.connection_timeout, 30);
        assert_eq!(config.request_timeout, 10);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_client_config_builder_pattern() {
        let config = ClientConfig::default()
            .enable_debug()
            .with_connection_timeout(60)
            .with_request_timeout(20)
            .with_max_retries(5);

        assert!(config.debug_mode);
        assert_eq!(config.connection_timeout, 60);
        assert_eq!(config.request_timeout, 20);
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_client_config_individual_setters() {
        let config = ClientConfig::default()
            .with_connection_timeout(45)
            .with_request_timeout(15)
            .with_max_retries(2);

        assert!(!config.debug_mode);
        assert_eq!(config.connection_timeout, 45);
        assert_eq!(config.request_timeout, 15);
        assert_eq!(config.max_retries, 2);
    }

    #[test]
    fn test_client_config_debug_format() {
        let config = ClientConfig::with_debug();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("debug_mode: true"));
        assert!(debug_str.contains("connection_timeout"));
        assert!(debug_str.contains("request_timeout"));
        assert!(debug_str.contains("max_retries"));
    }

    #[test]
    fn test_client_config_clone() {
        let original = ClientConfig::with_debug()
            .with_connection_timeout(100)
            .with_request_timeout(50)
            .with_max_retries(10);

        let cloned = original.clone();

        assert_eq!(original.debug_mode, cloned.debug_mode);
        assert_eq!(original.connection_timeout, cloned.connection_timeout);
        assert_eq!(original.request_timeout, cloned.request_timeout);
        assert_eq!(original.max_retries, cloned.max_retries);
    }

    #[test]
    fn test_debug_enabled_initial_state() {
        // Note: This test might be affected by other tests that enable debug
        // but we can at least verify the function works
        let _enabled = is_debug_enabled();
        // We can't assert a specific value since other tests might have enabled it
        // Just verify the function is callable and returns a bool
    }

    #[test]
    fn test_init_debug_logging_multiple_calls() {
        // Test that multiple calls to init_debug_logging don't panic
        init_debug_logging();
        init_debug_logging();
        init_debug_logging();

        // After initialization, debug should be enabled
        assert!(is_debug_enabled());
    }

    #[test]
    fn test_logging_functions_with_debug_disabled() {
        // Temporarily disable debug for this test
        let original_state = DEBUG_ENABLED.load(Ordering::Relaxed);
        DEBUG_ENABLED.store(false, Ordering::Relaxed);

        // These should not panic even when debug is disabled
        debug_log("test debug message");
        trace_log("test trace message");
        log_connection_event("test connection", "localhost:9090");
        log_transaction_event("test transaction", Some("tx-123"));
        log_transaction_event("test transaction", None);
        log_operation_timing("test operation", 100);
        log_error("test context", "test error");
        log_network_operation("test network", Some(1024));
        log_network_operation("test network", None);

        // Restore original state
        DEBUG_ENABLED.store(original_state, Ordering::Relaxed);
    }

    #[test]
    fn test_logging_functions_with_debug_enabled() {
        // Ensure debug is enabled for this test
        DEBUG_ENABLED.store(true, Ordering::Relaxed);

        // These should not panic when debug is enabled
        debug_log("test debug message");
        trace_log("test trace message");
        log_connection_event("test connection", "localhost:9090");
        log_transaction_event("test transaction", Some("tx-123"));
        log_transaction_event("test transaction", None);
        log_operation_timing("test operation", 100);
        log_error("test context", "test error");
        log_network_operation("test network", Some(1024));
        log_network_operation("test network", None);

        // Verify debug is still enabled
        assert!(is_debug_enabled());
    }

    #[test]
    fn test_config_zero_values() {
        let config = ClientConfig::default()
            .with_connection_timeout(0)
            .with_request_timeout(0)
            .with_max_retries(0);

        assert_eq!(config.connection_timeout, 0);
        assert_eq!(config.request_timeout, 0);
        assert_eq!(config.max_retries, 0);
    }

    #[test]
    fn test_config_large_values() {
        let config = ClientConfig::default()
            .with_connection_timeout(u64::MAX)
            .with_request_timeout(u64::MAX)
            .with_max_retries(u32::MAX);

        assert_eq!(config.connection_timeout, u64::MAX);
        assert_eq!(config.request_timeout, u64::MAX);
        assert_eq!(config.max_retries, u32::MAX);
    }
}