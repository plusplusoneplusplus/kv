use std::sync::mpsc;

/// Tombstone marker for deleted keys - uses null bytes to avoid collision with user data
pub const TOMBSTONE_MARKER: &[u8] = b"\x00__TOMBSTONE__\x00";

#[derive(Debug)]
pub struct GetResult {
    pub value: Vec<u8>,
    pub found: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub struct GetRangeResult {
    pub key_values: Vec<KeyValue>,
    pub success: bool,
    pub error: String,
    pub has_more: bool,
}

#[derive(Debug)]
pub struct OpResult {
    pub success: bool,
    pub error: String,
    pub error_code: Option<String>,
}

#[derive(Debug)]
pub enum WriteOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug)]
pub struct WriteRequest {
    pub operation: WriteOperation,
    pub response_tx: mpsc::Sender<OpResult>,
}

#[derive(Debug, Clone)]
pub struct FaultInjectionConfig {
    pub fault_type: String,
    pub probability: f64,
    pub duration_ms: i32,
    pub target_operation: Option<String>,
}

// FoundationDB-style client-side transaction structures
#[derive(Debug, Clone)]
pub struct AtomicOperation {
    pub op_type: String,  // "set" or "delete"
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,  // Only for "set" operations
    pub column_family: Option<String>,
}

#[derive(Debug)]
pub struct AtomicCommitRequest {
    pub read_version: u64,
    pub operations: Vec<AtomicOperation>,
    pub read_conflict_keys: Vec<Vec<u8>>,
    pub timeout_seconds: u64,
}

#[derive(Debug)]
pub struct AtomicCommitResult {
    pub success: bool,
    pub error: String,
    pub error_code: Option<String>,
    pub committed_version: Option<u64>,
    pub generated_keys: Vec<Vec<u8>>,  // Keys generated for versionstamped operations
    pub generated_values: Vec<Vec<u8>>,  // Values generated for versionstamped operations
}

impl OpResult {
    pub fn success() -> Self {
        OpResult {
            success: true,
            error: String::new(),
            error_code: None,
        }
    }

    pub fn error(message: &str, code: Option<&str>) -> Self {
        OpResult {
            success: false,
            error: message.to_string(),
            error_code: code.map(|c| c.to_string()),
        }
    }

    pub fn from_result<T>(result: Result<T, &str>, error_code: Option<&str>) -> Self {
        match result {
            Ok(_) => Self::success(),
            Err(msg) => Self::error(msg, error_code),
        }
    }
}