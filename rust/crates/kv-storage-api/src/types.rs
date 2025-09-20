#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetResult {
    pub value: Vec<u8>,
    pub found: bool,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct KeyValue {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GetRangeResult {
    pub key_values: Vec<KeyValue>,
    pub success: bool,
    pub error: String,
    pub has_more: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OpResult {
    pub success: bool,
    pub error: String,
    pub error_code: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WriteOperation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FaultInjectionConfig {
    pub fault_type: String,
    pub probability: f64,
    pub duration_ms: i32,
    pub target_operation: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AtomicOperation {
    pub op_type: String, // "set" or "delete"
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>, // Only for "set" operations
    pub column_family: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AtomicCommitRequest {
    pub read_version: u64,
    pub operations: Vec<AtomicOperation>,
    pub read_conflict_keys: Vec<Vec<u8>>,
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AtomicCommitResult {
    pub success: bool,
    pub error: String,
    pub error_code: Option<String>,
    pub committed_version: Option<u64>,
    pub generated_keys: Vec<Vec<u8>>, // Keys generated for versionstamped operations
    pub generated_values: Vec<Vec<u8>>, // Values generated for versionstamped operations
}

impl WriteOperation {
    /// Serialize operation for consensus transmission
    pub fn serialize(&self) -> Result<Vec<u8>, bincode::Error> {
        bincode::serialize(self)
    }

    /// Deserialize operation from consensus
    pub fn deserialize(data: &[u8]) -> Result<Self, bincode::Error> {
        bincode::deserialize(data)
    }
}
