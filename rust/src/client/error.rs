use std::fmt;

#[derive(Debug, Clone)]
pub enum KvError {
    TransportError(String),
    ProtocolError(String),
    TransactionNotFound(String),
    TransactionConflict(String),
    TransactionTimeout(String),
    InvalidKey(String),
    InvalidValue(String),
    InvalidArgument(String),
    NetworkError(String),
    ServerError(String),
    Unknown(String),
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::TransportError(msg) => write!(f, "Transport error: {}", msg),
            KvError::ProtocolError(msg) => write!(f, "Protocol error: {}", msg),
            KvError::TransactionNotFound(msg) => write!(f, "Transaction not found: {}", msg),
            KvError::TransactionConflict(msg) => write!(f, "Transaction conflict: {}", msg),
            KvError::TransactionTimeout(msg) => write!(f, "Transaction timeout: {}", msg),
            KvError::InvalidKey(msg) => write!(f, "Invalid key: {}", msg),
            KvError::InvalidValue(msg) => write!(f, "Invalid value: {}", msg),
            KvError::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            KvError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            KvError::ServerError(msg) => write!(f, "Server error: {}", msg),
            KvError::Unknown(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for KvError {}

impl From<thrift::Error> for KvError {
    fn from(err: thrift::Error) -> Self {
        match err {
            thrift::Error::Transport(e) => KvError::TransportError(e.to_string()),
            thrift::Error::Protocol(e) => KvError::ProtocolError(e.to_string()),
            thrift::Error::Application(e) => KvError::ServerError(e.to_string()),
            _ => KvError::Unknown(err.to_string()),
        }
    }
}

pub type KvResult<T> = Result<T, KvError>;

// C-compatible error codes
#[repr(i32)]
#[derive(Debug, Clone, Copy)]
pub enum KvErrorCode {
    Success = 0,
    TransportError = 1000,
    ProtocolError = 1001,
    TransactionNotFound = 2000,
    TransactionConflict = 2001,
    TransactionTimeout = 2002,
    InvalidKey = 3000,
    InvalidValue = 3001,
    InvalidArgument = 3002,
    NetworkError = 4000,
    ServerError = 5000,
    Unknown = 9999,
}

impl From<&KvError> for KvErrorCode {
    fn from(err: &KvError) -> Self {
        match err {
            KvError::TransportError(_) => KvErrorCode::TransportError,
            KvError::ProtocolError(_) => KvErrorCode::ProtocolError,
            KvError::TransactionNotFound(_) => KvErrorCode::TransactionNotFound,
            KvError::TransactionConflict(_) => KvErrorCode::TransactionConflict,
            KvError::TransactionTimeout(_) => KvErrorCode::TransactionTimeout,
            KvError::InvalidKey(_) => KvErrorCode::InvalidKey,
            KvError::InvalidValue(_) => KvErrorCode::InvalidValue,
            KvError::InvalidArgument(_) => KvErrorCode::InvalidArgument,
            KvError::NetworkError(_) => KvErrorCode::NetworkError,
            KvError::ServerError(_) => KvErrorCode::ServerError,
            KvError::Unknown(_) => KvErrorCode::Unknown,
        }
    }
}