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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use thrift::Error as ThriftError;
    use thrift::{TransportError, ProtocolError, ApplicationError, TransportErrorKind, ProtocolErrorKind};

    #[test]
    fn test_kv_error_display() {
        assert_eq!(
            KvError::TransportError("connection failed".to_string()).to_string(),
            "Transport error: connection failed"
        );
        assert_eq!(
            KvError::ProtocolError("invalid format".to_string()).to_string(),
            "Protocol error: invalid format"
        );
        assert_eq!(
            KvError::TransactionNotFound("tx-123".to_string()).to_string(),
            "Transaction not found: tx-123"
        );
        assert_eq!(
            KvError::TransactionConflict("key conflict".to_string()).to_string(),
            "Transaction conflict: key conflict"
        );
        assert_eq!(
            KvError::TransactionTimeout("expired".to_string()).to_string(),
            "Transaction timeout: expired"
        );
        assert_eq!(
            KvError::InvalidKey("empty key".to_string()).to_string(),
            "Invalid key: empty key"
        );
        assert_eq!(
            KvError::InvalidValue("null value".to_string()).to_string(),
            "Invalid value: null value"
        );
        assert_eq!(
            KvError::InvalidArgument("negative limit".to_string()).to_string(),
            "Invalid argument: negative limit"
        );
        assert_eq!(
            KvError::NetworkError("timeout".to_string()).to_string(),
            "Network error: timeout"
        );
        assert_eq!(
            KvError::ServerError("internal error".to_string()).to_string(),
            "Server error: internal error"
        );
        assert_eq!(
            KvError::Unknown("mysterious".to_string()).to_string(),
            "Unknown error: mysterious"
        );
    }

    #[test]
    fn test_kv_error_debug() {
        let error = KvError::TransportError("test".to_string());
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("TransportError"));
        assert!(debug_str.contains("test"));
    }

    #[test]
    fn test_kv_error_clone() {
        let original = KvError::TransactionConflict("conflict".to_string());
        let cloned = original.clone();
        assert_eq!(format!("{}", original), format!("{}", cloned));
    }

    #[test]
    fn test_from_thrift_transport_error() {
        let transport_error = ThriftError::Transport(TransportError::new(
            TransportErrorKind::Unknown, "transport failed"
        ));
        let kv_error: KvError = transport_error.into();

        match kv_error {
            KvError::TransportError(msg) => {
                // Just verify it's a transport error with some message
                assert!(!msg.is_empty());
            }
            _ => panic!("Expected TransportError"),
        }
    }

    #[test]
    fn test_from_thrift_protocol_error() {
        let protocol_error = ThriftError::Protocol(ProtocolError::new(
            ProtocolErrorKind::Unknown, "protocol failed"
        ));
        let kv_error: KvError = protocol_error.into();

        match kv_error {
            KvError::ProtocolError(msg) => {
                // Just verify it's a protocol error with some message
                assert!(!msg.is_empty());
            }
            _ => panic!("Expected ProtocolError"),
        }
    }

    #[test]
    fn test_from_thrift_application_error() {
        let app_error = ThriftError::Application(ApplicationError::new(
            thrift::ApplicationErrorKind::Unknown,
            "application failed"
        ));
        let kv_error: KvError = app_error.into();

        match kv_error {
            KvError::ServerError(msg) => {
                // Just verify it's a server error with some message
                assert!(!msg.is_empty());
            }
            _ => panic!("Expected ServerError"),
        }
    }

    #[test]
    fn test_error_code_conversion() {
        assert_eq!(KvErrorCode::from(&KvError::TransportError("".to_string())), KvErrorCode::TransportError);
        assert_eq!(KvErrorCode::from(&KvError::ProtocolError("".to_string())), KvErrorCode::ProtocolError);
        assert_eq!(KvErrorCode::from(&KvError::TransactionNotFound("".to_string())), KvErrorCode::TransactionNotFound);
        assert_eq!(KvErrorCode::from(&KvError::TransactionConflict("".to_string())), KvErrorCode::TransactionConflict);
        assert_eq!(KvErrorCode::from(&KvError::TransactionTimeout("".to_string())), KvErrorCode::TransactionTimeout);
        assert_eq!(KvErrorCode::from(&KvError::InvalidKey("".to_string())), KvErrorCode::InvalidKey);
        assert_eq!(KvErrorCode::from(&KvError::InvalidValue("".to_string())), KvErrorCode::InvalidValue);
        assert_eq!(KvErrorCode::from(&KvError::InvalidArgument("".to_string())), KvErrorCode::InvalidArgument);
        assert_eq!(KvErrorCode::from(&KvError::NetworkError("".to_string())), KvErrorCode::NetworkError);
        assert_eq!(KvErrorCode::from(&KvError::ServerError("".to_string())), KvErrorCode::ServerError);
        assert_eq!(KvErrorCode::from(&KvError::Unknown("".to_string())), KvErrorCode::Unknown);
    }

    #[test]
    fn test_error_code_values() {
        assert_eq!(KvErrorCode::Success as i32, 0);
        assert_eq!(KvErrorCode::TransportError as i32, 1000);
        assert_eq!(KvErrorCode::ProtocolError as i32, 1001);
        assert_eq!(KvErrorCode::TransactionNotFound as i32, 2000);
        assert_eq!(KvErrorCode::TransactionConflict as i32, 2001);
        assert_eq!(KvErrorCode::TransactionTimeout as i32, 2002);
        assert_eq!(KvErrorCode::InvalidKey as i32, 3000);
        assert_eq!(KvErrorCode::InvalidValue as i32, 3001);
        assert_eq!(KvErrorCode::InvalidArgument as i32, 3002);
        assert_eq!(KvErrorCode::NetworkError as i32, 4000);
        assert_eq!(KvErrorCode::ServerError as i32, 5000);
        assert_eq!(KvErrorCode::Unknown as i32, 9999);
    }

    #[test]
    fn test_error_code_debug() {
        let code = KvErrorCode::TransactionConflict;
        let debug_str = format!("{:?}", code);
        assert!(debug_str.contains("TransactionConflict"));
    }

    #[test]
    fn test_kv_result_type() {
        let success: KvResult<i32> = Ok(42);
        assert_eq!(success.unwrap(), 42);

        let failure: KvResult<i32> = Err(KvError::InvalidKey("test".to_string()));
        assert!(failure.is_err());
    }

    #[test]
    fn test_std_error_trait() {
        let error = KvError::NetworkError("connection timeout".to_string());

        // Test that it implements std::error::Error
        let _: &dyn std::error::Error = &error;

        // Test source() returns None (no nested errors)
        assert!(error.source().is_none());
    }
}