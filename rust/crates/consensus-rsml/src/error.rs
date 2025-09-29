//! RSML-specific error types
//!
//! This module defines error types that preserve RSML-specific error details
//! while being compatible with the consensus-api error system.

use consensus_api::ConsensusError;
use serde::{Deserialize, Serialize};
use std::fmt;

pub type RsmlResult<T> = Result<T, RsmlError>;

/// RSML-specific error types that preserve detailed error information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RsmlError {
    /// Configuration validation errors
    ConfigurationError {
        field: String,
        message: String,
    },
    /// Network transport errors
    NetworkError {
        node_id: Option<String>,
        message: String,
    },
    /// Consensus protocol errors
    ConsensusError {
        view: Option<u64>,
        slot: Option<u64>,
        message: String,
    },
    /// Storage/persistence errors
    StorageError {
        operation: String,
        message: String,
    },
    /// State machine errors
    StateMachineError {
        operation: String,
        message: String,
    },
    /// Leadership/view change errors
    LeadershipError {
        current_leader: Option<String>,
        attempted_view: Option<u64>,
        message: String,
    },
    /// Timeout errors
    TimeoutError {
        operation: String,
        timeout_ms: u64,
    },
    /// Feature not available (disabled by feature flags)
    FeatureUnavailable {
        feature: String,
        message: String,
    },
    /// Internal RSML library errors
    InternalError {
        component: String,
        message: String,
    },
}

impl fmt::Display for RsmlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RsmlError::ConfigurationError { field, message } => {
                write!(f, "Configuration error in field '{}': {}", field, message)
            }
            RsmlError::NetworkError { node_id, message } => {
                if let Some(node) = node_id {
                    write!(f, "Network error with node '{}': {}", node, message)
                } else {
                    write!(f, "Network error: {}", message)
                }
            }
            RsmlError::ConsensusError { view, slot, message } => {
                let view_str = view.map(|v| format!("view {}", v)).unwrap_or_else(|| "unknown view".to_string());
                let slot_str = slot.map(|s| format!("slot {}", s)).unwrap_or_else(|| "unknown slot".to_string());
                write!(f, "Consensus error ({}, {}): {}", view_str, slot_str, message)
            }
            RsmlError::StorageError { operation, message } => {
                write!(f, "Storage error during '{}': {}", operation, message)
            }
            RsmlError::StateMachineError { operation, message } => {
                write!(f, "State machine error during '{}': {}", operation, message)
            }
            RsmlError::LeadershipError { current_leader, attempted_view, message } => {
                let leader_str = current_leader.as_deref().unwrap_or("unknown");
                let view_str = attempted_view.map(|v| format!(" (attempted view {})", v)).unwrap_or_default();
                write!(f, "Leadership error with leader '{}'{}: {}", leader_str, view_str, message)
            }
            RsmlError::TimeoutError { operation, timeout_ms } => {
                write!(f, "Timeout error during '{}' after {}ms", operation, timeout_ms)
            }
            RsmlError::FeatureUnavailable { feature, message } => {
                write!(f, "Feature '{}' unavailable: {}", feature, message)
            }
            RsmlError::InternalError { component, message } => {
                write!(f, "Internal error in component '{}': {}", component, message)
            }
        }
    }
}

impl std::error::Error for RsmlError {}

/// Convert RsmlError to the generic ConsensusError API
impl From<RsmlError> for ConsensusError {
    fn from(error: RsmlError) -> Self {
        ConsensusError::Other {
            message: error.to_string(),
        }
    }
}

/// Convert generic ConsensusError to RsmlError
impl From<ConsensusError> for RsmlError {
    fn from(error: ConsensusError) -> Self {
        match error {
            ConsensusError::SerializationError { message } => {
                RsmlError::InternalError {
                    component: "serialization".to_string(),
                    message,
                }
            }
            ConsensusError::TransportError(message) => {
                RsmlError::NetworkError {
                    node_id: None,
                    message,
                }
            }
            ConsensusError::Other { message } => {
                RsmlError::InternalError {
                    component: "unknown".to_string(),
                    message,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let config_error = RsmlError::ConfigurationError {
            field: "node_id".to_string(),
            message: "cannot be empty".to_string(),
        };
        assert_eq!(
            config_error.to_string(),
            "Configuration error in field 'node_id': cannot be empty"
        );

        let timeout_error = RsmlError::TimeoutError {
            operation: "consensus proposal".to_string(),
            timeout_ms: 5000,
        };
        assert_eq!(
            timeout_error.to_string(),
            "Timeout error during 'consensus proposal' after 5000ms"
        );
    }

    #[test]
    fn test_error_conversion() {
        let rsml_error = RsmlError::NetworkError {
            node_id: Some("node-1".to_string()),
            message: "connection refused".to_string(),
        };

        let consensus_error: ConsensusError = rsml_error.clone().into();
        match &consensus_error {
            ConsensusError::Other { message } => {
                assert!(message.contains("Network error with node 'node-1'"));
            }
            _ => panic!("Expected Other error"),
        }

        let back_to_rsml: RsmlError = consensus_error.into();
        match back_to_rsml {
            RsmlError::InternalError { component, message } => {
                assert_eq!(component, "unknown");
                assert!(message.contains("Network error"));
            }
            _ => panic!("Expected InternalError"),
        }
    }
}