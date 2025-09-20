use serde::{Deserialize, Serialize};
use std::fmt;

pub type ConsensusResult<T> = Result<T, ConsensusError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusError {
    SerializationError {
        message: String,
    },
    Other {
        message: String,
    },
}

impl fmt::Display for ConsensusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConsensusError::SerializationError { message } => {
                write!(f, "Serialization error: {}", message)
            }
            ConsensusError::Other { message } => {
                write!(f, "Other error: {}", message)
            }
        }
    }
}

impl std::error::Error for ConsensusError {}