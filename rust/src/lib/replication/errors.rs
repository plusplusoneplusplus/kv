use std::fmt;

/// Errors that can occur during routing operations in a distributed system
#[derive(Debug, Clone)]
pub enum RoutingError {
    /// Database operation failed with an error message
    DatabaseError(String),
    /// This node is not the leader for write operations
    NotLeader {
        leader: Option<String>,
    },
    /// Operation timed out
    Timeout,
    /// Node is unavailable
    NodeUnavailable,
    /// Invalid operation for current state
    InvalidOperation(String),
}

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoutingError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            RoutingError::NotLeader { leader } => {
                if let Some(leader_addr) = leader {
                    write!(f, "Not leader, try leader at: {}", leader_addr)
                } else {
                    write!(f, "Not leader, no known leader")
                }
            }
            RoutingError::Timeout => write!(f, "Operation timed out"),
            RoutingError::NodeUnavailable => write!(f, "Node unavailable"),
            RoutingError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
        }
    }
}

impl std::error::Error for RoutingError {}

/// Result type for routing operations
pub type RoutingResult<T> = Result<T, RoutingError>;