use std::fmt;

/// Errors that can occur during routing operations in a distributed system
#[derive(Debug, Clone)]
pub enum RoutingError {
    /// Database operation failed with an error message
    DatabaseError(String),
    /// This node is not the leader for write operations
    NotLeader {
        leader: Option<String>,
        leader_endpoint: Option<String>,
        leader_id: Option<u32>,
    },
    /// No leader is currently available in the cluster
    NoLeaderAvailable,
    /// Operation timed out
    Timeout,
    /// Node is unavailable
    NodeUnavailable,
    /// Invalid operation for current state
    InvalidOperation(String),
    /// Sequence error for state machine operations
    SequenceError(String),
}

impl fmt::Display for RoutingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RoutingError::DatabaseError(msg) => write!(f, "Database error: {}", msg),
            RoutingError::NotLeader { leader, leader_endpoint, leader_id } => {
                match (leader_endpoint, leader_id) {
                    (Some(endpoint), Some(id)) => write!(f, "Not leader, try leader at: {} (node {})", endpoint, id),
                    (Some(endpoint), None) => write!(f, "Not leader, try leader at: {}", endpoint),
                    (None, Some(id)) => write!(f, "Not leader, leader is node {}", id),
                    _ => {
                        if let Some(leader_addr) = leader {
                            write!(f, "Not leader, try leader at: {}", leader_addr)
                        } else {
                            write!(f, "Not leader, no known leader")
                        }
                    }
                }
            }
            RoutingError::NoLeaderAvailable => write!(f, "No leader available in cluster"),
            RoutingError::Timeout => write!(f, "Operation timed out"),
            RoutingError::NodeUnavailable => write!(f, "Node unavailable"),
            RoutingError::InvalidOperation(msg) => write!(f, "Invalid operation: {}", msg),
            RoutingError::SequenceError(msg) => write!(f, "Sequence error: {}", msg),
        }
    }
}

impl std::error::Error for RoutingError {}

/// Result type for routing operations
pub type RoutingResult<T> = Result<T, RoutingError>;