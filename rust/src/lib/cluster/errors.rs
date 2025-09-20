use std::fmt;

#[derive(Debug)]
pub enum ClusterError {
    NodeNotFound(u32),
    NoLeaderFound,
    NetworkError(String),
    ConfigurationError(String),
    HealthCheckFailed(String),
    TimeoutError(String),
}

pub type ClusterResult<T> = Result<T, ClusterError>;

impl fmt::Display for ClusterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClusterError::NodeNotFound(id) => write!(f, "Node {} not found", id),
            ClusterError::NoLeaderFound => write!(f, "No leader found in cluster"),
            ClusterError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            ClusterError::ConfigurationError(msg) => write!(f, "Configuration error: {}", msg),
            ClusterError::HealthCheckFailed(msg) => write!(f, "Health check failed: {}", msg),
            ClusterError::TimeoutError(msg) => write!(f, "Timeout error: {}", msg),
        }
    }
}

impl std::error::Error for ClusterError {}