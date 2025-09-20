use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub id: u32,
    pub endpoint: String,
    pub is_leader: bool,
    pub last_seen: SystemTime,
    pub status: NodeStatus,
    pub term: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum NodeStatus {
    Healthy,
    Degraded,
    Unreachable,
    Unknown,
}

impl NodeInfo {
    pub fn new(id: u32, endpoint: String) -> Self {
        Self {
            id,
            endpoint,
            is_leader: false,
            last_seen: SystemTime::now(),
            status: NodeStatus::Unknown,
            term: 0,
        }
    }

    pub fn update_status(&mut self, status: NodeStatus) {
        self.status = status;
        self.last_seen = SystemTime::now();
    }

    pub fn update_leader_status(&mut self, is_leader: bool, term: u64) {
        self.is_leader = is_leader;
        self.term = term;
        self.last_seen = SystemTime::now();
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.status, NodeStatus::Healthy)
    }

    pub fn time_since_last_seen(&self) -> Result<std::time::Duration, std::time::SystemTimeError> {
        SystemTime::now().duration_since(self.last_seen)
    }
}