// For now, let's use a simpler approach and just import what we need
// We'll use the consensus types from the main crate when available

// Minimal Thrift type definitions for consensus
#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub term: i64,
    pub index: i64,
    pub data: Vec<u8>,
    pub entry_type: String,
}

impl LogEntry {
    pub fn new(term: i64, index: i64, data: Vec<u8>, entry_type: String) -> Self {
        Self { term, index, data, entry_type }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppendEntriesRequest {
    pub term: i64,
    pub leader_id: i32,
    pub prev_log_index: i64,
    pub prev_log_term: i64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: i64,
}

impl AppendEntriesRequest {
    pub fn new(
        term: i64,
        leader_id: i32,
        prev_log_index: i64,
        prev_log_term: i64,
        entries: Vec<LogEntry>,
        leader_commit: i64,
    ) -> Self {
        Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AppendEntriesResponse {
    pub term: i64,
    pub success: bool,
    pub last_log_index: Option<i64>,
    pub error: Option<String>,
}

impl AppendEntriesResponse {
    pub fn new(
        term: i64,
        success: bool,
        last_log_index: Option<i64>,
        error: Option<String>,
    ) -> Self {
        Self {
            term,
            success,
            last_log_index,
            error,
        }
    }
}

// Mock client trait for testing
pub trait TConsensusServiceSyncClient {
    fn append_entries(&mut self, request: AppendEntriesRequest) -> Result<AppendEntriesResponse, thrift::Error>;
}

// Mock client implementation for testing
pub struct MockConsensusServiceSyncClient {
    pub responses: std::collections::VecDeque<Result<AppendEntriesResponse, thrift::Error>>,
}

impl MockConsensusServiceSyncClient {
    pub fn new() -> Self {
        Self {
            responses: std::collections::VecDeque::new(),
        }
    }

    pub fn add_response(&mut self, response: Result<AppendEntriesResponse, thrift::Error>) {
        self.responses.push_back(response);
    }
}

impl TConsensusServiceSyncClient for MockConsensusServiceSyncClient {
    fn append_entries(&mut self, _request: AppendEntriesRequest) -> Result<AppendEntriesResponse, thrift::Error> {
        self.responses.pop_front().unwrap_or_else(|| {
            Ok(AppendEntriesResponse::new(0, true, Some(0), None))
        })
    }
}