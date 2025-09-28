use async_trait::async_trait;
use consensus_api::{ConsensusError, ConsensusResult, Index, LogEntry, NodeId};
use consensus_mock::transport::{
    AppendEntryRequest, AppendEntryResponse, CommitNotificationRequest, CommitNotificationResponse, NetworkTransport,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TTcpChannel, TIoChannel};
use tracing::{debug, info};

use crate::generated::kvstore::{
    ConsensusServiceSyncClient, TConsensusServiceSyncClient,
    AppendEntriesRequest as GenAppendEntriesRequest,
    AppendEntriesResponse as GenAppendEntriesResponse,
    LogEntry as GenLogEntry,
};

#[derive(Clone, Debug)]
pub struct GeneratedThriftTransport {
    node_id: NodeId,
    endpoints: Arc<Mutex<HashMap<NodeId, String>>>,
}

impl GeneratedThriftTransport {
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id, endpoints: Arc::new(Mutex::new(HashMap::new())) }
    }

    pub async fn with_endpoints(node_id: NodeId, endpoints: HashMap<NodeId, String>) -> Self {
        let transport = Self::new(node_id);
        {
            let mut map = transport.endpoints.lock().await;
            map.extend(endpoints);
        }
        transport
    }

    fn convert_entry(entry: &LogEntry) -> GenLogEntry {
        GenLogEntry::new(entry.term as i64, entry.index as i64, entry.data.clone(), "operation".to_string())
    }

    fn convert_request(req: &AppendEntryRequest) -> GenAppendEntriesRequest {
        let gen_entry = Self::convert_entry(&req.entry);
        let leader_id_num: i32 = req.leader_id.parse().unwrap_or(0);
        GenAppendEntriesRequest::new(
            req.leader_term as i64,
            leader_id_num,
            req.prev_log_index as i64,
            req.prev_log_term as i64,
            vec![gen_entry],
            req.prev_log_index as i64, // use prev_log_index as leader_commit for mock
        )
    }

    fn convert_response(target: &NodeId, resp: &GenAppendEntriesResponse) -> AppendEntryResponse {
        AppendEntryResponse {
            node_id: target.clone(),
            success: resp.success,
            index: resp.last_log_index.unwrap_or(0) as Index,
            error: resp.error.clone(),
        }
    }
}

#[async_trait]
impl NetworkTransport for GeneratedThriftTransport {
    async fn send_append_entry(
        &self,
        target_node: &NodeId,
        request: AppendEntryRequest,
    ) -> ConsensusResult<AppendEntryResponse> {
        let endpoint = {
            let endpoints = self.endpoints.lock().await;
            endpoints.get(target_node).cloned().ok_or_else(|| {
                ConsensusError::TransportError(format!("No endpoint found for node {}", target_node))
            })?
        };

        // Parse endpoint
        let parts: Vec<&str> = endpoint.split(':').collect();
        if parts.len() != 2 {
            return Err(ConsensusError::TransportError(format!(
                "Invalid endpoint format for node {}: {}",
                target_node, endpoint
            )));
        }
        let host = parts[0].to_string();
        let port: u16 = parts[1].parse().map_err(|e| {
            ConsensusError::TransportError(format!("Invalid port in endpoint {}: {}", endpoint, e))
        })?;

        // Open TCP channel
        let mut tcp = TTcpChannel::new();
        tcp.open(&format!("{}:{}", host, port)).map_err(|e| {
            ConsensusError::TransportError(format!("Failed to connect to {}: {}", endpoint, e))
        })?;

        let (read_chan, write_chan) = tcp.split().map_err(|e| {
            ConsensusError::TransportError(format!("Failed to split TCP channel: {}", e))
        })?;

        let mut client = {
            let i = TBinaryInputProtocol::new(read_chan, true);
            let o = TBinaryOutputProtocol::new(write_chan, true);
            ConsensusServiceSyncClient::new(i, o)
        };

        let gen_req = Self::convert_request(&request);
        let gen_resp = client
            .append_entries(gen_req)
            .map_err(|e| ConsensusError::TransportError(format!("Thrift append_entries error: {}", e)))?;

        info!(
            "GeneratedThriftTransport: append_entries to {} at {} idx={} success={}",
            target_node,
            endpoint,
            request.entry.index,
            gen_resp.success
        );

        Ok(Self::convert_response(target_node, &gen_resp))
    }

    async fn send_commit_notification(
        &self,
        target_node: &NodeId,
        request: CommitNotificationRequest,
    ) -> ConsensusResult<CommitNotificationResponse> {
        // Simple reachability check
        if !self.is_node_reachable(target_node).await {
            return Err(ConsensusError::TransportError(format!(
                "Cannot reach target node {} for commit notification",
                target_node
            )));
        }
        debug!(
            "GeneratedThriftTransport: commit notification to {} commit_index={}",
            target_node, request.commit_index
        );
        Ok(CommitNotificationResponse { success: true })
    }

    fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    fn node_endpoints(&self) -> &HashMap<NodeId, String> {
        use std::sync::LazyLock;
        static EMPTY: LazyLock<HashMap<NodeId, String>> = LazyLock::new(|| HashMap::new());
        &EMPTY
    }

    async fn update_node_endpoint(&mut self, node_id: NodeId, endpoint: String) -> ConsensusResult<()> {
        let mut map = self.endpoints.lock().await;
        map.insert(node_id, endpoint);
        Ok(())
    }

    async fn remove_node_endpoint(&mut self, node_id: &NodeId) -> ConsensusResult<()> {
        let mut map = self.endpoints.lock().await;
        map.remove(node_id);
        Ok(())
    }

    async fn is_node_reachable(&self, node_id: &NodeId) -> bool {
        let endpoint = {
            let map = self.endpoints.lock().await;
            if let Some(ep) = map.get(node_id) { ep.clone() } else { return false; }
        };
        let parts: Vec<&str> = endpoint.split(':').collect();
        if parts.len() != 2 { return false; }
        let host = parts[0].to_string();
        let port: u16 = match parts[1].parse() { Ok(p) => p, Err(_) => return false };

        let mut tcp = TTcpChannel::new();
        tcp.open(&format!("{}:{}", host, port)).is_ok()
    }
}
