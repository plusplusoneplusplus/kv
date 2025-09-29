use async_trait::async_trait;
use kv_storage_api::KvDatabase;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use crate::lib::kv_state_machine::{ConsensusKvDatabase, KvStateMachine};
use crate::lib::replication::executor::KvStoreExecutor;
use crate::lib::consensus_plugin::{ConsensusNodeRole, ConsensusPlugin};
use crate::lib::consensus_plugin_mock::MockConsensusPlugin;

/// Simple consensus setup that handles both database and server creation
pub struct ConsensusSetup {
    pub database: Arc<ConsensusKvDatabase>,
    pub server_handle: Option<ConsensusServerHandle>,
}

pub struct ConsensusServerHandle {
    pub port: u16,
    pub handle: std::thread::JoinHandle<()>,
}

/// Clean, single-purpose consensus factory
#[async_trait]
pub trait ConsensusFactory: Send + Sync {
    /// Create consensus database and server for the given node configuration
    /// Returns None for server_handle in single-node mode
    async fn create_consensus(
        &self,
        node_id: u32,
        endpoints: Option<Vec<String>>, // None = single node, Some = multi-node cluster
        database: Arc<dyn KvDatabase>,
    ) -> Result<ConsensusSetup, String>;
}

pub struct DefaultConsensusFactory {
    plugin: Box<dyn ConsensusPlugin>,
}

impl DefaultConsensusFactory {
    pub fn new() -> Self {
        Self { plugin: Box::new(MockConsensusPlugin::new()) }
    }

    pub fn with_plugin(plugin: Box<dyn ConsensusPlugin>) -> Self {
        Self { plugin }
    }
}

#[async_trait]
impl ConsensusFactory for DefaultConsensusFactory {
    async fn create_consensus(
        &self,
        node_id: u32,
        endpoints: Option<Vec<String>>,
        database: Arc<dyn KvDatabase>,
    ) -> Result<ConsensusSetup, String> {
        match endpoints {
            None => {
                // Single-node mode
                info!("Creating single-node consensus for node {}", node_id);
                let consensus_db = ConsensusKvDatabase::new_with_mock(
                    node_id.to_string(),
                    database,
                );
                Ok(ConsensusSetup {
                    database: Arc::new(consensus_db),
                    server_handle: None,
                })
            }
            Some(cluster_endpoints) => {
                // Multi-node mode
                info!("Creating multi-node consensus for node {} with {} peers", node_id, cluster_endpoints.len());

                if node_id >= cluster_endpoints.len() as u32 {
                    return Err(format!("Node ID {} out of range for cluster size {}", node_id, cluster_endpoints.len()));
                }

                let consensus_db = self.create_multi_node_consensus(node_id, &cluster_endpoints, database).await?;
                let server_handle = self.create_consensus_server(node_id, &cluster_endpoints, &consensus_db).await?;

                Ok(ConsensusSetup {
                    database: consensus_db,
                    server_handle: Some(server_handle),
                })
            }
        }
    }
}

impl DefaultConsensusFactory {
    async fn create_multi_node_consensus(
        &self,
        node_id: u32,
        endpoints: &[String],
        database: Arc<dyn KvDatabase>,
    ) -> Result<Arc<ConsensusKvDatabase>, String> {
        // Use the plugin's network to construct the transport
        let network = self.plugin.network();

        // Convert endpoints to consensus endpoints (port - 2000)
        let consensus_endpoints: Result<HashMap<String, String>, String> = endpoints
            .iter()
            .enumerate()
            .map(|(i, endpoint)| {
                let consensus_endpoint = convert_to_consensus_port(endpoint)?;
                Ok((i.to_string(), consensus_endpoint))
            })
            .collect();

        let consensus_endpoints = consensus_endpoints?;

        // Create transport via network impl
        let transport = network.build_transport(node_id.to_string(), consensus_endpoints.clone());

        // Create state machine
        let executor = Arc::new(KvStoreExecutor::new(database.clone()));
        let state_machine = Box::new(KvStateMachine::new(executor));

        // Create consensus engine via plugin
        let role = if node_id == 0 { ConsensusNodeRole::Leader } else { ConsensusNodeRole::Follower };
        let consensus_engine = self
            .plugin
            .build_engine(node_id.to_string(), state_machine, Arc::clone(&transport), role);

        let consensus_db = ConsensusKvDatabase::new(
            consensus_engine,
            database,
        );

        // Configure cluster membership without holding a non-Send guard across .await
        {
            let engine_ref = consensus_db.consensus_engine();
            let rt = tokio::runtime::Runtime::new()
                .map_err(|e| format!("Failed to create runtime for membership config: {}", e))?;

            for (other_node_id, endpoint) in &consensus_endpoints {
                if other_node_id.parse::<u32>().unwrap_or(u32::MAX) != node_id {
                    let mut engine = engine_ref.write();
                    match rt.block_on(engine.add_node(other_node_id.clone(), endpoint.clone())) {
                        Ok(()) => {
                            info!("Added node {} at {} to cluster", other_node_id, endpoint);
                        }
                        Err(e) => {
                            tracing::warn!("Failed to add node {} to cluster: {}", other_node_id, e);
                        }
                    }
                }
            }
        }

        Ok(Arc::new(consensus_db))
    }

    async fn create_consensus_server(
        &self,
        node_id: u32,
        endpoints: &[String],
        consensus_db: &ConsensusKvDatabase,
    ) -> Result<ConsensusServerHandle, String> {
        // Delegate to the plugin's network to spawn the RPC server
        let network = self.plugin.network();

        // Get this node's consensus port
        if node_id >= endpoints.len() as u32 {
            return Err(format!("Node ID {} out of range", node_id));
        }

        let my_endpoint = &endpoints[node_id as usize];
        let consensus_endpoint = convert_to_consensus_port(my_endpoint)?;
        let port = extract_port(&consensus_endpoint)?;

        info!("Starting consensus server on port {}", port);
        let handle = network
            .spawn_server(port, node_id, consensus_db.consensus_engine().clone())
            .map_err(|e| format!("Failed to start consensus server: {}", e))?;

        Ok(ConsensusServerHandle { port, handle })
    }
}

/// Convert replica endpoint to consensus endpoint (port - 2000)
fn convert_to_consensus_port(endpoint: &str) -> Result<String, String> {
    let parts: Vec<&str> = endpoint.split(':').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid endpoint format: {}", endpoint));
    }

    let host = parts[0];
    let port: u16 = parts[1].parse()
        .map_err(|e| format!("Invalid port: {}", e))?;

    if port < 2000 {
        return Err(format!("Port {} too low for consensus conversion", port));
    }

    Ok(format!("{}:{}", host, port - 2000))
}

fn extract_port(endpoint: &str) -> Result<u16, String> {
    let parts: Vec<&str> = endpoint.split(':').collect();
    if parts.len() != 2 {
        return Err(format!("Invalid endpoint format: {}", endpoint));
    }

    parts[1].parse()
        .map_err(|e| format!("Invalid port: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_to_consensus_port() {
        assert_eq!(convert_to_consensus_port("localhost:9090").unwrap(), "localhost:7090");
        assert_eq!(convert_to_consensus_port("127.0.0.1:9091").unwrap(), "127.0.0.1:7091");

        // Error cases
        assert!(convert_to_consensus_port("invalid").is_err());
        assert!(convert_to_consensus_port("localhost:1000").is_err()); // Too low
        assert!(convert_to_consensus_port("localhost:abc").is_err());
    }

    #[test]
    fn test_extract_port() {
        assert_eq!(extract_port("localhost:9090").unwrap(), 9090);
        assert_eq!(extract_port("127.0.0.1:7091").unwrap(), 7091);

        assert!(extract_port("invalid").is_err());
        assert!(extract_port("localhost:abc").is_err());
    }
}
