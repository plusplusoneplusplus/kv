use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use rocksdb_server::lib::consensus_plugin::{ConsensusNodeRole, ConsensusPlugin};
use rocksdb_server::lib::consensus_plugin_mock::MockConsensusPlugin;
use rocksdb_server::lib::consensus_network_thrift::ThriftConsensusNetwork;
use rocksdb_server::lib::kv_state_machine::KvStateMachine;
use rocksdb_server::lib::replication::KvStoreExecutor;
use kv_storage_api::KvDatabase;

#[tokio::test(flavor = "multi_thread")]
async fn test_plugin_builds_engine_and_spawns_server() {
    // Build endpoints for two nodes (addresses unused here)
    let mut eps = HashMap::new();
    eps.insert("0".to_string(), "127.0.0.1:0".to_string());
    eps.insert("1".to_string(), "127.0.0.1:0".to_string());

    // Create plugin and network
    let plugin = MockConsensusPlugin::new();
    let network = plugin.network();

    // Build transport for node 0
    let transport = network.build_transport("0".to_string(), eps);

    // Build a simple KV state machine
    let db = Arc::new(kv_storage_mockdb::MockDatabase::new()) as Arc<dyn KvDatabase>;
    let executor = Arc::new(KvStoreExecutor::new(db));
    let state_machine = Box::new(KvStateMachine::new(executor));

    // Build engine via plugin (leader role)
    let engine = plugin.build_engine(
        "0".to_string(),
        state_machine,
        Arc::clone(&transport),
        ConsensusNodeRole::Leader,
    );

    // Spawn consensus RPC server on ephemeral port 0
    // We don't join the handle; server exits when process ends
    let _handle = network
        .spawn_server(0, 0, Arc::new(RwLock::new(engine)))
        .expect("failed to spawn consensus RPC server");
}

