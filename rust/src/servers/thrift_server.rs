use clap::Parser;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use tracing::{debug, error, info};

use rocksdb_server::generated::kvstore::*;
use rocksdb_server::lib::replication::RoutingManager;
use rocksdb_server::lib::thrift_adapter::ThriftKvAdapter;
use rocksdb_server::lib::cluster::ClusterManager;
use rocksdb_server::lib::config::Config as ClusterConfig;
use rocksdb_server::lib::kv_state_machine::{ConsensusKvDatabase, KvStateMachine};
use rocksdb_server::lib::replication::KvStoreExecutor;
use rocksdb_server::{Config, KvDatabase, TransactionalKvDatabase};
use consensus_mock::{MockConsensusEngine, ThriftTransport};
use std::collections::HashMap;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Enable verbose logging
    #[arg(short, long, default_value_t = false)]
    verbose: bool,

    /// Set the port to listen on
    #[arg(short, long, default_value_t = 9090)]
    port: u16,

    /// Set the database path
    #[arg(short, long)]
    db_path: Option<String>,

    /// Path to cluster configuration file (optional - if not provided, runs in single-node mode)
    #[arg(short, long)]
    config: Option<String>,

    /// This node's ID in the cluster (required for multi-node mode)
    #[arg(short, long)]
    node_id: Option<u32>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let mut args = Args::parse();

    // Override port from environment variable if set (for testing)
    if let Ok(env_port) = std::env::var("THRIFT_PORT") {
        if let Ok(port) = env_port.parse::<u16>() {
            args.port = port;
        }
    }

    // Initialize tracing with appropriate level
    let log_level = if args.verbose { "debug" } else { "info" };
    std::env::set_var("RUST_LOG", log_level);
    tracing_subscriber::fmt::init();

    if args.verbose {
        info!("Verbose logging enabled");
        debug!("Command line arguments: {:?}", args);
    }

    // Determine deployment mode based on configuration
    let (cluster_config, rocksdb_config, is_multi_node, node_id) = if let Some(config_path) = &args.config {
        // Multi-node mode: load cluster configuration
        let config_path = PathBuf::from(config_path);
        let cluster_config = match ClusterConfig::load_from_file(&config_path) {
            Ok(config) => {
                info!("Loaded cluster configuration from {}", config_path.display());
                if args.verbose {
                    debug!("Cluster configuration loaded successfully");
                }
                config
            }
            Err(e) => {
                error!("Failed to load configuration from {}: {}", config_path.display(), e);
                return Err(e.into());
            }
        };

        // Check if this is actually a multi-node deployment
        let is_multi_node = cluster_config.deployment.replica_endpoints.is_some();
        let node_id = if is_multi_node {
            args.node_id.unwrap_or_else(|| {
                error!("Node ID is required for multi-node deployment");
                std::process::exit(1);
            })
        } else {
            0 // Default node ID for single-node
        };

        let rocksdb_config = cluster_config.to_rocksdb_config();
        (Some(cluster_config), rocksdb_config, is_multi_node, node_id)
    } else {
        // Single-node mode: load traditional RocksDB configuration
        let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
        let exe_dir = exe_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let config_path = exe_dir.join("db_config.toml");

        let rocksdb_config = match Config::load_from_file(&config_path) {
            Ok(config) => {
                info!("Loaded configuration from {}", config_path.display());
                if args.verbose {
                    debug!("Configuration loaded successfully");
                }
                config
            }
            Err(e) => {
                info!(
                    "Could not load {} ({}), using default configuration",
                    config_path.display(),
                    e
                );
                if args.verbose {
                    debug!("Using default configuration due to error: {}", e);
                }
                Config::default()
            }
        };

        (None, rocksdb_config, false, 0)
    };

    // Create database path
    let db_path = if let Some(custom_path) = args.db_path {
        custom_path
    } else if is_multi_node {
        format!("{}/node_{}", rocksdb_config.get_db_path("multi_node"), node_id)
    } else {
        rocksdb_config.get_db_path("thrift")
    };
    std::fs::create_dir_all(&db_path)?;
    info!("Using database path: {}", db_path);

    // Create database with configuration
    if args.verbose {
        debug!("Creating database at path: {}", db_path);
    }
    let database = TransactionalKvDatabase::new(&db_path, &rocksdb_config, &[])?;
    let database = Arc::new(database);

    // Create cluster manager based on deployment mode
    let cluster_manager = if is_multi_node {
        let cluster_config_ref = cluster_config.as_ref().unwrap();
        let endpoints = cluster_config_ref.deployment.replica_endpoints.as_ref().unwrap().clone();

        // Validate node ID is within cluster range
        if node_id >= endpoints.len() as u32 {
            error!("Node ID {} is out of range for cluster size {}", node_id, endpoints.len());
            return Err("Invalid node ID".into());
        }

        info!("Starting as Node {} in {}-node cluster", node_id, endpoints.len());
        info!("Cluster endpoints: {:?}", endpoints);

        Arc::new(ClusterManager::new(
            node_id,
            endpoints,
            cluster_config_ref.cluster.clone().unwrap_or_default(),
        ))
    } else {
        info!("Starting in single-node mode");
        Arc::new(ClusterManager::single_node(node_id))
    };

    // Start cluster manager background tasks for multi-node deployments
    if is_multi_node {
        let cluster_manager_clone = cluster_manager.clone();
        tokio::spawn(async move {
            cluster_manager_clone.start_health_monitoring().await;
        });

        let cluster_manager_discovery = cluster_manager.clone();
        tokio::spawn(async move {
            if let Err(e) = cluster_manager_discovery.start_discovery().await {
                error!("Cluster discovery failed: {}", e);
            }
        });
    }

    // Create consensus database based on deployment mode
    let consensus_database = if is_multi_node {
        // Multi-node: use real consensus with ThriftTransport
        info!("Creating multi-node consensus database for Node {}", node_id);

        let cluster_config_ref = cluster_config.as_ref().unwrap();

        // Use consensus endpoints if available, otherwise fall back to replica endpoints with offset
        let consensus_endpoints = cluster_config_ref.consensus
            .as_ref()
            .and_then(|c| c.endpoints.as_ref())
            .cloned()
            .unwrap_or_else(|| {
                // Fallback: generate consensus ports from replica endpoints
                cluster_config_ref.deployment.replica_endpoints.as_ref().unwrap()
                    .iter()
                    .map(|endpoint| {
                        // Convert 9090 -> 7090, 9091 -> 7091, etc.
                        endpoint.replace("9090", "7090").replace("9091", "7091").replace("9092", "7092")
                    })
                    .collect()
            });

        info!("Consensus endpoints: {:?}", consensus_endpoints);

        // Create endpoint map for ThriftTransport
        let mut endpoint_map = HashMap::new();
        for (i, endpoint) in consensus_endpoints.iter().enumerate() {
            endpoint_map.insert(i.to_string(), endpoint.clone());
        }

        // Create ThriftTransport
        let transport = ThriftTransport::with_endpoints(
            node_id.to_string(),
            endpoint_map,
        ).await;

        // Create state machine with executor
        let executor = Arc::new(KvStoreExecutor::new(database.clone()));
        let state_machine = Box::new(KvStateMachine::new(executor));

        // Create consensus engine
        let consensus_engine = if node_id == 0 {
            // Node 0 starts as leader
            MockConsensusEngine::with_network_transport(
                node_id.to_string(),
                state_machine,
                Arc::new(transport),
            )
        } else {
            // Other nodes start as followers
            MockConsensusEngine::follower_with_network_transport(
                node_id.to_string(),
                state_machine,
                Arc::new(transport),
            )
        };

        let consensus_db = ConsensusKvDatabase::new(
            Box::new(consensus_engine),
            database as Arc<dyn KvDatabase>
        );

        info!("Multi-node consensus database created for Node {}", node_id);
        Arc::new(consensus_db)
    } else {
        // Single-node: use mock consensus for immediate execution
        info!("Creating single-node consensus database");

        let consensus_db = ConsensusKvDatabase::new_with_mock(
            node_id.to_string(),
            database as Arc<dyn KvDatabase>
        );

        info!("Single-node consensus database created");
        Arc::new(consensus_db)
    };

    // Start consensus engine
    if let Err(e) = consensus_database.start().await {
        error!("Failed to start consensus engine: {}", e);
        return Err(format!("Consensus engine startup failed: {}", e).into());
    }
    info!("Consensus engine started successfully");

    // Create routing manager
    let routing_manager = Arc::new(RoutingManager::new(
        consensus_database.clone(),
        rocksdb_config.deployment.mode.clone(),
        Some(node_id),
        cluster_manager.clone(),
    ));

    // Use port from command line arguments
    let listen_address = format!("0.0.0.0:{}", args.port);
    let server_type = if is_multi_node { "Multi-node" } else { "Single-node" };
    info!("Starting {} Thrift server on {}", server_type, listen_address);
    if args.verbose {
        debug!(
            "Server configuration: port={}, verbose={}, db_path={}, mode={:?}",
            args.port, args.verbose, db_path, rocksdb_config.deployment.mode
        );
    }

    // Create TCP listener
    let listener = TcpListener::bind(listen_address)?;
    if args.verbose {
        debug!("TCP listener created successfully");
    }

    info!("Server started successfully, waiting for connections...");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let routing_manager = Arc::clone(&routing_manager);
                let verbose = args.verbose;
                let server_node_id = node_id;
                let peer_addr = stream
                    .peer_addr()
                    .unwrap_or_else(|_| "unknown".parse().unwrap());

                thread::spawn(move || {
                    let log_prefix = if is_multi_node {
                        format!("Node {}", server_node_id)
                    } else {
                        "Server".to_string()
                    };

                    info!("{}: Accepted connection from {}", log_prefix, peer_addr);
                    if verbose {
                        debug!(
                            "{}: Creating handler and processor for connection from {}",
                            log_prefix, peer_addr
                        );
                    }

                    // Create handler for this connection
                    let handler = ThriftKvAdapter::new(routing_manager);
                    let processor = TransactionalKVSyncProcessor::new(handler);

                    // Create buffered transports
                    let read_transport = TBufferedReadTransport::new(stream.try_clone().unwrap());
                    let write_transport = TBufferedWriteTransport::new(stream);

                    // Create protocols
                    let mut input_protocol = TBinaryInputProtocol::new(read_transport, true);
                    let mut output_protocol = TBinaryOutputProtocol::new(write_transport, true);

                    if verbose {
                        debug!(
                            "{}: Connection setup complete for {}, entering request processing loop",
                            log_prefix, peer_addr
                        );
                    }

                    // Handle the connection in a loop to process multiple requests
                    let mut request_count = 0;
                    loop {
                        match processor.process(&mut input_protocol, &mut output_protocol) {
                            Ok(()) => {
                                request_count += 1;
                                if verbose {
                                    debug!(
                                        "{}: Request {} from {} processed successfully",
                                        log_prefix, request_count, peer_addr
                                    );
                                }
                            }
                            Err(thrift::Error::Transport(ref e))
                                if e.kind == thrift::TransportErrorKind::EndOfFile =>
                            {
                                info!(
                                    "{}: Client {} closed connection after {} requests",
                                    log_prefix, peer_addr, request_count
                                );
                                break;
                            }
                            Err(e) => {
                                error!(
                                    "{}: Error processing request {} from {}: {}",
                                    log_prefix, request_count + 1, peer_addr, e
                                );
                                if verbose {
                                    debug!("{}: Connection from {} terminating due to error after {} successful requests",
                                           log_prefix, peer_addr, request_count);
                                }
                                break;
                            }
                        }
                    }

                    if verbose {
                        debug!("{}: Connection thread for {} terminating", log_prefix, peer_addr);
                    }
                });
            }
            Err(e) => {
                let log_prefix = if is_multi_node {
                    format!("Node {}", node_id)
                } else {
                    "Server".to_string()
                };
                error!("{}: Error accepting connection: {}", log_prefix, e);
                if args.verbose {
                    debug!("{}: Connection acceptance failed with error: {}", log_prefix, e);
                }
            }
        }
    }

    Ok(())
}
