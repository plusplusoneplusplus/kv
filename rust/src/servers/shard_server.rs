use clap::Parser;
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use tracing::{debug, error, info};
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt};

use rocksdb_server::generated::kvstore::*;
use rocksdb_server::lib::replication::RoutingManager;
use rocksdb_server::lib::thrift_adapter::ThriftKvAdapter;
use rocksdb_server::lib::cluster::ClusterManager;
use rocksdb_server::lib::config::Config as ClusterConfig;
use rocksdb_server::{Config, KvDatabase, TransactionalKvDatabase};
use rocksdb_server::lib::consensus_factory::{DefaultConsensusFactory, ConsensusFactory};

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

    /// Directory to write log files (defaults to logs/)
    #[arg(long, default_value = "logs")]
    log_dir: String,
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

    // Initialize tracing with file-based logging
    let log_level = if args.verbose { "debug" } else { "info" };
    std::env::set_var("RUST_LOG", log_level);

    // Determine node ID for log file naming
    let node_id_for_logs = args.node_id.unwrap_or(0);

    // Create logs directory if it doesn't exist
    std::fs::create_dir_all(&args.log_dir).unwrap_or_else(|e| {
        eprintln!("Failed to create logs directory {}: {}", args.log_dir, e);
        std::process::exit(1);
    });

    // Create file appender with daily rotation
    let file_appender = RollingFileAppender::builder()
        .rotation(Rotation::DAILY)
        .filename_prefix(format!("thrift-server-node-{}", node_id_for_logs))
        .filename_suffix("log")
        .build(&args.log_dir)
        .unwrap_or_else(|e| {
            eprintln!("Failed to create log file appender: {}", e);
            std::process::exit(1);
        });

    // Setup subscriber with both console and file output
    let (non_blocking_file, _guard) = tracing_appender::non_blocking(file_appender);
    let (non_blocking_stdout, _guard2) = tracing_appender::non_blocking(std::io::stdout());

    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_writer(non_blocking_file)
                .with_ansi(false)
                .with_target(true)
                .with_thread_ids(true)
                .with_file(true)
                .with_line_number(true)
        )
        .with(
            fmt::layer()
                .with_writer(non_blocking_stdout)
                .with_target(false)
                .compact()
        )
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    // Store guards to prevent early cleanup
    std::mem::forget(_guard);
    std::mem::forget(_guard2);

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

    // Create consensus using simplified factory
    let consensus_factory = DefaultConsensusFactory::new();

    let endpoints = if is_multi_node {
        let cluster_config_ref = cluster_config.as_ref().unwrap();
        // Prefer consensus endpoints if provided; fall back to replica_endpoints
        let consensus_eps = cluster_config_ref
            .consensus
            .as_ref()
            .and_then(|c| c.endpoints.clone())
            .filter(|v| !v.is_empty());
        if let Some(eps) = consensus_eps {
            Some(eps)
        } else {
            Some(
                cluster_config_ref
                    .deployment
                    .replica_endpoints
                    .as_ref()
                    .unwrap()
                    .clone(),
            )
        }
    } else {
        None
    };

    let consensus_setup = consensus_factory.create_consensus(
        node_id,
        endpoints,
        database as Arc<dyn KvDatabase>
    ).await.map_err(|e| format!("Failed to create consensus: {}", e))?;

    let consensus_database = consensus_setup.database;

    // Start consensus engine
    if let Err(e) = consensus_database.start().await {
        error!("Failed to start consensus engine: {}", e);
        return Err(format!("Consensus engine startup failed: {}", e).into());
    }
    info!("Consensus engine started successfully");

    // Store consensus server handle (if any)
    let _consensus_server_handle = consensus_setup.server_handle.map(|handle| {
        info!("Consensus server started on port {}", handle.port);
        handle.handle
    });

    // Create routing manager
    let routing_manager = Arc::new(RoutingManager::new(
        consensus_database.clone(),
        rocksdb_config.deployment.mode.clone(),
        Some(node_id),
        cluster_manager.clone(),
    ));

    // Use port from command line arguments for KV service
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
