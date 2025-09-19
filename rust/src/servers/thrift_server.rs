use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::path::PathBuf;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use tracing::{info, debug, error};
use clap::Parser;

use rocksdb_server::lib::db::TransactionalKvDatabase;
use rocksdb_server::lib::db_trait::KvDatabase;
use rocksdb_server::generated::kvstore::*;
use rocksdb_server::lib::config::Config;
use rocksdb_server::lib::thrift_adapter::ThriftKvAdapter;
use rocksdb_server::lib::replication::RoutingManager;

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
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    // Load configuration from binary's directory
    let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    let exe_dir = exe_path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let config_path = exe_dir.join("db_config.toml");

    let config = match Config::load_from_file(&config_path) {
        Ok(config) => {
            info!("Loaded configuration from {}", config_path.display());
            if args.verbose {
                debug!("Configuration loaded successfully");
            }
            config
        },
        Err(e) => {
            info!("Could not load {} ({}), using default configuration", config_path.display(), e);
            if args.verbose {
                debug!("Using default configuration due to error: {}", e);
            }
            Config::default()
        }
    };

    // Create data directory if it doesn't exist
    let db_path = if let Some(custom_path) = args.db_path {
        custom_path
    } else {
        config.get_db_path("thrift")
    };
    std::fs::create_dir_all(&db_path)?;
    info!("Using database path: {}", db_path);

    // Create database with configuration - no column families for Thrift compatibility
    if args.verbose {
        debug!("Creating database at path: {}", db_path);
    }
    let database = TransactionalKvDatabase::new(&db_path, &config, &[])?;

    let database = Arc::new(database);

    // Use port from command line arguments
    let listen_address = format!("0.0.0.0:{}", args.port);
    info!("Starting Transactional Thrift server on {}", listen_address);
    if args.verbose {
        debug!("Server configuration: port={}, verbose={}, db_path={}",
               args.port, args.verbose, db_path);
    }

    // Create TCP listener
    let listener = TcpListener::bind(listen_address)?;
    if args.verbose {
        debug!("TCP listener created successfully");
    }

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let database = Arc::clone(&database);
                let config = config.clone();
                let verbose = args.verbose;
                let peer_addr = stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());

                thread::spawn(move || {
                    info!("Accepted connection from {}", peer_addr);
                    if verbose {
                        debug!("Creating handler and processor for connection from {}", peer_addr);
                    }

                    // Create routing manager and handler for this connection
                    let routing_manager = Arc::new(RoutingManager::new(
                        database as Arc<dyn KvDatabase>,
                        config.deployment.mode.clone(),
                        config.deployment.instance_id,
                    ));
                    let handler = ThriftKvAdapter::new(routing_manager);
                    let processor = TransactionalKVSyncProcessor::new(handler);

                    // Create buffered transports
                    let read_transport = TBufferedReadTransport::new(stream.try_clone().unwrap());
                    let write_transport = TBufferedWriteTransport::new(stream);

                    // Create protocols
                    let mut input_protocol = TBinaryInputProtocol::new(read_transport, true);
                    let mut output_protocol = TBinaryOutputProtocol::new(write_transport, true);

                    if verbose {
                        debug!("Connection setup complete for {}, entering request processing loop", peer_addr);
                    }

                    // Handle the connection in a loop to process multiple requests
                    let mut request_count = 0;
                    loop {
                        match processor.process(&mut input_protocol, &mut output_protocol) {
                            Ok(()) => {
                                request_count += 1;
                                if verbose {
                                    debug!("Request {} from {} processed successfully", request_count, peer_addr);
                                }
                            }
                            Err(thrift::Error::Transport(ref e)) if e.kind == thrift::TransportErrorKind::EndOfFile => {
                                info!("Client {} closed connection after {} requests", peer_addr, request_count);
                                break;
                            }
                            Err(e) => {
                                error!("Error processing request {} from {}: {}", request_count + 1, peer_addr, e);
                                if verbose {
                                    debug!("Connection from {} terminating due to error after {} successful requests",
                                           peer_addr, request_count);
                                }
                                break;
                            }
                        }
                    }

                    if verbose {
                        debug!("Connection thread for {} terminating", peer_addr);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting connection: {}", e);
                if args.verbose {
                    debug!("Connection acceptance failed with error: {}", e);
                }
            }
        }
    }

    Ok(())
}