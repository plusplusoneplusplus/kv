use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::path::PathBuf;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use tokio::runtime::Handle;
use tracing::info;
use clap::Parser;

mod db;
mod kvstore;
mod config;

use crate::db::KvDatabase;
use crate::kvstore::*;
use crate::config::Config;

struct KvStoreThriftHandler {
    database: Arc<KvDatabase>,
    runtime_handle: Handle,
}

impl KvStoreThriftHandler {
    fn new(database: Arc<KvDatabase>, runtime_handle: Handle) -> Self {
        Self { database, runtime_handle }
    }
}

impl KVStoreSyncHandler for KvStoreThriftHandler {
    fn handle_get(&self, req: GetRequest) -> thrift::Result<GetResponse> {
        // Use the shared runtime handle instead of creating a new runtime
        let result = self.runtime_handle.block_on(self.database.get(&req.key));
        
        match result {
            Ok(get_result) => Ok(GetResponse::new(get_result.value, get_result.found)),
            Err(e) => Err(thrift::Error::Application(thrift::ApplicationError::new(
                thrift::ApplicationErrorKind::InternalError,
                e,
            ))),
        }
    }

    fn handle_put(&self, req: PutRequest) -> thrift::Result<PutResponse> {
        let result = self.runtime_handle.block_on(self.database.put(&req.key, &req.value));
        
        let error = if result.error.is_empty() { None } else { Some(result.error) };
        Ok(PutResponse::new(result.success, error))
    }

    fn handle_delete_key(&self, req: DeleteRequest) -> thrift::Result<DeleteResponse> {
        let result = self.runtime_handle.block_on(self.database.delete(&req.key));
        
        let error = if result.error.is_empty() { None } else { Some(result.error) };
        Ok(DeleteResponse::new(result.success, error))
    }

    fn handle_list_keys(&self, req: ListKeysRequest) -> thrift::Result<ListKeysResponse> {
        let prefix = req.prefix.as_deref().unwrap_or("");
        let limit = req.limit.unwrap_or(1000) as u32;
        
        let result = self.runtime_handle.block_on(self.database.list_keys(prefix, limit));
        
        match result {
            Ok(keys) => Ok(ListKeysResponse::new(keys)),
            Err(e) => Err(thrift::Error::Application(thrift::ApplicationError::new(
                thrift::ApplicationErrorKind::InternalError,
                e,
            ))),
        }
    }

    fn handle_ping(&self, req: PingRequest) -> thrift::Result<PingResponse> {
        let server_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
            
        let message = req.message.unwrap_or_else(|| "pong".to_string());
        let timestamp = req.timestamp.unwrap_or(server_timestamp);
        
        Ok(PingResponse::new(message, timestamp, server_timestamp))
    }
}

#[derive(Parser)]
#[command(name = "rocksdbserver-thrift")]
#[command(about = "A RocksDB-based Thrift key-value store server")]
struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,
    
    /// Server bind address
    #[arg(short, long, default_value = "0.0.0.0:9090")]
    addr: String,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let args = Args::parse();

    // Load configuration
    let config = match args.config {
        Some(config_path) => {
            match Config::load_from_file(&config_path) {
                Ok(config) => {
                    info!("Loaded configuration from {}", config_path.display());
                    config
                },
                Err(e) => {
                    eprintln!("Error: Could not load config file {} ({})", config_path.display(), e);
                    std::process::exit(1);
                }
            }
        },
        None => {
            // Fallback to default location for backward compatibility
            let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
            let exe_dir = exe_path.parent().unwrap_or_else(|| std::path::Path::new("."));
            let config_path = exe_dir.join("db_config.toml");
            
            match Config::load_from_file(&config_path) {
                Ok(config) => {
                    info!("Loaded configuration from {}", config_path.display());
                    config
                },
                Err(e) => {
                    info!("Could not load {} ({}), using default configuration", config_path.display(), e);
                    Config::default()
                }
            }
        }
    };

    // Create data directory if it doesn't exist
    let db_path = config.get_db_path("thrift");
    std::fs::create_dir_all(&db_path)?;
    info!("Using database path: {}", db_path);

    // Create a Tokio runtime that will be shared across all requests
    let rt = tokio::runtime::Runtime::new().unwrap();
    let runtime_handle = rt.handle().clone();

    // Create database in the shared runtime with configuration
    let database = rt.block_on(async {
        KvDatabase::new(&db_path, &config)
    })?;
    
    let database = Arc::new(database);
    info!("Starting Thrift server on {}", args.addr);

    // Create TCP listener
    let listener = TcpListener::bind(&args.addr)?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let database = Arc::clone(&database);
                let runtime_handle = runtime_handle.clone();
                let peer_addr = stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
                
                thread::spawn(move || {
                    info!("Accepted connection from {}", peer_addr);
                    
                    // Create handler and processor for this connection with shared runtime handle
                    let handler = KvStoreThriftHandler::new(database, runtime_handle);
                    let processor = KVStoreSyncProcessor::new(handler);
                    
                    // Create buffered transports
                    let read_transport = TBufferedReadTransport::new(stream.try_clone().unwrap());
                    let write_transport = TBufferedWriteTransport::new(stream);
                    
                    // Create protocols
                    let mut input_protocol = TBinaryInputProtocol::new(read_transport, true);
                    let mut output_protocol = TBinaryOutputProtocol::new(write_transport, true);
                    
                    // Handle the connection in a loop to process multiple requests
                    loop {
                        match processor.process(&mut input_protocol, &mut output_protocol) {
                            Ok(()) => {
                                // Request processed successfully, continue to next request
                            }
                            Err(thrift::Error::Transport(ref e)) if e.kind == thrift::TransportErrorKind::EndOfFile => {
                                // Client closed connection, exit gracefully
                                break;
                            }
                            Err(e) => {
                                eprintln!("Error processing request from {}: {}", peer_addr, e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => eprintln!("Error accepting connection: {}", e),
        }
    }
    
    Ok(())
}