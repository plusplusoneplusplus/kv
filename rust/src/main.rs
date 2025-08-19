use tonic::transport::Server;
use tracing::info;
use std::path::PathBuf;
use clap::Parser;

mod db;
mod service;
mod config;

// Include the generated protobuf code
pub mod kvstore {
    tonic::include_proto!("kvstore");
}

use crate::db::TransactionalKvDatabase;
use crate::service::KvStoreGrpcService;
use crate::config::Config;
use kvstore::kv_store_server::KvStoreServer;

#[derive(Parser)]
#[command(name = "rocksdbserver-rust")]
#[command(about = "A RocksDB-based gRPC key-value store server")]
struct Args {
    /// Path to configuration file
    #[arg(short, long)]
    config: Option<PathBuf>,
    
    /// Server bind address
    #[arg(short, long, default_value = "0.0.0.0:50051")]
    addr: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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
    let db_path = config.get_db_path("rust");
    std::fs::create_dir_all(&db_path)?;
    info!("Using database path: {}", db_path);

    // Create database and server with configuration - no column families for gRPC compatibility
    let db = TransactionalKvDatabase::new(&db_path, &config, &[])?;
    let service = KvStoreGrpcService::new(db);
    
    let addr = args.addr.parse()?;
    info!("Starting Rust gRPC server on {}", addr);

    Server::builder()
        .add_service(KvStoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
