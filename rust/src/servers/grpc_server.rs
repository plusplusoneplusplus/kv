use std::path::PathBuf;
use tonic::transport::Server;
use tracing::info;

use rocksdb_server::lib::proto::kv_store_server::KvStoreServer;
use rocksdb_server::lib::service::KvStoreGrpcService;
use rocksdb_server::{Config, TransactionalKvDatabase};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Load configuration from binary's directory
    let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    let exe_dir = exe_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let config_path = exe_dir.join("db_config.toml");

    let config = match Config::load_from_file(&config_path) {
        Ok(config) => {
            info!("Loaded configuration from {}", config_path.display());
            config
        }
        Err(e) => {
            info!(
                "Could not load {} ({}), using default configuration",
                config_path.display(),
                e
            );
            Config::default()
        }
    };

    // Create data directory if it doesn't exist
    let db_path = config.get_db_path("rust");
    std::fs::create_dir_all(&db_path)?;
    info!("Using database path: {}", db_path);

    // Create database and server with configuration - no column families for gRPC compatibility
    let db = TransactionalKvDatabase::new(&db_path, &config, &[])?;
    let service = KvStoreGrpcService::new(db);

    let addr = "0.0.0.0:50051".parse()?;
    info!("Starting Rust gRPC server on {}", addr);

    Server::builder()
        .add_service(KvStoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
