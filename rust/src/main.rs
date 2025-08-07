use tonic::transport::Server;
use tracing::info;

mod db;
mod service;

// Include the generated protobuf code
pub mod kvstore {
    tonic::include_proto!("kvstore");
}

use crate::db::KvDatabase;
use crate::service::KvStoreGrpcService;
use kvstore::kv_store_server::KvStoreServer;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create data directory if it doesn't exist
    let db_path = "./data/rocksdb-rust";
    std::fs::create_dir_all(db_path)?;

    // Create database and server
    let db = KvDatabase::new(db_path)?;
    let service = KvStoreGrpcService::new(db);
    
    let addr = "0.0.0.0:50051".parse()?;
    info!("Starting Rust gRPC server on {}", addr);

    Server::builder()
        .add_service(KvStoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
