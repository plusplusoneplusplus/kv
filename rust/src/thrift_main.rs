use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use tracing::info;

mod db;
mod kvstore;

use crate::db::KvDatabase;
use crate::kvstore::*;

struct KvStoreThriftHandler {
    database: Arc<KvDatabase>,
}

impl KvStoreThriftHandler {
    fn new(database: Arc<KvDatabase>) -> Self {
        Self { database }
    }
}

impl KVStoreSyncHandler for KvStoreThriftHandler {
    fn handle_get(&self, req: GetRequest) -> thrift::Result<GetResponse> {
        // Create a new Tokio runtime for this operation
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(self.database.get(&req.key));
        
        match result {
            Ok(get_result) => Ok(GetResponse::new(get_result.value, get_result.found)),
            Err(e) => Err(thrift::Error::Application(thrift::ApplicationError::new(
                thrift::ApplicationErrorKind::InternalError,
                e,
            ))),
        }
    }

    fn handle_put(&self, req: PutRequest) -> thrift::Result<PutResponse> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(self.database.put(&req.key, &req.value));
        
        let error = if result.error.is_empty() { None } else { Some(result.error) };
        Ok(PutResponse::new(result.success, error))
    }

    fn handle_delete_key(&self, req: DeleteRequest) -> thrift::Result<DeleteResponse> {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(self.database.delete(&req.key));
        
        let error = if result.error.is_empty() { None } else { Some(result.error) };
        Ok(DeleteResponse::new(result.success, error))
    }

    fn handle_list_keys(&self, req: ListKeysRequest) -> thrift::Result<ListKeysResponse> {
        let prefix = req.prefix.as_deref().unwrap_or("");
        let limit = req.limit.unwrap_or(1000) as u32;
        
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(self.database.list_keys(prefix, limit));
        
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create data directory if it doesn't exist
    let db_path = "./data/rocksdb-thrift";
    std::fs::create_dir_all(db_path)?;

    // Create database in a Tokio runtime for initialization
    let rt = tokio::runtime::Runtime::new().unwrap();
    let database = rt.block_on(async {
        KvDatabase::new(db_path)
    })?;
    
    let database = Arc::new(database);
    let listen_address = "0.0.0.0:9090";
    info!("Starting Thrift server on {}", listen_address);

    // Create TCP listener
    let listener = TcpListener::bind(listen_address)?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let database = Arc::clone(&database);
                let peer_addr = stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
                
                thread::spawn(move || {
                    info!("Accepted connection from {}", peer_addr);
                    
                    // Create handler and processor for this connection
                    let handler = KvStoreThriftHandler::new(database);
                    let processor = KVStoreSyncProcessor::new(handler);
                    
                    // Create buffered transports
                    let read_transport = TBufferedReadTransport::new(stream.try_clone().unwrap());
                    let write_transport = TBufferedWriteTransport::new(stream);
                    
                    // Create protocols
                    let mut input_protocol = TBinaryInputProtocol::new(read_transport, true);
                    let mut output_protocol = TBinaryOutputProtocol::new(write_transport, true);
                    
                    // Handle the connection
                    if let Err(e) = processor.process(&mut input_protocol, &mut output_protocol) {
                        eprintln!("Error processing connection from {}: {}", peer_addr, e);
                    }
                });
            }
            Err(e) => eprintln!("Error accepting connection: {}", e),
        }
    }
    
    Ok(())
}