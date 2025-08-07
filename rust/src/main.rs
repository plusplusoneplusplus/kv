use rocksdb::{TransactionDB, TransactionDBOptions, Options, TransactionOptions};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, error};

// Include the generated protobuf code
pub mod kvstore {
    tonic::include_proto!("kvstore");
}

use kvstore::kv_store_server::{KvStore, KvStoreServer};
use kvstore::{
    GetRequest, GetResponse, PutRequest, PutResponse, DeleteRequest, DeleteResponse,
    ListKeysRequest, ListKeysResponse, PingRequest, PingResponse,
};

pub struct KvStoreService {
    db: Arc<TransactionDB>,
    read_semaphore: Arc<Semaphore>,
    write_semaphore: Arc<Semaphore>,
}

impl KvStoreService {
    pub fn new(db_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Set up RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        // Set up transaction database options
        let txn_db_opts = TransactionDBOptions::default();
        
        // Open transaction database
        let db = TransactionDB::open(&opts, &txn_db_opts, db_path)?;
        
        // Configure concurrency limits (matching Go implementation)
        let max_read_concurrency = 32;
        let max_write_concurrency = 16;
        
        Ok(Self {
            db: Arc::new(db),
            read_semaphore: Arc::new(Semaphore::new(max_read_concurrency)),
            write_semaphore: Arc::new(Semaphore::new(max_write_concurrency)),
        })
    }
}

#[tonic::async_trait]
impl KvStore for KvStoreService {
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        
        if req.key.is_empty() {
            return Err(Status::invalid_argument("key cannot be empty"));
        }

        // Acquire read semaphore to limit concurrent read transactions
        let _permit = self.read_semaphore
            .acquire()
            .await
            .map_err(|_| Status::deadline_exceeded("timeout waiting for read transaction slot"))?;

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        match txn.get(&req.key) {
            Ok(Some(value)) => {
                let value_str = String::from_utf8_lossy(&value).to_string();
                Ok(Response::new(GetResponse {
                    value: value_str,
                    found: true,
                }))
            }
            Ok(None) => {
                Ok(Response::new(GetResponse {
                    value: String::new(),
                    found: false,
                }))
            }
            Err(e) => {
                error!("Failed to get value: {}", e);
                Err(Status::internal(format!("failed to get value: {}", e)))
            }
        }
    }

    async fn put(&self, request: Request<PutRequest>) -> Result<Response<PutResponse>, Status> {
        let req = request.into_inner();
        
        if req.key.is_empty() {
            return Ok(Response::new(PutResponse {
                success: false,
                error: "key cannot be empty".to_string(),
            }));
        }

        // Acquire write semaphore to limit concurrent write transactions
        let _permit = self.write_semaphore
            .acquire()
            .await
            .map_err(|_| Status::deadline_exceeded("timeout waiting for write transaction slot"))?;

        // Create a transaction for pessimistic locking
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        // Put the key-value pair within the transaction
        match txn.put(&req.key, &req.value) {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => Ok(Response::new(PutResponse {
                        success: true,
                        error: String::new(),
                    })),
                    Err(e) => {
                        error!("Failed to commit transaction: {}", e);
                        Ok(Response::new(PutResponse {
                            success: false,
                            error: format!("failed to commit transaction: {}", e),
                        }))
                    }
                }
            }
            Err(e) => {
                error!("Failed to put value: {}", e);
                let _ = txn.rollback();
                Ok(Response::new(PutResponse {
                    success: false,
                    error: format!("failed to put value: {}", e),
                }))
            }
        }
    }

    async fn delete(&self, request: Request<DeleteRequest>) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        
        if req.key.is_empty() {
            return Ok(Response::new(DeleteResponse {
                success: false,
                error: "key cannot be empty".to_string(),
            }));
        }

        // Acquire write semaphore to limit concurrent write transactions
        let _permit = self.write_semaphore
            .acquire()
            .await
            .map_err(|_| Status::deadline_exceeded("timeout waiting for write transaction slot"))?;

        // Create a transaction for pessimistic locking
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        // Delete the key within the transaction
        match txn.delete(&req.key) {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => Ok(Response::new(DeleteResponse {
                        success: true,
                        error: String::new(),
                    })),
                    Err(e) => {
                        error!("Failed to commit transaction: {}", e);
                        Ok(Response::new(DeleteResponse {
                            success: false,
                            error: format!("failed to commit transaction: {}", e),
                        }))
                    }
                }
            }
            Err(e) => {
                error!("Failed to delete key: {}", e);
                let _ = txn.rollback();
                Ok(Response::new(DeleteResponse {
                    success: false,
                    error: format!("failed to delete key: {}", e),
                }))
            }
        }
    }

    async fn list_keys(&self, request: Request<ListKeysRequest>) -> Result<Response<ListKeysResponse>, Status> {
        let req = request.into_inner();
        
        // Acquire read semaphore to limit concurrent read transactions
        let _permit = self.read_semaphore
            .acquire()
            .await
            .map_err(|_| Status::deadline_exceeded("timeout waiting for read transaction slot"))?;

        // Create a read-only transaction for consistent snapshot
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        let iter = txn.iterator(rocksdb::IteratorMode::Start);
        let mut keys = Vec::new();
        let mut count = 0;
        let limit = if req.limit > 0 { req.limit as usize } else { 1000 };
        
        for item in iter {
            if count >= limit {
                break;
            }
            
            match item {
                Ok((key, _value)) => {
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    
                    // If prefix is specified, check if key starts with prefix
                    if !req.prefix.is_empty() && !key_str.starts_with(&req.prefix) {
                        continue;
                    }
                    
                    keys.push(key_str);
                    count += 1;
                }
                Err(e) => {
                    error!("Iterator error: {}", e);
                    return Err(Status::internal(format!("iterator error: {}", e)));
                }
            }
        }

        Ok(Response::new(ListKeysResponse { keys }))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        let req = request.into_inner();
        
        // Get current timestamp in microseconds
        let server_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Status::internal("failed to get system time"))?
            .as_micros() as i64;
        
        // Echo back the message with timestamps
        Ok(Response::new(PingResponse {
            message: req.message,
            timestamp: req.timestamp,
            server_timestamp,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Create data directory if it doesn't exist
    let db_path = "./data/rocksdb-rust";
    std::fs::create_dir_all(db_path)?;

    // Create server
    let service = KvStoreService::new(db_path)?;
    
    let addr = "0.0.0.0:50051".parse()?;
    info!("Starting Rust gRPC server on {}", addr);

    Server::builder()
        .add_service(KvStoreServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
