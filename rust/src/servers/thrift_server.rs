use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::path::PathBuf;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use tracing::info;

use rocksdb_server::lib::db::TransactionalKvDatabase;
use rocksdb_server::lib::kvstore::*;
use rocksdb_server::lib::config::Config;

struct TransactionalKvStoreThriftHandler {
    database: Arc<TransactionalKvDatabase>,
}

impl TransactionalKvStoreThriftHandler {
    fn new(database: Arc<TransactionalKvDatabase>) -> Self {
        Self { database }
    }
}

impl TransactionalKVSyncHandler for TransactionalKvStoreThriftHandler {
    // Transaction lifecycle methods
    fn handle_begin_transaction(&self, req: BeginTransactionRequest) -> thrift::Result<BeginTransactionResponse> {
        let column_families = req.column_families.unwrap_or_default();
        let timeout_seconds = req.timeout_seconds.unwrap_or(60) as u64;
        
        let result = self.database.begin_transaction(column_families, timeout_seconds);
        
        Ok(BeginTransactionResponse::new(
            result.transaction_id,
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) }
        ))
    }

    fn handle_commit_transaction(&self, req: CommitTransactionRequest) -> thrift::Result<CommitTransactionResponse> {
        let result = self.database.commit_transaction(&req.transaction_id)
;
        
        Ok(CommitTransactionResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    fn handle_abort_transaction(&self, req: AbortTransactionRequest) -> thrift::Result<AbortTransactionResponse> {
        let result = self.database.abort_transaction(&req.transaction_id)
;
        
        Ok(AbortTransactionResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) }
        ))
    }

    // Core transactional operations
    fn handle_get(&self, req: GetRequest) -> thrift::Result<GetResponse> {
        let result = self.database.transactional_get(&req.transaction_id, &req.key, req.column_family.as_deref())
;
        
        match result {
            Ok(get_result) => Ok(GetResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Err(e) => Ok(GetResponse::new(
                String::new(),
                false,
                Some(e)
            )),
        }
    }

    fn handle_set_key(&self, req: SetRequest) -> thrift::Result<SetResponse> {
        let result = self.database.transactional_set(&req.transaction_id, &req.key, &req.value, req.column_family.as_deref())
;
        
        Ok(SetResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    fn handle_delete_key(&self, req: DeleteRequest) -> thrift::Result<DeleteResponse> {
        let result = self.database.transactional_delete(&req.transaction_id, &req.key, req.column_family.as_deref())
;
        
        Ok(DeleteResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    // Range operations
    fn handle_get_range(&self, req: GetRangeRequest) -> thrift::Result<GetRangeResponse> {
        let limit = req.limit.unwrap_or(1000) as u32;
        let result = self.database.transactional_get_range(
                &req.transaction_id,
                &req.start_key,
                req.end_key.as_deref(),
                limit,
                req.column_family.as_deref()
            )
;
        
        match result {
            Ok(key_values) => {
                let thrift_key_values: Vec<KeyValue> = key_values
                    .into_iter()
                    .map(|(key, value)| KeyValue::new(key, value))
                    .collect();
                
                Ok(GetRangeResponse::new(
                    thrift_key_values,
                    true,
                    None
                ))
            }
            Err(e) => Ok(GetRangeResponse::new(
                Vec::new(),
                false,
                Some(e)
            )),
        }
    }

    // Snapshot operations
    fn handle_snapshot_get(&self, req: SnapshotGetRequest) -> thrift::Result<SnapshotGetResponse> {
        let result = self.database.snapshot_get(&req.transaction_id, &req.key, req.read_version, req.column_family.as_deref())
;
        
        match result {
            Ok(get_result) => Ok(SnapshotGetResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Err(e) => Ok(SnapshotGetResponse::new(
                String::new(),
                false,
                Some(e)
            )),
        }
    }

    fn handle_snapshot_get_range(&self, req: SnapshotGetRangeRequest) -> thrift::Result<SnapshotGetRangeResponse> {
        let limit = req.limit.unwrap_or(1000) as u32;
        let result = self.database.snapshot_get_range(
                &req.transaction_id,
                &req.start_key,
                req.end_key.as_deref(),
                req.read_version,
                limit,
                req.column_family.as_deref()
            )
;
        
        match result {
            Ok(key_values) => {
                let thrift_key_values: Vec<KeyValue> = key_values
                    .into_iter()
                    .map(|(key, value)| KeyValue::new(key, value))
                    .collect();
                
                Ok(SnapshotGetRangeResponse::new(
                    thrift_key_values,
                    true,
                    None
                ))
            }
            Err(e) => Ok(SnapshotGetRangeResponse::new(
                Vec::new(),
                false,
                Some(e)
            )),
        }
    }

    // Conflict detection
    fn handle_add_read_conflict(&self, req: AddReadConflictRequest) -> thrift::Result<AddReadConflictResponse> {
        let result = self.database.add_read_conflict(&req.transaction_id, &req.key, req.column_family.as_deref())
;
        
        Ok(AddReadConflictResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) }
        ))
    }

    fn handle_add_read_conflict_range(&self, req: AddReadConflictRangeRequest) -> thrift::Result<AddReadConflictRangeResponse> {
        let result = self.database.add_read_conflict_range(&req.transaction_id, &req.start_key, &req.end_key, req.column_family.as_deref())
;
        
        Ok(AddReadConflictRangeResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) }
        ))
    }

    // Version management
    fn handle_set_read_version(&self, req: SetReadVersionRequest) -> thrift::Result<SetReadVersionResponse> {
        let result = self.database.set_read_version(&req.transaction_id, req.version)
;
        
        Ok(SetReadVersionResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) }
        ))
    }

    fn handle_get_committed_version(&self, req: GetCommittedVersionRequest) -> thrift::Result<GetCommittedVersionResponse> {
        let result = self.database.get_committed_version(&req.transaction_id)
;
        
        match result {
            Ok(version) => Ok(GetCommittedVersionResponse::new(
                version,
                true,
                None
            )),
            Err(e) => Ok(GetCommittedVersionResponse::new(
                0,
                false,
                Some(e)
            )),
        }
    }

    // Versionstamped operations
    fn handle_set_versionstamped_key(&self, req: SetVersionstampedKeyRequest) -> thrift::Result<SetVersionstampedKeyResponse> {
        let result = self.database.set_versionstamped_key(&req.transaction_id, &req.key_prefix, &req.value, req.column_family.as_deref())
;
        
        match result {
            Ok(generated_key) => Ok(SetVersionstampedKeyResponse::new(
                generated_key,
                true,
                None
            )),
            Err(e) => Ok(SetVersionstampedKeyResponse::new(
                String::new(),
                false,
                Some(e)
            )),
        }
    }

    fn handle_set_versionstamped_value(&self, req: SetVersionstampedValueRequest) -> thrift::Result<SetVersionstampedValueResponse> {
        let result = self.database.set_versionstamped_value(&req.transaction_id, &req.key, &req.value_prefix, req.column_family.as_deref())
;
        
        match result {
            Ok(generated_value) => Ok(SetVersionstampedValueResponse::new(
                generated_value,
                true,
                None
            )),
            Err(e) => Ok(SetVersionstampedValueResponse::new(
                String::new(),
                false,
                Some(e)
            )),
        }
    }

    // Fault injection for testing
    fn handle_set_fault_injection(&self, req: FaultInjectionRequest) -> thrift::Result<FaultInjectionResponse> {
        use rocksdb_server::lib::db::FaultInjectionConfig;
        
        let config = if req.probability.map(|p| p.into_inner()).unwrap_or(0.0) > 0.0 {
            Some(FaultInjectionConfig {
                fault_type: req.fault_type,
                probability: req.probability.map(|p| p.into_inner()).unwrap_or(0.0),
                duration_ms: req.duration_ms.unwrap_or(0),
                target_operation: req.target_operation,
            })
        } else {
            None // Disable fault injection
        };
        
        let result = self.database.set_fault_injection(config)
;
        
        Ok(FaultInjectionResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) }
        ))
    }

    // Health check
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

    // Load configuration from binary's directory
    let exe_path = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("."));
    let exe_dir = exe_path.parent().unwrap_or_else(|| std::path::Path::new("."));
    let config_path = exe_dir.join("db_config.toml");
    
    let config = match Config::load_from_file(&config_path) {
        Ok(config) => {
            info!("Loaded configuration from {}", config_path.display());
            config
        },
        Err(e) => {
            info!("Could not load {} ({}), using default configuration", config_path.display(), e);
            Config::default()
        }
    };

    // Create data directory if it doesn't exist
    let db_path = config.get_db_path("thrift");
    std::fs::create_dir_all(&db_path)?;
    info!("Using database path: {}", db_path);

    // Create database with configuration - no column families for Thrift compatibility
    let database = TransactionalKvDatabase::new(&db_path, &config, &[])?;
    
    let database = Arc::new(database);
    
    // Get port from environment variable or use default
    let port = std::env::var("THRIFT_PORT")
        .unwrap_or_else(|_| "9090".to_string())
        .parse::<u16>()
        .unwrap_or(9090);
    let listen_address = format!("0.0.0.0:{}", port);
    info!("Starting Transactional Thrift server on {}", listen_address);

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
                    let handler = TransactionalKvStoreThriftHandler::new(database);
                    let processor = TransactionalKVSyncProcessor::new(handler);
                    
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