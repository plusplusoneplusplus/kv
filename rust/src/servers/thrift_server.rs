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
    // FoundationDB-style client-side transaction methods (NEW)
    
    fn handle_get_read_version(&self, _req: GetReadVersionRequest) -> thrift::Result<GetReadVersionResponse> {
        let read_version = self.database.get_read_version();
        
        Ok(GetReadVersionResponse::new(
            read_version as i64,
            true,
            None
        ))
    }
    
    fn handle_snapshot_read(&self, req: SnapshotReadRequest) -> thrift::Result<SnapshotReadResponse> {
        let result = self.database.snapshot_read(&req.key, req.read_version as u64, req.column_family.as_deref());
        
        match result {
            Ok(get_result) => Ok(SnapshotReadResponse::new(
                get_result.value,
                get_result.found,
                None
            )),
            Err(e) => Ok(SnapshotReadResponse::new(
                String::new(),
                false,
                Some(e)
            ))
        }
    }
    
    fn handle_atomic_commit(&self, req: AtomicCommitRequest) -> thrift::Result<AtomicCommitResponse> {
        // Convert Thrift operations to internal format
        let operations: Vec<rocksdb_server::lib::db::AtomicOperation> = req.operations.into_iter()
            .map(|op| rocksdb_server::lib::db::AtomicOperation {
                op_type: op.type_,
                key: op.key,
                value: op.value,
                column_family: op.column_family,
            })
            .collect();
        
        let atomic_request = rocksdb_server::lib::db::AtomicCommitRequest {
            read_version: req.read_version as u64,
            operations,
            read_conflict_keys: req.read_conflict_keys,
            timeout_seconds: req.timeout_seconds.unwrap_or(60) as u64,
        };
        
        let result = self.database.atomic_commit(atomic_request);
        
        Ok(AtomicCommitResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code,
            result.committed_version.map(|v| v as i64)
        ))
    }
    

    // Non-transactional operations for backward compatibility
    fn handle_get(&self, req: GetRequest) -> thrift::Result<GetResponse> {
        let result = self.database.get(&req.key);
        
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
        let result = self.database.put(&req.key, &req.value);
        
        Ok(SetResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    fn handle_delete_key(&self, req: DeleteRequest) -> thrift::Result<DeleteResponse> {
        let result = self.database.delete(&req.key);
        
        Ok(DeleteResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    // Range operations - not supported in simplified implementation
    fn handle_get_range(&self, _req: GetRangeRequest) -> thrift::Result<GetRangeResponse> {
        Ok(GetRangeResponse::new(
            Vec::new(),
            false,
            Some("Range operations not supported in client-side transaction model".to_string())
        ))
    }

    // Backward compatibility snapshot operations
    fn handle_snapshot_get(&self, req: SnapshotGetRequest) -> thrift::Result<SnapshotGetResponse> {
        let result = self.database.snapshot_read(&req.key, req.read_version as u64, req.column_family.as_deref());
        
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

    fn handle_snapshot_get_range(&self, _req: SnapshotGetRangeRequest) -> thrift::Result<SnapshotGetRangeResponse> {
        Ok(SnapshotGetRangeResponse::new(
            Vec::new(),
            false,
            Some("Range operations not supported in client-side transaction model".to_string())
        ))
    }

    // Conflict detection stubs - handled client-side in FoundationDB model
    fn handle_add_read_conflict(&self, _req: AddReadConflictRequest) -> thrift::Result<AddReadConflictResponse> {
        Ok(AddReadConflictResponse::new(
            false,
            Some("Conflict detection handled client-side in FoundationDB model".to_string())
        ))
    }

    fn handle_add_read_conflict_range(&self, _req: AddReadConflictRangeRequest) -> thrift::Result<AddReadConflictRangeResponse> {
        Ok(AddReadConflictRangeResponse::new(
            false,
            Some("Conflict detection handled client-side in FoundationDB model".to_string())
        ))
    }

    // Version management stubs - handled client-side in FoundationDB model
    fn handle_set_read_version(&self, _req: SetReadVersionRequest) -> thrift::Result<SetReadVersionResponse> {
        Ok(SetReadVersionResponse::new(
            false,
            Some("Read version managed client-side in FoundationDB model".to_string())
        ))
    }

    fn handle_get_committed_version(&self, _req: GetCommittedVersionRequest) -> thrift::Result<GetCommittedVersionResponse> {
        Ok(GetCommittedVersionResponse::new(
            0,
            false,
            Some("Committed version managed client-side in FoundationDB model".to_string())
        ))
    }

    // Versionstamped operation stubs - not supported in simplified model
    fn handle_set_versionstamped_key(&self, _req: SetVersionstampedKeyRequest) -> thrift::Result<SetVersionstampedKeyResponse> {
        Ok(SetVersionstampedKeyResponse::new(
            String::new(),
            false,
            Some("Versionstamped operations not supported in simplified model".to_string())
        ))
    }

    fn handle_set_versionstamped_value(&self, _req: SetVersionstampedValueRequest) -> thrift::Result<SetVersionstampedValueResponse> {
        Ok(SetVersionstampedValueResponse::new(
            String::new(),
            false,
            Some("Versionstamped operations not supported in simplified model".to_string())
        ))
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
        
        let result = self.database.set_fault_injection(config);
        
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