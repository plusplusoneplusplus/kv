use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::path::PathBuf;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::server::TProcessor;
use thrift::transport::{TBufferedReadTransport, TBufferedWriteTransport};
use tracing::{info, debug, trace, warn, error};
use clap::Parser;

use rocksdb_server::lib::db::TransactionalKvDatabase;
use rocksdb_server::generated::kvstore::*;
use rocksdb_server::lib::config::Config;

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

struct TransactionalKvStoreThriftHandler {
    database: Arc<TransactionalKvDatabase>,
    verbose: bool,
}

impl TransactionalKvStoreThriftHandler {
    fn new(database: Arc<TransactionalKvDatabase>, verbose: bool) -> Self {
        Self { database, verbose }
    }
}

impl TransactionalKVSyncHandler for TransactionalKvStoreThriftHandler {
    // FoundationDB-style client-side transaction methods (NEW)
    
    fn handle_get_read_version(&self, _req: GetReadVersionRequest) -> thrift::Result<GetReadVersionResponse> {
        if self.verbose {
            debug!("Getting read version");
        }
        let read_version = self.database.get_read_version();
        if self.verbose {
            debug!("Read version retrieved: {}", read_version);
        }
        
        Ok(GetReadVersionResponse::new(
            read_version as i64,
            true,
            None
        ))
    }
    
    fn handle_snapshot_read(&self, req: SnapshotReadRequest) -> thrift::Result<SnapshotReadResponse> {
        if self.verbose {
            debug!("Snapshot read: key={:?}, read_version={}, column_family={:?}", 
                   req.key, req.read_version, req.column_family);
        }
        let result = self.database.snapshot_read(&req.key, req.read_version as u64, req.column_family.as_deref());
        
        match result {
            Ok(get_result) => {
                if self.verbose {
                    debug!("Snapshot read result: found={}, value_len={}", 
                           get_result.found, get_result.value.len());
                }
                Ok(SnapshotReadResponse::new(
                    get_result.value,
                    get_result.found,
                    None
                ))
            },
            Err(e) => {
                if self.verbose {
                    warn!("Snapshot read error: {}", e);
                }
                Ok(SnapshotReadResponse::new(
                    Vec::new(),
                    false,
                    Some(e)
                ))
            }
        }
    }
    
    fn handle_atomic_commit(&self, req: AtomicCommitRequest) -> thrift::Result<AtomicCommitResponse> {
        if self.verbose {
            debug!("Atomic commit: read_version={}, operations_count={}, read_conflict_keys_count={}, timeout={}s", 
                   req.read_version, req.operations.len(), req.read_conflict_keys.len(), 
                   req.timeout_seconds.unwrap_or(60));
            for (i, op) in req.operations.iter().enumerate() {
                trace!("Operation {}: type={}, key={:?}, value_len={}, column_family={:?}", 
                       i, op.type_, op.key, 
                       op.value.as_ref().map(|v| v.len()).unwrap_or(0), 
                       op.column_family);
            }
        }
        
        // Convert Thrift operations to internal format
        let operations: Vec<rocksdb_server::lib::db::AtomicOperation> = req.operations.into_iter()
            .map(|op| rocksdb_server::lib::db::AtomicOperation {
                op_type: op.type_,
                key: op.key,
                value: op.value,
                column_family: op.column_family,
            })
            .collect();
        
        let read_conflict_keys: Vec<Vec<u8>> = req.read_conflict_keys;
        
        let atomic_request = rocksdb_server::lib::db::AtomicCommitRequest {
            read_version: req.read_version as u64,
            operations,
            read_conflict_keys,
            timeout_seconds: req.timeout_seconds.unwrap_or(60) as u64,
        };
        
        let result = self.database.atomic_commit(atomic_request);
        
        if self.verbose {
            debug!("Atomic commit result: success={}, error_code={:?}, committed_version={:?}", 
                   result.success, result.error_code, result.committed_version);
            if !result.error.is_empty() {
                warn!("Atomic commit error: {}", result.error);
            }
        }
        
        Ok(AtomicCommitResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code,
            result.committed_version.map(|v| v as i64)
        ))
    }
    

    // Non-transactional operations for backward compatibility
    fn handle_get(&self, req: GetRequest) -> thrift::Result<GetResponse> {
        if self.verbose {
            debug!("Get: key={:?}", req.key);
        }
        let result = self.database.get(&req.key);
        
        match result {
            Ok(get_result) => {
                if self.verbose {
                    debug!("Get result: key={:?}, found={}, value_len={}", 
                           req.key, get_result.found, get_result.value.len());
                }
                Ok(GetResponse::new(
                    get_result.value,
                    get_result.found,
                    None
                ))
            },
            Err(e) => {
                if self.verbose {
                    warn!("Get error for key {:?}: {}", req.key, e);
                }
                Ok(GetResponse::new(
                    Vec::new(),
                    false,
                    Some(e)
                ))
            },
        }
    }

    fn handle_set_key(&self, req: SetRequest) -> thrift::Result<SetResponse> {
        if self.verbose {
            debug!("Set: key={:?}, value_len={}", req.key, req.value.len());
        }
        let result = self.database.put(&req.key, &req.value);
        
        if self.verbose {
            debug!("Set result: key={:?}, success={}, error_code={:?}", 
                   req.key, result.success, result.error_code);
            if !result.error.is_empty() {
                warn!("Set error for key {:?}: {}", req.key, result.error);
            }
        }
        
        Ok(SetResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    fn handle_delete_key(&self, req: DeleteRequest) -> thrift::Result<DeleteResponse> {
        if self.verbose {
            debug!("Delete: key={:?}", req.key);
        }
        let result = self.database.delete(&req.key);
        
        if self.verbose {
            debug!("Delete result: key={:?}, success={}, error_code={:?}", 
                   req.key, result.success, result.error_code);
            if !result.error.is_empty() {
                warn!("Delete error for key {:?}: {}", req.key, result.error);
            }
        }
        
        Ok(DeleteResponse::new(
            result.success,
            if result.error.is_empty() { None } else { Some(result.error) },
            result.error_code
        ))
    }

    // Range operations
    fn handle_get_range(&self, req: GetRangeRequest) -> thrift::Result<GetRangeResponse> {
        // Apply FoundationDB-style defaults for missing keys
        let begin_key = req.begin_key.as_deref().unwrap_or(b""); // Empty string = beginning of keyspace
        let end_key = req.end_key.as_deref().unwrap_or(&[0xFF]); // Single 0xFF = end of keyspace
        
        if self.verbose {
            debug!("Get range: begin_key={:?}, end_key={:?}, begin_offset={}, begin_or_equal={}, end_offset={}, end_or_equal={}, limit={}, column_family={:?}",
                   begin_key, end_key, req.begin_offset.unwrap_or(0), req.begin_or_equal.unwrap_or(true), 
                   req.end_offset.unwrap_or(0), req.end_or_equal.unwrap_or(false), req.limit.unwrap_or(1000), req.column_family);
        }
        
        let result = self.database.get_range(
            begin_key,
            end_key,
            req.begin_offset.unwrap_or(0),
            req.begin_or_equal.unwrap_or(true),
            req.end_offset.unwrap_or(0),
            req.end_or_equal.unwrap_or(false),
            req.limit
        );
        
        if self.verbose {
            debug!("Get range result: success={}, key_values_count={}, error='{}'",
                   result.success, result.key_values.len(), result.error);
        }
        
        if result.success {
            let key_values: Vec<KeyValue> = result.key_values.into_iter()
                .map(|kv| KeyValue::new(kv.key, kv.value))
                .collect();
            
            Ok(GetRangeResponse::new(
                key_values,
                true,
                None,
                result.has_more
            ))
        } else {
            Ok(GetRangeResponse::new(
                Vec::new(),
                false,
                Some(result.error),
                false
            ))
        }
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
                Vec::new(),
                false,
                Some(e)
            )),
        }
    }

    fn handle_snapshot_get_range(&self, req: SnapshotGetRangeRequest) -> thrift::Result<SnapshotGetRangeResponse> {
        // Apply FoundationDB-style defaults for missing keys
        let begin_key = req.begin_key.as_deref().unwrap_or(b""); // Empty string = beginning of keyspace
        let end_key = req.end_key.as_deref().unwrap_or(&[0xFF]); // Single 0xFF = end of keyspace
        
        if self.verbose {
            debug!("Snapshot get range: begin_key={:?}, end_key={:?}, begin_offset={}, begin_or_equal={}, end_offset={}, end_or_equal={}, read_version={}, limit={}, column_family={:?}",
                   begin_key, end_key, req.begin_offset.unwrap_or(0), req.begin_or_equal.unwrap_or(true), 
                   req.end_offset.unwrap_or(0), req.end_or_equal.unwrap_or(false), req.read_version, req.limit.unwrap_or(1000), req.column_family);
        }
        
        let result = self.database.snapshot_get_range(
            begin_key,
            end_key,
            req.begin_offset.unwrap_or(0),
            req.begin_or_equal.unwrap_or(true),
            req.end_offset.unwrap_or(0),
            req.end_or_equal.unwrap_or(false),
            req.read_version as u64,
            req.limit
        );
        
        if self.verbose {
            debug!("Snapshot get range result: success={}, key_values_count={}, error='{}'",
                   result.success, result.key_values.len(), result.error);
        }
        
        if result.success {
            let key_values: Vec<KeyValue> = result.key_values.into_iter()
                .map(|kv| KeyValue::new(kv.key, kv.value))
                .collect();
            
            Ok(SnapshotGetRangeResponse::new(
                key_values,
                true,
                None,
                result.has_more
            ))
        } else {
            Ok(SnapshotGetRangeResponse::new(
                Vec::new(),
                false,
                Some(result.error),
                false
            ))
        }
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

    // Versionstamped operation implementation
    fn handle_set_versionstamped_key(&self, req: SetVersionstampedKeyRequest) -> thrift::Result<SetVersionstampedKeyResponse> {
        if self.verbose {
            debug!("Set versionstamped key: key_prefix={:?}, value_len={}, column_family={:?}", 
                   req.key_prefix, req.value.len(), req.column_family);
        }
        
        // Get current read version for the transaction
        let read_version = self.database.get_read_version();
        
        // Create a single versionstamped operation
        let versionstamp_operation = rocksdb_server::lib::db::AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_KEY".to_string(),
            key: req.key_prefix,
            value: Some(req.value),
            column_family: req.column_family,
        };
        
        // Create atomic commit request with this single operation
        let atomic_request = rocksdb_server::lib::db::AtomicCommitRequest {
            read_version,
            operations: vec![versionstamp_operation],
            read_conflict_keys: vec![], // No read conflicts for single versionstamp operation
            timeout_seconds: 60, // Default timeout
        };
        
        let result = self.database.atomic_commit(atomic_request);
        
        if self.verbose {
            debug!("Versionstamped key result: success={}, generated_keys_count={}, committed_version={:?}", 
                   result.success, result.generated_keys.len(), result.committed_version);
            if !result.error.is_empty() {
                warn!("Versionstamped key error: {}", result.error);
            }
        }
        
        if result.success && !result.generated_keys.is_empty() {
            // Return the generated key on success
            Ok(SetVersionstampedKeyResponse::new(
                result.generated_keys[0].clone(),
                true,
                None
            ))
        } else {
            // Return error response
            Ok(SetVersionstampedKeyResponse::new(
                Vec::new(),
                false,
                if result.error.is_empty() { Some("Failed to generate versionstamped key".to_string()) } else { Some(result.error) }
            ))
        }
    }

    fn handle_set_versionstamped_value(&self, req: SetVersionstampedValueRequest) -> thrift::Result<SetVersionstampedValueResponse> {
        if self.verbose {
            debug!("Set versionstamped value: key={:?}, value_prefix_len={}, column_family={:?}", 
                   req.key, req.value_prefix.len(), req.column_family);
        }
        
        // Get current read version for the transaction
        let read_version = self.database.get_read_version();
        
        // Create a single versionstamped value operation
        let versionstamp_operation = rocksdb_server::lib::db::AtomicOperation {
            op_type: "SET_VERSIONSTAMPED_VALUE".to_string(),
            key: req.key,
            value: Some(req.value_prefix),
            column_family: req.column_family,
        };
        
        // Create atomic commit request with this single operation
        let atomic_request = rocksdb_server::lib::db::AtomicCommitRequest {
            read_version,
            operations: vec![versionstamp_operation],
            read_conflict_keys: vec![], // No read conflicts for single versionstamp operation
            timeout_seconds: 60, // Default timeout
        };
        
        let result = self.database.atomic_commit(atomic_request);
        
        if self.verbose {
            debug!("Versionstamped value result: success={}, generated_values_count={}, committed_version={:?}", 
                   result.success, result.generated_values.len(), result.committed_version);
            if !result.error.is_empty() {
                warn!("Versionstamped value error: {}", result.error);
            }
        }
        
        if result.success && !result.generated_values.is_empty() {
            // Return the generated value on success
            Ok(SetVersionstampedValueResponse::new(
                result.generated_values[0].clone(),
                true,
                None
            ))
        } else {
            // Return error response
            Ok(SetVersionstampedValueResponse::new(
                Vec::new(),
                false,
                if result.error.is_empty() { Some("Failed to generate versionstamped value".to_string()) } else { Some(result.error) }
            ))
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
            
        let message = req.message.unwrap_or_else(|| "pong".to_string().into_bytes());
        let timestamp = req.timestamp.unwrap_or(server_timestamp);
        
        Ok(PingResponse::new(message, timestamp, server_timestamp))
    }
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
                let verbose = args.verbose;
                let peer_addr = stream.peer_addr().unwrap_or_else(|_| "unknown".parse().unwrap());
                
                thread::spawn(move || {
                    info!("Accepted connection from {}", peer_addr);
                    if verbose {
                        debug!("Creating handler and processor for connection from {}", peer_addr);
                    }
                    
                    // Create handler and processor for this connection
                    let handler = TransactionalKvStoreThriftHandler::new(database, verbose);
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
                                    trace!("Request {} from {} processed successfully", request_count, peer_addr);
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