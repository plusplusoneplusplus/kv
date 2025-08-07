use rocksdb::{TransactionDB, TransactionDBOptions, Options, TransactionOptions, IteratorMode};
use std::sync::Arc;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tracing::error;

pub struct KvDatabase {
    db: Arc<TransactionDB>,
    read_semaphore: Arc<Semaphore>,
    write_queue_tx: mpsc::UnboundedSender<WriteRequest>,
}

#[derive(Debug)]
pub struct GetResult {
    pub value: String,
    pub found: bool,
}

#[derive(Debug)]
pub struct OpResult {
    pub success: bool,
    pub error: String,
}

#[derive(Debug)]
enum WriteOperation {
    Put { key: String, value: String },
    Delete { key: String },
}

struct WriteRequest {
    operation: WriteOperation,
    response_tx: oneshot::Sender<OpResult>,
}

impl KvDatabase {
    pub fn new(db_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Set up RocksDB options
        let mut opts = Options::default();
        opts.create_if_missing(true);
        
        // Set up transaction database options
        let txn_db_opts = TransactionDBOptions::default();
        
        // Open transaction database
        let db = TransactionDB::open(&opts, &txn_db_opts, db_path)?;
        let db = Arc::new(db);
        
        // Configure concurrency limits for reads
        let max_read_concurrency = 32;
        
        // Create write queue channel
        let (write_queue_tx, write_queue_rx) = mpsc::unbounded_channel::<WriteRequest>();
        
        // Spawn write worker task to serialize write operations
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            Self::write_worker(db_clone, write_queue_rx).await;
        });
        
        Ok(Self {
            db,
            read_semaphore: Arc::new(Semaphore::new(max_read_concurrency)),
            write_queue_tx,
        })
    }

    async fn write_worker(db: Arc<TransactionDB>, mut write_queue_rx: mpsc::UnboundedReceiver<WriteRequest>) {
        while let Some(request) = write_queue_rx.recv().await {
            let result = match request.operation {
                WriteOperation::Put { key, value } => Self::execute_put(&db, &key, &value),
                WriteOperation::Delete { key } => Self::execute_delete(&db, &key),
            };
            
            // Send the result back, ignoring if the receiver is dropped
            let _ = request.response_tx.send(result);
        }
    }

    fn execute_put(db: &TransactionDB, key: &str, value: &str) -> OpResult {
        if key.is_empty() {
            return OpResult {
                success: false,
                error: "key cannot be empty".to_string(),
            };
        }

        // Create a transaction for pessimistic locking
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&Default::default(), &txn_opts);
        
        // Put the key-value pair within the transaction
        match txn.put(key, value) {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => OpResult {
                        success: true,
                        error: String::new(),
                    },
                    Err(e) => {
                        error!("Failed to commit transaction: {}", e);
                        OpResult {
                            success: false,
                            error: format!("failed to commit transaction: {}", e),
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to put value: {}", e);
                let _ = txn.rollback();
                OpResult {
                    success: false,
                    error: format!("failed to put value: {}", e),
                }
            }
        }
    }

    fn execute_delete(db: &TransactionDB, key: &str) -> OpResult {
        if key.is_empty() {
            return OpResult {
                success: false,
                error: "key cannot be empty".to_string(),
            };
        }

        // Create a transaction for pessimistic locking
        let txn_opts = TransactionOptions::default();
        let txn = db.transaction_opt(&Default::default(), &txn_opts);
        
        // Delete the key within the transaction
        match txn.delete(key) {
            Ok(_) => {
                match txn.commit() {
                    Ok(_) => OpResult {
                        success: true,
                        error: String::new(),
                    },
                    Err(e) => {
                        error!("Failed to commit transaction: {}", e);
                        OpResult {
                            success: false,
                            error: format!("failed to commit transaction: {}", e),
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to delete key: {}", e);
                let _ = txn.rollback();
                OpResult {
                    success: false,
                    error: format!("failed to delete key: {}", e),
                }
            }
        }
    }

    pub async fn get(&self, key: &str) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

        // Acquire read semaphore to limit concurrent read transactions
        let _permit = self.read_semaphore
            .acquire()
            .await
            .map_err(|_| "timeout waiting for read transaction slot".to_string())?;

        // Create a read-only transaction
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        match txn.get(key) {
            Ok(Some(value)) => {
                let value_str = String::from_utf8_lossy(&value).to_string();
                Ok(GetResult {
                    value: value_str,
                    found: true,
                })
            }
            Ok(None) => {
                Ok(GetResult {
                    value: String::new(),
                    found: false,
                })
            }
            Err(e) => {
                error!("Failed to get value: {}", e);
                Err(format!("failed to get value: {}", e))
            }
        }
    }

    pub async fn put(&self, key: &str, value: &str) -> OpResult {
        let (response_tx, response_rx) = oneshot::channel();
        let write_request = WriteRequest {
            operation: WriteOperation::Put {
                key: key.to_string(),
                value: value.to_string(),
            },
            response_tx,
        };

        // Send write request to the queue
        if let Err(_) = self.write_queue_tx.send(write_request) {
            return OpResult {
                success: false,
                error: "write queue channel closed".to_string(),
            };
        }

        // Wait for the response
        match response_rx.await {
            Ok(result) => result,
            Err(_) => OpResult {
                success: false,
                error: "failed to receive response from write worker".to_string(),
            },
        }
    }

    pub async fn delete(&self, key: &str) -> OpResult {
        let (response_tx, response_rx) = oneshot::channel();
        let write_request = WriteRequest {
            operation: WriteOperation::Delete {
                key: key.to_string(),
            },
            response_tx,
        };

        // Send write request to the queue
        if let Err(_) = self.write_queue_tx.send(write_request) {
            return OpResult {
                success: false,
                error: "write queue channel closed".to_string(),
            };
        }

        // Wait for the response
        match response_rx.await {
            Ok(result) => result,
            Err(_) => OpResult {
                success: false,
                error: "failed to receive response from write worker".to_string(),
            },
        }
    }

    pub async fn list_keys(&self, prefix: &str, limit: u32) -> Result<Vec<String>, String> {
        // Acquire read semaphore to limit concurrent read transactions
        let _permit = self.read_semaphore
            .acquire()
            .await
            .map_err(|_| "timeout waiting for read transaction slot".to_string())?;

        // Create a read-only transaction for consistent snapshot
        let txn_opts = TransactionOptions::default();
        let txn = self.db.transaction_opt(&Default::default(), &txn_opts);
        
        let iter = txn.iterator(IteratorMode::Start);
        let mut keys = Vec::new();
        let mut count = 0;
        let limit = if limit > 0 { limit as usize } else { 1000 };
        
        for item in iter {
            if count >= limit {
                break;
            }
            
            match item {
                Ok((key, _value)) => {
                    let key_str = String::from_utf8_lossy(&key).to_string();
                    
                    // If prefix is specified, check if key starts with prefix
                    if !prefix.is_empty() && !key_str.starts_with(prefix) {
                        continue;
                    }
                    
                    keys.push(key_str);
                    count += 1;
                }
                Err(e) => {
                    error!("Iterator error: {}", e);
                    return Err(format!("iterator error: {}", e));
                }
            }
        }

        Ok(keys)
    }
}