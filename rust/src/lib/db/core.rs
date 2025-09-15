use std::sync::{Arc, RwLock, mpsc};
use std::collections::HashMap;
use rocksdb::{TransactionDB, TransactionDBOptions, Options, BlockBasedOptions, Cache};
use super::super::config::Config;
use super::types::{WriteRequest, FaultInjectionConfig};

/// Core database initialization and management
pub struct Core;

impl Core {
    /// Create a new TransactionalKvDatabase instance
    pub fn create_database(
        db_path: &str,
        config: &Config,
        column_families: &[&str]
    ) -> Result<(Arc<TransactionDB>, HashMap<String, String>, mpsc::Sender<WriteRequest>, Arc<RwLock<Option<FaultInjectionConfig>>>, Arc<std::sync::atomic::AtomicU64>), Box<dyn std::error::Error>> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Configure performance settings
        opts.set_max_background_jobs(6);
        opts.set_bytes_per_sync(1048576);
        opts.set_wal_bytes_per_sync(1048576);

        // Configure block cache
        let cache = Cache::new_lru_cache(256 * 1024 * 1024); // 256MB default
        let mut table_opts = BlockBasedOptions::default();
        table_opts.set_block_cache(&cache);
        table_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&table_opts);

        // Create column families vector
        let cfs: Vec<&str> = column_families.iter().cloned().collect();

        // Create the TransactionDB
        let txn_db_opts = TransactionDBOptions::default();
        let db = TransactionDB::open_cf(&opts, &txn_db_opts, db_path, &cfs)?;

        // Initialize column family handles mapping
        let mut cf_handles = HashMap::new();
        cf_handles.insert("default".to_string(), "default".to_string());
        for cf_name in column_families {
            cf_handles.insert(cf_name.to_string(), cf_name.to_string());
        }

        // Create write queue channel (for potential async writes)
        let (write_queue_tx, _write_queue_rx) = mpsc::channel();

        // Initialize fault injection
        let fault_injection = Arc::new(RwLock::new(None));

        // Initialize version counter
        let current_version = Arc::new(std::sync::atomic::AtomicU64::new(1));

        Ok((
            Arc::new(db),
            cf_handles,
            write_queue_tx,
            fault_injection,
            current_version
        ))
    }

    /// Validate column family exists
    pub fn validate_column_family(
        cf_handles: &HashMap<String, String>,
        column_family: Option<&str>
    ) -> Result<(), String> {
        if let Some(cf_name) = column_family {
            if !cf_handles.contains_key(cf_name) {
                return Err(format!("Column family '{}' not found", cf_name));
            }
        }
        Ok(())
    }
}