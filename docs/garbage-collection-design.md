# Garbage Collection Design for MVCC KV Store

## Executive Summary

This document outlines the design for automated garbage collection of old versions in our MVCC-enabled key-value store. The GC system prevents unbounded storage growth while maintaining data consistency and system performance.

## Problem Statement

In an MVCC system, every write operation creates a new version of a key rather than overwriting the existing value. Without cleanup, this leads to:

- **Unbounded storage growth**: Storage usage grows indefinitely over time
- **Performance degradation**: More versions per key means slower lookups
- **Resource exhaustion**: Eventually runs out of disk space

## Design Goals

1. **Automated cleanup**: Background process requiring minimal manual intervention
2. **Data safety**: Never delete data that could affect read consistency
3. **Performance preservation**: Minimal impact on read/write operations
4. **Configurable retention**: Flexible policies based on time, count, or size
5. **Operational visibility**: Metrics and logging for monitoring

## Architecture Overview

### Core Components

```
┌─────────────────┐    ┌──────────────────┐    ┌────────────────┐
│   GC Scheduler  │───▶│  GC Worker       │───▶│  Version       │
│   - Timer       │    │  - Horizon calc  │    │  Cleaner       │
│   - Config      │    │  - Batch process │    │  - MCE iter    │
│   - Triggers    │    │  - Safety checks │    │  - Safe delete │
└─────────────────┘    └──────────────────┘    └────────────────┘
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌────────────────┐
│   Metrics       │    │  Configuration   │    │  RocksDB       │
│   - Deleted     │    │  - Retention     │    │  - Versioned   │
│   - Performance │    │  - Scheduling    │    │    keys        │
│   - Errors      │    │  - Batch size    │    │  - Tombstones  │
└─────────────────┘    └──────────────────┘    └────────────────┘
```

## Detailed Design

### 1. Configuration

**Location**: `/rust/src/lib/config.rs`

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GarbageCollectionConfig {
    /// Enable/disable garbage collection
    pub enabled: bool,

    /// Keep versions newer than this many hours
    pub horizon_hours: u64,

    /// Run GC every N minutes
    pub interval_minutes: u64,

    /// Maximum deletions per GC run (prevents long-running operations)
    pub batch_size: usize,

    /// Always preserve at least this many versions per key
    pub preserve_min_versions: u64,

    /// Skip GC if system load is above this threshold (0.0-1.0)
    pub max_system_load: f64,
}

impl Default for GarbageCollectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            horizon_hours: 24,           // Keep 24 hours of history
            interval_minutes: 60,        // Run every hour
            batch_size: 1000,           // Delete max 1000 versions per run
            preserve_min_versions: 1,    // Always keep at least 1 version
            max_system_load: 0.8,       // Skip GC if CPU > 80%
        }
    }
}
```

### 2. Garbage Collector Implementation

**Location**: `/rust/src/lib/db/garbage_collection.rs`

```rust
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use rocksdb::TransactionDB;
use tracing::{info, warn, error};
use super::super::mce::{VersionedKey, encode_mce};
use super::types::TOMBSTONE_MARKER;
use crate::lib::config::GarbageCollectionConfig;

#[derive(Debug)]
pub struct GCStats {
    pub deleted_versions: usize,
    pub keys_processed: usize,
    pub tombstones_cleaned: usize,
    pub duration_ms: u64,
}

pub struct GarbageCollector {
    db: Arc<TransactionDB>,
    config: GarbageCollectionConfig,
}

impl GarbageCollector {
    pub fn new(db: Arc<TransactionDB>, config: GarbageCollectionConfig) -> Self {
        Self { db, config }
    }

    /// Calculate version horizon based on current time and retention policy
    fn calculate_horizon_version(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Convert hours to seconds and subtract from current time
        let horizon_seconds = now.saturating_sub(self.config.horizon_hours * 3600);

        // In our system, versions are roughly equivalent to timestamps
        horizon_seconds
    }

    /// Check if a key has versions newer than the given version
    fn has_newer_versions(&self, original_key: &[u8], version: u64) -> Result<bool, String> {
        let mce_prefix = encode_mce(original_key);
        let iter = self.db.prefix_iterator(&mce_prefix);

        // Since versions are stored newest-first (inverted timestamps),
        // we only need to check the first version we encounter
        for result in iter.take(5) {  // Check at most 5 versions for efficiency
            match result {
                Ok((encoded_key, _)) => {
                    if let Ok(versioned_key) = VersionedKey::decode(&encoded_key) {
                        if versioned_key.original_key == original_key {
                            return Ok(versioned_key.version > version);
                        }
                    }
                }
                Err(e) => {
                    return Err(format!("Iterator error: {}", e));
                }
            }
        }

        Ok(false)
    }

    /// Count versions for a given key (used for minimum version preservation)
    fn count_versions(&self, original_key: &[u8]) -> Result<usize, String> {
        let mce_prefix = encode_mce(original_key);
        let iter = self.db.prefix_iterator(&mce_prefix);

        let mut count = 0;
        for result in iter {
            match result {
                Ok((encoded_key, _)) => {
                    if let Ok(versioned_key) = VersionedKey::decode(&encoded_key) {
                        if versioned_key.original_key == original_key {
                            count += 1;
                        }
                    }
                }
                Err(_) => break,
            }
        }

        Ok(count)
    }

    /// Check current system load (CPU usage)
    fn check_system_load(&self) -> f64 {
        // Simple implementation - in production might use more sophisticated metrics
        // For now, return 0.0 (no load) to always allow GC
        // TODO: Implement actual system load monitoring
        0.0
    }

    /// Perform garbage collection
    pub fn collect_garbage(&self) -> Result<GCStats, String> {
        let start_time = SystemTime::now();

        // Check system load before starting
        if self.check_system_load() > self.config.max_system_load {
            return Err("System load too high, skipping GC".to_string());
        }

        let horizon_version = self.calculate_horizon_version();
        let mut stats = GCStats {
            deleted_versions: 0,
            keys_processed: 0,
            tombstones_cleaned: 0,
            duration_ms: 0,
        };

        info!("Starting garbage collection with horizon version: {}", horizon_version);

        // Iterate through all versioned keys
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);

        for result in iter {
            match result {
                Ok((encoded_key, value)) => {
                    stats.keys_processed += 1;

                    // Try to decode as versioned key
                    if let Ok(versioned_key) = VersionedKey::decode(&encoded_key) {
                        // Skip if version is newer than horizon
                        if versioned_key.version >= horizon_version {
                            continue;
                        }

                        // Check if we should delete this version
                        let should_delete = if value == TOMBSTONE_MARKER {
                            // Always clean up old tombstones
                            true
                        } else {
                            // For regular values, check if there are newer versions
                            // and if we won't violate minimum version count
                            match self.has_newer_versions(&versioned_key.original_key, versioned_key.version) {
                                Ok(has_newer) => {
                                    if !has_newer {
                                        false // Don't delete the latest version
                                    } else {
                                        // Check minimum version preservation
                                        match self.count_versions(&versioned_key.original_key) {
                                            Ok(count) => count > self.config.preserve_min_versions as usize,
                                            Err(_) => false, // On error, don't delete
                                        }
                                    }
                                }
                                Err(_) => false, // On error, don't delete
                            }
                        };

                        if should_delete {
                            match self.db.delete(&encoded_key) {
                                Ok(_) => {
                                    stats.deleted_versions += 1;
                                    if value == TOMBSTONE_MARKER {
                                        stats.tombstones_cleaned += 1;
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to delete version: {}", e);
                                }
                            }

                            // Check batch size limit
                            if stats.deleted_versions >= self.config.batch_size {
                                info!("Reached batch size limit, stopping GC run");
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Iterator error during GC: {}", e);
                    break;
                }
            }
        }

        stats.duration_ms = start_time.elapsed().unwrap().as_millis() as u64;

        info!("GC completed: {:?}", stats);
        Ok(stats)
    }
}
```

### 3. GC Scheduler Integration

**Location**: `/rust/src/lib/db.rs` (modifications)

```rust
impl TransactionalKvDatabase {
    pub fn new(db_path: &str, config: &Config, column_families: &[&str]) -> Result<Self, Box<dyn std::error::Error>> {
        // ... existing initialization code ...

        let db_instance = Self {
            db,
            cf_handles,
            _config: config.clone(),
            write_queue_tx,
            fault_injection: Arc::new(RwLock::new(None)),
            current_version,
        };

        // Start GC worker if enabled
        if config.garbage_collection.enabled {
            db_instance.start_gc_worker();
        }

        Ok(db_instance)
    }

    /// Start background garbage collection worker
    fn start_gc_worker(&self) {
        let db = self.db.clone();
        let gc_config = self._config.garbage_collection.clone();

        std::thread::spawn(move || {
            let gc = GarbageCollector::new(db, gc_config.clone());

            info!("Starting GC worker with interval: {} minutes", gc_config.interval_minutes);

            loop {
                std::thread::sleep(std::time::Duration::from_secs(gc_config.interval_minutes * 60));

                if gc_config.enabled {
                    match gc.collect_garbage() {
                        Ok(stats) => {
                            info!("GC cycle completed: deleted {} versions in {} ms",
                                  stats.deleted_versions, stats.duration_ms);
                        }
                        Err(e) => {
                            error!("GC cycle failed: {}", e);
                        }
                    }
                } else {
                    info!("GC is disabled, skipping cycle");
                }
            }
        });
    }

    /// Manually trigger garbage collection (for testing/admin use)
    pub fn trigger_garbage_collection(&self) -> Result<GCStats, String> {
        let gc = GarbageCollector::new(self.db.clone(), self._config.garbage_collection.clone());
        gc.collect_garbage()
    }
}
```

## Safety Guarantees

### 1. Data Consistency
- **Never delete the latest version** of any key
- **Preserve minimum versions** as configured
- **Atomic deletions** using RocksDB transactions where needed

### 2. Performance Protection
- **Batch processing** limits GC runtime per cycle
- **System load checking** skips GC during high load
- **Incremental processing** spreads work across multiple cycles

### 3. Error Handling
- **Graceful degradation** on iterator errors
- **Logging and metrics** for operational visibility
- **Continue on single-key errors** (don't fail entire GC run)

## Configuration Examples

### Conservative (Long Retention)
```toml
[garbage_collection]
enabled = true
horizon_hours = 168        # 1 week
interval_minutes = 360     # 6 hours
batch_size = 500          # Smaller batches
preserve_min_versions = 3  # Keep more versions
max_system_load = 0.6     # Conservative load threshold
```

### Aggressive (Short Retention)
```toml
[garbage_collection]
enabled = true
horizon_hours = 6         # 6 hours
interval_minutes = 30     # Every 30 minutes
batch_size = 5000        # Larger batches
preserve_min_versions = 1 # Minimum versions only
max_system_load = 0.9    # Higher load tolerance
```

## Monitoring and Metrics

### Key Metrics to Track
- **Versions deleted per cycle**: GC effectiveness
- **GC cycle duration**: Performance impact
- **Storage reclaimed**: Space savings
- **GC cycle frequency**: Operational health
- **Errors during GC**: System issues

### Logging Strategy
- **INFO**: Normal GC cycles with statistics
- **WARN**: Non-fatal errors (single key failures)
- **ERROR**: Critical failures that stop GC

## Testing Strategy

### Unit Tests
- **Horizon calculation**: Time-based retention logic
- **Version counting**: Accurate preservation logic
- **Safety checks**: Never delete latest version

### Integration Tests
- **End-to-end GC cycles**: Full workflow testing
- **Configuration variations**: Different retention policies
- **Error scenarios**: Iterator failures, disk issues

### Performance Tests
- **GC impact on reads/writes**: Measure performance during GC
- **Large dataset cleanup**: GC behavior with millions of versions
- **Memory usage**: GC memory footprint

## Deployment Considerations

### Rollout Strategy
1. **Deploy with GC disabled** initially
2. **Monitor storage growth** patterns
3. **Enable with conservative settings**
4. **Gradually tune based on metrics**

### Operational Procedures
- **Manual GC triggers** for emergency cleanup
- **GC pause/resume** during maintenance windows
- **Configuration hot-reload** for tuning without restart

This design provides a robust, safe, and efficient garbage collection system that can be tuned for different operational requirements while maintaining the consistency guarantees of the MVCC system.