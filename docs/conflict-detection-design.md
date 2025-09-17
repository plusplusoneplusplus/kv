# Read/Write Conflict Detection Design

## Executive Summary

This document outlines the design for implementing proper read/write conflict detection in our MVCC-enabled transactional key-value store. The current implementation has placeholder conflict detection that always returns `false`. This design provides a robust, production-ready conflict detection system that ensures transaction isolation and consistency.

## Problem Statement

### Current State
- `has_key_been_modified_since()` always returns `false` (placeholder)
- No actual conflict detection during transaction commits
- Potential for lost updates and inconsistent reads
- Missing serializable isolation guarantee

### Production Requirements
- **Read-Write Conflicts**: Detect when a key read during transaction was modified before commit
- **Write-Write Conflicts**: Detect concurrent writes to the same key
- **Range Conflicts**: Detect conflicts on range operations
- **Performance**: Minimize overhead of conflict checking
- **Correctness**: Ensure serializable isolation

## Architecture Overview

### Conflict Detection Components

```
┌─────────────────┐    ┌──────────────────┐    ┌────────────────┐
│   Read Tracker  │───▶│  Conflict        │───▶│  Transaction   │
│   - Track reads │    │  Detector        │    │  Validator     │
│   - Version log │    │  - Key conflicts │    │  - Abort/retry │
│   - Range reads │    │  - Range check   │    │  - Error codes │
└─────────────────┘    └──────────────────┘    └────────────────┘
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌────────────────┐
│   Read Set      │    │  Write Set       │    │  Version       │
│   Cache         │    │  Cache           │    │  Oracle        │
│   - Per txn     │    │  - Pending       │    │  - Global      │
│   - Key->Ver    │    │  - Committed     │    │  - Timestamp   │
└─────────────────┘    └──────────────────┘    └────────────────┘
```

## Detailed Design

### 1. Read Set Tracking

**Purpose**: Track all keys and their versions read during a transaction.

**Implementation**: `/rust/src/lib/db/conflict_detection.rs`

```rust
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use crate::lib::mce::{VersionedKey, encode_mce};

#[derive(Debug, Clone)]
pub struct ReadEntry {
    pub key: Vec<u8>,
    pub read_version: u64,
    pub actual_version: u64,  // Version of data actually read
}

#[derive(Debug, Clone)]
pub struct RangeReadEntry {
    pub begin_key: Vec<u8>,
    pub end_key: Vec<u8>,
    pub read_version: u64,
    pub keys_found: HashSet<Vec<u8>>,  // Keys that existed in range
}

pub struct ReadSet {
    /// Point reads: key -> version read
    point_reads: HashMap<Vec<u8>, ReadEntry>,
    /// Range reads: begin_key -> range info
    range_reads: Vec<RangeReadEntry>,
}

impl ReadSet {
    pub fn new() -> Self {
        Self {
            point_reads: HashMap::new(),
            range_reads: Vec::new(),
        }
    }

    pub fn add_read(&mut self, key: &[u8], read_version: u64, actual_version: u64) {
        self.point_reads.insert(key.to_vec(), ReadEntry {
            key: key.to_vec(),
            read_version,
            actual_version,
        });
    }

    pub fn add_range_read(&mut self, begin_key: &[u8], end_key: &[u8],
                         read_version: u64, keys_found: HashSet<Vec<u8>>) {
        self.range_reads.push(RangeReadEntry {
            begin_key: begin_key.to_vec(),
            end_key: end_key.to_vec(),
            read_version,
            keys_found,
        });
    }
}
```

### 2. Conflict Detection Engine

```rust
pub struct ConflictDetector {
    db: Arc<TransactionDB>,
}

impl ConflictDetector {
    pub fn new(db: Arc<TransactionDB>) -> Self {
        Self { db }
    }

    /// Check if a specific key has been modified since a given version
    pub fn has_key_been_modified_since(&self, key: &[u8], since_version: u64) -> Result<bool, String> {
        let mce_prefix = encode_mce(key);
        let iter = self.db.prefix_iterator(&mce_prefix);

        for result in iter.take(10) {  // Limit iterations for performance
            match result {
                Ok((encoded_key, _)) => {
                    if let Ok(versioned_key) = VersionedKey::decode(&encoded_key) {
                        if versioned_key.original_key == key {
                            // Found a version of this key - check if it's newer
                            if versioned_key.version > since_version {
                                return Ok(true);  // Conflict detected
                            }
                            // Since versions are stored newest-first (inverted timestamps),
                            // if we find a version <= since_version, no newer versions exist
                            return Ok(false);
                        }
                    }
                }
                Err(e) => {
                    return Err(format!("Iterator error during conflict check: {}", e));
                }
            }
        }

        Ok(false)  // No versions found or no conflicts
    }

    /// Check for range conflicts - detect if new keys appeared in range
    pub fn has_range_been_modified_since(&self, begin_key: &[u8], end_key: &[u8],
                                       since_version: u64, original_keys: &HashSet<Vec<u8>>)
                                       -> Result<bool, String> {
        // Get current keys in range at current version
        let current_keys = self.get_keys_in_range(begin_key, end_key)?;

        // Check if any new keys appeared
        for key in &current_keys {
            if !original_keys.contains(key) {
                // New key appeared - check when it was written
                if self.has_key_been_modified_since(key, since_version)? {
                    return Ok(true);  // Conflict: new key added after read
                }
            }
        }

        // Check if any original keys were modified
        for key in original_keys {
            if self.has_key_been_modified_since(key, since_version)? {
                return Ok(true);  // Conflict: existing key modified
            }
        }

        Ok(false)
    }

    fn get_keys_in_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<HashSet<Vec<u8>>, String> {
        let mut keys = HashSet::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::From(begin_key, rocksdb::Direction::Forward));

        for result in iter {
            match result {
                Ok((key_bytes, _)) => {
                    if key_bytes.as_ref() >= end_key {
                        break;  // Beyond range
                    }

                    if let Ok(versioned_key) = VersionedKey::decode(&key_bytes) {
                        keys.insert(versioned_key.original_key);
                    }
                }
                Err(e) => {
                    return Err(format!("Iterator error: {}", e));
                }
            }
        }

        Ok(keys)
    }

    /// Main conflict detection method for transaction commit
    pub fn check_conflicts(&self, read_set: &ReadSet, read_conflict_keys: &[Vec<u8>])
                          -> Result<Vec<String>, String> {
        let mut conflicts = Vec::new();

        // Check explicit read conflict keys
        for key in read_conflict_keys {
            if let Some(read_entry) = read_set.point_reads.get(key) {
                if self.has_key_been_modified_since(key, read_entry.read_version)? {
                    conflicts.push(format!("Read conflict on key: {:?}", key));
                }
            }
        }

        // Check all point reads in read set
        for (key, read_entry) in &read_set.point_reads {
            if self.has_key_been_modified_since(key, read_entry.read_version)? {
                conflicts.push(format!("Read-write conflict on key: {:?}", key));
            }
        }

        // Check range reads
        for range_read in &read_set.range_reads {
            if self.has_range_been_modified_since(
                &range_read.begin_key,
                &range_read.end_key,
                range_read.read_version,
                &range_read.keys_found
            )? {
                conflicts.push(format!("Range conflict on range: {:?} to {:?}",
                                     range_read.begin_key, range_read.end_key));
            }
        }

        Ok(conflicts)
    }
}
```

### 3. Integration with Transaction System

**Modifications to existing files**:

#### Update `versioning.rs`:

```rust
impl Versioning {
    /// Enhanced conflict detection with proper version checking
    pub fn has_key_been_modified_since(
        db: &Arc<TransactionDB>,
        key: &[u8],
        since_version: u64
    ) -> Result<bool, String> {
        let detector = ConflictDetector::new(db.clone());
        detector.has_key_been_modified_since(key, since_version)
    }
}
```

#### Update `transactions.rs`:

```rust
impl Transactions {
    /// Enhanced conflict checking with detailed error reporting
    pub fn check_conflicts(
        db: &Arc<TransactionDB>,
        request: &AtomicCommitRequest,
        read_set: &ReadSet,
        current_version: u64
    ) -> Option<AtomicCommitResult> {
        if request.read_version >= current_version {
            return None;  // No conflicts possible
        }

        let detector = ConflictDetector::new(db.clone());

        match detector.check_conflicts(read_set, &request.read_conflict_keys) {
            Ok(conflicts) => {
                if !conflicts.is_empty() {
                    Some(AtomicCommitResult {
                        success: false,
                        error: conflicts.join("; "),
                        error_code: Some("CONFLICT_DETECTED".to_string()),
                        committed_version: None,
                        generated_keys: Vec::new(),
                        generated_values: Vec::new(),
                    })
                } else {
                    None  // No conflicts
                }
            }
            Err(e) => {
                Some(AtomicCommitResult {
                    success: false,
                    error: format!("Conflict detection failed: {}", e),
                    error_code: Some("CONFLICT_CHECK_ERROR".to_string()),
                    committed_version: None,
                    generated_keys: Vec::new(),
                    generated_values: Vec::new(),
                })
            }
        }
    }
}
```

### 4. Read Set Instrumentation

**Integration points** where reads must be tracked:

```rust
// In operations.rs - track point reads
impl Operations {
    pub fn get_with_read_tracking(
        db: &Arc<TransactionDB>,
        current_version: &Arc<AtomicU64>,
        key: &[u8],
        read_set: &mut ReadSet,
        read_version: u64
    ) -> OpResult {
        let result = Self::get(db, current_version, key);

        // Track this read for conflict detection
        if let Ok(get_result) = &result {
            let actual_version = Self::get_key_version(db, key)?;
            read_set.add_read(key, read_version, actual_version);
        }

        result
    }
}

// In range operations - track range reads
impl RangeOperations {
    pub fn get_range_with_read_tracking(
        db: &Arc<TransactionDB>,
        begin_key: &[u8],
        end_key: &[u8],
        read_version: u64,
        read_set: &mut ReadSet,
        // ... other parameters
    ) -> GetRangeResult {
        let result = Self::get_range(/* parameters */);

        // Track range read
        if result.success {
            let keys_found: HashSet<Vec<u8>> = result.key_values.iter()
                .map(|kv| kv.key.clone())
                .collect();
            read_set.add_range_read(begin_key, end_key, read_version, keys_found);
        }

        result
    }
}
```

## Performance Considerations

### Optimization Strategies

1. **Version Index Caching**: Cache recent version information for hot keys
2. **Conflict Detection Batching**: Check multiple keys in single iteration
3. **Early Termination**: Stop checking once conflict found
4. **Read Set Pruning**: Limit size of read sets for large transactions

### Performance Benchmarks

**Target Metrics**:
- Conflict detection overhead: < 5% of transaction latency
- Memory overhead: < 100KB per transaction for typical workloads
- False positive rate: < 0.1%

### Memory Management

```rust
pub struct ConflictDetectionConfig {
    pub max_read_set_size: usize,           // Default: 10,000
    pub max_range_reads: usize,             // Default: 100
    pub version_cache_size: usize,          // Default: 100,000
    pub conflict_check_timeout_ms: u64,     // Default: 1000
}
```

## Error Handling Strategy

### Error Types

```rust
#[derive(Debug, Clone)]
pub enum ConflictDetectionError {
    DatabaseError(String),
    VersionDecodingError(String),
    TimeoutError,
    MemoryLimitExceeded,
}

pub type ConflictResult<T> = Result<T, ConflictDetectionError>;
```

### Error Recovery

1. **Database Errors**: Retry with exponential backoff
2. **Timeout Errors**: Return conflict detected (conservative)
3. **Memory Errors**: Clear read sets and continue
4. **Decoding Errors**: Skip corrupted entries, log warning

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_read_write_conflict() {
        // 1. Transaction A reads key X at version 5
        // 2. Transaction B writes key X at version 6
        // 3. Transaction A tries to commit - should detect conflict
    }

    #[test]
    fn test_range_conflict_detection() {
        // 1. Transaction A reads range [a, c) at version 5, finds keys [a1, b1]
        // 2. Transaction B adds key b2 at version 6
        // 3. Transaction A tries to commit - should detect range conflict
    }

    #[test]
    fn test_no_false_positives() {
        // Ensure non-conflicting concurrent transactions don't interfere
    }

    #[test]
    fn test_conflict_detection_performance() {
        // Measure overhead with large read sets
    }
}
```

### Integration Tests

1. **Concurrent Transaction Scenarios**: Simulate real conflict conditions
2. **Large Read Set Handling**: Test memory and performance limits
3. **Range Conflict Edge Cases**: Empty ranges, overlapping ranges
4. **Error Recovery**: Database failures during conflict checking

### Stress Tests

1. **High Contention**: Many transactions touching same keys
2. **Large Transactions**: Transactions with thousands of reads
3. **Mixed Workloads**: Point reads, range reads, and writes combined

## Deployment Considerations

### Configuration

```rust
// In config.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictDetectionConfig {
    pub enabled: bool,                      // Default: true
    pub strict_mode: bool,                  // Default: true (serializable)
    pub max_read_set_size: usize,          // Default: 10000
    pub max_conflict_check_time_ms: u64,   // Default: 1000
    pub version_cache_enabled: bool,        // Default: true
    pub version_cache_size_mb: usize,      // Default: 100
}
```

### Rollout Strategy

1. **Phase 1**: Deploy with logging only (no actual conflicts)
2. **Phase 2**: Enable conflict detection with high thresholds
3. **Phase 3**: Tune thresholds based on production metrics
4. **Phase 4**: Enable strict serializable isolation

### Monitoring

**Key Metrics**:
- Conflict detection rate
- False positive rate
- Average conflict check latency
- Read set size distribution
- Memory usage for conflict detection

## Future Enhancements

1. **Optimistic Conflict Detection**: Assume no conflicts, validate at commit
2. **Conflict Prediction**: Machine learning to predict likely conflicts
3. **Distributed Conflict Detection**: Support for multi-node deployments
4. **Read Set Compression**: Compress large read sets to save memory

This design provides a robust foundation for production-grade conflict detection while maintaining the performance characteristics needed for a high-throughput transactional system.