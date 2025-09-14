# Per-Row Versioning MVCC Design Document

## Executive Summary

This document details the technical design for implementing per-row versioning in our RocksDB-based KV store, enabling true Multi-Version Concurrency Control (MVCC) with FoundationDB-compatible semantics.

## Architecture Overview

### Current State
```
Global Version Counter: __version_counter__ → 12345
Data Keys: user:123 → "john"
```

### Target State
```
Global Version Counter: __version_counter__ → 12345
Versioned Keys: user:123@12340 → "john"
                user:123@12343 → "john_updated"
Version Index: __version_idx__user:123 → [12340, 12343]
```

## Key Design Decisions

### 1. Key Encoding Strategy

**Selected: Memory Comparable Encoding (MCE) + Timestamp**

MCE is detailed in [memory-comparable-encoding.md](memory-comparable-encoding.md). The key benefits:
- **Self-describing format**: Embedded boundary markers eliminate parsing ambiguity
- **Order preservation**: Maintains lexicographic ordering of original keys
- **Perfect prefix iteration**: No false positives in RocksDB prefix searches
- **Deterministic boundaries**: Unambiguous separation between key and version data

```rust
// Format: MCE(original_key) + 8_byte_inverted_timestamp
fn encode_versioned_key(original_key: &[u8], version: u64) -> Vec<u8> {
    let mut result = encode_mce(original_key);  // MCE with 9-byte groups (8 data + 1 marker)
    let inverted_version = !version;  // Bitwise inversion for reverse chronological order
    result.extend_from_slice(&inverted_version.to_be_bytes());
    result
}
// Example: MCE(b"user:123") = [117,115,101,114,58,49,50,51,255,0,0,0,0,0,0,0,0,247]
//          + inverted version [0xFF,0xFF,0xFF,0xFF,0xFF,0xFF,0xCF,0xC7] (for version 12345)
```

**Why MCE + Timestamp:**
- ✅ Preserves key prefix for efficient range queries
- ✅ Enables perfect RocksDB prefix iteration (no false positives)
- ✅ Newest versions sort first (optimal for most reads)
- ✅ Zero collision risk - deterministic boundary detection
- ✅ Self-describing format with embedded structure
- ✅ Maintains lexicographic ordering of original keys
- ✅ Battle-tested encoding algorithm

**Rejected Alternatives:**
- **Character Delimiters** (`@`, `~`, etc.): Risk of collision with user keys
- **Length Prefixing**: Additional complexity, less efficient for small keys
- **Simple Binary Concatenation**: Ambiguous key boundaries
- **Separate Metadata Table**: Requires additional lookups, complex consistency

### 2. Version Lookup Algorithm

**MCE-Based Prefix Iteration Strategy:**

MCE's deterministic boundary detection enables perfect prefix matching without false positives:

```rust
fn find_version_at_or_before(key: &[u8], read_version: u64) -> Option<Vec<u8>> {
    // MCE prefix ensures we only match keys with this exact original_key
    // No risk of matching longer keys that happen to start with same bytes
    let mce_prefix = encode_mce(key);
    let iter = self.db.prefix_iterator(&mce_prefix);

    for (found_key, value) in iter {
        // MCE decode guarantees successful parsing and exact key match
        if let Ok(versioned_key) = VersionedKey::decode(&found_key) {
            // Double-check: MCE should ensure this, but verify exact match
            if versioned_key.original_key == key {
                // Since versions are inverted, iteration is newest-first
                // First version <= read_version is our snapshot target
                if versioned_key.version <= read_version {
                    return Some(value.to_vec());
                }
            }
        }
    }
    None
}
```

### 3. Storage Layout

#### Versioned Data Storage
```
Key Space Layout:
┌─────────────────┬──────────────────┬─────────────────┐
│ Versioned Keys  │ Versioned Keys   │ Metadata        │
│ user:123{v_new} │ user:456{v_new}  │ __version_cnt__ │
│ user:123{v_old} │ user:456{v_old}  │ __gc_horizon__  │
│                 │                  │                 │
└─────────────────┴──────────────────┴─────────────────┘

Where {v_new} represents newer inverted version bytes
Where {v_old} represents older inverted version bytes
```

#### MCE-Based Version Encoding Format

This implementation follows the MCE specification from [memory-comparable-encoding.md](memory-comparable-encoding.md):

```rust
use super::mce::{encode_mce, decode_mce};

pub struct VersionedKey {
    pub original_key: Vec<u8>,
    pub version: u64,
}

impl VersionedKey {
    /// Encode key using MCE + inverted timestamp
    /// MCE processes data in 8-byte groups with marker bytes for self-describing format
    pub fn encode(&self) -> Vec<u8> {
        let mut result = encode_mce(&self.original_key);

        // Append inverted version for reverse chronological ordering (newer versions first)
        let inverted_version = !self.version;
        result.extend_from_slice(&inverted_version.to_be_bytes());
        result
    }

    /// Decode MCE-encoded versioned key
    /// MCE guarantees deterministic boundary detection between key and version
    pub fn decode(encoded: &[u8]) -> Result<Self, String> {
        // MCE decode returns (original_data, boundary_position)
        let (original_key, mce_end) = decode_mce(encoded)
            .map_err(|e| format!("MCE decode error: {}", e))?;

        // Extract 8-byte version timestamp after MCE boundary
        if encoded.len() < mce_end + 8 {
            return Err("Missing 8-byte version timestamp".to_string());
        }

        let version_bytes = &encoded[mce_end..mce_end + 8];
        let version_array: [u8; 8] = version_bytes.try_into()
            .map_err(|_| "Invalid version bytes")?;
        let inverted_version = u64::from_be_bytes(version_array);
        let version = !inverted_version;  // Un-invert to get original version

        Ok(VersionedKey {
            original_key,
            version,
        })
    }
}
```

## Implementation Details

### Core Infrastructure

#### Version-Aware Key Management
```rust
impl TransactionalKvDatabase {
    /// Encode key with version for storage
    fn encode_versioned_key(&self, key: &[u8], version: u64) -> Vec<u8> {
        VersionedKey {
            original_key: key.to_vec(),
            version
        }.encode()
    }

    /// Find latest version of key <= read_version
    fn find_key_at_version(&self, key: &[u8], read_version: u64) -> Option<Vec<u8>> {
        // Implementation as described above
    }

    /// Store value with version
    fn put_versioned(&self, key: &[u8], value: &[u8], version: u64) -> Result<(), String> {
        let versioned_key = self.encode_versioned_key(key, version);
        self.db.put(versioned_key, value)
            .map_err(|e| format!("Failed to store versioned key: {}", e))
    }
}
```

#### Version Index Management
```rust
/// Optional: Maintain index for faster version lookups
impl TransactionalKvDatabase {
    fn update_version_index(&self, key: &[u8], version: u64) -> Result<(), String> {
        let index_key = format!("__version_idx__{}", String::from_utf8_lossy(key));

        // Read existing versions
        let mut versions: Vec<u64> = match self.db.get(&index_key) {
            Ok(Some(data)) => bincode::deserialize(&data).unwrap_or_default(),
            _ => Vec::new(),
        };

        // Add new version and sort (newest first due to inversion in main storage)
        versions.push(version);
        versions.sort_unstable_by(|a, b| b.cmp(a)); // Descending order

        // Store updated index
        let serialized = bincode::serialize(&versions)?;
        self.db.put(&index_key, serialized)
            .map_err(|e| format!("Failed to update version index: {}", e))
    }
}
```

### Read Operations

#### Version-Aware Snapshot Reads
```rust
impl TransactionalKvDatabase {
    pub fn snapshot_read(&self, key: &[u8], read_version: u64, column_family: Option<&str>) -> Result<GetResult, String> {
        if key.is_empty() {
            return Err("key cannot be empty".to_string());
        }

        // Find the appropriate version
        match self.find_key_at_version(key, read_version) {
            Some(value) => Ok(GetResult { value, found: true }),
            None => Ok(GetResult { value: Vec::new(), found: false })
        }
    }
}
```

#### Version-Aware Range Queries
```rust
impl TransactionalKvDatabase {
    pub fn snapshot_get_range(
        &self,
        begin_key: &[u8],
        end_key: &[u8],
        begin_offset: i32,
        begin_or_equal: bool,
        end_offset: i32,
        end_or_equal: bool,
        read_version: u64,
        limit: Option<i32>
    ) -> GetRangeResult {
        let mut result_keys = Vec::new();
        let mut seen_original_keys = std::collections::HashSet::new();

        // Iterate over the key range
        let iter = self.db.iterator(IteratorMode::From(begin_key, Direction::Forward));

        for (key, _) in iter {
            if key >= end_key { break; }

            // Extract original key from versioned key
            if let Ok(versioned) = VersionedKey::decode(&key) {
                // Skip if this version is newer than our read version
                if versioned.version > read_version { continue; }

                let original_key = versioned.original_key;

                // Only include each original key once (since versions are ordered newest-first,
                // the first valid version we encounter is the correct one)
                if !seen_original_keys.contains(&original_key) {
                    if let Some(value) = self.find_key_at_version(&original_key, read_version) {
                        result_keys.push(KeyValue {
                            key: original_key.clone(),
                            value,
                        });
                        seen_original_keys.insert(original_key);
                    }
                }
            }

            if let Some(limit) = limit {
                if result_keys.len() >= limit as usize { break; }
            }
        }

        GetRangeResult {
            key_values: result_keys,
            success: true,
            error: String::new(),
            has_more: false, // TODO: Implement proper pagination
        }
    }
}
```

### Write Operations

#### Atomic Commit with Versioning
```rust
impl TransactionalKvDatabase {
    pub fn atomic_commit(&self, request: AtomicCommitRequest) -> AtomicCommitResult {
        let mut write_opts = WriteOptions::default();
        let mut txn_opts = TransactionOptions::default();
        txn_opts.set_snapshot(true);

        let rocksdb_txn = self.db.transaction_opt(&write_opts, &txn_opts);

        // Get commit version for all operations
        let commit_version = self.increment_version();
        let mut generated_keys = Vec::new();
        let mut generated_values = Vec::new();

        // Apply all operations with the same version
        for operation in &request.operations {
            match operation.op_type.as_str() {
                "set" => {
                    if let Some(value) = &operation.value {
                        let versioned_key = self.encode_versioned_key(&operation.key, commit_version);
                        if let Err(e) = rocksdb_txn.put(&versioned_key, value) {
                            return AtomicCommitResult::error(&format!("Set operation failed: {}", e));
                        }
                        // Update version index
                        let _ = self.update_version_index(&operation.key, commit_version);
                    }
                }
                "delete" => {
                    // Store tombstone marker with new version
                    let versioned_key = self.encode_versioned_key(&operation.key, commit_version);
                    if let Err(e) = rocksdb_txn.put(&versioned_key, b"__DELETED__") {
                        return AtomicCommitResult::error(&format!("Delete operation failed: {}", e));
                    }
                    let _ = self.update_version_index(&operation.key, commit_version);
                }
                _ => return AtomicCommitResult::error("Unknown operation type"),
            }
        }

        // Commit transaction atomically
        match rocksdb_txn.commit() {
            Ok(_) => AtomicCommitResult {
                success: true,
                error: String::new(),
                error_code: None,
                committed_version: Some(commit_version),
                generated_keys,
                generated_values,
            },
            Err(e) => AtomicCommitResult::error(&format!("Transaction commit failed: {}", e)),
        }
    }
}
```

### Garbage Collection

#### Version Cleanup Strategy
```rust
impl TransactionalKvDatabase {
    /// Clean up versions older than horizon
    pub fn garbage_collect_versions(&self, horizon_version: u64) -> Result<usize, String> {
        let mut deleted_count = 0;

        // Iterate through all versioned keys
        let iter = self.db.iterator(IteratorMode::Start);

        for (key, _) in iter {
            if let Ok(versioned) = VersionedKey::decode(&key) {
                if versioned.version < horizon_version {
                    // Keep at least one version for each key
                    if self.has_newer_version(&versioned.original_key, versioned.version)? {
                        self.db.delete(&key)?;
                        deleted_count += 1;
                    }
                }
            }
        }

        Ok(deleted_count)
    }

    fn has_newer_version(&self, original_key: &[u8], version: u64) -> Result<bool, String> {
        let mce_prefix = encode_mce(original_key);
        let iter = self.db.prefix_iterator(&mce_prefix);

        for (key, _) in iter {
            if let Ok(versioned) = VersionedKey::decode(&key) {
                // MCE guarantees exact key match, no false positives
                if versioned.original_key == original_key && versioned.version > version {
                    return Ok(true);
                }
                // Since versions are stored newest-first (inverted), we can break early
                // if we find a version <= our target, as no newer versions will follow
                if versioned.original_key == original_key && versioned.version <= version {
                    break;
                }
            }
        }
        Ok(false)
    }
}
```


## Performance Considerations

### Read Performance
- **Latest Version Reads**: O(log n) with prefix iteration
- **Historical Reads**: O(log n + k) where k is number of versions to scan
- **Range Queries**: O(n log n) for deduplication across versions

### Write Performance
- **Version Storage**: Additional write per operation
- **Index Maintenance**: Optional, can be disabled for write-heavy workloads
- **Garbage Collection**: Background process, minimal impact

### Storage Overhead

Based on MCE specification (see [memory-comparable-encoding.md](memory-comparable-encoding.md)):

| Original Key Length | MCE Overhead | Version Bytes | Total Overhead |
|-------------------|--------------|---------------|----------------|
| 1-8 bytes | 1 marker byte | 8 bytes | 112.5% - 900% |
| 9-16 bytes | 2 marker bytes | 8 bytes | 62.5% - 111% |
| 17-24 bytes | 3 marker bytes | 8 bytes | 45.8% - 64.7% |
| 64 bytes | 8 marker bytes | 8 bytes | 25% |
| Large keys (>64B) | ~12.5% MCE | 8 bytes | ~12.5% + fixed 8B |

**Key Insights:**
- **Small Keys**: High relative overhead due to fixed 8-byte version cost
- **Large Keys**: MCE overhead approaches 12.5%, version cost becomes negligible
- **Typical Web Keys** (8-64 bytes): 25-111% overhead, acceptable for MVCC benefits

## Testing Strategy

### Unit Tests
- **MCE Integration**: Verify MCE encode/decode with versioned keys
- **Boundary Detection**: Test deterministic key/version separation
- **Ordering Preservation**: Confirm lexicographic ordering maintained
- **Edge Cases**: Empty keys, maximum versions, malformed MCE data

### Integration Tests
- End-to-end snapshot read consistency
- Multi-version atomic commits
- Historical point-in-time reads

### Performance Tests
- Read/write throughput benchmarks
- Memory usage with multiple versions
- Garbage collection performance

## Monitoring and Observability

### Metrics
- `versioned_keys_count`: Number of versioned keys in database
- `version_lookup_latency`: Time to find version for read
- `garbage_collection_deleted`: Versions cleaned up per GC run
- `storage_amplification_ratio`: Storage overhead from versioning

### Alerts
- High version lookup latency (> 10ms P99)
- Excessive storage growth (> 2x amplification)
- Garbage collection failures

This design provides a robust foundation for true MVCC capabilities while maintaining backward compatibility and reasonable performance characteristics.