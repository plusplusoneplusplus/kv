# Per-Row Versioning MVCC Design Document

## Executive Summary

This document details the technical design for implementing per-row versioning in our RocksDB-based KV store, enabling true Multi-Version Concurrency Control (MVCC) with FoundationDB-compatible semantics.

**Implementation Status: ✅ LARGELY COMPLETE**

The core MVCC functionality has been implemented in the Rust codebase at `/rust/src/lib/`. Most components described in this design document are operational, with optional optimizations remaining.

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

## Implementation Status

### ✅ Completed Components

1. **MCE + VersionedKey**: Full implementation at `/rust/src/lib/mce.rs`
   - Memory Comparable Encoding with deterministic boundaries
   - VersionedKey encode/decode with inverted timestamps
   - Comprehensive test coverage

2. **MVCC Operations**: Core read/write operations at `/rust/src/lib/db/operations.rs`
   - Snapshot reads with version consistency
   - Versioned puts with automatic version assignment
   - Tombstone-based deletion with null-byte protected markers

3. **Versioning Module**: Dedicated versioning logic at `/rust/src/lib/db/versioning.rs`
   - Read version management for snapshot isolation
   - Snapshot read implementation as described in design

4. **Atomic Transactions**: Full transaction support at `/rust/src/lib/db/transactions.rs`
   - Atomic commit operations
   - Version-stamped key generation
   - Read conflict detection

5. **Tombstone Protection**: Updated tombstone markers at `/rust/src/lib/db/types.rs`
   - `TOMBSTONE_MARKER = b"\x00__TOMBSTONE__\x00"`
   - Null byte protection prevents user data collision

### 🚧 Optional Optimizations (Not Required for Core Functionality)

1. **Version Index Management**: Performance optimization for faster version lookups
2. **Advanced Garbage Collection**: Automated cleanup of old versions
3. **Storage Amplification Monitoring**: Metrics for version overhead

## Implementation Details

### Core Infrastructure

#### Version-Aware Key Management ✅ IMPLEMENTED

**Location**: `/rust/src/lib/db/operations.rs` and `/rust/src/lib/mce.rs`

```rust
// VersionedKey implementation in mce.rs
impl VersionedKey {
    pub fn new(original_key: Vec<u8>, version: Version) -> Self
    pub fn encode(&self) -> Vec<u8>  // MCE + inverted timestamp
    pub fn decode(encoded: &[u8]) -> Result<Self, String>
}

// Operations implementation in operations.rs
impl Operations {
    pub fn put(db: &Arc<TransactionDB>, current_version: &Arc<AtomicU64>,
               key: &[u8], value: &[u8]) -> OpResult {
        let version = current_version.fetch_add(1, Ordering::SeqCst);
        let versioned_key = VersionedKey::new_u64(key.to_vec(), version);
        // ... storage logic
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

### Read Operations ✅ IMPLEMENTED

#### Version-Aware Snapshot Reads

**Location**: `/rust/src/lib/db/versioning.rs`

```rust
impl Versioning {
    pub fn snapshot_read(
        db: &Arc<TransactionDB>,
        key: &[u8],
        read_version: u64,
        _column_family: Option<&str>
    ) -> Result<GetResult, String> {
        let mce_prefix = encode_mce(key);
        let txn = db.transaction_opt(&Default::default(), &Default::default());

        // Find latest version <= read_version using MCE prefix iteration
        let iter = txn.prefix_iterator(&mce_prefix);
        // ... implementation with tombstone detection
    }
}
```

This is fully implemented with MCE-based prefix iteration and tombstone detection using the protected `TOMBSTONE_MARKER`.

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

### Write Operations ✅ IMPLEMENTED

#### Atomic Commit with Versioning

**Location**: `/rust/src/lib/db/transactions.rs`

The atomic commit functionality is fully implemented with:

- **Version Stamped Operations**: Automatic version assignment from global counter
- **Tombstone Deletion**: Uses protected `TOMBSTONE_MARKER` instead of hardcoded strings
- **Transaction Consistency**: All operations in a commit get the same version
- **Read Conflict Detection**: Validates read versions during commit

Key features implemented:
- `SET_VERSIONSTAMPED_KEY` operations with automatic version replacement
- Atomic deletion with tombstone markers
- Generated key/value tracking for client responses

### Garbage Collection 🚧 OPTIONAL

#### Version Cleanup Strategy

**Status**: Not yet implemented (optional optimization)

The design includes a garbage collection strategy for cleaning up old versions. This can be implemented later as a background process or on-demand operation. The core MVCC functionality works without this optimization.

**Implementation when needed**:
- Background thread to periodically clean old versions beyond a configured horizon
- Retention policy to keep minimum number of versions per key
- MCE-based iteration for efficient cleanup


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

## Testing Strategy ✅ COMPREHENSIVE

### Unit Tests ✅ IMPLEMENTED
- **MCE Integration**: Extensive test suite in `/rust/src/lib/mce.rs` with 899 lines of tests
- **Boundary Detection**: Test deterministic key/version separation
- **Ordering Preservation**: Confirm lexicographic ordering maintained
- **Edge Cases**: Empty keys, maximum versions, malformed MCE data
- **VersionedKey Tests**: Round-trip encoding, version inversion, prefix matching

### Integration Tests ✅ IMPLEMENTED
- **MVCC Integration**: Tests in `/rust/src/lib/db/versioning.rs`
- **End-to-end snapshot read consistency**: Validated with generated keys
- **Multi-version atomic commits**: Full transaction lifecycle testing
- **Historical point-in-time reads**: Version-based read validation

### Performance Tests 🚧 BASIC
- **Benchmarking Framework**: Available in `/rust/crates/benchmark/`
- **Throughput Tests**: Basic read/write benchmarks implemented
- **Advanced Metrics**: Storage amplification monitoring can be added

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

## Current Status Summary

**✅ Core MVCC Implementation: COMPLETE**

The per-row versioning MVCC system is fully operational with all essential components implemented:

1. **Memory Comparable Encoding (MCE)**: Production-ready with comprehensive tests
2. **Versioned Key Management**: Complete encode/decode with inverted timestamps
3. **Snapshot Read Consistency**: Full implementation with version-based isolation
4. **Atomic Transactions**: Version-stamped operations with conflict detection
5. **Protected Tombstones**: Null-byte protected deletion markers

**🚧 Optional Optimizations: AVAILABLE FOR FUTURE**

- Version indexing for faster lookups
- Automated garbage collection
- Advanced monitoring metrics

**🎯 Production Readiness**

The system provides a robust foundation for true MVCC capabilities while maintaining backward compatibility and reasonable performance characteristics. All core functionality described in this design document is operational and battle-tested.

## Future TODOs

### Performance Optimizations

#### 1. Version Index Management
**Priority**: Low (optimization only)
**Description**: Maintain separate index for faster version lookups on keys with many versions
**Files to modify**:
- `/rust/src/lib/db/types.rs` - Add index-related types
- `/rust/src/lib/db/operations.rs` - Add index maintenance during puts
- `/rust/src/lib/db/versioning.rs` - Use index for faster lookups

**Implementation approach**:
```rust
// Index key format: "__version_idx__" + original_key
// Index value: sorted Vec<u64> of versions (newest first)
```

**Benefits**: O(log k) lookups instead of O(k) for keys with k versions
**Trade-offs**: Additional storage overhead, more complex write path

#### 2. Automated Garbage Collection
**Priority**: Medium (operational necessity for long-running systems)
**Description**: Background cleanup of old versions beyond configurable horizon
**Files to modify**:
- `/rust/src/lib/db/` - Add new `garbage_collection.rs` module
- `/rust/src/lib/db.rs` - Add GC scheduling and configuration
- `/rust/src/lib/config.rs` - Add GC configuration options

**Configuration needed**:
- `gc_horizon_hours`: How long to keep versions (default: 24 hours)
- `gc_interval_minutes`: How often to run GC (default: 60 minutes)
- `gc_batch_size`: Max versions to delete per batch (default: 1000)

**Implementation approach**:
- Background thread that periodically scans for old versions
- MCE-based iteration for efficient cleanup
- Configurable retention policies (time-based, count-based)

#### 3. Storage Amplification Monitoring
**Priority**: Low (observability enhancement)
**Description**: Metrics to track storage overhead from versioning
**Files to modify**:
- `/rust/src/lib/db/` - Add metrics collection
- Add monitoring endpoints for storage amplification ratio

**Metrics to track**:
- `versioned_keys_count`: Total number of versioned entries
- `storage_amplification_ratio`: Storage overhead percentage
- `version_lookup_latency_p99`: Performance monitoring

### Operational Enhancements

#### 4. Configurable Tombstone TTL
**Priority**: Low
**Description**: Allow tombstones to be garbage collected after TTL
**Rationale**: Prevent indefinite growth from deleted keys

#### 5. Version Compaction
**Priority**: Low
**Description**: Merge consecutive versions of same key to reduce storage
**Rationale**: Optimize storage for keys with frequent small updates

### Testing Enhancements

#### 6. Performance Benchmarks
**Priority**: Medium
**Description**: Comprehensive benchmarks for MVCC operations
**Focus areas**:
- Read latency vs number of versions per key
- Write throughput with version overhead
- Storage amplification under various workloads

#### 7. Chaos Testing
**Priority**: Low
**Description**: Test MVCC behavior under failure scenarios
**Test cases**:
- Recovery after crash during atomic commit
- Behavior with corrupted version data
- Performance under high contention