# Memory Comparable Encoding (MCE) Specification

## Overview

Memory Comparable Encoding (MCE) is a binary encoding scheme that preserves the lexicographic ordering of original data while providing deterministic boundaries for variable-length keys. This encoding is essential for implementing efficient per-row versioning in RocksDB where keys need to be concatenated with timestamps while maintaining correct sort order and unambiguous parsing.

## Problem Statement

When implementing MVCC with binary key concatenation, we face the fundamental challenge:

```rust
// How do we know where the key ends and version begins?
let combined_key = original_key + version_bytes;

// These could be ambiguous:
b"user:123" + [version] vs b"user:12" + [different_data_that_looks_like_3_plus_version]
```

Traditional solutions like delimiters have collision risks, and fixed-width keys waste space. MCE solves this by making the encoded key self-describing with embedded boundary information.

## MCE Algorithm

### Core Principle

MCE processes data in **8-byte groups** with **marker bytes** that indicate padding, making the encoding self-describing and maintaining lexicographic order.

### Encoding Algorithm

```rust
pub fn encode_mce(data: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();

    for chunk in data.chunks(8) {
        // Create 8-byte group with zero padding
        let mut group = [0u8; 8];
        group[..chunk.len()].copy_from_slice(chunk);
        result.extend_from_slice(&group);

        // Calculate marker: 0xFF - number_of_padding_zeros
        let padding_count = 8 - chunk.len();
        let marker = 0xFF - padding_count as u8;
        result.push(marker);
    }

    result
}
```

### Decoding Algorithm

```rust
pub fn decode_mce(encoded: &[u8]) -> Result<(Vec<u8>, usize), String> {
    let mut original = Vec::new();
    let mut pos = 0;

    while pos < encoded.len() {
        if pos + 9 > encoded.len() {
            return Err("Incomplete MCE group".to_string());
        }

        let group = &encoded[pos..pos + 8];
        let marker = encoded[pos + 8];
        let padding_count = 0xFF - marker;

        if padding_count > 8 {
            return Err("Invalid MCE marker".to_string());
        }

        let data_len = 8 - padding_count as usize;
        original.extend_from_slice(&group[..data_len]);
        pos += 9;

        // Last group detected by non-zero padding
        if padding_count > 0 {
            break;
        }
    }

    Ok((original, pos))
}
```

## Encoding Examples

### Basic Examples

| Original Data | MCE Encoding | Explanation |
|---------------|--------------|-------------|
| `[]` (empty) | `[0,0,0,0,0,0,0,0,247]` | All padding (8 zeros) → marker = 255-8 = 247 |
| `[1,2,3]` | `[1,2,3,0,0,0,0,0,250]` | 5 padding zeros → marker = 255-5 = 250 |
| `[1,2,3,0]` | `[1,2,3,0,0,0,0,0,251]` | 4 padding zeros → marker = 255-4 = 251 |
| `[1,2,3,4,5,6,7,8]` | `[1,2,3,4,5,6,7,8,255,0,0,0,0,0,0,0,0,247]` | No padding in first group → marker = 255, second group all padding → marker = 247 |

### String Examples

```rust
// "abc"
"abc".as_bytes() → [97,98,99,0,0,0,0,0,250]

// "user:123"
"user:123".as_bytes() → [117,115,101,114,58,49,50,51,255,0,0,0,0,0,0,0,0,247]

// "very_long_key_name"
"very_long_key_name".as_bytes() →
[118,101,114,121,95,108,111,110,255,  // "very_lon" + marker 255 (no padding)
 103,95,107,101,121,95,110,97,255,    // "g_key_na" + marker 255 (no padding)
 109,101,0,0,0,0,0,0,249]             // "me" + 6 zeros + marker 249 (6 padding)
```

## MCE Properties

### 1. Order Preservation

MCE maintains lexicographic ordering of the original data:

```rust
let a = "apple";
let b = "banana";
assert!(a < b);
assert!(encode_mce(a.as_bytes()) < encode_mce(b.as_bytes()));
```

### 2. Self-Describing Format

The encoding contains all information needed to decode without external metadata:

```rust
let (original, mce_length) = decode_mce(&encoded_data)?;
// mce_length tells us exactly where MCE data ends
```

### 3. Deterministic Boundaries

When concatenating MCE-encoded keys with other data:

```rust
let mce_key = encode_mce(b"user:123");
let timestamp = 12345u64.to_be_bytes();
let combined = [mce_key, timestamp.to_vec()].concat();

// Can always separate unambiguously:
let (original_key, mce_end) = decode_mce(&combined)?;
let timestamp_bytes = &combined[mce_end..mce_end + 8];
```

## Integration with MVCC

### Versioned Key Encoding

```rust
pub struct VersionedKey {
    pub original_key: Vec<u8>,
    pub version: u64,
}

impl VersionedKey {
    pub fn encode(&self) -> Vec<u8> {
        let mut result = encode_mce(&self.original_key);

        // Append inverted timestamp for newest-first ordering
        let inverted_version = !self.version;
        result.extend_from_slice(&inverted_version.to_be_bytes());

        result
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, String> {
        // Decode MCE portion
        let (original_key, mce_end) = decode_mce(encoded)?;

        // Extract timestamp
        if encoded.len() < mce_end + 8 {
            return Err("Missing version timestamp".to_string());
        }

        let version_bytes = &encoded[mce_end..mce_end + 8];
        let version_array: [u8; 8] = version_bytes.try_into()
            .map_err(|_| "Invalid version bytes")?;
        let inverted_version = u64::from_be_bytes(version_array);
        let version = !inverted_version;

        Ok(VersionedKey { original_key, version })
    }
}
```

### Prefix Iteration

MCE enables perfect prefix iteration for version lookup:

```rust
impl TransactionalKvDatabase {
    fn find_versions_for_key(&self, key: &[u8]) -> impl Iterator<Item = (Vec<u8>, u64)> {
        let mce_prefix = encode_mce(key);

        self.db.prefix_iterator(&mce_prefix)
            .filter_map(|(encoded_key, value)| {
                VersionedKey::decode(&encoded_key)
                    .map(|vk| (value.to_vec(), vk.version))
                    .ok()
            })
    }

    fn find_version_at_or_before(&self, key: &[u8], read_version: u64) -> Option<Vec<u8>> {
        // MCE ensures this only matches exact key, not prefixes of other keys
        for (value, version) in self.find_versions_for_key(key) {
            if version <= read_version {
                return Some(value); // First match due to newest-first ordering
            }
        }
        None
    }
}
```

## Performance Characteristics

### Space Overhead

| Original Key Length | MCE Overhead | Total Overhead |
|-------------------|--------------|----------------|
| 1-8 bytes | 1 marker byte | 12.5% - 100% |
| 9-16 bytes | 2 marker bytes | 12.5% - 22% |
| 17-24 bytes | 3 marker bytes | 12.5% - 18% |
| n bytes | ⌈n/8⌉ marker bytes | ~12.5% for large keys |

### Time Complexity

- **Encoding**: O(n) where n is original key length
- **Decoding**: O(n) where n is original key length
- **Prefix Matching**: O(1) boundary detection vs O(n) string searching

### RocksDB Benefits

- **Perfect Prefix Iteration**: No false positives in key matching
- **Optimal Sort Order**: Maintains lexicographic ordering
- **Efficient Parsing**: Deterministic boundary detection
- **Cache Friendly**: Fixed 9-byte group structure

## Implementation Guidelines

### Error Handling

```rust
#[derive(Debug, PartialEq)]
pub enum MCEError {
    IncompleteGroup,
    InvalidMarker(u8),
    TruncatedData,
}

impl fmt::Display for MCEError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MCEError::IncompleteGroup => write!(f, "MCE data contains incomplete 9-byte group"),
            MCEError::InvalidMarker(m) => write!(f, "Invalid MCE marker byte: {}", m),
            MCEError::TruncatedData => write!(f, "MCE data truncated unexpectedly"),
        }
    }
}
```

### Validation

```rust
pub fn validate_mce(encoded: &[u8]) -> Result<(), MCEError> {
    if encoded.len() % 9 != 0 && encoded.len() > 0 {
        return Err(MCEError::IncompleteGroup);
    }

    for chunk in encoded.chunks(9) {
        if chunk.len() != 9 {
            return Err(MCEError::IncompleteGroup);
        }

        let marker = chunk[8];
        let padding_count = 0xFF - marker;
        if padding_count > 8 {
            return Err(MCEError::InvalidMarker(marker));
        }
    }

    Ok(())
}
```

### Testing Strategy

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mce_roundtrip() {
        let test_cases = vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"hello".to_vec(),
            b"user:123".to_vec(),
            b"very_long_key_that_spans_multiple_groups".to_vec(),
            vec![0, 1, 2, 255, 254], // Binary data
        ];

        for original in test_cases {
            let encoded = encode_mce(&original);
            let (decoded, _) = decode_mce(&encoded).unwrap();
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn test_mce_ordering() {
        let keys = vec![b"a", b"aa", b"ab", b"b", b"ba"];
        let mut encoded_keys: Vec<_> = keys.iter()
            .map(|k| encode_mce(k))
            .collect();

        encoded_keys.sort();

        for (i, encoded) in encoded_keys.iter().enumerate() {
            let (decoded, _) = decode_mce(encoded).unwrap();
            assert_eq!(decoded, keys[i].to_vec());
        }
    }

    #[test]
    fn test_versioned_key_boundary_detection() {
        let vk = VersionedKey {
            original_key: b"user:123".to_vec(),
            version: 12345,
        };

        let encoded = vk.encode();
        let decoded = VersionedKey::decode(&encoded).unwrap();

        assert_eq!(vk.original_key, decoded.original_key);
        assert_eq!(vk.version, decoded.version);
    }
}
```

## Migration Considerations

### From Simple Binary Concatenation

If migrating from a system using simple binary concatenation:

1. **Dual-Write Phase**: Write both formats during transition
2. **MCE Prefix Detection**: Use MCE marker validation to distinguish formats
3. **Gradual Migration**: Convert existing keys to MCE format over time

### Compatibility Layer

```rust
pub fn decode_legacy_or_mce(encoded: &[u8]) -> Result<VersionedKey, String> {
    // Try MCE first
    if let Ok((key, mce_end)) = decode_mce(encoded) {
        if encoded.len() >= mce_end + 8 {
            let version_bytes = &encoded[mce_end..mce_end + 8];
            let version = u64::from_be_bytes(version_bytes.try_into().unwrap());
            return Ok(VersionedKey { original_key: key, version: !version });
        }
    }

    // Fall back to legacy format (e.g., length-prefixed)
    decode_legacy_format(encoded)
}
```

## Conclusion

Memory Comparable Encoding provides an elegant solution to the key boundary detection problem in MVCC implementations. By embedding structural information directly in the encoding, MCE eliminates ambiguity while preserving ordering properties essential for efficient database operations.

The 12.5% space overhead for large keys is minimal compared to the benefits of deterministic parsing, perfect prefix iteration, and elimination of delimiter collision risks. For high-performance key-value stores requiring MVCC capabilities, MCE represents a proven approach to variable-length key encoding.