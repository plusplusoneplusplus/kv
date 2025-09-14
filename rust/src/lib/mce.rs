//! Memory Comparable Encoding (MCE) Implementation
//!
//! This module implements Memory Comparable Encoding as specified in the design document.
//! MCE provides deterministic boundary detection for variable-length keys while preserving
//! lexicographic ordering, making it ideal for MVCC implementations in key-value stores.

use std::fmt;

/// MCE processing errors
#[derive(Debug, PartialEq)]
pub enum MCEError {
    /// MCE data contains incomplete 9-byte group
    IncompleteGroup,
    /// Invalid MCE marker byte
    InvalidMarker(u8),
    /// MCE data truncated unexpectedly
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

impl std::error::Error for MCEError {}

/// A versioned key for MVCC implementation
///
/// Combines an original key with a version timestamp using MCE for deterministic
/// boundary detection. Versions are stored inverted for newest-first ordering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedKey {
    pub original_key: Vec<u8>,
    pub version: u64,
}

impl VersionedKey {
    /// Create a new versioned key
    pub fn new(original_key: Vec<u8>, version: u64) -> Self {
        Self { original_key, version }
    }

    /// Encode key using MCE + inverted timestamp
    ///
    /// MCE processes data in 8-byte groups with marker bytes for self-describing format.
    /// The version is inverted and appended as 8 bytes for newest-first ordering.
    ///
    /// # Returns
    /// MCE-encoded key with appended inverted version timestamp
    pub fn encode(&self) -> Vec<u8> {
        let mut result = encode_mce(&self.original_key);

        // Append inverted version for reverse chronological ordering (newer versions first)
        let inverted_version = !self.version;
        result.extend_from_slice(&inverted_version.to_be_bytes());

        result
    }

    /// Decode MCE-encoded versioned key
    ///
    /// MCE guarantees deterministic boundary detection between key and version.
    /// Returns the original key and version, or an error if the format is invalid.
    ///
    /// # Arguments
    /// * `encoded` - MCE-encoded versioned key data
    ///
    /// # Returns
    /// Decoded VersionedKey or error message
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
        let version = !inverted_version; // Un-invert to get original version

        Ok(VersionedKey {
            original_key,
            version,
        })
    }

    /// Get the MCE prefix for this key (without version)
    ///
    /// This is useful for prefix iteration to find all versions of a specific key.
    ///
    /// # Returns
    /// MCE-encoded original key without version data
    pub fn mce_prefix(&self) -> Vec<u8> {
        encode_mce(&self.original_key)
    }
}

impl PartialOrd for VersionedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VersionedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare encoded versions for proper ordering
        self.encode().cmp(&other.encode())
    }
}

/// Encode data using Memory Comparable Encoding
///
/// MCE processes data in 8-byte groups with marker bytes that indicate padding,
/// making the encoding self-describing and maintaining lexicographic order.
///
/// # Arguments
/// * `data` - The input data to encode
///
/// # Returns
/// MCE-encoded data with embedded boundary markers
///
/// # Example
/// ```
/// use rocksdb_server::lib::mce::encode_mce;
///
/// let data = b"user:123";
/// let encoded = encode_mce(data);
/// // encoded contains the data in 9-byte groups with marker bytes
/// ```
pub fn encode_mce(data: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();

    // Handle empty data - still need one group with all padding
    if data.is_empty() {
        result.extend_from_slice(&[0u8; 8]);
        result.push(0xFF - 8); // marker = 247
        return result;
    }

    for chunk in data.chunks(8) {
        // Create 8-byte group with zero padding
        let mut group = [0u8; 8];
        group[..chunk.len()].copy_from_slice(chunk);
        result.extend_from_slice(&group);

        // Calculate marker: 0xFF - number_of_padding_zeros
        let padding_count = 8 - chunk.len();
        let marker = 0xFF - padding_count as u8;
        result.push(marker);

        // If this chunk had padding, we're done (last group)
        if padding_count > 0 {
            break;
        }
    }

    // If we processed all chunks and the last chunk was exactly 8 bytes,
    // we need an additional empty group to indicate the end
    if !data.is_empty() && data.len() % 8 == 0 {
        result.extend_from_slice(&[0u8; 8]);
        result.push(0xFF - 8); // marker = 247 (all padding)
    }

    result
}

/// Decode MCE-encoded data
///
/// Returns the original data and the position where MCE encoding ends,
/// enabling deterministic boundary detection for concatenated data.
///
/// # Arguments
/// * `encoded` - MCE-encoded data
///
/// # Returns
/// A tuple of (original_data, mce_end_position) or MCEError
///
/// # Example
/// ```
/// use rocksdb_server::lib::mce::{encode_mce, decode_mce};
///
/// let original = b"user:123";
/// let encoded = encode_mce(original);
/// let (decoded, boundary) = decode_mce(&encoded).unwrap();
/// assert_eq!(original, &decoded[..]);
/// ```
pub fn decode_mce(encoded: &[u8]) -> Result<(Vec<u8>, usize), MCEError> {
    let mut original = Vec::new();
    let mut pos = 0;

    while pos < encoded.len() {
        if pos + 9 > encoded.len() {
            return Err(MCEError::IncompleteGroup);
        }

        let group = &encoded[pos..pos + 8];
        let marker = encoded[pos + 8];
        let padding_count = 0xFF - marker;

        if padding_count > 8 {
            return Err(MCEError::InvalidMarker(marker));
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

/// Validate MCE-encoded data
///
/// Checks that the encoded data is properly formatted according to MCE rules
/// without fully decoding it.
///
/// # Arguments
/// * `encoded` - MCE-encoded data to validate
///
/// # Returns
/// Ok(()) if valid, MCEError if invalid
pub fn validate_mce(encoded: &[u8]) -> Result<(), MCEError> {
    if encoded.is_empty() {
        return Ok(());
    }

    let mut pos = 0;
    while pos < encoded.len() {
        if pos + 9 > encoded.len() {
            return Err(MCEError::IncompleteGroup);
        }

        let marker = encoded[pos + 8];
        let padding_count = 0xFF - marker;
        if padding_count > 8 {
            return Err(MCEError::InvalidMarker(marker));
        }

        pos += 9;

        // If we found padding, this should be the last group
        if padding_count > 0 {
            if pos != encoded.len() {
                return Err(MCEError::TruncatedData);
            }
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_data() {
        let data = b"";
        let encoded = encode_mce(data);
        let expected = vec![0, 0, 0, 0, 0, 0, 0, 0, 247]; // All padding (8 zeros) → marker = 255-8 = 247
        assert_eq!(encoded, expected);

        let (decoded, boundary) = decode_mce(&encoded).unwrap();
        assert_eq!(decoded, data.to_vec());
        assert_eq!(boundary, 9);
    }

    #[test]
    fn test_small_data() {
        let data = b"abc";
        let encoded = encode_mce(data);
        let expected = vec![97, 98, 99, 0, 0, 0, 0, 0, 250]; // 5 padding zeros → marker = 255-5 = 250
        assert_eq!(encoded, expected);

        let (decoded, boundary) = decode_mce(&encoded).unwrap();
        assert_eq!(decoded, data.to_vec());
        assert_eq!(boundary, 9);
    }

    #[test]
    fn test_exact_eight_bytes() {
        let data = b"12345678";
        let encoded = encode_mce(data);
        let expected = vec![49, 50, 51, 52, 53, 54, 55, 56, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247];
        // First group: no padding → marker = 255
        // Second group: all padding → marker = 247
        assert_eq!(encoded, expected);

        let (decoded, boundary) = decode_mce(&encoded).unwrap();
        assert_eq!(decoded, data.to_vec());
        assert_eq!(boundary, 18);
    }

    #[test]
    fn test_multi_group_data() {
        let data = b"very_long_key_name";
        let encoded = encode_mce(data);

        let (decoded, _) = decode_mce(&encoded).unwrap();
        assert_eq!(decoded, data.to_vec());
    }

    #[test]
    fn test_binary_data() {
        let data = vec![0, 1, 2, 255, 254];
        let encoded = encode_mce(&data);

        let (decoded, _) = decode_mce(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_invalid_marker() {
        let mut encoded = encode_mce(b"test");
        encoded[8] = 246; // Invalid marker (would indicate 9 padding bytes)

        assert_eq!(decode_mce(&encoded), Err(MCEError::InvalidMarker(246)));
    }

    #[test]
    fn test_incomplete_group() {
        let encoded = vec![1, 2, 3, 4, 5, 6, 7, 8]; // Missing marker byte

        assert_eq!(decode_mce(&encoded), Err(MCEError::IncompleteGroup));
    }

    #[test]
    fn test_validation() {
        let data = b"test_data";
        let encoded = encode_mce(data);

        assert!(validate_mce(&encoded).is_ok());
        assert!(validate_mce(&[]).is_ok());

        // Test invalid cases
        let incomplete = vec![1, 2, 3, 4];
        assert_eq!(validate_mce(&incomplete), Err(MCEError::IncompleteGroup));

        let mut invalid_marker = encoded.clone();
        invalid_marker[8] = 246;
        assert_eq!(validate_mce(&invalid_marker), Err(MCEError::InvalidMarker(246)));
    }

    #[test]
    fn test_ordering_preservation() {
        // Test that MCE preserves lexicographic ordering
        let test_keys = vec![
            b"".to_vec(),
            b"a".to_vec(),
            b"aa".to_vec(),
            b"ab".to_vec(),
            b"b".to_vec(),
            b"ba".to_vec(),
            b"user:1".to_vec(),
            b"user:10".to_vec(),
            b"user:123".to_vec(),
            b"user:2".to_vec(),
            b"very_long_key_name_that_spans_multiple_groups".to_vec(),
        ];

        // Encode all keys
        let mut encoded_keys: Vec<_> = test_keys.iter()
            .map(|k| (encode_mce(k), k.clone()))
            .collect();

        // Sort the encoded keys
        encoded_keys.sort_by(|a, b| a.0.cmp(&b.0));

        // Extract the original keys from sorted encoded keys
        let sorted_original_keys: Vec<_> = encoded_keys.iter()
            .map(|(_, orig)| orig.clone())
            .collect();

        // Create a reference sorted list of original keys
        let mut reference_keys = test_keys.clone();
        reference_keys.sort();

        // The ordering should be preserved
        assert_eq!(sorted_original_keys, reference_keys);
    }

    #[test]
    fn test_roundtrip_comprehensive() {
        // Test various edge cases for round-trip encoding/decoding
        let test_cases = vec![
            // Empty data
            vec![],
            // Single byte
            vec![0],
            vec![1],
            vec![255],
            // Various lengths around 8-byte boundary
            vec![1; 1],
            vec![1; 7],
            vec![1; 8],
            vec![1; 9],
            vec![1; 15],
            vec![1; 16],
            vec![1; 17],
            vec![1; 24],
            // Binary data with special bytes
            vec![0, 255, 128, 1, 254],
            // String-like data
            b"hello".to_vec(),
            b"user:123".to_vec(),
            b"very_long_key_name_that_exceeds_single_group".to_vec(),
            // Large data
            vec![42; 1000],
        ];

        for (i, original) in test_cases.iter().enumerate() {
            let encoded = encode_mce(original);
            let (decoded, boundary) = decode_mce(&encoded)
                .unwrap_or_else(|e| panic!("Failed to decode test case {}: {:?}", i, e));

            assert_eq!(&decoded, original, "Round-trip failed for test case {}", i);
            assert_eq!(boundary, encoded.len(), "Boundary incorrect for test case {}", i);

            // Verify validation passes
            assert!(validate_mce(&encoded).is_ok(), "Validation failed for test case {}", i);
        }
    }

    #[test]
    fn test_boundary_detection_with_concatenation() {
        // Test that MCE enables deterministic boundary detection
        let key1 = b"user:123";
        let key2 = b"product:456";
        let timestamp = 12345u64.to_be_bytes();

        // Encode first key and concatenate with timestamp
        let mce1 = encode_mce(key1);
        let combined1 = [mce1.as_slice(), &timestamp].concat();

        // Decode should correctly identify the boundary
        let (decoded1, boundary1) = decode_mce(&combined1).unwrap();
        assert_eq!(decoded1, key1.to_vec());
        assert_eq!(&combined1[boundary1..boundary1 + 8], &timestamp);

        // Same for second key
        let mce2 = encode_mce(key2);
        let combined2 = [mce2.as_slice(), &timestamp].concat();

        let (decoded2, boundary2) = decode_mce(&combined2).unwrap();
        assert_eq!(decoded2, key2.to_vec());
        assert_eq!(&combined2[boundary2..boundary2 + 8], &timestamp);
    }

    #[test]
    fn test_prefix_matching() {
        // Test that MCE-encoded keys with same prefix can be distinguished
        let keys = vec![
            b"user".to_vec(),
            b"user:".to_vec(),
            b"user:1".to_vec(),
            b"user:12".to_vec(),
            b"user:123".to_vec(),
            b"user:1234".to_vec(),
        ];

        let encoded_keys: Vec<_> = keys.iter().map(|k| encode_mce(k)).collect();

        // All encoded keys should be different
        for i in 0..encoded_keys.len() {
            for j in i + 1..encoded_keys.len() {
                assert_ne!(encoded_keys[i], encoded_keys[j],
                    "Keys '{}' and '{}' have identical MCE encoding",
                    String::from_utf8_lossy(&keys[i]),
                    String::from_utf8_lossy(&keys[j]));
            }
        }

        // All should decode back to originals
        for (i, encoded) in encoded_keys.iter().enumerate() {
            let (decoded, _) = decode_mce(encoded).unwrap();
            assert_eq!(decoded, keys[i]);
        }
    }

    #[test]
    fn test_edge_case_markers() {
        // Test all possible marker values
        for padding in 0..=8 {
            let data_len = 8 - padding;
            let data = vec![42u8; data_len];

            let encoded = encode_mce(&data);
            let expected_marker = 0xFF - padding as u8;

            // Find the first marker (should be at position 8)
            assert_eq!(encoded[8], expected_marker,
                "Incorrect marker for {} padding bytes", padding);

            // Verify round-trip
            let (decoded, _) = decode_mce(&encoded).unwrap();
            assert_eq!(decoded, data);
        }
    }

    // VersionedKey tests
    #[test]
    fn test_versioned_key_basic() {
        let key = b"user:123".to_vec();
        let version = 12345u64;
        let vk = VersionedKey::new(key.clone(), version);

        assert_eq!(vk.original_key, key);
        assert_eq!(vk.version, version);

        // Test encoding and decoding
        let encoded = vk.encode();
        let decoded = VersionedKey::decode(&encoded).unwrap();

        assert_eq!(decoded.original_key, key);
        assert_eq!(decoded.version, version);
    }

    #[test]
    fn test_versioned_key_ordering() {
        // Test that versioned keys sort properly: first by key, then by version (newest first)
        let test_cases = vec![
            VersionedKey::new(b"a".to_vec(), 100),
            VersionedKey::new(b"a".to_vec(), 200), // Should come before version 100
            VersionedKey::new(b"a".to_vec(), 50),
            VersionedKey::new(b"b".to_vec(), 100),
            VersionedKey::new(b"b".to_vec(), 200),
        ];

        let mut encoded_keys: Vec<_> = test_cases.iter()
            .map(|vk| vk.encode())
            .collect();

        // Sort encoded keys
        encoded_keys.sort();

        // Decode back and verify ordering
        let sorted_versioned_keys: Vec<_> = encoded_keys.iter()
            .map(|encoded| VersionedKey::decode(encoded).unwrap())
            .collect();

        // Expected order: a@200, a@100, a@50, b@200, b@100
        // (key "a" before "b", within each key newest versions first)
        let expected = vec![
            VersionedKey::new(b"a".to_vec(), 200),
            VersionedKey::new(b"a".to_vec(), 100),
            VersionedKey::new(b"a".to_vec(), 50),
            VersionedKey::new(b"b".to_vec(), 200),
            VersionedKey::new(b"b".to_vec(), 100),
        ];

        assert_eq!(sorted_versioned_keys, expected);
    }

    #[test]
    fn test_versioned_key_prefix() {
        let vk1 = VersionedKey::new(b"user:123".to_vec(), 100);
        let vk2 = VersionedKey::new(b"user:123".to_vec(), 200);
        let vk3 = VersionedKey::new(b"user:456".to_vec(), 100);

        // Same key should have same prefix
        assert_eq!(vk1.mce_prefix(), vk2.mce_prefix());

        // Different keys should have different prefixes
        assert_ne!(vk1.mce_prefix(), vk3.mce_prefix());

        // Prefix should be the MCE encoding of the original key
        assert_eq!(vk1.mce_prefix(), encode_mce(&vk1.original_key));
    }

    #[test]
    fn test_versioned_key_boundary_detection() {
        // Test that versioned keys can be concatenated with other data
        let vk = VersionedKey::new(b"product:abc".to_vec(), 12345);
        let extra_data = b"additional_metadata";

        let encoded_vk = vk.encode();
        let _combined = [encoded_vk.as_slice(), extra_data].concat();

        // Should be able to decode just the versioned key part
        let decoded_vk = VersionedKey::decode(&encoded_vk).unwrap();
        assert_eq!(decoded_vk, vk);

        // Verify the boundary is correct
        let (mce_key, mce_end) = decode_mce(&encoded_vk).unwrap();
        assert_eq!(mce_key, vk.original_key);
        assert_eq!(mce_end + 8, encoded_vk.len()); // MCE + 8 bytes for version
    }

    #[test]
    fn test_versioned_key_edge_cases() {
        // Test with empty key
        let vk_empty = VersionedKey::new(vec![], 12345);
        let encoded_empty = vk_empty.encode();
        let decoded_empty = VersionedKey::decode(&encoded_empty).unwrap();
        assert_eq!(decoded_empty, vk_empty);

        // Test with maximum version
        let vk_max = VersionedKey::new(b"key".to_vec(), u64::MAX);
        let encoded_max = vk_max.encode();
        let decoded_max = VersionedKey::decode(&encoded_max).unwrap();
        assert_eq!(decoded_max, vk_max);

        // Test with zero version
        let vk_zero = VersionedKey::new(b"key".to_vec(), 0);
        let encoded_zero = vk_zero.encode();
        let decoded_zero = VersionedKey::decode(&encoded_zero).unwrap();
        assert_eq!(decoded_zero, vk_zero);

        // Test with large key
        let large_key = vec![42u8; 1000];
        let vk_large = VersionedKey::new(large_key.clone(), 12345);
        let encoded_large = vk_large.encode();
        let decoded_large = VersionedKey::decode(&encoded_large).unwrap();
        assert_eq!(decoded_large.original_key, large_key);
        assert_eq!(decoded_large.version, 12345);
    }

    #[test]
    fn test_versioned_key_invalid_data() {
        // Test with truncated data
        let vk = VersionedKey::new(b"test".to_vec(), 12345);
        let encoded = vk.encode();

        // Remove some bytes to make it invalid
        let truncated = &encoded[..encoded.len() - 4];
        assert!(VersionedKey::decode(truncated).is_err());

        // Test with invalid MCE data
        let invalid_mce = vec![1, 2, 3, 4, 5, 6, 7, 8, 246]; // Invalid marker
        let with_version = [invalid_mce, vec![0; 8]].concat();
        assert!(VersionedKey::decode(&with_version).is_err());
    }

    #[test]
    fn test_versioned_key_version_inversion() {
        // Test that version inversion works correctly for ordering
        let vk_old = VersionedKey::new(b"key".to_vec(), 100);
        let vk_new = VersionedKey::new(b"key".to_vec(), 200);

        let encoded_old = vk_old.encode();
        let encoded_new = vk_new.encode();

        // Newer version should sort before older version (reverse chronological)
        assert!(encoded_new < encoded_old);

        // Verify the actual inversion
        let inverted_100 = !100u64;
        let inverted_200 = !200u64;
        assert!(inverted_200 < inverted_100); // Inverted values should be in reverse order
    }
}