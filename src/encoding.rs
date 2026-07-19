//! Shared UTF-16 encoding utilities.
//! 
//! Consolidates duplicate UTF-16 decoding logic from csv_parser.rs and ready.rs
//! into a single, well-tested implementation that correctly handles surrogate pairs.

/// Decode UTF-16 LE bytes to a Rust String (with optional BOM stripping).
pub fn decode_utf16_le(raw: &[u8]) -> String {
    let data = if raw.starts_with(&[0xFF, 0xFE]) {
        &raw[2..] // Strip BOM
    } else {
        raw
    };

    // Convert pairs of u16 to chars using String::from_utf16 (handles surrogate pairs correctly)
    let utf16_chars: Vec<u16> = data
        .chunks_exact(2)
        .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
        .collect();

    String::from_utf16(&utf16_chars).unwrap_or_default()
}

/// Decode UTF-16 BE bytes to a Rust String (with optional BOM stripping).
pub fn decode_utf16_be(raw: &[u8]) -> String {
    let data = if raw.starts_with(&[0xFE, 0xFF]) {
        &raw[2..] // Strip BOM
    } else {
        raw
    };

    let utf16_chars: Vec<u16> = data
        .chunks_exact(2)
        .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
        .collect();

    String::from_utf16(&utf16_chars).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_utf16_le_basic() {
        let raw = vec![0xFF, 0xFE, b'H', 0x00, b'e', 0x00, b'l', 0x00, b'l', 0x00, b'o', 0x00];
        assert_eq!(decode_utf16_le(&raw), "Hello");
    }

    #[test]
    fn test_decode_utf16_le_no_bom() {
        let raw = vec![b'H', 0x00, b'e', 0x00, b'l', 0x00];
        assert_eq!(decode_utf16_le(&raw), "Hel");
    }

    #[test]
    fn test_decode_utf16_be_basic() {
        let raw = vec![0xFE, 0xFF, 0x00, b'H', 0x00, b'e'];
        assert_eq!(decode_utf16_be(&raw), "He");
    }

    #[test]
    fn test_decode_surrogate_pairs() {
        // U+1F600 (😀) encoded as surrogate pair: D83D DE00 in LE
        let raw = vec![
            0x3D, 0xD8, 0x00, 0xDE, // 😀 surrogate pair
        ];
        assert_eq!(decode_utf16_le(&raw), "😀");
    }

    #[test]
    fn test_decode_empty() {
        let raw: Vec<u8> = vec![];
        assert_eq!(decode_utf16_le(&raw), "");
    }

    #[test]
    fn test_decode_odd_bytes() {
        // Odd number of bytes — last byte is dropped (correct UTF-16 behavior)
        let raw = vec![b'H', 0x00, b'e'];
        assert_eq!(decode_utf16_le(&raw), "H"); // only 'H' decodes; trailing 'e' is incomplete
    }
}
