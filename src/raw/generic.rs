use super::bitmask::BitMaskIter;

pub const BITMASK_STRIDE: usize = 8;
// We only care about the highest bit of each byte for the mask.
pub const BITMASK_MASK: u64 = 0x80_8080_8080_8080;

/// Helper function to replicate a byte across a `u64`.
#[inline]
fn repeat(byte: u8) -> u64 {
    u64::from_ne_bytes([byte; 8])
}

/// Returns a `BitMask` indicating all bytes in the group which *may*
/// have the given value.
///
/// This function may return a false positive in certain cases where
/// the byte in the group differs from the searched value only in its
/// lowest bit. This is fine because:
/// - The check for key equality will catch these.
/// - This only happens if there is at least 1 true match.
/// - The chance of this happening is very low (< 1% chance per byte).
#[inline]
pub(crate) fn match_byte(_valid_bits: u64, hashes: u64, value: u8) -> BitMaskIter {
    // This algorithm is derived from
    // http://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
    let cmp = hashes ^ repeat(value);
    //TODO how to handle the valid_bits masking?
    BitMaskIter::new((cmp.wrapping_sub(repeat(0x01)) & !cmp & BITMASK_MASK).to_le(), BITMASK_STRIDE)
}
