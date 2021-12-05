use super::bitmask::BitMaskIter;
use packed_simd::u8x8;

/// Returns a `BitMask` indicating all hash bytes in the group which have
/// the given value.
///
/// This implementation uses 128-bit SSE instructions.
#[cfg(not(feature = "no-simd"))]
#[inline]
pub(crate) fn match_byte(valid_bits: u8, hashes: u64, value: u8) -> BitMaskIter {
    let hashes: u8x8 = hashes.to_ne_bytes().into();
    let values = u8x8::splat(value);
    BitMaskIter::new(hashes.eq(values).bitmask() as u8 & valid_bits)
}

#[cfg(not(feature = "no-simd"))]
pub(crate) fn count_locked_slots(valid_bits: u8, hashes: u64) -> isize {
    let hashes: u8x8 = hashes.to_ne_bytes().into();
    let values = 0x0u64.to_ne_bytes().into();

    (hashes.ne(values).bitmask() as u8 & valid_bits).count_ones() as isize
}

#[cfg(feature = "no-simd")]
#[inline]
pub(crate) fn match_byte(valid_bits: u64, hashes: u64, value: u8) -> BitMaskIter {
    let hashes = hashes.to_ne_bytes();
    let mut result: u64 = 0;
    let iter = BitMaskIter::new(valid_bits);
    for i in iter {
        if hashes[i] == value {
            result |= 1 << i;
        }
    }
    BitMaskIter::new(result & valid_bits)
}

#[cfg(feature = "no-simd")]
pub(crate) fn count_locked_slots(valid_bits: u64, hashes: u64) -> isize {
    let hashes = hashes.to_ne_bytes();
    let mut possibly_locked: u64 = 0;
    let iter = BitMaskIter::new(valid_bits);
    for i in iter {
        if hashes[i] != 0 {
            possibly_locked |= 1 << i;
        }
    }

    (possibly_locked & valid_bits).count_ones() as isize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_bit(index: usize) -> u8 {
        1 << index
    }
    fn h2_bits(h2: u64, index: usize) -> u64 {
        h2 << (8 * index)
    }
    #[test]
    fn no_valid_values() {
        let mut iter = match_byte(0x00, 0, 0);
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn first_slot_valid() {
        let mut iter = match_byte(valid_bit(0), h2_bits(42, 0), 42);
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn last_slot_valid() {
        let mut iter = match_byte(valid_bit(6), h2_bits(42, 6), 42);
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn multiple_slot_valid() {
        let valid_bits = valid_bit(0) | valid_bit(4) | valid_bit(6);
        let hashes = h2_bits(42, 0) | h2_bits(42, 4) | h2_bits(42, 6);
        let mut iter = match_byte(valid_bits, hashes, 42);
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), Some(4));
        assert_eq!(iter.next(), Some(6));
        assert_eq!(iter.next(), None);
    }
    #[test]
    fn multiple_slot_match_but_only_one_valid() {
        let valid_bits = valid_bit(0);
        let hashes = h2_bits(42, 0) | h2_bits(42, 4);
        let mut iter = match_byte(valid_bits, hashes, 42);
        assert_eq!(iter.next(), Some(0));
        assert_eq!(iter.next(), None);
    }
}
