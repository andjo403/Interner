use super::bitmask::BitMaskIter;
use std::simd::{u8x8, SimdPartialEq, ToBitMask};

/// Returns a `BitMaskIter` indicating all hash bytes in the group which have
/// the given value.
#[inline]
pub(crate) fn match_byte(valid_bits: u8, hashes: u64, value: u8) -> BitMaskIter {
    let hashes = u8x8::from_slice(&hashes.to_ne_bytes());
    let values = u8x8::splat(value);
    BitMaskIter::new(hashes.simd_eq(values).to_bitmask() & valid_bits)
}

pub(crate) fn count_locked_slots(valid_bits: u8, hashes: u64) -> isize {
    let hashes = u8x8::from_slice(&hashes.to_ne_bytes());
    let values = u8x8::from_slice(&0x0u64.to_ne_bytes());

    (hashes.simd_ne(values).to_bitmask() & !valid_bits & 0x7f).count_ones() as isize
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
    #[test]
    fn count_locked_slots_test() {
        assert_eq!(2, count_locked_slots(0x3, 0x1200320011));
        assert_eq!(0, count_locked_slots(0x1f, 0x1200320011));
        assert_eq!(3, count_locked_slots(0x0, 0x1200320011));
    }
}
