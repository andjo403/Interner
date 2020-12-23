use super::bitmask::BitMaskIter;

#[cfg(target_arch = "x86")]
use core::arch::x86;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64 as x86;

pub const BITMASK_STRIDE: usize = 1;

/// Returns a `BitMask` indicating all hash bytes in the group which have
/// the given value.
///
/// This implementation uses 128-bit SSE instructions.
/*
#[inline]
pub(crate) fn match_byte(valid_bits: u64, hashes: u64, value: u8) -> BitMaskIter {
    unsafe {
        let hashes = x86::_mm_set_epi64x(0, hashes as i64);
        let cmp = x86::_mm_cmpeq_epi8(hashes, x86::_mm_set1_epi8(value as i8));
        BitMaskIter::new(x86::_mm_movemask_epi8(cmp) as u64 & valid_bits, BITMASK_STRIDE)
    }
}
*/
#[inline]
pub(crate) fn match_byte(valid_bits: u64, hashes: u64, value: u8) -> BitMaskIter {
    let hashes = hashes.to_ne_bytes();
    let mut result: u64 = 0;
    for i in 0..7 {
        if hashes[i] == value {
            result |= 1 << i;
        }
    }
    BitMaskIter::new(result & valid_bits, BITMASK_STRIDE)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_bit(index: usize) -> u64 {
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
