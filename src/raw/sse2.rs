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
#[inline]
pub(crate) fn match_byte(valid_bits: u64, hashes: u64, value: u8) -> BitMaskIter {
    unsafe {
        let hashes = x86::_mm_set_epi64x(0, hashes as i64);
        let cmp = x86::_mm_cmpeq_epi8(hashes, x86::_mm_set1_epi8(value as i8));
        BitMaskIter::new(x86::_mm_movemask_epi8(cmp) as u64 & valid_bits, BITMASK_STRIDE)
    }
}
