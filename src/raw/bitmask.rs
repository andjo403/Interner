use core::intrinsics;

/// A bit mask which contains the result of a `Match` operation on a `Group` and
/// allows iterating through them.
///
/// The bit mask is arranged so that low-order bits represent lower memory
/// addresses for group match results.
///
/// For implementation reasons, the bits in the set may be sparsely packed, so
/// that there is only one bit-per-byte used (the high bit, 7). If this is the
/// case, `BITMASK_STRIDE` will be 8 to indicate a divide-by-8 should be
/// performed on counts/indices to normalize this difference. `BITMASK_MASK` is
/// similarly a mask of all the actually-used bits.
/// Iterator over the contents of a `u64`, returning the indicies of set bits.
pub(crate) struct BitMaskIter {
    bit_mask: u64,
    stride: usize,
}

impl BitMaskIter {
    pub(crate) fn new(bit_mask: u64, stride: usize) -> Self {
        Self { bit_mask, stride }
    }
}

impl Iterator for BitMaskIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.bit_mask == 0 {
            None
        } else {
            //safe due to bit_mask = 0 is checked
            let result = unsafe { intrinsics::cttz_nonzero(self.bit_mask) as usize / self.stride };
            self.bit_mask &= self.bit_mask - 1;
            Some(result)
        }
    }
}
