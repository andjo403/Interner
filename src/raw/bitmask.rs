/// Iterator over the contents of a `u8`, returning the indicies of set bits.
pub(crate) struct BitMaskIter {
    bit_mask: u8,
}

impl BitMaskIter {
    pub(crate) fn new(bit_mask: u8) -> Self {
        Self { bit_mask }
    }
}

impl Iterator for BitMaskIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        let bit_mask = std::num::NonZeroU8::new(self.bit_mask)?;
        self.bit_mask &= self.bit_mask - 1;
        Some(bit_mask.trailing_zeros() as usize)
    }
}
