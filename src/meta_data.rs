use crate::bitmask::BitMaskIter;
use std::{
    simd::{u8x8, SimdPartialEq, ToBitMask},
    sync::atomic::Ordering,
};

pub(crate) trait MetaDataHandling {
    fn load_meta_data(&self, order: Ordering) -> MetaData;
    fn store_moved_flag_to_meta_data(&self, order: Ordering) -> MetaData;
    fn compare_exchange_weak_meta_data(
        &self,
        current: &mut MetaData,
        new: MetaData,
        success: Ordering,
        failure: Ordering,
    ) -> bool;
}

/// # State table when the valid bit is not set othrewise the bits is set to h2:
///
///  PARKED_BIT | LOCKED_BIT | Description
///      0      |     0      | waiting to be locked
/// ------------+------------+------------------------------------------------------------------
///      0      |     1      | The mutex is locked by exactly one thread. No other thread is
///             |            | waiting for it.
/// ------------+------------+------------------------------------------------------------------
///      1      |     0      | invalid state, The mutex is not locked and one or more thread is
///             |            | parked or about to park. When the lock is released it is not
///             |            | setting the LOCKED_BIT as the lock shall not be retaken.
/// ------------+------------+------------------------------------------------------------------
///      1      |     1      | The mutex is locked by exactly one thread. One or more thread is
///             |            | parked or about to park, waiting for the lock to become available.
///
/// |1 group moved bit | 7 slot valid bits| 7 slots of 8 bit h2 hash or 6 bit h2 hash and lock bits when valid bit unset
pub(crate) struct MetaData(u64);

impl MetaData {
    /// This bit is set instead of h2 if valid bit is not set when that mutex is locked by some thread.
    pub const LOCKED_BIT: u8 = 0x80;
    const PARK_BIT: u8 = 0x40;
    const VALID_BIT_MASK: u8 = 0x7F;

    const GROUP_FULL_BIT_MASK: u64 = 0xFE00_0000_0000_0000;
    const GROUP_MOVED_BIT_MASK: u64 = 0x0100_0000_0000_0000;

    #[inline]
    pub(crate) fn new(meta_data: u64) -> Self {
        Self(meta_data)
    }

    #[inline]
    fn valid_bit(index: usize) -> u64 {
        1 << (64 - 7 + index)
    }
    #[inline]
    pub(crate) fn test_valid_bit(&self, index: usize) -> bool {
        self.0 & Self::valid_bit(index) != 0
    }
    #[inline]
    fn h2_bits(h2: u8, index: usize) -> u64 {
        (h2 as u64) << (8 * index)
    }

    #[inline]
    pub(crate) fn h2_from_meta(&self, index: usize) -> u8 {
        (self.0 >> (8 * index)) as u8
    }

    #[inline]
    fn lock_bit(index: usize) -> u64 {
        (Self::LOCKED_BIT as u64) << (8 * index)
    }
    #[inline]
    pub(crate) fn test_lock_bit(&self, index: usize) -> bool {
        self.0 & Self::lock_bit(index) != 0
    }

    #[inline]
    pub(crate) fn lock(&self, h2: u8, index: usize) -> Self {
        Self(self.0 | Self::h2_bits(h2 & 0x3F | Self::LOCKED_BIT, index))
    }

    #[inline]
    pub(crate) fn unlock(&self, h2: u8, index: usize) -> Self {
        Self(
            (self.0 & !Self::h2_bits(0xff, index))
                | Self::valid_bit(index)
                | Self::h2_bits(h2, index),
        )
    }

    #[inline]
    pub(crate) fn park_bit(index: usize) -> u64 {
        (Self::PARK_BIT as u64) << (8 * index)
    }
    #[inline]
    pub(crate) fn test_park_bit(&self, index: usize) -> bool {
        self.0 & Self::park_bit(index) != 0
    }
    #[inline]
    pub(crate) fn park(&self, index: usize) -> Self {
        Self(self.0 | MetaData::park_bit(index))
    }
    #[inline]
    pub(crate) fn bucket_moved(&self) -> bool {
        self.0 & Self::GROUP_MOVED_BIT_MASK == Self::GROUP_MOVED_BIT_MASK
    }
    #[inline]
    pub(crate) fn bucket_full(&self) -> bool {
        self.0 & Self::GROUP_FULL_BIT_MASK == Self::GROUP_FULL_BIT_MASK
    }

    #[inline]
    pub(crate) fn get_valid_bits(&self) -> u8 {
        ((self.0 & Self::GROUP_FULL_BIT_MASK) >> (64 - 7)) as u8
    }

    #[inline]
    pub(crate) fn valid_indexes_iter(&self) -> BitMaskIter {
        BitMaskIter::new(self.get_valid_bits())
    }

    #[inline]
    pub(crate) fn not_valid_indexes_iter(&self) -> BitMaskIter {
        BitMaskIter::new(!self.get_valid_bits() & Self::VALID_BIT_MASK)
    }

    /// Returns a `BitMaskIter` indicating all hash bytes in the group which have
    /// the given value.
    #[inline]
    pub(crate) fn match_indexes_iter(&self, value: u8) -> BitMaskIter {
        let hashes = u8x8::from_slice(&self.0.to_ne_bytes());
        let values = u8x8::splat(value);
        BitMaskIter::new(hashes.simd_eq(values).to_bitmask() & self.get_valid_bits())
    }

    pub(crate) fn count_locked_slots(&self) -> isize {
        let hashes = u8x8::from_slice(&self.0.to_ne_bytes());
        const NOT_USED: u8x8 = u8x8::from_slice(&0x0u64.to_ne_bytes());

        (hashes.simd_ne(NOT_USED).to_bitmask() & !self.get_valid_bits() & Self::VALID_BIT_MASK)
            .count_ones() as isize
    }
}

impl MetaDataHandling for std::sync::atomic::AtomicU64 {
    fn load_meta_data(&self, order: Ordering) -> MetaData {
        MetaData::new(self.load(order))
    }

    fn store_moved_flag_to_meta_data(&self, order: Ordering) -> MetaData {
        MetaData::new(self.fetch_or(MetaData::GROUP_MOVED_BIT_MASK, order))
    }

    fn compare_exchange_weak_meta_data(
        &self,
        current: &mut MetaData,
        new: MetaData,
        success: Ordering,
        failure: Ordering,
    ) -> bool {
        match self.compare_exchange_weak(current.0, new.0, success, failure) {
            Ok(_) => {
                *current = new;
                true
            }
            Err(new_meta_data) => {
                *current = MetaData::new(new_meta_data);
                false
            }
        }
    }
}
