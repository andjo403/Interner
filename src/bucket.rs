use crate::meta_data::{MetaData, MetaDataHandling};
use crate::raw_interner::RawInterner;
use parking_lot_core::{self, DEFAULT_PARK_TOKEN, DEFAULT_UNPARK_TOKEN};
use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash};
use std::mem::MaybeUninit;
use std::sync::atomic::{fence, AtomicU64, Ordering};

#[derive(Debug, PartialEq, Eq)]
pub enum ReserveResult {
    Reserved,
    OccupiedWithSameH2,
    AlreadyReservedWithOtherH2,
    AlreadyReservedWithSameH2,
    SlotAvailableButGroupMoved,
}

#[repr(align(64))]
pub(crate) struct Bucket<T> {
    pub meta_data: AtomicU64,
    pub refs: [MaybeUninit<UnsafeCell<T>>; 7],
}

impl<T> Bucket<T> {
    #[inline]
    pub unsafe fn set_slot(&self, index: usize, value: T) {
        UnsafeCell::raw_get(self.refs.get_unchecked(index).as_ptr()).write(value);
    }

    #[inline]
    pub fn get_ref_to_slot(&self, index: usize) -> &T {
        unsafe { &*self.refs.get_unchecked(index).assume_init_ref().get() }
    }

    // move all valid slots from this bucket to the next interner
    pub fn transfer_bucket(
        &self,
        new_raw_interner: &RawInterner<T>,
        hash_builder: &impl BuildHasher,
    ) -> isize
    where
        T: Copy + Hash,
    {
        let group_meta_data = self.meta_data.store_moved_flag_to_meta_data(Ordering::Acquire);

        if group_meta_data.bucket_moved() {
            return 0; //already moved
        }
        let iter = group_meta_data.valid_indexes_iter();
        for index in iter {
            let value = self.get_ref_to_slot(index);
            new_raw_interner.transfer_in_to(*value, hash_builder);
        }
        group_meta_data.count_locked_slots() + 1 // add one to markbucket as done
    }

    #[inline]
    pub fn get_metadata_acquire(&self) -> MetaData {
        MetaData::new(self.meta_data.load(Ordering::Acquire))
    }

    fn lock_addr(&self, index: usize) -> usize {
        (&self.meta_data as *const _ as usize) + index
    }

    #[inline]
    pub(crate) fn reserve(
        &self,
        group_meta_data: &mut MetaData,
        h2: u8,
        index: usize,
    ) -> ReserveResult {
        loop {
            if group_meta_data.test_valid_bit(index) {
                if group_meta_data.h2_from_meta(index) == h2 {
                    return ReserveResult::OccupiedWithSameH2;
                }
                return ReserveResult::AlreadyReservedWithOtherH2;
            }
            if group_meta_data.test_lock_bit(index) {
                if (group_meta_data.h2_from_meta(index) & 0x3F) == (h2 & 0x3F) {
                    return ReserveResult::AlreadyReservedWithSameH2;
                }
                return ReserveResult::AlreadyReservedWithOtherH2;
            }
            if group_meta_data.bucket_moved() {
                return ReserveResult::SlotAvailableButGroupMoved;
            }
            let new_group_meta_data = group_meta_data.lock(h2, index);
            if self.meta_data.compare_exchange_weak_meta_data(
                group_meta_data,
                new_group_meta_data,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                return ReserveResult::Reserved;
            }
        }
    }

    #[cold]
    pub(crate) fn wait_on_lock_release(&self, out_meta_data: &mut MetaData, index: usize) {
        let mut group_meta_data = self.meta_data.load_meta_data(Ordering::Relaxed);
        let addr = self.lock_addr(index);
        let validate = || !self.meta_data.load_meta_data(Ordering::Relaxed).test_valid_bit(index);
        let before_sleep = || {};
        let timed_out = |_, _| {};

        loop {
            if group_meta_data.test_valid_bit(index) {
                *out_meta_data = group_meta_data;
                fence(Ordering::Acquire);
                return;
            }

            // Set the parked bit
            if !group_meta_data.test_park_bit(index) {
                let new_group_meta_data = group_meta_data.park(index);
                if !self.meta_data.compare_exchange_weak_meta_data(
                    &mut group_meta_data,
                    new_group_meta_data,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    continue;
                }
            }

            // Park our thread until we are woken up by an unlock

            // SAFETY:
            //   * `addr` is an address we control.
            //   * `validate`/`timed_out` does not panic or call into any function of `parking_lot`.
            //   * `before_sleep` does not call `park`, nor does it panic.
            unsafe {
                parking_lot_core::park(
                    addr,
                    validate,
                    before_sleep,
                    timed_out,
                    DEFAULT_PARK_TOKEN,
                    None,
                );
            }

            // Loop back and check if the valid bit was set
            group_meta_data = self.meta_data.load_meta_data(Ordering::Relaxed);
        }
    }

    pub(crate) fn set_valid_and_unpark(
        &self,
        mut group_meta_data: MetaData,
        h2: u8,
        index: usize,
    ) -> bool {
        loop {
            let new_group_meta_data = group_meta_data.unlock(h2, index);
            if self.meta_data.compare_exchange_weak_meta_data(
                &mut group_meta_data,
                new_group_meta_data,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                if group_meta_data.test_park_bit(index) {
                    let addr = self.lock_addr(index);
                    // SAFETY:
                    //   * `addr` is an address we control.
                    unsafe {
                        parking_lot_core::unpark_all(addr, DEFAULT_UNPARK_TOKEN);
                    }
                }
                return group_meta_data.bucket_moved();
            }
        }
    }
}
