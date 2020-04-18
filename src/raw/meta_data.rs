use core::sync::atomic::{AtomicU64, Ordering};
use parking_lot_core::{self, SpinWait, UnparkToken, DEFAULT_PARK_TOKEN};

// UnparkToken used to indicate that reference is stored. not used only to have a value to set in unpark_all
const TOKEN_VALUE_SET: UnparkToken = UnparkToken(0);

/// This bit is set instead of h2 if valid bit is not set when that mutex is locked by some thread.
const LOCKED_BIT: u64 = 0b1;
/// This bit is set instead of h2 if valid bit is not set just before parking a thread.
/// A thread is being parked if it wants to lock the mutex, but it is currently being held by some other thread.
//const PARKED_BIT: u64 = 0b10;
#[derive(Debug, PartialEq, Eq)]
pub enum ReserveResult {
    AlreadyReservedWithOtherH2,
    Reserved,
    Occupied,
}

/// Once Reference type backed by the parking lot.
pub(crate) struct MetaData {
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
    /// |1 spare| 7 valid_bits| 7*8 h2 hash or lock and part of h2 hash when valid bit unset
    meta_data: AtomicU64,
}

#[inline]
fn valid_bit(index: usize) -> u64 {
    1 << (64 - 8 + index)
}
#[inline]
fn valid_bit_set(group_meta_data: u64, index: usize) -> bool {
    group_meta_data & valid_bit(index) != 0
}
#[inline]
fn h2_bits(h2: u64, index: usize) -> u64 {
    h2 << (8 * index)
}

#[inline]
fn lock_bit(index: usize) -> u64 {
    1 << (8 * index)
}
#[inline]
fn lock_bit_set(group_meta_data: u64, index: usize) -> bool {
    group_meta_data & lock_bit(index) != 0
}

#[inline]
fn park_bit(index: usize) -> u64 {
    1 << (8 * index + 1)
}
#[inline]
fn park_bit_set(group_meta_data: u64, index: usize) -> bool {
    group_meta_data & park_bit(index) != 0
}

impl MetaData {
    #[inline]
    pub(crate) fn reserve(
        &self,
        group_meta_data: &mut u64,
        h2: u64,
        index: usize,
    ) -> ReserveResult {
        loop {
            let new_group_meta_data = *group_meta_data | h2_bits(h2 & 0xFC | LOCKED_BIT, index);
            if valid_bit_set(*group_meta_data, index) {
                return ReserveResult::Occupied;
            }
            if lock_bit_set(*group_meta_data, index) {
                let h2bits = h2_bits(h2 as u64 & 0xFC, index);
                if h2bits & *group_meta_data == h2bits {
                    return self.wait_on_lock_release(group_meta_data, index);
                }
                return ReserveResult::AlreadyReservedWithOtherH2;
            }
            match self.meta_data.compare_exchange_weak(
                *group_meta_data,
                new_group_meta_data,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    *group_meta_data = new_group_meta_data;
                    return ReserveResult::Reserved;
                }
                Err(new_meta_data) => {
                    *group_meta_data = new_meta_data;
                    continue;
                }
            }
        }
    }

    #[cold]
    fn wait_on_lock_release(&self, out_meta_data: &mut u64, index: usize) -> ReserveResult {
        let mut group_meta_data = self.meta_data.load(Ordering::Relaxed);
        let mut spinwait = SpinWait::new();
        loop {
            if valid_bit_set(group_meta_data, index) {
                *out_meta_data = group_meta_data;
                return ReserveResult::Occupied;
            }

            // If there is no queue, try spinning a few times
            if !park_bit_set(group_meta_data, index) && spinwait.spin() {
                group_meta_data = self.meta_data.load(Ordering::Relaxed);
                continue;
            }

            // Set the parked bit
            if !park_bit_set(group_meta_data, index) {
                if let Err(x) = self.meta_data.compare_exchange_weak(
                    group_meta_data,
                    group_meta_data | park_bit(index),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    group_meta_data = x;
                    continue;
                }
            }

            // Park our thread until we are woken up by an unlock
            let addr = self as *const _ as usize;
            let validate = || self.meta_data.load(Ordering::Relaxed) == group_meta_data;
            let before_sleep = || {};
            let timed_out = |_, _| {};
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
            group_meta_data = self.meta_data.load(Ordering::Relaxed);
        }
    }

    #[inline]
    pub(crate) fn set_valid_and_unpark(&self, mut group_meta_data: u64, h2: u64, index: usize) {
        let mut park_bit = false;
        loop {
            if park_bit_set(group_meta_data, index) {
                park_bit = true;
            }
            let new_group_meta_data =
                (group_meta_data & !h2_bits(0xff, index)) | valid_bit(index) | h2_bits(h2, index);
            if let Some(new_meta_data) = self
                .meta_data
                .compare_exchange_weak(
                    group_meta_data,
                    new_group_meta_data,
                    Ordering::Release,
                    Ordering::Relaxed,
                )
                .err()
            {
                group_meta_data = new_meta_data;
                continue;
            }
            break;
        }
        if park_bit {
            self.unpark_all();
        }
    }

    #[cold]
    fn unpark_all(&self) {
        let addr = self as *const _ as usize;
        // SAFETY:
        //   * `addr` is an address we control.
        unsafe {
            parking_lot_core::unpark_all(addr, TOKEN_VALUE_SET);
        }
    }

    #[inline]
    pub fn get_metadata_relaxed(&self) -> u64 {
        self.meta_data.load(Ordering::Relaxed)
    }
}

#[test]
fn reserve_reserved() {
    let meta_data = MetaData { meta_data: AtomicU64::new(0) };
    let mut group_meta_data = 0;
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 0);
    assert_eq!(meta_data.get_metadata_relaxed(), 0xFD);
    assert_eq!(result, ReserveResult::Reserved);
    assert_eq!(group_meta_data, 0xFD);
}

#[test]
fn reserve_reserved_index1() {
    let meta_data = MetaData { meta_data: AtomicU64::new(valid_bit(0) | h2_bits(0xA8, 0)) };
    let mut group_meta_data = valid_bit(0) | h2_bits(0xA8, 0);
    let result = meta_data.reserve(&mut group_meta_data, 0xFF, 1);
    assert_eq!(meta_data.get_metadata_relaxed(), 0x100_0000_0000_FDA8);
    assert_eq!(result, ReserveResult::Reserved);
    assert_eq!(group_meta_data, 0x100_0000_0000_FDA8);
}

#[test]
fn reserve_occupied() {
    let meta_data = MetaData { meta_data: AtomicU64::new(valid_bit(0) | h2_bits(0xAB, 0)) };
    let mut group_meta_data = 0;
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 0);
    assert_eq!(meta_data.get_metadata_relaxed(), 0x100_0000_0000_00AB);
    assert_eq!(result, ReserveResult::Occupied);
    assert_eq!(group_meta_data, 0x100_0000_0000_00AB);
}

#[test]
fn reserve_occupied_index1() {
    let meta_data = MetaData {
        meta_data: AtomicU64::new(
            valid_bit(0) | h2_bits(0xAB, 0) | valid_bit(1) | h2_bits(0xAA, 1),
        ),
    };
    let mut group_meta_data = 0;
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 1);
    assert_eq!(meta_data.get_metadata_relaxed(), 0x300_0000_0000_AAAB);
    assert_eq!(result, ReserveResult::Occupied);
    assert_eq!(group_meta_data, 0x300_0000_0000_AAAB);
}

#[test]
fn reserve_already_reserved_with_other_h2() {
    let meta_data = MetaData { meta_data: AtomicU64::new(h2_bits(0xAD, 0)) };
    let mut group_meta_data = 0;
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 0);
    assert_eq!(meta_data.get_metadata_relaxed(), 0xAD);
    assert_eq!(result, ReserveResult::AlreadyReservedWithOtherH2);
    assert_eq!(group_meta_data, 0xAD);
}

#[test]
fn reserve_already_reserved_with_other_h2_index1() {
    let meta_data =
        MetaData { meta_data: AtomicU64::new(valid_bit(0) | h2_bits(0xAB, 0) | h2_bits(0xAD, 1)) };
    let mut group_meta_data = valid_bit(0) | h2_bits(0xAB, 0);
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 1);
    assert_eq!(meta_data.get_metadata_relaxed(), 0x100_0000_0000_ADAB);
    assert_eq!(result, ReserveResult::AlreadyReservedWithOtherH2);
    assert_eq!(group_meta_data, 0x100_0000_0000_ADAB);
}

#[test]
fn set_valid() {
    let meta_data =
        MetaData { meta_data: AtomicU64::new(valid_bit(0) | h2_bits(0xAB, 0) | h2_bits(0xAD, 1)) };
    let group_meta_data = valid_bit(0) | h2_bits(0xAB, 0) | h2_bits(0xAD, 1);
    meta_data.set_valid_and_unpark(group_meta_data, 0xA8, 1);
    assert_eq!(meta_data.get_metadata_relaxed(), 0x300_0000_0000_A8AB);
}
