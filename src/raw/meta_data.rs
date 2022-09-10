use crate::sync::{fence, AtomicU64, Condvar, Mutex, Ordering};
use std::mem::{self, MaybeUninit};
use std::sync::LazyLock;

/// This bit is set instead of h2 if valid bit is not set when that mutex is locked by some thread.
const LOCKED_BIT: u8 = 0x01;

const GROUP_FULL_BIT_MASK: u64 = 0x7f00_0000_0000_0000;
const GROUP_MOVED_BIT_MASK: u64 = 0x8000_0000_0000_0000;
#[derive(Debug, PartialEq, Eq)]
pub enum ReserveResult {
    AlreadyReservedWithOtherH2,
    AlreadyReservedWithSameH2,
    Reserved,
    Occupied,
    SlotAvailableButGroupMoved,
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
    /// |1 group moved bit | 7 slot valid bits| 7 slots of 8 bit h2 hash or 6 bit h2 hash and lock bits when valid bit unset
    meta_data: AtomicU64,
}

static CONDVARS: LazyLock<[(Mutex<()>, Condvar); 64]> = LazyLock::new(|| {
    // Create an uninitialized array of `MaybeUninit`. The `assume_init` is
    // safe because the type we are claiming to have initialized here is a
    // bunch of `MaybeUninit`s, which do not require initialization.
    let mut condvars: [MaybeUninit<(Mutex<()>, Condvar)>; 64] =
        unsafe { MaybeUninit::uninit().assume_init() };

    // Dropping a `MaybeUninit` does nothing. Thus using raw pointer
    // assignment instead of `ptr::write` does not cause the old
    // uninitialized value to be dropped. Also if there is a panic during
    // this loop, we have a memory leak, but there is no memory safety
    // issue.
    for elem in &mut condvars[..] {
        elem.write(Default::default());
    }

    // Everything is initialized. Transmute the array to the
    // initialized type.
    unsafe { mem::transmute::<_, [(Mutex<()>, Condvar); 64]>(condvars) }
});

#[inline]
fn valid_bit(index: usize) -> u64 {
    1 << (64 - 8 + index)
}
#[inline]
fn test_valid_bit(group_meta_data: u64, index: usize) -> bool {
    group_meta_data & valid_bit(index) != 0
}
#[inline]
fn h2_bits(h2: u8, index: usize) -> u64 {
    (h2 as u64) << (8 * index)
}

#[inline]
fn h2_from_meta(group_meta_data: u64, index: usize) -> u8 {
    ((group_meta_data >> ((8 * index) + 2)) & 0x3f) as u8
}

#[inline]
fn lock_bit(index: usize) -> u64 {
    1 << (8 * index)
}
#[inline]
fn test_lock_bit(group_meta_data: u64, index: usize) -> bool {
    group_meta_data & lock_bit(index) != 0
}

#[inline]
fn park_bit(index: usize) -> u64 {
    1 << (8 * index + 1)
}
#[inline]
fn test_park_bit(group_meta_data: u64, index: usize) -> bool {
    group_meta_data & park_bit(index) != 0
}
#[inline]
pub(crate) fn bucket_moved(group_meta_data: u64) -> bool {
    group_meta_data & GROUP_MOVED_BIT_MASK == GROUP_MOVED_BIT_MASK
}
#[inline]
pub(crate) fn bucket_full(group_meta_data: u64) -> bool {
    group_meta_data & GROUP_FULL_BIT_MASK == GROUP_FULL_BIT_MASK
}

#[inline]
pub(crate) fn get_valid_bits(group_meta_data: u64) -> u8 {
    ((group_meta_data & GROUP_FULL_BIT_MASK) >> (64 - 8)) as u8
}

impl MetaData {
    #[inline]
    pub(crate) fn reserve(&self, group_meta_data: &mut u64, h2: u8, index: usize) -> ReserveResult {
        loop {
            if test_valid_bit(*group_meta_data, index) {
                return ReserveResult::Occupied;
            }
            if test_lock_bit(*group_meta_data, index) {
                if h2_from_meta(*group_meta_data, index) == (h2 >> 2) {
                    return ReserveResult::AlreadyReservedWithSameH2;
                }
                return ReserveResult::AlreadyReservedWithOtherH2;
            }
            if bucket_moved(*group_meta_data) {
                return ReserveResult::SlotAvailableButGroupMoved;
            }
            let new_group_meta_data = *group_meta_data | h2_bits(h2 & 0xFC | LOCKED_BIT, index);
            match self.meta_data.compare_exchange_weak(
                *group_meta_data,
                new_group_meta_data,
                Ordering::Acquire,
                Ordering::Acquire,
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
    pub(crate) fn wait_on_lock_release(&self, out_meta_data: &mut u64, index: usize, h2: u8) {
        let mut group_meta_data = self.meta_data.load(Ordering::Relaxed);
        loop {
            if test_valid_bit(group_meta_data, index) {
                *out_meta_data = group_meta_data;
                fence(Ordering::Acquire);
                return;
            }

            // Set the parked bit
            if !test_park_bit(group_meta_data, index) {
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
            let h2 = (h2 >> 2) as usize;
            let (lock, condvar) = &CONDVARS[h2];
            let guard = lock.lock().unwrap();
            drop(condvar.wait(guard).unwrap());
            // Loop back and check if the valid bit was set
            group_meta_data = self.meta_data.load(Ordering::Relaxed);
        }
    }

    pub(crate) fn set_valid_and_unpark(
        &self,
        mut group_meta_data: u64,
        h2: u8,
        index: usize,
    ) -> bool {
        loop {
            let new_group_meta_data =
                (group_meta_data & !h2_bits(0xff, index)) | valid_bit(index) | h2_bits(h2, index);
            match self.meta_data.compare_exchange_weak(
                group_meta_data,
                new_group_meta_data,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    if test_park_bit(group_meta_data, index) {
                        self.unpark_all(h2);
                    }
                    return bucket_moved(group_meta_data);
                }
                Err(new_meta_data) => {
                    group_meta_data = new_meta_data;
                }
            }
        }
    }

    #[cold]
    fn unpark_all(&self, h2: u8) {
        let h2 = (h2 >> 2) as usize;
        let (lock, condvar) = &CONDVARS[h2];
        let _guard = lock.lock().unwrap();
        condvar.notify_all();
    }

    #[inline]
    pub fn get_metadata_acquire(&self) -> u64 {
        self.meta_data.load(Ordering::Acquire)
    }

    pub(crate) fn mark_as_moved(&self, group_meta_data: &mut u64) -> bool {
        let old_group_meta_data = self.meta_data.fetch_or(GROUP_MOVED_BIT_MASK, Ordering::AcqRel);
        *group_meta_data = old_group_meta_data | GROUP_MOVED_BIT_MASK;
        !bucket_moved(old_group_meta_data)
    }
}

#[test]
fn reserve_reserved() {
    let meta_data = MetaData { meta_data: AtomicU64::new(0) };
    let mut group_meta_data = 0;
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 0);
    assert_eq!(meta_data.get_metadata_acquire(), 0xFD);
    assert_eq!(result, ReserveResult::Reserved);
    assert_eq!(group_meta_data, 0xFD);
}

#[test]
fn reserve_reserved_index1() {
    let meta_data = MetaData { meta_data: AtomicU64::new(valid_bit(0) | h2_bits(0xA8, 0)) };
    let mut group_meta_data = valid_bit(0) | h2_bits(0xA8, 0);
    let result = meta_data.reserve(&mut group_meta_data, 0xFF, 1);
    assert_eq!(meta_data.get_metadata_acquire(), 0x100_0000_0000_FDA8);
    assert_eq!(result, ReserveResult::Reserved);
    assert_eq!(group_meta_data, 0x100_0000_0000_FDA8);
}

#[test]
fn reserve_occupied() {
    let meta_data = MetaData { meta_data: AtomicU64::new(valid_bit(0) | h2_bits(0xAB, 0)) };
    let mut group_meta_data = 0;
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 0);
    assert_eq!(meta_data.get_metadata_acquire(), 0x100_0000_0000_00AB);
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
    assert_eq!(meta_data.get_metadata_acquire(), 0x300_0000_0000_AAAB);
    assert_eq!(result, ReserveResult::Occupied);
    assert_eq!(group_meta_data, 0x300_0000_0000_AAAB);
}

#[test]
fn reserve_already_reserved_with_other_h2() {
    let meta_data = MetaData { meta_data: AtomicU64::new(h2_bits(0xAD, 0)) };
    let mut group_meta_data = 0;
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 0);
    assert_eq!(meta_data.get_metadata_acquire(), 0xAD);
    assert_eq!(result, ReserveResult::AlreadyReservedWithOtherH2);
    assert_eq!(group_meta_data, 0xAD);
}

#[test]
fn reserve_already_reserved_with_other_h2_index1() {
    let meta_data =
        MetaData { meta_data: AtomicU64::new(valid_bit(0) | h2_bits(0xAB, 0) | h2_bits(0xAD, 1)) };
    let mut group_meta_data = valid_bit(0) | h2_bits(0xAB, 0);
    let result = meta_data.reserve(&mut group_meta_data, 0xFC, 1);
    assert_eq!(meta_data.get_metadata_acquire(), 0x100_0000_0000_ADAB);
    assert_eq!(result, ReserveResult::AlreadyReservedWithOtherH2);
    assert_eq!(group_meta_data, 0x100_0000_0000_ADAB);
}

#[test]
fn set_valid() {
    let meta_data =
        MetaData { meta_data: AtomicU64::new(valid_bit(0) | h2_bits(0xAB, 0) | h2_bits(0xAD, 1)) };
    let group_meta_data = valid_bit(0) | h2_bits(0xAB, 0) | h2_bits(0xAD, 1);
    meta_data.set_valid_and_unpark(group_meta_data, 0xAC, 1);
    assert_eq!(meta_data.get_metadata_acquire(), 0x300_0000_0000_ACAB);
}
