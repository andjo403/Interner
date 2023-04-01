use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash, Hasher};
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicIsize, AtomicPtr, Ordering};
use std::sync::Once;

use crate::bitmask::BitMaskIter;

use crate::group_match::{count_locked_slots, match_byte};
use crate::meta_data::{bucket_full, bucket_moved, get_valid_bits, MetaData, ReserveResult};

/// Probe sequence based on triangular numbers, which is guaranteed (since our
/// table size is a power of two) to visit every group of elements exactly once.
///
/// A triangular probe has us jump by 1 more group every time. So first we
/// jump by 1 group (meaning we just continue our linear scan), then 2 groups
/// (skipping over 1 group), then 3 groups (skipping over 2 groups), and so on.
///
/// Proof that the probe will visit every group in the table:
/// <https://fgiesen.wordpress.com/2015/02/22/triangular-numbers-mod-2n/>
struct ProbeSeq {
    bucket_mask: usize,
    pos: usize,
    stride: usize,
    resize_limit: usize,
}

impl Iterator for ProbeSeq {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.stride >= self.resize_limit {
            return None;
        }
        let pos = (self.pos + self.stride) & self.bucket_mask;
        self.stride += 1;
        Some(pos)
    }
}

/// Primary hash function, used to select the initial bucket to probe from.
#[inline]
fn h1(hash: u64) -> usize {
    // On 32-bit platforms we simply ignore the higher hash bits.
    hash as usize
}

//While the hash is normally a full 64-bit value, some hash functions (such as FxHash)
//produce a usize result instead, which means that the top 32 bits are 0 on 32-bit platforms.
const HASH_BITS: u32 = if usize::BITS < u64::BITS { usize::BITS } else { u64::BITS };

/// Secondary hash function, saved in the meta data.
#[inline]
fn h2(hash: u64) -> u8 {
    // Grab the top 8 bits of the hash.
    (hash >> (HASH_BITS - 8)) as u8
}

/// Returns the number of buckets needed to hold the given number of items,
/// taking the maximum load factor into account.
///
/// Returns `None` if an overflow occurs.
#[inline]
fn capacity_to_buckets(cap: usize) -> Option<usize> {
    // begin with 1/7 slot per bucket to be empty (85.7% load)
    // and there is 7 elemnts per bucket
    let adjusted_buckets = (cap.checked_mul(7)? + 5) / 6;

    // Any overflows will have been caught by the checked_mul. Also, any
    // rounding errors from the division above will be cleaned up by
    // next_power_of_two (which can't overflow because of the previous divison).
    Some(adjusted_buckets.next_power_of_two())
}

/// Returns the maximum number of buckets to check before a resize is triggered.
#[inline]
fn buckets_to_resize_limit(buckets: usize) -> usize {
    if buckets <= 64 { 1 } else { 4 }
}

#[repr(align(64))]
struct Bucket<T> {
    pub meta_data: MetaData,
    pub refs: [MaybeUninit<UnsafeCell<T>>; 7],
}

impl<T> Bucket<T> {
    #[inline]
    unsafe fn set_slot(&self, index: usize, value: T) {
        UnsafeCell::raw_get(self.refs.get_unchecked(index).as_ptr()).write(value);
    }

    #[inline]
    fn get_ref_to_slot(&self, index: usize) -> &T {
        unsafe { &*self.refs.get_unchecked(index).assume_init_ref().get() }
    }

    // move all valid slots from this bucket to the next interner
    fn transfer_bucket(
        &self,
        new_raw_interner: &RawInterner<T>,
        hash_builder: &impl BuildHasher,
    ) -> isize
    where
        T: Copy + Hash,
    {
        let mut group_meta_data = 0;
        if !self.meta_data.mark_as_moved(&mut group_meta_data) {
            return 0; //already moved
        }
        let valid_bits = get_valid_bits(group_meta_data);
        let iter = BitMaskIter::new(valid_bits);
        let hasher = |x| make_hash(hash_builder, x);
        for index in iter {
            let value = self.get_ref_to_slot(index);
            let hash = hasher(value);
            let h2 = h2(hash);
            new_raw_interner.transfer_value_to_newer_interner(h2, hash, *value, hash_builder);
        }
        count_locked_slots(valid_bits, group_meta_data) + 1 // add one to markbucket as done
    }
}
pub(crate) struct LockedData {
    pos: usize,
    index: usize,
    group_meta_data: u64,
}
pub(crate) enum LockResult<T> {
    ResizeNeeded,
    Moved,
    Locked(LockedData),
    Found(T),
}

#[inline]
pub(crate) fn make_hash<K: Hash + ?Sized>(hash_builder: &impl BuildHasher, val: &K) -> u64 {
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}

/// A raw hash table with an unsafe API.
pub(crate) struct RawInterner<T> {
    // Mask to get an index from a hash value. The value is one less than the
    // number of buckets in the table.
    bucket_mask: usize,

    // Pointer to the array of Buckets
    buckets: *mut Bucket<T>,

    // Number of buckets that is checked before the table is resized
    resize_limit: usize,

    // Pointer to the next interner created during resize
    next_raw_interner: AtomicPtr<RawInterner<T>>,
    next_raw_interner_lock: Once,
    // count the slots that have not been moved when the bucket was moved due to the slot was looked at the time of the bucket move
    // when the sum is zero the transfer is compleate and only the new interner needs to be used.
    to_be_moved: AtomicIsize,
    phantom: PhantomData<T>,
}

unsafe impl<#[may_dangle] T> Drop for RawInterner<T> {
    fn drop(&mut self) {
        if !self.buckets.is_null() {
            let layout = Layout::array::<Bucket<T>>(self.bucket_mask + 1)
                .expect("Interner capacity overflow");
            let ptr = self.buckets as *mut u8;
            unsafe { dealloc(ptr, layout) };
            self.buckets = std::ptr::null_mut();
        }
        let temp = self.next_raw_interner.load(Ordering::Relaxed);
        if !temp.is_null() {
            let _next_raw_internere = unsafe { Box::from_raw(temp) };
        }
    }
}

impl<T> RawInterner<T> {
    /// Creates a new empty hash table without allocating any memory.
    ///
    /// In effect this returns a table with exactly 1 bucket. However we can
    /// leave the data pointer dangling since that bucket is never written to
    /// due to our load factor forcing us to always have at least 1 free bucket.
    #[inline]
    pub fn new() -> Self {
        Self {
            buckets: std::ptr::null_mut(),
            bucket_mask: 0,
            resize_limit: 0,
            next_raw_interner: AtomicPtr::default(),
            next_raw_interner_lock: Once::new(),
            to_be_moved: AtomicIsize::new(-1),
            phantom: PhantomData,
        }
    }

    /// Allocates a new hash table with the given number of buckets.
    ///
    /// The control bytes are left uninitialized.
    #[inline]
    fn new_uninitialized(buckets: usize) -> Self {
        debug_assert!(buckets.is_power_of_two());

        let layout = Layout::array::<Bucket<T>>(buckets).expect("Interner capacity overflow");
        Self {
            buckets: NonNull::new(unsafe { alloc_zeroed(layout) } as *mut Bucket<T>)
                .unwrap_or_else(|| handle_alloc_error(layout))
                .as_ptr(),
            bucket_mask: buckets - 1,
            resize_limit: buckets_to_resize_limit(buckets),
            next_raw_interner: AtomicPtr::default(),
            next_raw_interner_lock: Once::new(),
            to_be_moved: AtomicIsize::new(0 - (buckets as isize)),
            phantom: PhantomData,
        }
    }

    /// Allocates a new hash table with at least enough capacity for inserting
    /// the given number of elements without reallocating.
    pub fn with_capacity(capacity: usize) -> Self {
        if capacity == 0 {
            Self::new()
        } else {
            let buckets =
                capacity_to_buckets(capacity).expect("capacity to large to store in usize");
            Self::new_uninitialized(buckets)
        }
    }

    /// Returns an iterator for a probe sequence on the table.
    #[inline]
    fn probe_seq(&self, hash: u64) -> ProbeSeq {
        ProbeSeq {
            bucket_mask: self.bucket_mask,
            pos: h1(hash),
            stride: 0,
            resize_limit: self.resize_limit,
        }
    }

    /// Searches for an element in the table and if not found lockes a slot to be able to add the element
    #[inline]
    pub(crate) fn lock_or_get_slot<Q: ?Sized>(&self, hash: u64, value: &Q) -> LockResult<T>
    where
        T: Borrow<Q> + Copy,
        Q: Eq,
    {
        let h2 = h2(hash);
        for pos in self.probe_seq(hash) {
            // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
            let bucket = unsafe { &*self.buckets.add(pos) };
            let group_meta_data = bucket.meta_data.get_metadata_acquire();
            let valid_bits = get_valid_bits(group_meta_data);
            for index in match_byte(valid_bits, group_meta_data, h2) {
                let result = bucket.get_ref_to_slot(index);
                if (*value).eq((*result).borrow()) {
                    return LockResult::Found(*result);
                }
            }

            if bucket_full(group_meta_data) {
                // not found in this bucket and the bucket is full try the next bucket
                continue;
            }

            // the bucket was not full when the metadata was fetched but new values can have been added
            // during the search but even if the metadata have been updated and the index is now used the
            // value needs to be check as it can be the value that shall be added
            let iter = BitMaskIter::new((!valid_bits) & 0x7F);
            let mut group_meta_data = group_meta_data;
            for index in iter {
                match bucket.meta_data.reserve(&mut group_meta_data, h2, index) {
                    ReserveResult::Reserved => {
                        return LockResult::Locked(LockedData { pos, index, group_meta_data });
                    }
                    ReserveResult::AlreadyReservedWithSameH2 => {
                        bucket.meta_data.wait_on_lock_release(&mut group_meta_data, index);
                        let result = bucket.get_ref_to_slot(index);
                        if (*value).eq((*result).borrow()) {
                            return LockResult::Found(*result);
                        }
                        continue;
                    }
                    ReserveResult::AlreadyReservedWithOtherH2 => {
                        // needs to continue checking as there can still be slots matching
                        continue;
                    }
                    ReserveResult::SlotAvailableButGroupMoved => {
                        return LockResult::Moved;
                    }
                    ReserveResult::OccupiedWithSameH2 => {
                        let result = bucket.get_ref_to_slot(index);
                        if (*value).eq((*result).borrow()) {
                            return LockResult::Found(*result);
                        }
                        continue;
                    }
                }
            }
        }
        LockResult::ResizeNeeded
    }

    // the value is not allowed to be in this instance of 'RawInterner' and no other thread is allowed to try to intern it
    // this function is used for resize and the value is then in the previous instance of 'RawInterner'.
    fn lock_slot_for_transfer(&self, h2: u8, hash: u64) -> LockResult<T> {
        for pos in self.probe_seq(hash) {
            // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
            let bucket = unsafe { &*self.buckets.add(pos) };
            let group_meta_data = bucket.meta_data.get_metadata_acquire();

            if bucket_full(group_meta_data) {
                // this bucket is full try the next bucket
                continue;
            }

            // the bucket was not full when the metadata was fetched but new values can have been added
            // during the search but even if the metadata have been updated and the index is now used the
            // value needs to be check as it can be the value that shall be added
            let valid_bits = get_valid_bits(group_meta_data);
            let iter = BitMaskIter::new((!valid_bits) & 0x7F);
            let mut group_meta_data = group_meta_data;
            for index in iter {
                match bucket.meta_data.reserve(&mut group_meta_data, h2, index) {
                    ReserveResult::Reserved => {
                        return LockResult::Locked(LockedData { pos, index, group_meta_data });
                    }
                    ReserveResult::SlotAvailableButGroupMoved => {
                        return LockResult::Moved; // no need to update this table as as there is a newer table
                    }
                    ReserveResult::AlreadyReservedWithSameH2
                    | ReserveResult::AlreadyReservedWithOtherH2
                    | ReserveResult::OccupiedWithSameH2 => {
                        continue;
                    }
                }
            }
        }
        LockResult::ResizeNeeded
    }

    /// Searches for an element in the table
    #[inline]
    pub(crate) fn get(
        &self,
        hash: u64,
        is_match: &mut dyn FnMut(&T) -> bool,
    ) -> Option<Option<&T>> {
        let h2 = h2(hash);

        for pos in self.probe_seq(hash) {
            // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
            let bucket = unsafe { &*self.buckets.add(pos) };
            let group_meta_data = bucket.meta_data.get_metadata_acquire();
            let valid_bits = get_valid_bits(group_meta_data);
            for index in match_byte(valid_bits, group_meta_data, h2) {
                let result = bucket.get_ref_to_slot(index);
                if is_match(result) {
                    return Some(Some(result));
                }
            }

            if bucket_full(group_meta_data) {
                // not found in this bucket and the bucket is full try the next bucket
                continue;
            }

            if bucket_moved(group_meta_data) {
                return None;
            }
            break;
        }
        if self.next_raw_interner_lock.is_completed() { None } else { Some(None) }
    }

    pub(crate) fn get_next_raw_interner(&self) -> &Self {
        unsafe { &*self.next_raw_interner.load(Ordering::Acquire) }
    }

    // as the next interner can be moved before the current is moved we need to find the first interner that is not moved
    pub(crate) fn get_next_moved_raw_interner_ptr(&self) -> *mut Self {
        let mut moved_interner = self.next_raw_interner.load(Ordering::Acquire);
        loop {
            let raw_interner = unsafe { &*moved_interner };
            if raw_interner.next_raw_interner_lock.is_completed() {
                let next_interner = raw_interner.next_raw_interner.load(Ordering::Acquire);
                let next = unsafe { &*next_interner };
                if next.to_be_moved.load(Ordering::Relaxed) == 0 {
                    moved_interner = next_interner;
                } else {
                    return moved_interner;
                }
            } else {
                return moved_interner;
            }
        }
    }
}

impl<T> RawInterner<T>
where
    T: Copy + Hash,
{
    // unlock the slot by marking the element as valid unparks all threads blocked on this slot
    // and if the bucket is moved transer the value to the new interner also.
    #[inline]
    pub(crate) fn unlock_and_set_value(
        &self,
        hash: u64,
        value: T,
        locked_data: LockedData,
        hash_builder: &impl BuildHasher,
    ) -> bool {
        let LockedData { pos, index, group_meta_data } = locked_data;
        // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
        let bucket = unsafe { &*self.buckets.add(pos) };
        // SAFTY: as the index is caped
        unsafe { bucket.set_slot(index, value) };

        let h2 = h2(hash);
        if bucket.meta_data.set_valid_and_unpark(group_meta_data, h2, index) {
            self.transfer_value_to_newer_interner(h2, hash, value, hash_builder);
            self.to_be_moved.fetch_sub(1, Ordering::Relaxed) == 1
        } else {
            false
        }
    }

    pub(crate) fn create_and_stor_next_raw_interner(
        &self,
        hash_builder: &impl BuildHasher,
    ) -> bool {
        self.next_raw_interner_lock.call_once(|| {
            let new_number_of_buckets = (self.bucket_mask + 1) * 2;
            let raw_interner = Box::new(Self::new_uninitialized(new_number_of_buckets));
            self.next_raw_interner.store(Box::into_raw(raw_interner), Ordering::Release);
        });
        self.transfer(self.get_next_raw_interner(), hash_builder)
    }

    fn transfer(&self, new_raw_interner: &Self, hash_builder: &impl BuildHasher) -> bool {
        let mut to_be_moved = 0;
        if self.bucket_mask != 0 {
            for pos in 0..self.bucket_mask + 1 {
                let bucket = unsafe { &*self.buckets.add(pos) };
                to_be_moved += bucket.transfer_bucket(new_raw_interner, hash_builder);
            }
        } else {
            to_be_moved = 1;
        }
        self.to_be_moved.fetch_add(to_be_moved, Ordering::Relaxed) == -to_be_moved
    }

    // the value is not allowed to be in this instance of 'RawInterner' and no other thread is allowed to try to intern it
    // this function is used for resize and the value is then in the previous instance of 'RawInterner'.
    fn transfer_value_to_newer_interner(
        &self,
        h2: u8,
        hash: u64,
        value: T,
        hash_builder: &impl BuildHasher,
    ) {
        let mut raw_interner = self;
        loop {
            match raw_interner.lock_slot_for_transfer(h2, hash) {
                LockResult::ResizeNeeded => {
                    raw_interner.create_and_stor_next_raw_interner(hash_builder);
                    raw_interner = raw_interner.get_next_raw_interner();
                }
                LockResult::Moved => {
                    raw_interner = raw_interner.get_next_raw_interner();
                    continue;
                }
                LockResult::Locked(locked_data) => {
                    raw_interner.unlock_and_set_value(hash, value, locked_data, hash_builder);
                    break;
                }
                LockResult::Found(_) => panic!("can not be fund in new interner during transfer"),
            }
        }
    }
}

unsafe impl<T: Send + Sync> Sync for RawInterner<T> {}
unsafe impl<T: Send + Sync> Send for RawInterner<T> {}
