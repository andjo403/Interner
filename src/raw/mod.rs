use core::hash::{BuildHasher, Hash, Hasher};
use core::marker::PhantomData;
use core::sync::atomic::{AtomicIsize, AtomicPtr, Ordering};
use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::borrow::Borrow;
use std::mem;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Mutex;

mod bitmask;
use bitmask::BitMaskIter;
mod meta_data;

mod sse2;
use self::sse2::match_byte;
use meta_data::{
    bucket_full, bucket_moved, get_valid_bits, MetaData, ReserveResult, GROUP_MOVED_BIT_MASK,
};

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
        let result = self.pos;
        self.stride += 1;
        self.pos += self.stride;
        self.pos &= self.bucket_mask;
        Some(result)
    }
}

/// Primary hash function, used to select the initial bucket to probe from.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn h1(hash: u64) -> usize {
    // On 32-bit platforms we simply ignore the higher hash bits.
    hash as usize
}

/// Secondary hash function, saved in the meta data.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn h2(hash: u64) -> u8 {
    // Grab the top 8 bits of the hash. While the hash is normally a full 64-bit
    // value, some hash functions (such as FxHash) produce a usize result
    // instead, which means that the top 32 bits are 0 on 32-bit platforms.
    let hash_len = usize::min(std::mem::size_of::<usize>(), std::mem::size_of::<u64>());
    let top8 = hash >> (hash_len * 8 - 8);
    top8 as u8 // truncation
}

/// Returns the number of buckets needed to hold the given number of items,
/// taking the maximum load factor into account.
///
/// Returns `None` if an overflow occurs.
#[inline]
fn capacity_to_buckets(cap: usize) -> Option<usize> {
    // require 1/8 buckets to be empty (87.5% load)
    let adjusted_cap = cap.checked_mul(8)? / 7;
    // as there is 7 elemntes in each bucket
    let adjusted_buckets =
        if adjusted_cap % 7 == 0 { adjusted_cap / 7 } else { adjusted_cap / 7 + 1 };

    // Any overflows will have been caught by the checked_mul. Also, any
    // rounding errors from the division above will be cleaned up by
    // next_power_of_two (which can't overflow because of the previous divison).
    Some(adjusted_buckets.next_power_of_two())
}

/// Returns the maximum number of buckets to check before a resize is triggered.
#[inline]
fn buckets_to_resize_limit(buckets: usize) -> usize {
    if buckets <= 64 {
        1
    } else {
        4
    }
}

#[repr(align(64))]
struct Bucket<T> {
    pub meta_data: MetaData,
    pub refs: [T; 7],
}

impl<T> Bucket<T> {
    #[inline]
    fn get_ref_to_slot(&mut self, index: usize) -> &mut T {
        unsafe { self.refs.get_unchecked_mut(index) }
    }

    // move all valid slots from this bucket to the next interner
    fn transfer_bucket(
        &mut self,
        mut group_meta_data: u64,
        new_raw_interner: &mut RawInterner<T>,
        hasher: impl Fn(&T) -> u64,
    ) where
        T: Sync + Send + Copy,
    {
        if !self.meta_data.mark_as_moved(&mut group_meta_data) {
            return; //already moved
        }
        let valid_bits = get_valid_bits(group_meta_data);
        let iter = BitMaskIter::new(valid_bits, 1);
        for index in iter {
            let value = self.get_ref_to_slot(index);
            let hash = hasher(value);
            match new_raw_interner.transfer_value(hash) {
                LockResult::ResizeNeeded => panic!("ResizeNeeded during transfer"),
                LockResult::Moved => panic!("Moved during transfer"),
                LockResult::Locked(locked_data) => {
                    new_raw_interner.unlock_and_set_value(hash, *value, locked_data);
                }
                LockResult::Found(_) => panic!("can not be fund in new interner during transfer"),
            }
        }
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
    buckets: NonNull<Bucket<T>>,

    // Number of buckets that is checked before the table is resized
    resize_limit: usize,

    // Pointer to the next interner created during resize
    next_raw_interner: AtomicPtr<RawInterner<T>>,
    // count the slots that have not been moved when the bucket was moved due to the slot was looked at the time of the bucket move
    to_be_moved: AtomicIsize,
    resize_lock: Mutex<()>,
    phantom: PhantomData<T>,
}

impl<T> Drop for RawInterner<T> {
    fn drop(&mut self) {
        if self.bucket_mask != 0 {
            let layout = Layout::array::<Bucket<T>>(self.bucket_mask + 1)
                .expect("Interner capacity overflow");
            let ptr = self.buckets.as_ptr() as *mut u8;
            unsafe { dealloc(ptr, layout) };
        }
    }
}

impl<T> RawInterner<T>
where
    T: Sync + Send + Copy,
{
    /// Creates a new empty hash table without allocating any memory.
    ///
    /// In effect this returns a table with exactly 1 bucket. However we can
    /// leave the data pointer dangling since that bucket is never written to
    /// due to our load factor forcing us to always have at least 1 free bucket.
    #[inline]
    pub fn new() -> Self {
        static mut STATIC_BUCKET: [u64; 8] = [GROUP_MOVED_BIT_MASK; 8];
        Self {
            // SAFTY shall not mutate this as the capacity is to smal
            buckets: NonNull::new(unsafe { STATIC_BUCKET }.as_mut_ptr() as *mut Bucket<T>).unwrap(),
            bucket_mask: 0,
            resize_limit: 0,
            next_raw_interner: AtomicPtr::new(ptr::null_mut()),
            to_be_moved: AtomicIsize::new(0),
            resize_lock: Mutex::new(()),
            phantom: PhantomData,
        }
    }

    /// Allocates a new hash table with the given number of buckets.
    ///
    /// The control bytes are left uninitialized.
    #[inline]
    fn new_uninitialized(buckets: usize) -> Self {
        debug_assert!(buckets.is_power_of_two());
        debug_assert!(mem::size_of::<Bucket<T>>() == 64);

        let layout = Layout::array::<Bucket<T>>(buckets).expect("Interner capacity overflow");
        Self {
            buckets: NonNull::new(unsafe { alloc_zeroed(layout) } as *mut Bucket<T>)
                .unwrap_or_else(|| handle_alloc_error(layout)),
            bucket_mask: buckets - 1,
            resize_limit: buckets_to_resize_limit(buckets),
            next_raw_interner: AtomicPtr::new(ptr::null_mut()),
            to_be_moved: AtomicIsize::new(0),
            resize_lock: Mutex::new(()),
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
    #[cfg_attr(feature = "inline-more", inline)]
    fn probe_seq(&self, hash: u64) -> ProbeSeq {
        ProbeSeq {
            bucket_mask: self.bucket_mask,
            pos: h1(hash) & self.bucket_mask,
            stride: 0,
            resize_limit: self.resize_limit,
        }
    }

    pub(crate) fn get_next_raw_interner(&self) -> Option<&mut Self> {
        let next = self.next_raw_interner.load(Ordering::Acquire);
        if next.is_null() {
            None
        } else {
            Some(unsafe { &mut *next })
        }
    }

    pub(crate) fn create_and_stor_next_raw_interner(&self) -> &mut Self {
        let new_number_of_buckets = (self.bucket_mask + 1) * 2;
        let boxed_new_raw_interner = Box::new(Self::new_uninitialized(new_number_of_buckets));
        let new_raw_interner = Box::into_raw(boxed_new_raw_interner);
        self.next_raw_interner.store(new_raw_interner, Ordering::Release);
        unsafe { &mut *new_raw_interner }
    }
    pub(crate) fn transfer(&self, new_raw_interner: &mut Self, hasher: impl Fn(&T) -> u64) {
        let mut to_be_moved = 0;
        for pos in 0..self.bucket_mask {
            let bucket = unsafe { &mut *self.buckets.as_ptr().add(pos) };
            let group_meta_data = bucket.meta_data.get_metadata_relaxed();
            bucket.transfer_bucket(group_meta_data, new_raw_interner, &hasher);
        }
        to_be_moved += self.to_be_moved.fetch_add(to_be_moved, Ordering::Relaxed);
        if to_be_moved == 0 {
            // all slots have been moved the new table can replace this table as it contains all slots from this table
        }
    }

    /// Searches for an element in the table and if not found lockes a slot to be able to add the element
    #[inline]
    pub(crate) fn lock_or_get_slot<Q>(&mut self, hash: u64, value: &Q) -> LockResult<T>
    where
        T: Borrow<Q>,
        Q: Sync + Send + Eq,
    {
        let h2 = h2(hash);
        for pos in self.probe_seq(hash) {
            // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
            let bucket = unsafe { &mut *self.buckets.as_ptr().add(pos) };
            let group_meta_data = bucket.meta_data.get_metadata_relaxed();
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
            let iter = BitMaskIter::new((!valid_bits) & 0x7F, 1);
            let mut group_meta_data = group_meta_data;
            for index in iter {
                match bucket.meta_data.reserve(&mut group_meta_data, h2 as u64, index, true) {
                    ReserveResult::Reserved => {
                        return LockResult::Locked(LockedData { pos, index, group_meta_data });
                    }
                    // needs to continue checking as there can still be
                    ReserveResult::AlreadyReservedWithOtherH2 | ReserveResult::Moved => {
                        continue;
                    }
                    ReserveResult::Occupied => {
                        let result = bucket.get_ref_to_slot(index);
                        if value.eq((*result).borrow()) {
                            return LockResult::Found(*result);
                        }
                        continue;
                    }
                }
            }
            if !bucket_full(group_meta_data) && bucket_moved(group_meta_data) {
                return LockResult::Moved;
            }
        }
        LockResult::ResizeNeeded
    }

    // the value is not allowed to be in this instance of 'RawInterner' and no other thread is allowed to try to intern it
    // this function is used for resize and the value is then in the previous instance of 'RawInterner'.
    pub(crate) fn transfer_value(&mut self, hash: u64) -> LockResult<T> {
        let h2 = h2(hash);
        for pos in self.probe_seq(hash) {
            // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
            let bucket = unsafe { &mut *self.buckets.as_ptr().add(pos) };
            let group_meta_data = bucket.meta_data.get_metadata_relaxed();

            if bucket_full(group_meta_data) {
                // this bucket is full try the next bucket
                continue;
            }

            // the bucket was not full when the metadata was fetched but new values can have been added
            // during the search but even if the metadata have been updated and the index is now used the
            // value needs to be check as it can be the value that shall be added
            let valid_bits = get_valid_bits(group_meta_data);
            let iter = BitMaskIter::new((!valid_bits) & 0x7F, 1);
            let mut group_meta_data = group_meta_data;
            for index in iter {
                match bucket.meta_data.reserve(&mut group_meta_data, h2 as u64, index, false) {
                    ReserveResult::Reserved => {
                        return LockResult::Locked(LockedData { pos, index, group_meta_data });
                    }
                    ReserveResult::AlreadyReservedWithOtherH2 | ReserveResult::Occupied => {
                        continue;
                    }
                    ReserveResult::Moved => {
                        return LockResult::Moved; // no need to update this table as as there is a newer table
                    }
                }
            }
            if !bucket_full(group_meta_data) && bucket_moved(group_meta_data) {
                return LockResult::Moved;
            }
        }
        LockResult::ResizeNeeded
    }

    // unlock the slot by marking the element as valid unparks all threads blocked on this slot
    // and if the bucket is moved transer the value to the new interner also.
    #[inline]
    pub(crate) fn unlock_and_set_value(&mut self, hash: u64, value: T, locked_data: LockedData) {
        let LockedData { pos, index, group_meta_data } = locked_data;
        // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
        let bucket = unsafe { &mut *self.buckets.as_ptr().add(pos) };
        *bucket.get_ref_to_slot(index) = value;

        let h2 = h2(hash);
        if bucket.meta_data.set_valid_and_unpark(group_meta_data, h2 as u64, index) {
            self.to_be_moved.fetch_sub(1, Ordering::Relaxed);
            panic!("Moved during transfer");
            // let raw_interner = self.get_next_raw_interner().unwrap();
            // let locked_data = raw_interner.transfer_value(hash);
            // let LockResult::Locked(locked_data) = locked_data;
            // raw_interner.unlock_and_set_value(hash, value, locked_data);
        }
    }
}

impl<T> RawInterner<T>
where
    T: Sync + Send + Copy + Hash,
{
    #[inline]
    pub fn intern_ref<Q: Sized>(
        &mut self,
        hash_builder: &impl BuildHasher,
        value: &Q,
        make: impl FnOnce() -> T,
    ) -> T
    where
        T: Sync + Send + Borrow<Q> + Copy,
        Q: Sync + Send + Hash + Eq,
    {
        let hash = make_hash(hash_builder, value);
        let mut raw_interner = self;
        loop {
            match raw_interner.lock_or_get_slot(hash, value) {
                LockResult::Found(result) => {
                    return result;
                }
                LockResult::Locked(locked_data) => {
                    let result = make();
                    raw_interner.unlock_and_set_value(hash, result, locked_data);
                    return result;
                }
                LockResult::ResizeNeeded => {
                    if let Some(new_raw_interner) = raw_interner.get_next_raw_interner() {
                        raw_interner = new_raw_interner;
                    } else {
                        let _guard = raw_interner.resize_lock.lock();
                        if let Some(new_raw_interner) = raw_interner.get_next_raw_interner() {
                            raw_interner = new_raw_interner;
                        } else {
                            let new_raw_interner = raw_interner.create_and_stor_next_raw_interner();
                            raw_interner.transfer(new_raw_interner, |x| make_hash(hash_builder, x));
                            raw_interner = new_raw_interner;
                        }
                    }
                }
                LockResult::Moved => {
                    raw_interner = raw_interner.get_next_raw_interner().unwrap();
                    continue;
                }
            }
        }
    }
}
