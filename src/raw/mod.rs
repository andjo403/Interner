use core::sync::atomic::{AtomicPtr, Ordering};
use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::borrow::Borrow;
use std::mem;
use std::ptr;
use std::ptr::NonNull;

cfg_if::cfg_if! {
    // Use the SSE2 implementation if possible: it allows us to scan 16 buckets
    // at once instead of 8. We don't bother with AVX since it would require
    // runtime dispatch and wouldn't gain us much anyways: the probability of
    // finding a match drops off drastically after the first few buckets.
    //
    // I attempted an implementation on ARM using NEON instructions, but it
    // turns out that most NEON instructions have multi-cycle latency, which in
    // the end outweighs any gains over the generic implementation.
    if #[cfg(all(
        target_feature = "sse2",
        any(target_arch = "x86", target_arch = "x86_64"),
        not(miri)
    ))] {
        mod sse2;
        use sse2 as imp;
    } else {
        #[path = "generic.rs"]
        mod generic;
        use generic as imp;
    }
}

mod bitmask;
use bitmask::BitMaskIter;
mod meta_data;

use self::imp::match_byte;
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

/// Secondary hash function, saved in the low 7 bits of the control byte.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn h2(hash: u64) -> u8 {
    // Grab the top 8 bits of the hash. While the hash is normally a full 64-bit
    // value, some hash functions (such as FxHash) produce a usize result
    // instead, which means that the top 32 bits are 0 on 32-bit platforms.
    let hash_len = usize::min(std::mem::size_of::<usize>(), std::mem::size_of::<u64>());
    let top8 = hash >> (hash_len * 8 - 8);
    (top8 & 0xff) as u8 // truncation
}

/// Returns the number of buckets needed to hold the given number of items,
/// taking the maximum load factor into account.
///
/// Returns `None` if an overflow occurs.
#[inline]
fn capacity_to_buckets(cap: usize) -> Option<usize> {
    let adjusted_cap = if cap < 8 {
        // Need at least 1 free bucket on small tables
        cap + 1
    } else {
        // Otherwise require 1/8 buckets to be empty (87.5% load)
        //
        // Be careful when modifying this, calculate_layout relies on the
        // overflow check here.
        cap.checked_mul(8)? / 7
    };
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
    if buckets <= 64 { 1 } else { 4 }
}

#[repr(align(64))]
struct Bucket<T> {
    pub meta_data: MetaData,
    pub refs: [T; 7],
}

impl<T> Bucket<T> {
    #[inline]
    fn get_ref(&mut self, index: usize) -> &mut T {
        unsafe { self.refs.get_unchecked_mut(index) }
    }

    fn transfer_bucket(
        &mut self,
        mut group_meta_data: u64,
        new_raw_interner: &mut RawInterner<T>,
        hasher: impl Fn(&T) -> u64,
    ) where
        T: Sync + Send + Copy,
    {
        if self.meta_data.mark_as_moved(&mut group_meta_data).is_none() {
            return; //already moved
        }
        let valid_bits = get_valid_bits(group_meta_data);
        let iter = BitMaskIter::new(valid_bits, 1);
        for index in iter {
            let value = self.get_ref(index);
            let hash = hasher(value);
            new_raw_interner.transfer_value(hash, *value);
        }
    }
}

/// A raw hash table with an unsafe API.
pub(crate) struct RawInterner<T> {
    // Mask to get an index from a hash value. The value is one less than the
    // number of buckets in the table.
    bucket_mask: usize,

    // Pointer to the array of control bytes
    buckets: NonNull<Bucket<T>>,

    // Number of buckets that is checked before the table is resized
    resize_limit: usize,

    next_raw_interner: AtomicPtr<RawInterner<T>>,
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
        if next.is_null() { None } else { Some(unsafe { &mut *next }) }
    }

    pub(crate) fn resize(&self, hasher: impl Fn(&T) -> u64) -> &mut Self {
        let new_number_of_buckets = (self.bucket_mask + 1) * 2;
        let boxed_new_raw_interner = Box::new(Self::new_uninitialized(new_number_of_buckets));
        let new_raw_interner = Box::into_raw(boxed_new_raw_interner);
        self.next_raw_interner.store(new_raw_interner, Ordering::Release);
        let new_raw_interner = unsafe { &mut *new_raw_interner };
        for pos in 0..self.bucket_mask + 1 {
            let bucket = unsafe { &mut *self.buckets.as_ptr().add(pos) };
            let group_meta_data = bucket.meta_data.get_metadata_relaxed();
            bucket.transfer_bucket(group_meta_data, new_raw_interner, &hasher);
        }
        new_raw_interner
    }

    /// Searches for an element in the table.
    #[inline]
    pub(crate) fn intern<Q>(&mut self, hash: u64, value: &Q, make: impl FnOnce() -> T) -> Option<T>
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
                let result = bucket.get_ref(index);
                if (*value).eq((*result).borrow()) {
                    return Some(*result);
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
                match bucket.meta_data.reserve(&mut group_meta_data, h2 as u64, index) {
                    ReserveResult::Reserved => {}
                    ReserveResult::AlreadyReservedWithOtherH2 | ReserveResult::Moved => {
                        continue;
                    }
                    ReserveResult::Occupied => {
                        let result = bucket.get_ref(index);
                        if value.eq((*result).borrow()) {
                            return Some(*result);
                        }
                        continue;
                    }
                }
                let result = make();
                *bucket.get_ref(index) = result;

                if bucket.meta_data.set_valid_and_unpark(group_meta_data, h2 as u64, index) {
                    self.get_next_raw_interner().unwrap().transfer_value(hash, result);
                }
                return Some(result);
            }
            if bucket_moved(group_meta_data) {
                return None;
            }
        }
        None
    }

    // the value is not allowed to be in this instance of 'RawInterner' and no other thread is allowed to try to intern it
    // this function is used for resize and the value is then in the previous instance of 'RawInterner'.
    pub(crate) fn transfer_value(&mut self, hash: u64, value: T) -> Option<T> {
        let h2 = h2(hash);
        for pos in self.probe_seq(hash) {
            // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
            let bucket = unsafe { &mut *self.buckets.as_ptr().add(pos) };
            let group_meta_data = bucket.meta_data.get_metadata_relaxed();
            let valid_bits = get_valid_bits(group_meta_data);

            if bucket_full(group_meta_data) {
                // this bucket is full try the next bucket
                continue;
            }

            // the bucket was not full when the metadata was fetched but new values can have been added
            // during the search but even if the metadata have been updated and the index is now used the
            // value needs to be check as it can be the value that shall be added
            let iter = BitMaskIter::new((!valid_bits) & 0x7F, 1);
            let mut group_meta_data = group_meta_data;
            for index in iter {
                match bucket.meta_data.only_reserve(&mut group_meta_data, index) {
                    ReserveResult::Reserved => {}
                    ReserveResult::AlreadyReservedWithOtherH2
                    | ReserveResult::Occupied
                    | ReserveResult::Moved => {
                        continue;
                    }
                }

                *bucket.get_ref(index) = value;

                if bucket.meta_data.set_valid_and_unpark(group_meta_data, h2 as u64, index) {
                    self.get_next_raw_interner().unwrap().transfer_value(hash, value);
                }
                return Some(value);
            }
        }
        None
    }
}
