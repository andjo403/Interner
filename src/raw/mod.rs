use core::sync::atomic::{AtomicIsize, Ordering};
use std::alloc::{alloc_zeroed, dealloc, handle_alloc_error, Layout};
use std::borrow::Borrow;
use std::mem;
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
use meta_data::{MetaData, ReserveResult};

const GROUP_FULL_BIT_MASK: u64 = 0x7f00_0000_0000_0000;

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

/// Returns the maximum effective capacity for the given number of buckets, taking
/// the maximum load factor into account.
#[inline]
fn buckets_to_capacity(buckets: usize) -> usize {
    if buckets == 1 {
        // For tables with 1/2/4/8 buckets, we always reserve one empty slot.
        // Keep in mind that the bucket mask is one less than the bucket count.
        6
    } else {
        // For larger tables we reserve 12.5% of the slots as empty.
        let buckets_cap = buckets * 7;
        (buckets_cap / 8) * 7
    }
}

#[repr(align(64))]
struct CacheAligned<T>(T);

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
}

#[inline]
fn get_valid_bits(group_meta_data: u64) -> u64 {
    (group_meta_data & GROUP_FULL_BIT_MASK) >> (64 - 8)
}

/// A raw hash table with an unsafe API.
pub(crate) struct RawInterner<T> {
    // Mask to get an index from a hash value. The value is one less than the
    // number of buckets in the table.
    bucket_mask: usize,

    // Pointer to the array of control bytes
    buckets: NonNull<Bucket<T>>,

    // Number of elements that can be inserted before we need to grow the table
    growth_left: CacheAligned<AtomicIsize>,
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

impl<T> RawInterner<T> {
    /// Creates a new empty hash table without allocating any memory.
    ///
    /// In effect this returns a table with exactly 1 bucket. However we can
    /// leave the data pointer dangling since that bucket is never written to
    /// due to our load factor forcing us to always have at least 1 free bucket.
    #[inline]
    pub fn new() -> Self {
        static mut STATIC_BUCKET: [u64; 8] = [0; 8];
        Self {
            // SAFTY shall not mutate this as the capacity is to smal
            buckets: NonNull::new(unsafe { STATIC_BUCKET }.as_mut_ptr() as *mut Bucket<T>).unwrap(),
            bucket_mask: 0,
            growth_left: CacheAligned(AtomicIsize::new(0)),
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
            growth_left: CacheAligned(AtomicIsize::new(buckets_to_capacity(buckets) as isize)),
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

    fn bucket(&self, index: usize) -> &mut Bucket<T> {
        // SAFTY: as the index is caped by bucket_mask that is the size of buckets - 1
        unsafe { &mut *self.buckets.as_ptr().add(index & self.bucket_mask) }
    }

    /// Searches for an element in the table.
    #[inline]
    pub(crate) fn intern<Q>(&mut self, hash: u64, value: &Q, make: impl FnOnce() -> T) -> T
    where
        T: Sync + Send + Borrow<Q> + Copy,
        Q: Sync + Send + Eq,
    {
        let h2 = h2(hash);
        let mut stride = 0;
        let mut pos = h1(hash);
        loop {
            let bucket = self.bucket(pos);
            let group_meta_data = bucket.meta_data.get_metadata_relaxed();
            let valid_bits = get_valid_bits(group_meta_data);
            for index in match_byte(valid_bits, group_meta_data, h2) {
                let result = bucket.get_ref(index);
                if (*value).eq((*result).borrow()) {
                    return *result;
                }
            }

            if self.growth_left.0.load(Ordering::Relaxed) <= 0 {
                unimplemented!("resize")
            }

            if group_meta_data & GROUP_FULL_BIT_MASK == GROUP_FULL_BIT_MASK {
                // not found this bucket and the bucket is full try the new bucket
                stride += 1;
                pos = pos + stride;
                continue;
            }

            // the bucket was not full when the metadata was fetched but new values can have been added
            // during the search but even if the metadata have been updated and the index is now used the
            // value needs to be check as it can be the value that shall be added
            let iter = BitMaskIter::new((!valid_bits) & 0x7F, 1);
            let mut group_meta_data = group_meta_data;
            'indexLoop: for index in iter {
                loop {
                    match bucket.meta_data.reserve(&mut group_meta_data, h2 as u64, index) {
                        ReserveResult::Reserved => {
                            break;
                        }
                        ReserveResult::AlreadyReservedWithOtherH2 => {
                            continue 'indexLoop;
                        }
                        ReserveResult::Occupied => {
                            let result = bucket.get_ref(index);
                            if value.eq((*result).borrow()) {
                                return *result;
                            }
                            continue 'indexLoop;
                        }
                    }
                }

                self.growth_left.0.fetch_sub(1, Ordering::Relaxed);

                let result = make();
                *bucket.get_ref(index) = result;

                bucket.meta_data.set_valid_and_unpark(group_meta_data, h2 as u64, index);
                return result;
            }
            stride += 1;
            pos = pos + stride;
        }
    }

    /// Searches for an element in the table.
    #[inline]
    pub(crate) fn get<Q>(&mut self, hash: u64, value: &Q) -> Option<T>
    where
        T: Sync + Send + Borrow<Q> + Copy,
        Q: Sync + Send + Eq,
    {
        let h2 = h2(hash);
        let mut stride = 0;
        let mut pos = h1(hash);
        loop {
            let bucket = self.bucket(pos);
            let group_meta_data = bucket.meta_data.get_metadata_relaxed();
            let valid_bits = get_valid_bits(group_meta_data);
            for index in match_byte(valid_bits, group_meta_data, h2) {
                let result = bucket.get_ref(index);
                if value.eq((*result).borrow()) {
                    return Some(*result);
                }
            }
            if group_meta_data & GROUP_FULL_BIT_MASK == GROUP_FULL_BIT_MASK {
                // not found this bucket and the bucket is full try the new bucket
                stride += 1;
                pos = pos + stride;
                continue;
            }
            return None;
        }
    }
}
