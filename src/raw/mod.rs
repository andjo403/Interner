use core::sync::atomic::{AtomicUsize, Ordering};
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

/// Probe sequence based on triangular numbers, which is guaranteed (since our
/// table size is a power of two) to visit every bucket of elements exactly once.
///
/// A triangular probe has us jump by 1 more bucket every time. So first we
/// jump by 1 bucket (meaning we just continue our linear scan), then 2 buckets
/// (skipping over 1 bucket), then 3 buckets (skipping over 2 buckets), and so on.
///
/// Proof that the probe will visit every bucket in the table:
/// <https://fgiesen.wordpress.com/2015/02/22/triangular-numbers-mod-2n/>
struct ProbeSeq {
    bucket_mask: usize,
    pos: usize,
    stride: usize,
}

impl Iterator for ProbeSeq {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        // We should have found an empty bucket by now and ended the probe.
        debug_assert!(self.stride <= self.bucket_mask, "Went past end of probe sequence");

        let result = self.pos;
        self.stride += 1;
        self.pos += self.stride;
        self.pos &= self.bucket_mask;
        Some(result)
    }
}

/// Returns the number of buckets needed to hold the given number of items,
/// taking the maximum load factor into account.
///
/// Returns `None` if an overflow occurs.
#[inline]
fn capacity_to_buckets(cap: usize) -> Option<usize> {
    let cap = cap / 7; // as there is 7 elemntes in each bucket
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

    // Any overflows will have been caught by the checked_mul. Also, any
    // rounding errors from the division above will be cleaned up by
    // next_power_of_two (which can't overflow because of the previous divison).
    dbg!(Some(adjusted_cap.next_power_of_two()))
}

/// Returns the maximum effective capacity for the given bucket mask, taking
/// the maximum load factor into account.
#[inline]
fn bucket_mask_to_capacity(bucket_mask: usize) -> usize {
    let bucket_mask = dbg!(bucket_mask) * 7;
    if bucket_mask < 8 {
        // For tables with 1/2/4/8 buckets, we always reserve one empty slot.
        // Keep in mind that the bucket mask is one less than the bucket count.
        dbg!(bucket_mask)
    } else {
        // For larger tables we reserve 12.5% of the slots as empty.
        dbg!(((bucket_mask + 1) / 8) * 7)
    }
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
    growth_left: AtomicUsize,
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
            growth_left: AtomicUsize::new(0),
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
            growth_left: AtomicUsize::new(bucket_mask_to_capacity(buckets - 1)),
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
    ///
    /// This iterator never terminates, but is guaranteed to visit each bucket
    /// exactly once. The loop using `probe_seq` must terminate upon
    /// reaching a bucket that not is full.
    #[inline]
    fn probe_seq(&self, hash: u64) -> ProbeSeq {
        ProbeSeq { bucket_mask: self.bucket_mask, pos: h1(hash) & self.bucket_mask, stride: 0 }
    }

    unsafe fn bucket(&self, index: usize) -> &mut Bucket<T> {
        debug_assert!(index < self.bucket_mask + 1);
        &mut *self.buckets.as_ptr().add(index)
    }

    /// Searches for an element in the table.
    #[inline]
    pub(crate) fn intern<Q>(&mut self, hash: u64, value: &Q, make: impl FnOnce() -> T) -> T
    where
        T: Sync + Send + Borrow<Q> + Copy,
        Q: Sync + Send + Eq,
    {
        let h2 = h2(hash);
        for pos in self.probe_seq(hash) {
            let bucket = unsafe { self.bucket(pos) };
            let group_meta_data = unsafe { bucket.meta_data.get_metadata_optimistically() };
            let valid_bits = get_valid_bits(group_meta_data);
            for index in match_byte(valid_bits, group_meta_data, h2) {
                let result = bucket.get_ref(index);
                if (*value).eq((*result).borrow()) {
                    return *result;
                }
            }
            if group_meta_data & GROUP_FULL_BIT_MASK == GROUP_FULL_BIT_MASK {
                // not found this bucket and the bucket is full try the new bucket
                continue;
            }

            let mut old = self.growth_left.load(Ordering::SeqCst);
            loop {
                if old == 1 {
                    unimplemented!("resize")
                }
                match self.growth_left.compare_exchange_weak(
                    old,
                    old - 1,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(x) => old = x,
                }
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
                let result = make();
                *bucket.get_ref(index) = result;

                bucket.meta_data.set_valid_and_unpark(group_meta_data, h2 as u64, index);
                return result;
            }
        }

        // probe_seq never returns.
        unreachable!();
    }

    /// Searches for an element in the table.
    #[inline]
    pub(crate) fn get<Q>(&mut self, hash: u64, value: &Q) -> Option<T>
    where
        T: Sync + Send + Borrow<Q> + Copy,
        Q: Sync + Send + Eq,
    {
        let h2 = h2(hash);
        for pos in self.probe_seq(hash) {
            let bucket = unsafe { self.bucket(pos) };
            let group_meta_data = unsafe { bucket.meta_data.get_metadata_optimistically() };
            let valid_bits = get_valid_bits(group_meta_data);
            for index in match_byte(valid_bits, group_meta_data, h2) {
                let result = bucket.get_ref(index);
                if value.eq((*result).borrow()) {
                    return Some(*result);
                }
            }
            if group_meta_data & GROUP_FULL_BIT_MASK == GROUP_FULL_BIT_MASK {
                // not found this bucket and the bucket is full try the new bucket
                continue;
            }
            return None;
        }

        // probe_seq never returns.
        unreachable!();
    }
}
