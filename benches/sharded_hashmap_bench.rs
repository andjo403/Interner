#![feature(hash_raw_entry)]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fxhash::{FxBuildHasher, FxHasher};
use parking_lot::Mutex as Lock;
use rayon::prelude::*;
use smallvec::SmallVec;
use std::borrow::Borrow;
use std::collections::hash_map::RawEntryMut;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem;

#[repr(align(64))]
struct CacheAligned<T>(T);

const SHARD_BITS: usize = 5;

pub const SHARDS: usize = 1 << SHARD_BITS;

#[inline]
fn make_hash<K: Hash + ?Sized>(val: &K) -> u64 {
    let mut state = FxHasher::default();
    val.hash(&mut state);
    state.finish()
}

pub struct Interner<T> {
    shards: [CacheAligned<Lock<HashMap<T, (), FxBuildHasher>>>; SHARDS],
}

impl<T: Eq + Hash + Copy> Interner<T> {
    #[inline]
    pub fn new() -> Self {
        // Create a vector of the values we want
        let mut values: SmallVec<[_; SHARDS]> = (0..SHARDS)
            .map(|_| {
                CacheAligned(Lock::new(HashMap::<T, (), FxBuildHasher>::with_capacity_and_hasher(
                    1024 as usize,
                    FxBuildHasher::default(),
                )))
            })
            .collect();

        // Create an uninitialized array
        let mut shards: mem::MaybeUninit<
            [CacheAligned<Lock<HashMap<T, (), FxBuildHasher>>>; SHARDS],
        > = std::mem::MaybeUninit::uninit();

        unsafe {
            // Copy the values into our array
            let first =
                shards.as_mut_ptr() as *mut CacheAligned<Lock<HashMap<T, (), FxBuildHasher>>>;
            values.as_ptr().copy_to_nonoverlapping(first, SHARDS);

            // Ignore the content of the vector
            values.set_len(0);

            Interner { shards: shards.assume_init() }
        }
    }

    #[inline]
    pub fn get_shard_index_by_hash(&self, hash: u64) -> usize {
        let hash_len = std::mem::size_of::<usize>();
        // Ignore the top 7 bits as hashbrown uses these and get the next SHARD_BITS highest bits.
        // hashbrown also uses the lowest bits, so we can't use those
        let bits = (hash >> (hash_len * 8 - 7 - SHARD_BITS)) as usize;
        bits % SHARDS
    }

    #[inline]
    pub fn get_shard_by_hash(&self, hash: u64) -> &Lock<HashMap<T, (), FxBuildHasher>> {
        &self.shards[self.get_shard_index_by_hash(hash)].0
    }

    #[inline]
    pub fn intern_ref<Q: ?Sized>(&self, value: &Q, make: impl FnOnce() -> T) -> T
    where
        T: Borrow<Q>,
        Q: Hash + Eq,
    {
        let hash = make_hash(value);
        let mut shard = self.get_shard_by_hash(hash).lock();
        let entry = shard.raw_entry_mut().from_key_hashed_nocheck(hash, value);

        match entry {
            RawEntryMut::Occupied(e) => *e.key(),
            RawEntryMut::Vacant(e) => {
                let v = make();
                e.insert_hashed_nocheck(hash, v, ());
                v
            }
        }
    }
}

const ITER: u32 = 32 * 1024;

fn task_intern_u32refs(values: &[u32]) -> Interner<&'_ u32> {
    let map = Interner::new();
    (0..ITER).into_par_iter().for_each(|i: u32| {
        map.intern_ref(&i, || values.get(i as usize).unwrap());
    });
    map
}

fn intern_u32refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sharded_hashmap/intern_u32refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();
    let values: Vec<u32> = (0..ITER).collect();

    for threads in (1..=max).filter(|thread| *thread == 1 || *thread % 4 == 0) {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, &threads| {
                let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
                pool.install(|| bencher.iter(|| task_intern_u32refs(values.as_slice())));
            },
        );
    }

    group.finish();
}

fn task_get_interned_u32refs(interner: &Interner<&'_ u32>) {
    (0..ITER).into_par_iter().for_each(|i: u32| {
        interner.intern_ref(&i, || unimplemented!());
    });
}

fn get_already_interned_u32refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sharded_hashmap/get_already_interned_u32refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();
    let values: Vec<u32> = (0..ITER).collect();
    let interner = task_intern_u32refs(values.as_slice());

    for threads in (1..=max).filter(|thread| *thread == 1 || *thread % 4 == 0) {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, &threads| {
                let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
                pool.install(|| bencher.iter(|| task_get_interned_u32refs(&interner)));
            },
        );
    }

    group.finish();
}

fn single_task_intern_u32refs(values: &[u32]) -> Interner<&'_ u32> {
    let map = Interner::new();
    (0..ITER).for_each(|i: u32| {
        map.intern_ref(&i, || values.get(i as usize).unwrap());
    });
    map
}

fn single_intern_u32refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sharded_hashmap/single_thread_intern_u32refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let values: Vec<u32> = (0..ITER).collect();
    group.bench_function("1", |bencher| {
        bencher.iter(|| single_task_intern_u32refs(values.as_slice()))
    });
    group.finish();
}

fn single_task_get_interned_u32refs(interner: &mut Interner<&'_ u32>) {
    (0..ITER).for_each(|i: u32| {
        interner.intern_ref(&i, || unimplemented!());
    });
}

fn single_get_already_interned_u32refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sharded_hashmap/single_thread_get_already_interned_u32refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let values: Vec<u32> = (0..ITER).collect();
    let mut interner = task_intern_u32refs(values.as_slice());
    group.bench_function("1", |bencher| {
        bencher.iter(|| single_task_get_interned_u32refs(&mut interner))
    });
    group.finish();
}

criterion_group!(
    benches,
    single_intern_u32refs,
    single_get_already_interned_u32refs,
    intern_u32refs,
    get_already_interned_u32refs
);
criterion_main!(benches);
