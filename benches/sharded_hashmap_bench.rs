#![feature(hash_raw_entry)]
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fxhash::{FxBuildHasher, FxHasher};
use smallvec::SmallVec;
use std::borrow::Borrow;
use std::collections::hash_map::RawEntryMut;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem;
use std::{
    sync::{Arc, Barrier, Mutex as Lock},
    thread,
    time::{Duration, Instant},
};

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
    pub fn new(capacity: usize) -> Self {
        // Create a vector of the values we want
        let mut values: SmallVec<[_; SHARDS]> = (0..SHARDS)
            .map(|_| {
                CacheAligned(Lock::new(HashMap::<T, (), FxBuildHasher>::with_capacity_and_hasher(
                    capacity / SHARDS,
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
        let mut shard = self.get_shard_by_hash(hash).lock().unwrap();
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

#[derive(Clone)]
struct MultithreadedBench<T> {
    start: Arc<Barrier>,
    end: Arc<Barrier>,
    interner: T,
    num_threads: usize,
}

impl<T: Send + Sync + Clone + 'static> MultithreadedBench<T> {
    fn new(interner: T, num_threads: usize) -> Self {
        Self {
            start: Arc::new(Barrier::new(num_threads + 1)),
            end: Arc::new(Barrier::new(num_threads + 1)),
            interner,
            num_threads,
        }
    }

    fn thread(&self, f: impl FnOnce(&Barrier, &Barrier, &T) + Send + 'static) -> &Self {
        let start = self.start.clone();
        let end = self.end.clone();
        let mut interner = self.interner.clone();
        thread::spawn(move || {
            f(&*start, &*end, &mut interner);
        });
        self
    }

    fn run(&self) -> Duration {
        self.start.wait();
        let t0 = Instant::now();
        self.end.wait();
        let time = t0.elapsed();
        time
    }
}

fn intern_same_u32refs_in_all_threads(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sharded_hashmap/intern_same_u32refs_in_all_threads");
    let max = num_cpus::get();
    let values: Vec<u32> = (0..ITER).collect();
    let values: &'static [u32] = values.leak();

    for threads in (1..=max).filter(|thread| *thread == 1 || *thread % 4 == 0) {
        group.throughput(Throughput::Elements((ITER * threads as u32) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, &threads| {
                bencher.iter_custom(|iters| {
                    let mut total = Duration::from_secs(0);
                    for _ in 0..iters {
                        let new_interner = Arc::new(Interner::new(ITER as usize));
                        let bench = MultithreadedBench::new(new_interner, threads);
                        for _ in 0..threads {
                            bench.thread(move |start, end, interner| {
                                start.wait();
                                for i in 0..ITER {
                                    interner.intern_ref(&i, || values.get(i as usize).unwrap());
                                }
                                end.wait();
                            });
                        }
                        total += bench.run();
                    }
                    total
                })
            },
        );
    }

    group.finish();
}

fn intern_same_u32refs_in_all_threads_with_resize(c: &mut Criterion) {
    let mut group =
        c.benchmark_group("Sharded_hashmap/intern_same_u32refs_in_all_threads_with_resize");
    let max = num_cpus::get();
    let values: Vec<u32> = (0..ITER).collect();
    let values: &'static [u32] = values.leak();

    for threads in (1..=max).filter(|thread| *thread == 1 || *thread % 4 == 0) {
        group.throughput(Throughput::Elements((ITER * threads as u32) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, &threads| {
                bencher.iter_custom(|iters| {
                    let mut total = Duration::from_secs(0);
                    for _ in 0..iters {
                        let new_interner = Arc::new(Interner::new(0));
                        let bench = MultithreadedBench::new(new_interner, threads);
                        for _ in 0..threads {
                            bench.thread(move |start, end, interner| {
                                start.wait();
                                for i in 0..ITER {
                                    interner.intern_ref(&i, || values.get(i as usize).unwrap());
                                }
                                end.wait();
                            });
                        }
                        total += bench.run();
                    }
                    total
                })
            },
        );
    }

    group.finish();
}

fn intern_diffrent_u32refs_in_all_threads(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sharded_hashmap/intern_diffrent_u32refs_in_all_threads");
    let max = num_cpus::get();
    let values: Vec<u32> = (0..ITER).collect();
    let values: &'static [u32] = values.leak();

    for threads in (1..=max).filter(|thread| *thread == 1 || *thread % 4 == 0) {
        let chunk_size = ITER / threads as u32;
        group.throughput(Throughput::Elements((chunk_size * threads as u32) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, &threads| {
                bencher.iter_custom(|iters| {
                    let mut total = Duration::from_secs(0);
                    for _ in 0..iters {
                        let new_interner = Arc::new(Interner::new(ITER as usize));
                        let bench = MultithreadedBench::new(new_interner, threads);
                        let mut chunks = values.chunks_exact(chunk_size as usize);
                        for _ in 0..threads {
                            let my_values = chunks.next().unwrap();
                            bench.thread(move |start, end, interner| {
                                start.wait();
                                for &i in my_values {
                                    interner.intern_ref(&i, || values.get(i as usize).unwrap());
                                }
                                end.wait();
                            });
                        }
                        total += bench.run();
                    }
                    total
                })
            },
        );
    }

    group.finish();
}

fn get_already_interned_u32refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sharded_hashmap/get_already_interned_u32refs");
    let max = num_cpus::get();
    let values: Vec<u32> = (0..ITER).collect();
    let values: &'static [u32] = values.leak();
    let new_interner = Arc::new(Interner::new(ITER as usize));
    for i in 0..ITER {
        new_interner.intern_ref(&i, || values.get(i as usize).unwrap());
    }

    for threads in (1..=max).filter(|thread| *thread == 1 || *thread % 4 == 0) {
        let temp_interner = new_interner.clone();
        group.throughput(Throughput::Elements((ITER * threads as u32) as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, &threads| {
                bencher.iter_custom(|iters| {
                    let mut total = Duration::from_secs(0);
                    for _ in 0..iters {
                        let bench = MultithreadedBench::new(temp_interner.clone(), threads);
                        for _ in 0..threads {
                            bench.thread(move |start, end, interner| {
                                start.wait();
                                for i in 0..ITER {
                                    interner.intern_ref(&i, || unimplemented!());
                                }
                                end.wait();
                            });
                        }
                        total += bench.run();
                    }
                    total
                })
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    get_already_interned_u32refs,
    intern_same_u32refs_in_all_threads,
    intern_diffrent_u32refs_in_all_threads,
    intern_same_u32refs_in_all_threads_with_resize,
);
criterion_main!(benches);
