use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fxhash::FxBuildHasher;
use interner::Interner as Inter;
use std::{
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

type Interner<T> = Inter<T, FxBuildHasher>;

const ITER: u32 = 32 * 1024;

#[derive(Clone)]
struct MultithreadedBench<T> {
    start: Arc<Barrier>,
    end: Arc<Barrier>,
    interner: T,
    num_threads: usize,
}

impl<T: Send + Clone + 'static> MultithreadedBench<T> {
    fn new(interner: T, num_threads: usize) -> Self {
        Self {
            start: Arc::new(Barrier::new(num_threads + 1)),
            end: Arc::new(Barrier::new(num_threads + 1)),
            interner,
            num_threads,
        }
    }

    fn thread(&self, f: impl FnOnce(&Barrier, &Barrier, &mut T) + Send + 'static) -> &Self {
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
    let mut group = c.benchmark_group("Interner/intern_same_u32refs_in_all_threads");
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
                        let new_interner = Interner::with_capacity_and_hasher(
                            ITER as usize,
                            FxBuildHasher::default(),
                        );
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
    let mut group = c.benchmark_group("Interner/intern_same_u32refs_in_all_threads_with_resize");
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
                        let new_interner =
                            Interner::with_capacity_and_hasher(0, FxBuildHasher::default());
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
    let mut group = c.benchmark_group("Interner/intern_diffrent_u32refs_in_all_threads");
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
                        let new_interner = Interner::with_capacity_and_hasher(
                            ITER as usize,
                            FxBuildHasher::default(),
                        );
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
    let mut group = c.benchmark_group("Interner/get_already_interned_u32refs");
    let max = num_cpus::get();
    let values: Vec<u32> = (0..ITER).collect();
    let values: &'static [u32] = values.leak();
    let mut new_interner =
        Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default());
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
