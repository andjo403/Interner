use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fxhash::FxBuildHasher;
use interner::Interner as Inter;
use rayon::prelude::*;

type Interner<T> = Inter<T, FxBuildHasher>;

const ITER: u64 = 32 * 1024;

fn task_intern_u64refs(values: &[u64]) -> Interner<&'_ u64> {
    let map = Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default());
    (0..ITER).into_par_iter().for_each(|i: u64| {
        map.intern_ref(&i, || values.get(i as usize).unwrap());
    });
    map
}

fn intern_u64refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("intern_u64refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();
    let values: Vec<u64> = (0..ITER).collect();

    for threads in (1..=max).filter(|thread| *thread == 1 || *thread % 4 == 0) {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, &threads| {
                let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
                pool.install(|| bencher.iter(|| task_intern_u64refs(values.as_slice())));
            },
        );
    }

    group.finish();
}

fn task_get_interned_u64refs(interner: &Interner<&'_ u64>) {
    (0..ITER).into_par_iter().for_each(|i: u64| {
        interner.intern_ref(&i, || unimplemented!());
    });
}

fn get_already_interned_u64refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_already_interned_u64refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();
    let values: Vec<u64> = (0..ITER).collect();
    let interner = task_intern_u64refs(values.as_slice());

    for threads in (1..=max).filter(|thread| *thread == 1 || *thread % 4 == 0) {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |bencher, &threads| {
                let pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();
                pool.install(|| bencher.iter(|| task_get_interned_u64refs(&interner)));
            },
        );
    }

    group.finish();
}

fn single_task_intern_u64refs(values: &[u64]) -> Interner<&'_ u64> {
    let map = Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default());
    (0..ITER).for_each(|i: u64| {
        map.intern_ref(&i, || values.get(i as usize).unwrap());
    });
    map
}

fn single_intern_u64refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_intern_u64refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let values: Vec<u64> = (0..ITER).collect();
    group.bench_function("single_task_intern_u64refs", |bencher| {
        bencher.iter(|| single_task_intern_u64refs(values.as_slice()))
    });
    group.finish();
}

fn single_task_get_interned_u64refs(interner: &mut Interner<&'_ u64>) {
    (0..ITER).for_each(|i: u64| {
        interner.intern_ref(&i, || unimplemented!());
    });
}

fn single_get_already_interned_u64refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_get_already_interned_u64refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let values: Vec<u64> = (0..ITER).collect();
    let mut interner = task_intern_u64refs(values.as_slice());
    group.bench_function("single_task_get_interned_u64refs", |bencher| {
        bencher.iter(|| single_task_get_interned_u64refs(&mut interner))
    });
    group.finish();
}

criterion_group!(
    benches,
    single_intern_u64refs,
    single_get_already_interned_u64refs,
    intern_u64refs,
    get_already_interned_u64refs
);
criterion_main!(benches);
