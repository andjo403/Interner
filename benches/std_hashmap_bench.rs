#![feature(hash_raw_entry)]
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use fxhash::{FxBuildHasher, FxHasher};
use std::borrow::Borrow;
use std::collections::hash_map::RawEntryMut;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

type Interner<V> = HashMap<V, (), FxBuildHasher>;

#[inline]
fn make_hash<K: Hash + ?Sized>(val: &K) -> u64 {
    let mut state = FxHasher::default();
    val.hash(&mut state);
    state.finish()
}

#[inline]
fn intern_ref<K, Q: ?Sized>(interner: &mut Interner<K>, value: &Q, make: impl FnOnce() -> K) -> K
where
    K: Borrow<Q> + Hash + Eq + Copy,
    Q: Hash + Eq,
{
    let hash = make_hash(value);
    let entry = interner.raw_entry_mut().from_key_hashed_nocheck(hash, value);

    match entry {
        RawEntryMut::Occupied(e) => *e.key(),
        RawEntryMut::Vacant(e) => {
            let v = make();
            e.insert_hashed_nocheck(hash, v, ());
            v
        }
    }
}

const ITER: u64 = 32 * 1024;

fn task_intern_u64refs(values: &[u64]) -> Interner<&'_ u64> {
    let mut map = Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default());
    (0..ITER).for_each(|i: u64| {
        intern_ref(&mut map, &i, || values.get(i as usize).unwrap());
    });
    map
}

fn intern_u64refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("intern_u64refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let values: Vec<u64> = (0..ITER).collect();
    group.bench_function("task_intern_u64refs", |bencher| {
        bencher.iter(|| task_intern_u64refs(values.as_slice()))
    });
    group.finish();
}

fn task_get_interned_u64refs(mut interner: &mut Interner<&'_ u64>) {
    (0..ITER).for_each(|i: u64| {
        intern_ref(&mut interner, &i, || unimplemented!());
    });
}

fn get_already_interned_u64refs(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_already_interned_u64refs");
    group.throughput(Throughput::Elements(ITER as u64));
    let values: Vec<u64> = (0..ITER).collect();
    let mut interner = task_intern_u64refs(values.as_slice());
    group.bench_function("task_get_interned_u64refs", |bencher| {
        bencher.iter(|| task_get_interned_u64refs(&mut interner))
    });
    group.finish();
}

criterion_group!(benches, intern_u64refs, get_already_interned_u64refs);
criterion_main!(benches);