use fxhash::FxBuildHasher;
use interner::Interner;
use rayon::prelude::*;
use std::sync::Arc;

const ITER: u64 = 32 * 1024;

#[test]
fn drop_empty() {
    let interner: Interner<&i32> = Interner::new();
    drop(interner);
}
#[test]
fn intern_ref() {
    let value1: i32 = 42;
    let value2: i32 = 0;
    let value3: i32 = 31;
    let value4: i32 = 32;
    let value5: i32 = 33;
    let value6: i32 = 34;
    let value7: i32 = 42;
    let interner: Interner<&i32> = Interner::new();

    let result = interner.intern_ref(&value1, || &value1);
    assert_eq!(&value1, result);
    let result = interner.intern_ref(&value2, || &value2);
    assert_eq!(&value2, result);
    let result = interner.intern_ref(&value2, || unimplemented!());
    assert_eq!(&value2, result);

    let result = interner.intern_ref(&value3, || &value3);
    assert_eq!(&value3, result);
    let result = interner.intern_ref(&value4, || &value4);
    assert_eq!(&value4, result);
    let result = interner.intern_ref(&value5, || &value5);
    assert_eq!(&value5, result);
    let result = interner.intern_ref(&value6, || &value6);
    assert_eq!(&value6, result);
    let result = interner.intern_ref(&value7, || unimplemented!());
    assert_eq!(&value7, result);
}

#[test]
fn intern_ref_array() {
    use std::hash::{BuildHasher, Hash, Hasher};
    let array = [42, 2, 3, 4, 5, 6, 7, 8, 9];
    let interner: Interner<&[i32]> = Interner::new();
    let slice = &array[..];
    let mut state = interner.hasher().build_hasher();
    slice.hash(&mut state);
    let hash = state.finish();
    let is_match = |val: &&[i32]| *val == slice;

    let result = interner.intern_ref(slice, || slice);
    assert_eq!(slice, result);
    let result = interner.intern_ref(slice, || unimplemented!());
    assert_eq!(slice, result);
    let result = interner.get_from_hash(hash, is_match);
    assert_eq!(slice, *result.unwrap());
}

#[test]
fn intern_ref2() {
    let mut vector = Vec::<u64>::with_capacity(ITER as usize + 100);
    for i in 0..ITER {
        vector.push(i);
    }

    let vector2 = vector.clone();
    let slice = vector2.as_ptr();

    let interner = Interner::<&u64, FxBuildHasher>::with_capacity_and_hasher(
        ITER as usize,
        FxBuildHasher::default(),
    );

    for index in vector.iter() {
        let reference = unsafe { &*slice.offset(*index as isize) };
        let result = interner.intern_ref(index, || index);
        assert_eq!(*reference, *result);
        assert_eq!(index as *const _ as *const (), result as *const _ as *const ());
    }
    for index in vector.iter() {
        let reference = unsafe { &*slice.offset(*index as isize) };
        let result = interner.intern_ref(index, || unimplemented!());
        assert_eq!(*reference, *result);
        assert_eq!(index as *const _ as *const (), result as *const _ as *const ());
    }
}

#[test]
fn single_threaded_intern_ref3() {
    let values: Vec<u64> = (0..ITER).collect();
    let values = values.into_boxed_slice();

    let interner = Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default());
    (1..ITER).into_iter().for_each(|i: u64| {
        interner.intern_ref(&i, || values.get(i as usize).unwrap());
        interner.intern_ref(&i, || unimplemented!("value: {}", i));
    });

    (1..ITER).into_iter().for_each(|i: u64| {
        interner.intern_ref(&i, || unimplemented!("value: {}", i));
    });
}

#[test]
fn multi_threaded_intern_ref3() {
    let values: Arc<Vec<u64>> = Arc::new((0..ITER).collect());

    let interner: Arc<Interner<&u64, FxBuildHasher>> =
        Arc::new(Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default()));
    (1..ITER).into_par_iter().for_each(|i: u64| {
        interner.intern_ref(&i, || (*values).get(i as usize).unwrap());
        let result = interner.intern_ref(&i, || unimplemented!());
        assert_eq!(i, *result);
    });

    (1..ITER).into_iter().for_each(|i: u64| {
        let result = interner.intern_ref(&i, || panic!("value {}", i));
        assert_eq!(i, *result);
    });
}

#[test]
fn single_threaded_resize() {
    let values: Vec<u64> = (0..ITER).collect();
    let values = values.into_boxed_slice();

    let interner = Interner::with_hasher(FxBuildHasher::default());
    (1..ITER).into_iter().for_each(|i: u64| {
        interner.intern_ref(&i, || values.get(i as usize).unwrap());
        interner.intern_ref(&i, || unimplemented!("value: {}", i));
    });

    (1..ITER).into_iter().for_each(|i: u64| {
        interner.intern_ref(&i, || unimplemented!("value: {}", i));
    });
}

#[test]
fn multi_threaded_resize() {
    let values: Arc<Vec<u64>> = Arc::new((0..ITER).collect());

    let interner: Arc<Interner<&u64, FxBuildHasher>> =
        Arc::new(Interner::with_hasher(FxBuildHasher::default()));
    (1..ITER).into_par_iter().for_each(|i: u64| {
        interner.intern_ref(&i, || (*values).get(i as usize).unwrap());
        let result = interner.intern_ref(&i, || unimplemented!());
        assert_eq!(i, *result);
    });

    (1..ITER).into_iter().for_each(|i: u64| {
        let result = interner.intern_ref(&i, || unimplemented!());
        assert_eq!(i, *result);
    });
}

#[test]
fn intern_same_value_no_initial_capacity() {
    let x = vec![42];
    let static_ref: &'static [usize] = x.leak();
    let interner = Arc::new(Interner::with_capacity_and_hasher(0, FxBuildHasher::default()));

    let interner2 = Arc::clone(&interner);
    let thread = std::thread::spawn(move || {
        let result = interner2.intern_ref(&static_ref[0], || &static_ref[0]);
        assert_eq!(&42, result);
    });
    let result = interner.intern_ref(&static_ref[0], || &static_ref[0]);
    assert_eq!(&42, result);
    let result = interner.intern_ref(&static_ref[0], || unimplemented!());
    assert_eq!(&42, result);
    thread.join().unwrap();
}
