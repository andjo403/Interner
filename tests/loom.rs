#![cfg(loom)]

use loom::thread;

use fxhash::FxBuildHasher;
use interner::Interner;
use std::sync::Arc;
#[ignore = "once_cell not supported"]
#[test]
fn intern_two_value() {
    let x = vec![42, 1];
    let static_ref: &'static [usize] = x.leak();
    loom::model(move || {
        let interner = Arc::new(Interner::with_capacity_and_hasher(2, FxBuildHasher::default()));

        let interner2 = Arc::clone(&interner);
        let thread = thread::spawn(move || {
            let result = interner2.intern_ref(&static_ref[0], || &static_ref[0]);
            assert_eq!(&42, result);
            let result = interner2.intern_ref(&static_ref[0], || unimplemented!());
            assert_eq!(&42, result);
        });
        let result = interner.intern_ref(&static_ref[1], || &static_ref[1]);
        assert_eq!(&1, result);
        let result = interner.intern_ref(&static_ref[1], || unimplemented!());
        assert_eq!(&1, result);
        thread.join().unwrap();
    });
}

#[test]
fn intern_same_value() {
    let x = vec![42];
    let static_ref: &'static [usize] = x.leak();
    loom::model(move || {
        let interner = Arc::new(Interner::with_capacity_and_hasher(2, FxBuildHasher::default()));

        let interner2 = Arc::clone(&interner);
        let thread = thread::spawn(move || {
            let result = interner2.intern_ref(&static_ref[0], || &static_ref[0]);
            assert_eq!(&42, result);
        });
        let result = interner.intern_ref(&static_ref[0], || &static_ref[0]);
        assert_eq!(&42, result);
        let result = interner.intern_ref(&static_ref[0], || unimplemented!());
        assert_eq!(&42, result);
        thread.join().unwrap();
    });
}

#[ignore = "once_cell not supported"]
#[test]
fn intern_same_value_no_initial_capacity() {
    let x = vec![42];
    let static_ref: &'static [usize] = x.leak();
    loom::model(move || {
        let interner = Arc::new(Interner::with_capacity_and_hasher(0, FxBuildHasher::default()));

        let interner2 = Arc::clone(&interner);
        let thread = thread::spawn(move || {
            let result = interner2.intern_ref(&static_ref[0], || &static_ref[0]);
            assert_eq!(&42, result);
        });
        let result = interner.intern_ref(&static_ref[0], || &static_ref[0]);
        assert_eq!(&42, result);
        let result = interner.intern_ref(&static_ref[0], || unimplemented!());
        assert_eq!(&42, result);
        thread.join().unwrap();
    });
}
