#![cfg(loom)]

use loom::thread;

use fxhash::FxBuildHasher;
use interner::Interner;
use std::sync::Arc;

#[test]
fn intern_same_value() {
    let x = vec![0, 1];
    let static_ref: &'static [usize] = x.leak();
    loom::model(|| {
        let interner = Arc::new(Interner::with_capacity_and_hasher(2, FxBuildHasher::default()));

        /*
        let interner2 = Arc::clone(&interner);
        let thread = thread::spawn(move || {
            let result = interner2.intern_ref(&static_ref[0], || &static_ref[0]);
            assert_eq!(&0, result);
        });
        thread.join().unwrap();
        */
        let result = interner.intern_ref(&static_ref[1], || &static_ref[1]);
        assert_eq!(&1, result);
        let result = interner.intern_ref(&static_ref[1], || unimplemented!());
        assert_eq!(&1, result);
    });
}
