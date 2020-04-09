use crate::raw::{RawInterner, UsizeTReference};
use core::hash::{BuildHasher, Hash, Hasher};
use core::marker::PhantomData;
use core::sync::atomic::{AtomicPtr, Ordering};
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;

/// Default hasher for `HashMap`.
pub type DefaultHashBuilder = RandomState;

#[inline]
pub(crate) fn make_hash<K: Hash + ?Sized>(hash_builder: &impl BuildHasher, val: &K) -> u64 {
    let mut state = hash_builder.build_hasher();
    val.hash(&mut state);
    state.finish()
}
/// A concurrent interner implemented with quadratic probing and SIMD lookup.
pub struct Interner<T, S = DefaultHashBuilder> {
    hash_builder: S,
    raw_interner: AtomicPtr<RawInterner>,
    phantom: PhantomData<T>,
}

impl<T> Interner<T, DefaultHashBuilder> {
    /// Creates an empty `Interner`.
    ///
    /// The Interner is initially created with a capacity of 0, so it will not allocate until it
    /// is first inserted into.
    ///
    /// # Examples
    ///
    /// ```
    /// use interner::Interner;
    /// let mut interner: Interner<i32> = Interner::new();
    /// ```
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an empty `Interner` with the specified capacity.
    ///
    /// The Interner will be able to hold at least `capacity` elements without
    /// reallocating. If `capacity` is 0, the Interner will not allocate.
    ///
    /// # Examples
    ///
    /// ```
    /// use interner::Interner;
    /// let mut interner: Interner<i32> = Interner::with_capacity(10);
    /// ```
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacity_and_hasher(capacity, DefaultHashBuilder::default())
    }
}

impl<T, S> Interner<T, S> {
    /// Creates an empty `Interner` which will use the given hash builder to hash
    /// keys.
    ///
    /// The created map has the default initial capacity.
    ///
    /// Warning: `hash_builder` is normally randomly generated, and
    /// is designed to allow HashMaps to be resistant to attacks that
    /// cause many collisions and very poor performance. Setting it
    /// manually using this function can expose a DoS attack vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use interner::Interner;
    /// use interner::interner::DefaultHashBuilder;
    ///
    /// let s = DefaultHashBuilder::default();
    /// let mut interner: Interner<i32> = Interner::with_hasher(s);
    /// ```
    #[inline]
    pub fn with_hasher(hash_builder: S) -> Self {
        let raw_interner = Box::new(RawInterner::new());
        let raw_interner = Box::into_raw(raw_interner);
        let raw_interner = AtomicPtr::new(raw_interner);
        Self { hash_builder, raw_interner, phantom: PhantomData }
    }

    /// Creates an empty `HashMap` with the specified capacity, using `hash_builder`
    /// to hash the keys.
    ///
    /// The hash map will be able to hold at least `capacity` elements without
    /// reallocating. If `capacity` is 0, the hash map will not allocate.
    ///
    /// Warning: `hash_builder` is normally randomly generated, and
    /// is designed to allow HashMaps to be resistant to attacks that
    /// cause many collisions and very poor performance. Setting it
    /// manually using this function can expose a DoS attack vector.
    ///
    /// # Examples
    ///
    /// ```
    /// use interner::Interner;
    /// use interner::interner::DefaultHashBuilder;
    ///
    /// let s = DefaultHashBuilder::default();
    /// let mut interner: Interner<i32> = Interner::with_capacity_and_hasher(10, s);
    /// ```
    #[inline]
    pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Self {
        let raw_interner = Box::new(RawInterner::with_capacity(capacity));
        let raw_interner = Box::into_raw(raw_interner);
        let raw_interner = AtomicPtr::new(raw_interner);
        Self { hash_builder, raw_interner, phantom: PhantomData }
    }

    /// Returns a reference to the map's [`BuildHasher`].
    ///
    /// [`BuildHasher`]: https://doc.rust-lang.org/std/hash/trait.BuildHasher.html
    ///
    /// # Examples
    ///
    /// ```
    /// use interner::Interner;
    /// use interner::interner::DefaultHashBuilder;
    ///
    /// let s = DefaultHashBuilder::default();
    /// let mut interner: Interner<i32> = Interner::with_capacity_and_hasher(10, s);
    /// let hasher: &DefaultHashBuilder = interner.hasher();
    /// ```
    #[inline]
    pub fn hasher(&self) -> &S {
        &self.hash_builder
    }
}

impl<T, S> Interner<T, S>
where
    T: Eq + Hash,
    S: BuildHasher,
{
    /// Interns the value and returns a reference to the interned value.
    ///
    /// # Examples
    ///
    /// ```
    /// use interner::Interner;
    ///
    /// let interner: Interner<&i32> = Interner::with_capacity(2);
    /// let value1 :i32 = 42;
    /// let value2 :i32 = 300;
    /// let result = interner.intern_ref(&value1,|| {&value1});
    /// assert_eq!(&value1,result);
    /// let result = interner.intern_ref(&value2,|| {&value2});
    /// assert_eq!(&value2,result);
    /// let result = interner.intern_ref(&value2,|| {&value2});
    /// assert_eq!(&value2,result);
    /// ```
    #[inline]
    pub fn intern_ref<Q: Sized>(&self, value: &Q, make: impl Copy + FnOnce() -> T) -> T
    where
        T: Sync + Send + Borrow<Q> + Copy,
        Q: Sync + Send + Hash + Eq,
    {
        let hash = make_hash(&self.hash_builder, value);
        let eq =
            |stored_value: UsizeTReference| value.eq(unsafe { &*(stored_value.0 as *const Q) });
        let make = || unsafe { TransmuteHack { type_t: make() }.usize_t_reference };
        let raw_interner = unsafe { &mut *self.raw_interner.load(Ordering::Acquire) };
        let result = raw_interner.intern(hash, eq, make);
        let result = unsafe { TransmuteHack { usize_t_reference: result }.type_t };
        result
    }
}

union TransmuteHack<T: Copy> {
    usize_t_reference: UsizeTReference,
    type_t: T,
}

impl<T, S> Default for Interner<T, S>
where
    S: Default,
{
    /// Creates an empty `Interner<T, S>`, with the `Default` value for the hasher.
    #[inline]
    fn default() -> Self {
        Self::with_hasher(Default::default())
    }
}

impl<T, S> Drop for Interner<T, S> {
    fn drop(&mut self) {
        let raw_interner = self.raw_interner.load(Ordering::Acquire);
        let _raw_interner = unsafe { Box::from_raw(raw_interner) };
    }
}

#[test]
fn intern_ref() {
    use super::Interner;

    let interner: Interner<&i32> = Interner::with_capacity(2);
    let value1: i32 = 42;
    let value2: i32 = 300;
    let value3: i32 = 31;
    let value4: i32 = 32;
    let value5: i32 = 33;
    let value6: i32 = 34;
    let value7: i32 = 35;
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
    let result = interner.intern_ref(&value7, || &value7);
    assert_eq!(&value7, result);
}

#[test]
fn intern_ref2() {
    use fxhash::FxBuildHasher;
    const ITER: isize = 32 * 1024;
    let map = Interner::<&isize, FxBuildHasher>::with_capacity_and_hasher(
        ITER as usize,
        FxBuildHasher::default(),
    );
    let mut vector = Vec::<isize>::with_capacity(ITER as usize + 100);
    for i in 0..ITER {
        vector.push(i);
    }

    let vector2 = vector.clone();
    let slice = vector2.as_ptr();

    for index in vector.iter() {
        let reference = unsafe { &*slice.offset(*index) };
        let result = map.intern_ref(index, || index);
        assert_eq!(*reference, *result);
        assert_eq!(index as *const _ as *const (), result as *const _ as *const ());
    }
    for index in vector.iter() {
        let reference = unsafe { &*slice.offset(*index) };
        let result = map.intern_ref(index, || unimplemented!());
        assert_eq!(*reference, *result);
        assert_eq!(index as *const _ as *const (), result as *const _ as *const ());
    }
}

#[test]
fn intern_ref3() {
    use fxhash::FxBuildHasher;
    use rayon::prelude::*;
    const ITER: u64 = 32 * 1024;
    let interner = Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default());
    let values: Vec<u64> = (0..ITER).collect();
    (0..ITER).into_par_iter().for_each(|i: u64| {
        interner.intern_ref(&i, || values.get(i as usize).unwrap());
    });
}
