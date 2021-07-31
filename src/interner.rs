use crate::raw::{make_hash, LockResult, RawInterner};
use core::hash::{BuildHasher, Hash};
use core::sync::atomic::{AtomicPtr, Ordering};
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;

/// Default hasher for `HashMap`.
pub type DefaultHashBuilder = RandomState;

/// A concurrent interner implemented with quadratic probing and SIMD lookup.
pub struct Interner<T, S = DefaultHashBuilder> {
    hash_builder: S,
    raw_interners: RawInterner<T>,
    current_raw_interner: AtomicPtr<RawInterner<T>>,
}

impl<T> Interner<T, DefaultHashBuilder>
where
    T: Sync + Send + Copy,
{
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

impl<T, S> Interner<T, S>
where
    T: Sync + Send + Copy,
{
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
        Self::with_capacity_and_hasher(0, hash_builder)
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
        let mut result = Self {
            hash_builder,
            raw_interners: RawInterner::with_capacity(capacity),
            current_raw_interner: AtomicPtr::default(),
        };
        result.current_raw_interner = AtomicPtr::new(&mut result.raw_interners);
        result
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
    /// let value1 :i32 = 42;
    /// let value2 :i32 = 300;
    /// let mut interner: Interner<&i32> = Interner::with_capacity(2);
    /// let result = interner.intern_ref(&value1,|| {&value1});
    /// assert_eq!(&value1,result);
    /// let result = interner.intern_ref(&value2,|| {&value2});
    /// assert_eq!(&value2,result);
    /// let result = interner.intern_ref(&value2,|| {&value2});
    /// assert_eq!(&value2,result);
    /// ```
    pub fn intern_ref<Q: Sized>(&self, value: &Q, make: impl FnOnce() -> T) -> T
    where
        T: Sync + Send + Borrow<Q> + Copy,
        Q: Sync + Send + Hash + Eq,
    {
        let hash = make_hash(&self.hash_builder, value);
        let mut raw_interner = unsafe { &mut *self.current_raw_interner.load(Ordering::Acquire) };
        loop {
            match raw_interner.lock_or_get_slot(hash, value) {
                LockResult::Found(result) => {
                    return result;
                }
                LockResult::Locked(locked_data) => {
                    let result = make();
                    raw_interner.unlock_and_set_value(
                        hash,
                        result,
                        locked_data,
                        &self.hash_builder,
                    );
                    return result;
                }
                LockResult::ResizeNeeded => {
                    raw_interner =
                        raw_interner.create_and_stor_next_raw_interner(&self.hash_builder);
                }
                LockResult::Moved => {
                    raw_interner = raw_interner.get_next_raw_interner();
                }
            }
        }
    }
}

impl<T, S> Default for Interner<T, S>
where
    S: Default,
    T: Sync + Send + Copy,
{
    /// Creates an empty `Interner<T, S>`, with the `Default` value for the hasher.
    #[inline]
    fn default() -> Self {
        Self::with_hasher(Default::default())
    }
}

unsafe impl<T, S> Sync for Interner<T, S> {}
unsafe impl<T, S> Send for Interner<T, S> {}

#[cfg(test)]
mod tests {

    use super::*;
    use fxhash::FxBuildHasher;
    use rayon::prelude::*;
    use std::hash::Hasher;
    use std::sync::Arc;

    const ITER: u64 = 32 * 1024;

    #[inline]
    pub(crate) fn make_hash<K: Hash + ?Sized>(hash_builder: &impl BuildHasher, val: &K) -> u64 {
        let mut state = hash_builder.build_hasher();
        val.hash(&mut state);
        state.finish()
    }

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

        let hashbuilder = FxBuildHasher::default();
        let interner =
            Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default());
        (1..ITER).into_iter().for_each(|i: u64| {
            interner.intern_ref(&i, || values.get(i as usize).unwrap());
            interner.intern_ref(&i, || {
                unimplemented!("value: {}, Hash {:16x}", i, make_hash(&hashbuilder, &i))
            });
        });

        (1..ITER).into_iter().for_each(|i: u64| {
            interner.intern_ref(&i, || {
                unimplemented!("value: {}, Hash {:16x}", i, make_hash(&hashbuilder, &i))
            });
        });
    }

    #[test]
    fn multi_threaded_intern_ref3() {
        let values: Arc<Vec<u64>> = Arc::new((0..ITER).collect());

        let interner: Arc<Interner<&u64, FxBuildHasher>> =
            Arc::new(Interner::with_capacity_and_hasher(ITER as usize, FxBuildHasher::default()));
        (1..ITER).into_par_iter().for_each(| i: u64| {
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
    fn single_threaded_resize() {
        let values: Vec<u64> = (0..ITER).collect();
        let values = values.into_boxed_slice();

        let hashbuilder = FxBuildHasher::default();
        let interner = Interner::with_hasher(hashbuilder.clone());
        (1..ITER).into_iter().for_each(|i: u64| {
            interner.intern_ref(&i, || values.get(i as usize).unwrap());
            interner.intern_ref(&i, || {
                unimplemented!("value: {}, Hash {:16x}", i, make_hash(&hashbuilder, &i))
            });
        });

        (1..ITER).into_iter().for_each(|i: u64| {
            interner.intern_ref(&i, || {
                unimplemented!("value: {}, Hash {:16x}", i, make_hash(&hashbuilder, &i))
            });
        });
    }

    #[test]
    fn multi_threaded_resize() {
        let values: Arc<Vec<u64>> = Arc::new((0..ITER).collect());

        let interner: Arc<Interner<&u64, FxBuildHasher>> =
        Arc::new(Interner::with_hasher(FxBuildHasher::default()));
        (1..ITER).into_par_iter().for_each(| i: u64| {
            interner.intern_ref(&i, || (*values).get(i as usize).unwrap());
            let result = interner.intern_ref(&i, || unimplemented!());
            assert_eq!(i, *result);
        });

        (1..ITER).into_iter().for_each(|i: u64| {
            let result = interner.intern_ref(&i, || unimplemented!());
            assert_eq!(i, *result);
        });
    }
}
