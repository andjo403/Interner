use crate::raw::{make_hash, LockResult, RawInterner};
use crate::sync::{AtomicPtr, Ordering};
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};

/// Default hasher for `HashMap`.
pub type DefaultHashBuilder = RandomState;

/// A concurrent interner implemented with quadratic probing and SIMD lookup.
pub struct Interner<T, S = DefaultHashBuilder> {
    hash_builder: S,
    raw_interners: *mut RawInterner<T>,
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
        let raw_interner = Box::new(RawInterner::with_capacity(capacity));
        let raw_interners = Box::into_raw(raw_interner);
        let current_raw_interner = AtomicPtr::new(raw_interners);
        Self { hash_builder, raw_interners, current_raw_interner }
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
        let mut raw_interner = unsafe { &*self.current_raw_interner.load(Ordering::Acquire) };
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

impl<T, S> Drop for Interner<T, S> {
    fn drop(&mut self) {
        let _next_raw_internere = unsafe { Box::from_raw(self.raw_interners) };
    }
}

unsafe impl<T, S> Sync for Interner<T, S> {}
unsafe impl<T, S> Send for Interner<T, S> {}
