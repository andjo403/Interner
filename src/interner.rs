use crate::raw_interner::{make_hash, LockResult, RawInterner};
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash};
use std::sync::atomic::{AtomicPtr, Ordering};

/// Default hasher for `HashMap`.
pub type DefaultHashBuilder = RandomState;

/// A concurrent interner implemented with quadratic probing and SIMD lookup.
pub struct Interner<T, S = DefaultHashBuilder> {
    hash_builder: S,
    _raw_interners: Box<RawInterner<T>>,
    current_raw_interner: AtomicPtr<RawInterner<T>>,
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
        let mut raw_interners = Box::new(RawInterner::with_capacity(capacity));
        let current_raw_interner = AtomicPtr::new(&mut *raw_interners);
        Self { hash_builder, _raw_interners: raw_interners, current_raw_interner }
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
    pub fn intern_ref<Q: ?Sized>(&self, value: &Q, make: impl FnOnce() -> T) -> T
    where
        T: Borrow<Q> + Copy,
        Q: Hash + Eq,
    {
        let hash = make_hash(&self.hash_builder, value);
        let mut raw_interner = unsafe { &*self.current_raw_interner.load(Ordering::Relaxed) };
        let mut is_current_interner = true;
        loop {
            let lock_result = raw_interner.lock_or_get_slot(hash, value);
            if let LockResult::Found(result) = lock_result {
                return result;
            }
            if let LockResult::Locked(locked_data) = lock_result {
                let result = make();
                if raw_interner.unlock_and_set_value(hash, result, locked_data, &self.hash_builder)
                    && is_current_interner
                {
                    self.current_raw_interner
                        .store(raw_interner.get_next_moved_raw_interner_ptr(), Ordering::Relaxed);
                }
                return result;
            }
            if let LockResult::ResizeNeeded = lock_result {
                if raw_interner.create_and_stor_next_raw_interner(&self.hash_builder)
                    && is_current_interner
                {
                    self.current_raw_interner
                        .store(raw_interner.get_next_moved_raw_interner_ptr(), Ordering::Relaxed);
                }
            }
            raw_interner = raw_interner.get_next_raw_interner();
            is_current_interner = false;
        }
    }

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
    /// let result = interner.intern(&value1,|val| {&val});
    /// assert_eq!(&value1,result);
    /// let result = interner.intern(&value2,|val| {&val});
    /// assert_eq!(&value2,result);
    /// let result = interner.intern(&value2,|val| {&val});
    /// assert_eq!(&value2,result);
    /// ```
    pub fn intern<Q>(&self, value: Q, make: impl FnOnce(Q) -> T) -> T
    where
        T: Borrow<Q> + Copy,
        Q: Hash + Eq,
    {
        let hash = make_hash(&self.hash_builder, &value);
        let mut raw_interner = unsafe { &*self.current_raw_interner.load(Ordering::Relaxed) };
        let mut is_current_interner = true;
        loop {
            let lock_result = raw_interner.lock_or_get_slot(hash, &value);
            if let LockResult::Found(result) = lock_result {
                return result;
            }
            if let LockResult::Locked(locked_data) = lock_result {
                let result = make(value);
                if raw_interner.unlock_and_set_value(hash, result, locked_data, &self.hash_builder)
                    && is_current_interner
                {
                    self.current_raw_interner
                        .store(raw_interner.get_next_moved_raw_interner_ptr(), Ordering::Relaxed);
                }
                return result;
            }
            if let LockResult::ResizeNeeded = lock_result {
                if raw_interner.create_and_stor_next_raw_interner(&self.hash_builder)
                    && is_current_interner
                {
                    self.current_raw_interner
                        .store(raw_interner.get_next_moved_raw_interner_ptr(), Ordering::Relaxed);
                }
            }
            raw_interner = raw_interner.get_next_raw_interner();
            is_current_interner = false;
        }
    }

    /// get already interned value if available.
    ///
    /// # Examples
    ///
    /// ```
    /// use interner::Interner;
    /// use std::hash::{BuildHasher, Hash, Hasher };
    ///
    /// let value1 :i32 = 42;
    /// let mut interner: Interner<&i32> = Interner::with_capacity(2);
    /// let mut state = interner.hasher().build_hasher();
    /// value1.hash(&mut state);
    /// let hash = state.finish();
    /// assert!(interner.get_from_hash(hash,|val| {*val == &value1}).is_none());
    /// let result = interner.intern_ref(&value1,|| {&value1});
    /// assert_eq!(&value1,result);
    /// let result = interner.get_from_hash(hash, |val| {*val == &value1}).expect("was interned above");
    /// assert_eq!(&value1,*result);
    /// ```
    pub fn get_from_hash<F>(&self, hash: u64, mut is_match: F) -> Option<&T>
    where
        F: FnMut(&T) -> bool,
    {
        let mut raw_interner = unsafe { &*self.current_raw_interner.load(Ordering::Relaxed) };
        loop {
            match raw_interner.get(hash, &mut is_match) {
                Some(result) => {
                    return result;
                }
                None => {
                    raw_interner = raw_interner.get_next_raw_interner();
                }
            }
        }
    }
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
