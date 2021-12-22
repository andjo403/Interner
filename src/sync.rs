#[cfg(loom)]
pub(crate) use loom::sync::{
    atomic::{fence, AtomicIsize, AtomicPtr, AtomicU64, Ordering},
    Arc, Condvar, Mutex,
};

#[cfg(not(loom))]
pub(crate) use std::sync::{
    atomic::{fence, AtomicIsize, AtomicPtr, AtomicU64, Ordering},
    Arc, Condvar, Mutex,
};
