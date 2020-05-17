#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

//! This library provides an concurrent insert only interner.
//! Inserts is only locking one slot and store part of the hash in the look to let other inserts with eough diffrent hash to not block on the looked slot.
//! During resize insert is still possible and if the value was already interned only possibly extra lookup in newer interners is done.

#![feature(core_intrinsics)]
#![feature(alloc_layout_extra)]

mod raw;

/// A interner implemented with quadratic probing and SIMD lookup.
pub mod interner;

pub use crate::interner::Interner;
