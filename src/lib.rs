#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

//! This library provides an concurrent add only interner that looks only the value with the same hash during interning.
//! While resize only add of new values is blocked until the resize is done.

#![feature(core_intrinsics)]

#[macro_use]
mod macros;

mod raw;

/// A interner implemented with quadratic probing and SIMD lookup.
pub mod interner;

pub use interner::Interner;
