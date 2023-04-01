#![feature(dropck_eyepatch)]
#![feature(portable_simd)]

//! This library provides an concurrent insert only interner.
//! Inserts is only locking one slot and store part of the hash in the look to let other inserts with eough diffrent hash to not block on the looked slot.
//! During resize insert is still possible and if the value was already interned only possibly extra lookup in newer interners is done.

mod atomic_wait;
mod bitmask;
mod group_match;
/// A interner implemented with quadratic probing and SIMD lookup.
pub mod interner;
mod meta_data;
mod raw_interner;

pub use crate::interner::Interner;
