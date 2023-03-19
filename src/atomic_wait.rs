use crate::meta_data::test_valid_bit;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot_core::{self, DEFAULT_PARK_TOKEN, DEFAULT_UNPARK_TOKEN};

fn lock_addr(group_meta_data: &AtomicU64, index: usize) -> usize {
    (group_meta_data as *const _ as usize) + index
}

#[cold]
pub(crate) fn wait(group_meta_data: &AtomicU64, index: usize) {
    // Park our thread until we are woken up by an unlock
    let addr = lock_addr(group_meta_data, index);
    let validate = || test_valid_bit(group_meta_data.load(Ordering::Relaxed), index);
    let before_sleep = || {};
    let timed_out = |_, _| {};
    // SAFETY:
    //   * `addr` is an address we control.
    //   * `validate`/`timed_out` does not panic or call into any function of `parking_lot`.
    //   * `before_sleep` does not call `park`, nor does it panic.
    unsafe {
        parking_lot_core::park(addr, validate, before_sleep, timed_out, DEFAULT_PARK_TOKEN, None);
    }
}

#[cold]
pub(crate) fn wake_all(group_meta_data: &AtomicU64, index: usize) {
    let addr = lock_addr(group_meta_data, index);
    // SAFETY:
    //   * `addr` is an address we control.
    unsafe {
        parking_lot_core::unpark_all(addr, DEFAULT_UNPARK_TOKEN);
    }
}
