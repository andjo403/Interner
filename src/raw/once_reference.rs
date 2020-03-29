use crate::raw::UsizeTReference;
use core::sync::atomic::{AtomicUsize, Ordering};
use parking_lot_core::{self, SpinWait, UnparkToken, DEFAULT_PARK_TOKEN};

// UnparkToken used to indicate that reference is stored. not used only to have a value to set in unpark_all
const TOKEN_VALUE_SET: UnparkToken = UnparkToken(0);

/// This bit is zero in `state_and_data` of a `OnceReference` when `state_or_reference` contains a reference.
const REF_UNSET_BIT: usize = 0b01;
/// This bit is set in `state_and_data` of a `OnceReference` when that mutex is locked by some thread.
const LOCKED_BIT: usize = 0b10;
/// This bit is set in the `state_and_data` of a `OnceReference` just before parking a thread.
/// A thread is being parked if it wants to lock the mutex, but it is currently being held by some other thread.
const PARKED_BIT: usize = 0b100;

/// Once Reference type backed by the parking lot.
pub(crate) struct OnceReference {
    /// This atomic integer holds the current state or the reference.
    ///
    /// # State table:
    ///
    ///  PARKED_BIT | LOCKED_BIT | REF_UNSET_BIT | Description
    ///      0      |     0      |      0        | contains a reference
    /// ------------+------------+---------------+------------------------------------------------------------------
    ///      0      |     1      |      0        | invalid state, can not be locked and contain a reference
    /// ------------+------------+---------------+------------------------------------------------------------------
    ///      1      |     0      |      0        | invalid state, can not be parked and contain a reference
    /// ------------+------------+---------------+------------------------------------------------------------------
    ///      1      |     1      |      0        | invalid state, is locked and parked but contains a reference
    /// ------------+------------+---------------+------------------------------------------------------------------
    ///      0      |     0      |      1        |  waiting to be locked
    /// ------------+------------+---------------+------------------------------------------------------------------
    ///      0      |     1      |      1        | The mutex is locked by exactly one thread. No other thread is
    ///             |            |               | waiting for it.
    /// ------------+------------+---------------+------------------------------------------------------------------
    ///      1      |     0      |      1        | invalid state, The mutex is not locked and one or more thread is
    ///             |            |               | parked or about to park. When the lock is released it is not
    ///             |            |               | setting the LOCKED_BIT as the lock shall not be retaken.
    /// ------------+------------+---------------+------------------------------------------------------------------
    ///      1      |     1      |      1        | The mutex is locked by exactly one thread. One or more thread is
    ///             |            |               | parked or about to park, waiting for the lock to become available.
    state_or_reference: AtomicUsize,
}

impl Default for OnceReference {
    fn default() -> Self {
        OnceReference { state_or_reference: AtomicUsize::new(0b1) }
    }
}

impl OnceReference {
    pub(crate) fn intern(
        &self,
        hash: u64,
        eq: impl Copy + FnOnce(UsizeTReference) -> bool,
        make: impl Copy + FnOnce() -> UsizeTReference,
    ) -> Option<UsizeTReference> {
        if self
            .state_or_reference
            .compare_exchange(
                REF_UNSET_BIT,
                hash.wrapping_shl(3) as usize | LOCKED_BIT | REF_UNSET_BIT,
                Ordering::Relaxed,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return self.get(hash, eq);
        }

        let result = make();
        let reference = result.0;
        if self.state_or_reference.swap(reference, Ordering::SeqCst) & PARKED_BIT == PARKED_BIT {
            self.unpark_all();
        }
        Some(result)
    }

    pub(crate) fn get(
        &self,
        hash: u64,
        eq: impl Copy + FnOnce(UsizeTReference) -> bool,
    ) -> Option<UsizeTReference> {
        let mut state = self.state_or_reference.load(Ordering::Relaxed);

        if state & REF_UNSET_BIT == 0 {
            let result = UsizeTReference(state);
            if eq(result) {
                return Some(result);
            }
            return None;
        } else if state & LOCKED_BIT == 0 {
            return None;
        } else if state & !7 != (hash.wrapping_shl(3) as usize) {
            return None;
        }

        let mut spinwait = SpinWait::new();
        loop {
            // when the reference is stored return it
            if state & REF_UNSET_BIT == 0 {
                let result = UsizeTReference(state);
                if eq(result) {
                    return Some(result);
                }
                return None;
            }

            // If there is no queue, try spinning a few times
            if state & PARKED_BIT == 0 && spinwait.spin() {
                state = self.state_or_reference.load(Ordering::Relaxed);
                continue;
            }

            // Set the parked bit
            if state & PARKED_BIT == 0 {
                if let Err(x) = self.state_or_reference.compare_exchange_weak(
                    state,
                    state | PARKED_BIT,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    state = x;
                    continue;
                }
            }

            // Park our thread until we are woken up by an unlock
            let addr = self as *const _ as usize;
            let validate = || {
                self.state_or_reference.load(Ordering::Relaxed)
                    == LOCKED_BIT | PARKED_BIT | REF_UNSET_BIT
            };
            let before_sleep = || {};
            let timed_out = |_, _| {};
            // SAFETY:
            //   * `addr` is an address we control.
            //   * `validate`/`timed_out` does not panic or call into any function of `parking_lot`.
            //   * `before_sleep` does not call `park`, nor does it panic.
            unsafe {
                parking_lot_core::park(
                    addr,
                    validate,
                    before_sleep,
                    timed_out,
                    DEFAULT_PARK_TOKEN,
                    None,
                );
            }
            state = self.state_or_reference.load(Ordering::Relaxed);
        }
    }

    #[cold]
    fn unpark_all(&self) {
        let addr = self as *const _ as usize;
        // SAFETY:
        //   * `addr` is an address we control.
        unsafe {
            parking_lot_core::unpark_all(addr, TOKEN_VALUE_SET);
        }
    }
}
