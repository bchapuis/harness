//! Rebinding `ActorRef`s on decode (spec §4.4, invariant #10).
//!
//! An [`ActorRef`](crate::ActorRef) serializes to just its `ActorId`; on the
//! wire it carries no system handle. To deserialize one back into a usable
//! handle, the decoder must know which system to bind it to. The cluster sets
//! the *current system* around a message decode with [`with_decoding_system`],
//! and `ActorRef`'s `Deserialize` reads it through `current_decoding_system`.
//!
//! The handle is a thread-local stack, so per-thread nested decodes are safe.
//! Decoding is synchronous, so the current system never has to span an `.await`.

use std::any::Any;
use std::cell::RefCell;

use crate::system::ActorSystem;

thread_local! {
    /// A stack of the systems currently decoding on this thread; the top is used
    /// to rebind a decoded `ActorRef`. Boxed as `dyn Any` so one slot holds any
    /// concrete system type.
    static DECODING_SYSTEM: RefCell<Vec<Box<dyn Any>>> = const { RefCell::new(Vec::new()) };
}

/// Run `f` with `system` set as the current system for any [`ActorRef`] decoded
/// within it (spec §4.4). The previous system is restored afterward, even on
/// panic.
///
/// [`ActorRef`]: crate::ActorRef
pub fn with_decoding_system<S, R>(system: &S, f: impl FnOnce() -> R) -> R
where
    S: ActorSystem,
{
    struct Pop;
    impl Drop for Pop {
        fn drop(&mut self) {
            DECODING_SYSTEM.with(|s| {
                s.borrow_mut().pop();
            });
        }
    }
    DECODING_SYSTEM.with(|s| s.borrow_mut().push(Box::new(system.clone())));
    let _pop = Pop;
    f()
}

/// The current system to rebind a decoded `ActorRef` to, if
/// [`with_decoding_system`] is active and its system is of type `S`.
pub(crate) fn current_decoding_system<S: ActorSystem>() -> Option<S> {
    DECODING_SYSTEM.with(|s| {
        s.borrow()
            .last()
            .and_then(|boxed| boxed.downcast_ref::<S>().cloned())
    })
}
