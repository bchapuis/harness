//! Awaiting a durable outcome (workflow spec §9).
//!
//! Autonomous work outlives the request that started it. A caller that asked for
//! it therefore cannot receive it as a reply: a reply handle is resolved exactly
//! once within the delivery of one message (core §7) and the work spans many.
//! Every consumer of the workflow meets this the same way: an ack, a registered
//! reply-to, and a later notification (agent §7.2), so the pattern is supplied
//! here as a **component** with a normative discipline.
//!
//! It is a component and not a trait method because the *key* and the *payload*
//! are the consumer's, and because a consumer with no external callers (one
//! driven entirely by alarms) should carry none of it.
//!
//! The discipline (§9), which this type enforces where it can and documents
//! where it cannot:
//!
//! 1. The set MUST NOT be journaled. It is ephemeral activation state, rebuilt
//!    by callers re-contacting after a resume, like the claims and for the same
//!    reason: a reply-to is a routing address, not a fact about the work.
//! 2. A notification MUST be sent only **after** the record carrying the outcome
//!    commits (entity §6), so no caller holds a completion the journal could
//!    lose. [`take`](Subscribers::take) is therefore the only way to get the
//!    registered addresses out, and a consumer calls it from its post-commit
//!    pass.
//! 3. A notification is a `tell` (core §3.2): at-most-once, best-effort,
//!    **delivery and not the source of truth**. The committed record is. A lost
//!    notification is recovered by the caller re-contacting the entity, never by
//!    a transparent retry (core §1.1).
//! 4. A caller arriving after the outcome has committed MUST be answered
//!    immediately from the fold, not registered: otherwise it waits for a
//!    notification that has already been sent. That check is the consumer's,
//!    because only the consumer's fold knows whether the work is done.

use std::collections::BTreeMap;

/// The reply-tos registered against in-flight work (spec §9): ephemeral
/// activation state, never journaled.
///
/// `K` identifies the unit of work a caller is waiting on (the agent runtime's
/// `TurnId`); `V` is the consumer's own notification address (an `ActorRef` to a
/// reply mailbox). Several callers may wait on one `K`: that is what
/// "attaching to a live run" is.
pub struct Subscribers<K: Ord, V> {
    waiting: BTreeMap<K, Vec<V>>,
}

impl<K: Ord, V> Default for Subscribers<K, V> {
    fn default() -> Self {
        Subscribers {
            waiting: BTreeMap::new(),
        }
    }
}

impl<K: Ord, V> Subscribers<K, V> {
    /// Register `who` to be notified when `key`'s outcome commits.
    ///
    /// The caller MUST have established, from the fold, that the work is not
    /// already finished (discipline item 4); this type cannot check that,
    /// because only the consumer's fold knows.
    pub fn register(&mut self, key: K, who: V) {
        self.waiting.entry(key).or_default().push(who);
    }

    /// Take every address registered for `key`, leaving none.
    ///
    /// The only way out, so that a notification cannot be sent before its record
    /// commits (discipline item 2): a consumer calls this from the post-commit
    /// pass, never from the handler that decided the outcome.
    pub fn take(&mut self, key: &K) -> Vec<V> {
        self.waiting.remove(key).unwrap_or_default()
    }

    /// The units of work something is registered against, in key order.
    ///
    /// The post-commit pass reads this to decide what to
    /// [`take`](Subscribers::take): which of them just finished is a question
    /// only the consumer's fold can answer (discipline item 4), so the set of
    /// candidates has to come back out. Registrations are not exposed with it;
    /// `take` remains the only way to an address.
    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.waiting.keys()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn several_callers_attach_to_one_unit_of_work() {
        let mut s: Subscribers<u8, &str> = Subscribers::default();
        s.register(1, "a");
        s.register(1, "b");
        s.register(2, "c");
        assert_eq!(s.take(&1), vec!["a", "b"]);
        assert!(s.take(&1).is_empty());
        assert_eq!(s.take(&2), vec!["c"]);
    }

    #[test]
    fn the_post_commit_pass_sees_every_registered_key() {
        let mut s: Subscribers<u8, &str> = Subscribers::default();
        s.register(2, "c");
        s.register(1, "a");
        s.register(1, "b");
        assert_eq!(s.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
        s.take(&1);
        assert_eq!(
            s.keys().copied().collect::<Vec<_>>(),
            vec![2],
            "a taken key leaves the candidate set"
        );
    }

    #[test]
    fn taking_twice_notifies_once() {
        let mut s: Subscribers<u8, &str> = Subscribers::default();
        s.register(1, "a");
        assert_eq!(s.take(&1), vec!["a"]);
        assert!(
            s.take(&1).is_empty(),
            "a second post-commit pass must not re-notify"
        );
    }
}
