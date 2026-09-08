//! First-tick classification and policy resolution (workflow spec §4.4).
//!
//! An effect's intent is journaled before it launches and its outcome after
//! (§5). A crash, hibernation, or migration between the two leaves the effect
//! **orphaned**: the journal says it was wanted, and nothing says whether it
//! ran.
//!
//! The desired set makes the recovery rule short. **The desired set computed by
//! an activation's first tick is its orphan set**, by construction, since
//! every member was put there by a record that committed before this activation
//! drove anything, and no launch of this activation can yet have claimed it.
//! Every member appearing for the first time in a *later* tick is fresh.
//!
//! The classification happens at the activation's **first sight of committed
//! state**, which is not always the first tick. The engine ticks itself from
//! `on_activate`, but a command already in the mailbox is handled first, and a
//! handler sees the committed state *before* its own records: the rehydrated
//! state exactly, on a fresh activation. So the first handler that re-enters the
//! cycle ([`Engine::tick`](crate::Engine::tick)) or delivers an outcome
//! ([`Engine::settled`](crate::Engine::settled)) classifies from what it was given,
//! and the work its own records create is fresh, never orphaned. A consumer
//! handler that commits work without nudging leaves the classification to the
//! first tick, which then reads that work as orphaned: conservative for
//! [`Interrupt`] (settled `Interrupted` without having run, which is what the
//! policy promises), and exactly right for [`Reexecute`].
//!
//! [`Reexecute`]: crate::OnOrphan::Reexecute
//! [`Interrupt`]: crate::OnOrphan::Interrupt

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;

use crate::OnOrphan;
use crate::Pending;

/// The keys of every member of `desired` declaring [`OnOrphan::Interrupt`].
///
/// The engine settles each as `Err(EffectError::Interrupted)` without launching
/// anything. It does not settle them inline: the first tick cannot commit
/// (entity journals only through the per-command protocol, entity §6), so each
/// resolution rides the mailbox as a synthesized `Settled` and commits as an
/// ordinary command (§3.2).
pub(crate) fn interrupted<K: Clone + Ord, E>(desired: &[Pending<K, E>]) -> BTreeSet<K> {
    desired
        .iter()
        .filter(|p| p.on_orphan == OnOrphan::Interrupt)
        .map(|p| p.key.clone())
        .collect()
}

/// Run an effect's future, turning a panic into a value.
///
/// A panic inside [`Workflow::perform`](crate::Workflow::perform) is caught at the
/// task boundary and settled as `Failed` (§4.3), so a consumer bug in one effect
/// cannot take the activation with it, and, which matters more, cannot leave a
/// claimed key with no outcome, stranding the engine on a key it will never
/// relaunch and never let rest.
///
/// `AssertUnwindSafe` is the honest annotation here: the future owns everything
/// it touches (it is `'static` and holds no borrow of the entity, §4.1), and what
/// it might leave inconsistent on a panic is discarded with it.
pub(crate) async fn catch<T, F>(fut: F) -> Result<T, String>
where
    F: Future<Output = T>,
{
    match AssertUnwindSafe(fut).catch_unwind().await {
        Ok(value) => Ok(value),
        Err(payload) => Err(panic_message(&payload)),
    }
}

/// The message from a caught panic's payload, for the `Failed` it becomes.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        format!("effect panicked: {s}")
    } else if let Some(s) = payload.downcast_ref::<String>() {
        format!("effect panicked: {s}")
    } else {
        "effect panicked".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_interrupt_members_are_resolved_without_launching() {
        let desired = vec![
            Pending::reexecute(1u8, ()),
            Pending::interrupt(2u8, ()),
            Pending::reexecute(3u8, ()),
            Pending::interrupt(4u8, ()),
        ];
        assert_eq!(interrupted(&desired), BTreeSet::from([2, 4]));
    }

    #[test]
    fn an_empty_desired_set_dangles_nothing() {
        let desired: Vec<Pending<u8, ()>> = Vec::new();
        assert!(interrupted(&desired).is_empty());
    }

    #[test]
    fn a_panicking_effect_becomes_a_value_rather_than_a_lost_claim() {
        let caught = futures::executor::block_on(catch(async {
            panic!("seam exploded");
            #[allow(unreachable_code)]
            0u8
        }));
        let why = caught.expect_err("a panic is caught, not propagated");
        assert!(why.contains("seam exploded"), "{why}");
    }

    #[test]
    fn an_ordinary_effect_passes_through_untouched() {
        let out = futures::executor::block_on(catch(async { 7u8 }));
        assert_eq!(out, Ok(7));
    }
}
