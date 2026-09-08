//! The desired set: what a session's **committed** state calls for, as a pure
//! function of it (workflow spec §3.1, W1).
//!
//! This is the agent runtime's [`Workflow::pending`](workflow::Workflow::pending)
//! ahead of the trait impl that will call it, and since the environment
//! reconciliation moved ahead of the dispatch it is also what the dispatcher
//! now walks. [`Agent::advance`](crate::agent::Agent) reconciles the
//! activation's environment first (agent §5.4, §5.5, §5.6) and then launches
//! this set, member by member, with no gate of its own left but the launch-once
//! claim.
//!
//! **The obligation therefore runs both ways.** Soundness, that nothing
//! launches outside the set, is structural: the dispatch iterates the set and
//! nothing else. The converse is the one worth asserting, because it is what
//! the workflow's engine assumes and what a gate kept back would quietly break,
//! so `Agent::debug_assert_launchable` holds every unclaimed member of the set
//! to being launchable as it stands: its tier held, its environment bound, its
//! delegation recorded. Every suite in the crate, the seeded swarm and cluster
//! runs included, checks it on every round.

use serde::Deserialize;
use serde::Serialize;
use workflow::OnOrphan;
use workflow::Pending;

use crate::agent::StepKey;
use crate::kind::Kind;
use crate::session::CallId;
use crate::session::SessionState;
use crate::tool::DELEGATE;

/// One unit of work a session's committed state calls for: the agent runtime's
/// [`Workflow::Effect`](workflow::Workflow::Effect).
///
/// Opaque to the workflow and never journaled (workflow §3.1), so it carries
/// only what the launch needs; everything else the launch reads comes from the
/// activation's seams. The two variants are the loop's two external operations
/// (agent §3.1 steps 2 and 4): a delegation is a `Call`, since it is dispatched
/// by call id like any other tool and differs only in where it executes (§8.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Effect {
    /// The model call answering step `step` (§3.1 step 2).
    Model { step: u32 },
    /// The sandboxed tool call or delegation `call` names (§3.1 step 4).
    Call { call: CallId, name: String },
}

/// The effects `state` calls for (workflow §3.1).
///
/// Pure and deterministic: it reads the fold and the kind (deployment
/// configuration, identical on every node and covered by the kind digest, agent
/// §7.1) and nothing else. It MUST NOT read the activation (no sandbox slot, no
/// held tiers, no launch claims) which is exactly the property that makes a
/// resumed activation agree with the one it replaced about what is outstanding
/// (W1).
///
/// The shape mirrors the dispatcher's own two branches:
///
/// - No live run ⇒ **empty**. A queued turn is not an outstanding effect: the
///   run has not started, and starting it is a record the dispatcher decides
///   (§3.1), not work performed outside the entity.
/// - A live run with no pending calls ⇒ the **model call** for the run's current
///   step, unless the budget forbids one (§3.1 step 2). A completion request is
///   side-effect-free, so an orphaned one re-executes (§4.2).
/// - A live run with pending calls ⇒ **every journaled intent lacking an
///   outcome**, each under its declared recovery policy (§5.5).
///
/// `budget_floor` is the deployment's reserve ([`HarnessConfig`](crate::HarnessConfig)),
/// passed rather than read so this stays a function of its arguments.
pub(crate) fn desired(
    kind: &Kind,
    state: &SessionState,
    budget_floor: u64,
) -> Vec<Pending<StepKey, Effect>> {
    let Some(live) = state.live.as_ref() else {
        return Vec::new();
    };
    if live.pending.is_empty() {
        // The budget gate is fold state (`spend` and `budget` are both folded),
        // so it belongs here: a run past its budget calls for no further model
        // call, and the dispatcher ends it instead of launching one.
        if !live.spend.allows_call(&live.budget, budget_floor) {
            return Vec::new();
        }
        let step = live.spend.own_steps;
        return vec![Pending::reexecute(
            StepKey::Model(step),
            Effect::Model { step },
        )];
    }
    live.pending
        .iter()
        .map(|(call, pending)| Pending {
            key: StepKey::Call(call.clone()),
            effect: Effect::Call {
                call: call.clone(),
                name: pending.name.clone(),
            },
            on_orphan: orphan_policy(kind, &pending.name),
        })
        .collect()
}

/// The recovery policy for an orphaned call (agent §5.5): `delegate`
/// re-executes by construction (its re-submission dedups, §8.1); a sandboxed
/// tool follows its declaration, defaulting to `Interrupt` if the declaration
/// vanished; the kind changed under a resumed session, and an unknown side
/// effect is not repeated on a guess.
///
/// Kept beside [`desired`] rather than on the agent because it is part of the
/// pure function: a [`Pending`] carries its policy (workflow §3.1), so deriving
/// one is deriving the other.
pub(crate) fn orphan_policy(kind: &Kind, name: &str) -> OnOrphan {
    if name == DELEGATE {
        OnOrphan::Reexecute
    } else {
        kind.tools
            .get(name)
            .map(|d| d.on_orphan)
            .unwrap_or(OnOrphan::Interrupt)
    }
}
