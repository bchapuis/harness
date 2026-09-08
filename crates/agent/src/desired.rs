//! The desired set: what a session's **committed** state calls for, as a pure
//! function of it (workflow spec §3.1, W1).
//!
//! This is the agent runtime's [`Workflow::pending`](workflow::Workflow::pending)
//! ahead of the trait impl that will call it. The agent drives itself through a
//! hand-built dispatcher ([`Agent::advance`](crate::agent::Agent), agent §3.1)
//! whose launch decision reads both the fold *and* the activation's ephemeral
//! environment; the workflow's mechanism requires the fold alone. [`desired`] is
//! the fold-only half, extracted and held against the dispatcher by
//! [`Agent::debug_assert_desired`](crate::agent::Agent) at both launch sites, so
//! every existing suite (including the seeded swarm and cluster runs) checks
//! the two agree before anything is rewired.
//!
//! **The obligation it checks is one-directional and deliberate:** every effect
//! the dispatcher launches MUST be a member of this set (soundness; the pure
//! function never *misses* work the dispatcher does). The converse does not
//! hold and must not be asserted: a member may be blocked this round by a claim
//! or by the environment reconciliation the dispatcher still performs inline
//! (§5.4, §5.5, §5.6). Those three gates are what the inversion has to hoist out
//! of the dispatch path; until it does, the set is a superset of what launches.

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
