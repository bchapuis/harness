//! The desired set: what a session's **committed** state calls for, as a pure
//! function of it (workflow spec §3.1, W1).
//!
//! This is the agent runtime's [`Workflow::pending`](workflow::Workflow::pending),
//! and [`Agent`](crate::agent::Agent)'s impl is one call to [`desired`]. Every
//! effect it returns is **complete**: a launch takes the description as it
//! stands and adds only the seams (the model, the sandbox handle, the child
//! directory), because [`perform`](workflow::Workflow::perform) is handed no
//! state and must not want any.
//!
//! Two things follow from purity rather than from taste.
//!
//! A call the fold can already answer is an [`Effect::Fail`] and not a special
//! case: an unknown tool and a malformed input are decidable from the kind and
//! the journal, so they are *work* whose performance is synthesizing an outcome
//! (§5.4). A delegation whose `ChildRun` is not yet journaled, by contrast, is
//! **omitted**: `child.is_none()` is fold state too, and a set that offered an
//! unlaunchable member would ask the engine to honor a gate it cannot see. The
//! record comes first and the effect appears behind it (§8.1).
//!
//! What stays outside is the activation's environment (a held tier, a bound
//! sandbox, a lost workspace), which no fold records. `Agent::reconcile` settles
//! that ahead of every tick, and `Agent::debug_assert_launchable` holds the two
//! together: every unclaimed member of this set is launchable as it stands.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use workflow::OnOrphan;
use workflow::Pending;

use crate::agent::StepKey;
use crate::agent::delegate_spec;
use crate::client::HarnessConfig;
use crate::kind::Kind;
use crate::model::ModelRequest;
use crate::model::ToolSpec;
use crate::sandbox::Tier;
use crate::session::CallId;
use crate::session::ChildRef;
use crate::session::LiveRun;
use crate::session::SessionId;
use crate::session::SessionState;
use crate::session::TurnId;
use crate::tool::DELEGATE;
use crate::tool::DelegateInput;
use crate::tool::ToolError;

/// One unit of work a session's committed state calls for: the agent runtime's
/// [`Workflow::Effect`](workflow::Workflow::Effect).
///
/// Opaque to the workflow and never journaled (workflow §3.1), so it carries
/// exactly what the launch needs and nothing that would have to be kept
/// consistent with the journal. The four variants are the loop's two external
/// operations (agent §3.1 steps 2 and 4), the delegation that executes in the
/// loop rather than the sandbox (§8.1), and the outcome the fold can synthesize
/// without executing anything (§5.4).
pub enum Effect {
    /// The model call answering step `step` (§3.1 step 2).
    Model { step: u32, request: ModelRequest },
    /// The sandboxed tool call `call` names (§3.1 step 4, §5.3).
    Tool {
        call: CallId,
        tier: Tier,
        name: String,
        input: Value,
        timeout: Duration,
    },
    /// The delegation `call` names, its `ChildRun` already journaled (§8.1).
    Delegate {
        call: CallId,
        turn: TurnId,
        prompt: String,
        child: ChildRef,
        root: SessionId,
    },
    /// A call the fold answers on its own: nothing is executed (§5.4).
    Fail { call: CallId, error: ToolError },
}

/// The effects `state` calls for (workflow §3.1).
///
/// Pure and deterministic: it reads the fold, the kind, and the deployment
/// config (both identical on every node, and the kind digest-covered, agent
/// §7.1), and nothing else. It MUST NOT read the activation (no sandbox slot, no
/// held tiers, no launch claims), which is exactly the property that makes a
/// resumed activation agree with the one it replaced about what is outstanding
/// (W1).
///
/// - No live run ⇒ **empty**. A queued turn is not an outstanding effect: the
///   run has not started, and starting it is a record the tick decides (§3.1),
///   not work performed outside the entity.
/// - A live run with no pending calls ⇒ the **model call** for the run's current
///   step, unless the budget forbids one, in which case the set is empty and the
///   run has ended by arithmetic (§3.1 step 2). A completion request is
///   side-effect-free, so an orphaned one re-executes (workflow §4.2).
/// - A live run with pending calls ⇒ **every journaled intent lacking an
///   outcome** that can be launched as it stands, each under its declared
///   recovery policy (§5.5).
pub fn desired(
    kind: &Kind,
    config: &HarnessConfig,
    state: &SessionState,
) -> Vec<Pending<StepKey, Effect>> {
    let Some(live) = state.live.as_ref() else {
        return Vec::new();
    };
    if live.pending.is_empty() {
        // The budget gate is fold state (`spend` and `budget` are both folded),
        // so it belongs here: a run past its budget calls for no further model
        // call, and the tick ends it instead of launching one.
        if !live.spend.allows_call(&live.budget, config.budget_floor) {
            return Vec::new();
        }
        let step = live.spend.own_steps;
        return vec![Pending::reexecute(
            StepKey::Model(live.turn.clone(), step),
            Effect::Model {
                step,
                request: build_request(kind, state, live),
            },
        )];
    }
    // The lineage root a delegation carries (§8.1). `SessionCreated` is journaled
    // with the very first turn, so a live run always has one; a delegation
    // without it is unreachable rather than defaulted, and drops out of the set
    // the way any unlaunchable member does.
    let root = state.created.as_ref().map(|c| c.root.clone());
    live.pending
        .iter()
        .filter_map(|(call, pending)| {
            let effect = if pending.name == DELEGATE {
                // Not yet launchable: the `ChildRun` intent this call needs is
                // still to be journaled (§8.1 step 1), so the set omits it.
                let child = pending.child.clone()?;
                let root = root.clone()?;
                match serde_json::from_value::<DelegateInput>(pending.input.clone()) {
                    // Validated once when the intent was journaled, so a failure
                    // here is a drifted schema; resolve rather than strand.
                    Err(e) => Effect::Fail {
                        call: call.clone(),
                        error: ToolError::InvalidArguments(e.to_string()),
                    },
                    Ok(request) => Effect::Delegate {
                        call: call.clone(),
                        turn: live.turn.clone(),
                        prompt: request.prompt,
                        child,
                        root,
                    },
                }
            } else {
                match kind.tools.get(&pending.name) {
                    None => Effect::Fail {
                        call: call.clone(),
                        error: ToolError::UnknownTool {
                            name: pending.name.clone(),
                        },
                    },
                    Some(_) if !pending.input.is_object() => Effect::Fail {
                        call: call.clone(),
                        error: ToolError::InvalidArguments(
                            "tool input must be a JSON object".into(),
                        ),
                    },
                    Some(decl) => Effect::Tool {
                        call: call.clone(),
                        tier: decl.tier,
                        name: pending.name.clone(),
                        input: pending.input.clone(),
                        timeout: decl.timeout.unwrap_or(config.tool_timeout),
                    },
                }
            };
            Some(Pending {
                key: StepKey::Call(live.turn.clone(), call.clone()),
                effect,
                on_orphan: orphan_policy(kind, &pending.name),
            })
        })
        .collect()
}

/// The tier a desired call needs, or `None` for one that runs in the loop or
/// synthesizes its own outcome: what `reconcile` acquires before the launch
/// (§5.6).
pub(crate) fn tier_of(effect: &Effect) -> Option<Tier> {
    match effect {
        Effect::Tool { tier, .. } => Some(*tier),
        Effect::Model { .. } | Effect::Delegate { .. } | Effect::Fail { .. } => None,
    }
}

/// The model request for the run's current step (§4.1), clamping `max_tokens` to
/// the remaining budget (§9.1 item 2). Part of the pure function: a request is a
/// projection of the transcript and the kind, and `perform` is given no state to
/// build one from.
fn build_request(kind: &Kind, state: &SessionState, live: &LiveRun) -> ModelRequest {
    let max_tokens = kind
        .params
        .max_tokens
        .min(live.spend.remaining_tokens(&live.budget));
    let mut tools: Vec<ToolSpec> = kind.tools.specs();
    if !kind.delegates.is_empty() {
        tools.push(delegate_spec(kind));
    }
    ModelRequest {
        system_prompt: kind.system_prompt.clone(),
        params: kind.params.clone(),
        tools,
        transcript: Arc::clone(&state.transcript),
        max_tokens,
    }
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
