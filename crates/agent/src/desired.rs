//! The desired set: what a session's **committed** state calls for, as a pure
//! function of it (workflow spec §4.1, W1).
//!
//! This is the agent runtime's [`Workflow::pending`](workflow::Workflow::pending),
//! and [`Agent`](crate::agent::Agent)'s impl is one call to [`desired`]. Every
//! effect it returns is **complete**: a launch takes the description as it
//! stands and adds only the seams (the model, the sandbox handle, the child
//! directory), because [`perform`](workflow::Workflow::perform) is handed no
//! state and must not want any.
//!
//! A call the fold can already answer is an [`Effect::Fail`](crate::desired::Effect::Fail): an unknown tool
//! and a malformed input are decidable from the kind and the journal, so they
//! are *work* whose performance is synthesizing an outcome (§5.2). A delegation
//! whose `ChildRun` is not yet journaled, by contrast, is **omitted**:
//! `child.is_none()` is fold state too, and a set that offered an unlaunchable
//! member would ask the engine to honor a gate it cannot see. The record comes
//! first and the effect appears behind it (§8.1).
//!
//! What stays outside is the activation's environment (a held tier, a bound
//! sandbox, a lost workspace), which no fold records. `Agent::reconcile` settles
//! that ahead of every tick, and `Agent::debug_assert_launchable` holds the two
//! together: every unclaimed member of this set is launchable as it stands.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use workflow::OnOrphan;
use workflow::Pending;

use crate::config::HarnessConfig;
use crate::kind::Kind;
use crate::model::ModelParams;
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

/// One launched effect of a run (§4.2): the agent runtime's
/// [`Workflow::Key`](workflow::Workflow::Key). A model call is keyed by the step it answers (`own_steps`
/// at issue, §4.2 step 2); a tool call or delegation by its `CallId` (§4.2 step
/// 4).
///
/// The engine claims a key at launch and drops the claim when the key leaves the
/// desired set, which is the moment its outcome commits: a committed response
/// advances `own_steps`, and a committed `ToolOutcome` takes the call out of the
/// pending set. A straggler therefore has nothing to "unlock" into a duplicate
/// launch (§9.1 item 4, §9.2 item 4).
///
/// **Both variants carry the turn**, which neither half of the key supplies on
/// its own: `own_steps` restarts at zero with every run, and a synthesized
/// `call-{step}-{i}` id repeats across runs the same way. A key must be unique
/// over the whole journal, not merely within one desired set (workflow §4.1),
/// because the straggler filter's only test is presence in that set: a key that
/// left and later re-entered would let an outcome launched under the first
/// occurrence be settled as the second's.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum StepKey {
    /// The model call answering step `own_steps` of `turn` (§4.2 step 2).
    Model(TurnId, u32),
    /// The tool call (or delegation) `CallId` names, within `turn` (§4.2 step 4).
    Call(TurnId, CallId),
    /// The cancel owed to the child the delegation `(turn, call)` started
    /// (§9.2). A variant of its own rather than the `Call` key again: that key
    /// left the set when the run ended, and a key must never re-enter (workflow
    /// §4.1).
    Cancel(TurnId, CallId),
}

/// One unit of work a session's committed state calls for: the agent runtime's
/// [`Workflow::Effect`](workflow::Workflow::Effect).
///
/// Opaque to the workflow and never journaled (workflow §4.1), so it carries
/// exactly what the launch needs and nothing that would have to be kept
/// consistent with the journal. The five variants are the loop's two external
/// operations (agent §4.2 steps 2 and 4), the delegation that executes in the
/// loop rather than the sandbox (§8.1), the cancel a cancelled run owes each
/// child it started (§9.2), and the outcome the fold can synthesize without
/// executing anything (§5.2).
pub enum Effect {
    /// The model call its key names (§4.2 step 2).
    Model { request: ModelRequest },
    /// The sandboxed tool call `call` names (§4.2 step 4, §3.3).
    Tool {
        call: CallId,
        tier: Tier,
        name: String,
        input: Value,
        timeout: Duration,
    },
    /// The delegation its key names, its `ChildRun` already journaled (§8.1).
    Delegate {
        turn: TurnId,
        prompt: String,
        child: ChildRef,
        root: SessionId,
    },
    /// A call the fold answers on its own: nothing is executed (§5.2).
    Fail { error: ToolError },
    /// The `Cancel` still owed to `child` (§9.2 item 2): fold state
    /// (`cancels_owed`) until the child's ack journals `CancelDelivered`. Which
    /// delegation owes it is the key's to say ([`StepKey::Cancel`]), so the effect
    /// carries only what the launch needs.
    Cancel { child: ChildRef },
}

/// The effects `state` calls for (workflow §4.1).
///
/// Pure and deterministic: it reads the fold, the kind, and the deployment
/// config (both identical on every node, and the kind digest-covered, agent
/// §6.2), and nothing else. It MUST NOT read the activation (no sandbox slot, no
/// held tiers, no launch claims), which is exactly the property that makes a
/// resumed activation agree with the one it replaced about what is outstanding
/// (W1).
///
/// - Every cancel the fold still owes (§9.2 item 2), whatever the run state: a
///   cancelled run's recorded children are a debt that outlives the run, and
///   the next run may start while it is still being paid.
/// - No live run ⇒ nothing more. A queued turn is not an outstanding effect:
///   the run has not started, and starting it is a record the tick decides
///   (§4.2), not work performed outside the entity.
/// - A live run with no pending calls ⇒ the **model call** for the run's current
///   step, unless the budget forbids one, in which case the set is empty and the
///   run has ended by arithmetic (§4.2 step 2). A completion request is
///   side-effect-free, so an orphaned one re-executes (workflow §5.3).
/// - A live run with pending calls ⇒ **every journaled intent lacking an
///   outcome** that can be launched as it stands, each under its declared
///   recovery policy (§5.4); one the fold answers on its own carries
///   `Reexecute`, since nothing ran (§5.2).
pub fn desired(
    kind: &Kind,
    config: &HarnessConfig,
    state: &SessionState,
) -> Vec<Pending<StepKey, Effect>> {
    // A cancel is idempotent on the child's side (§9.2 item 1), so a repeat
    // after a lost ack re-executes.
    let mut set: Vec<Pending<StepKey, Effect>> = state
        .cancels_owed
        .iter()
        .flat_map(|(turn, owed)| {
            owed.iter().map(|(call, child)| {
                Pending::reexecute(
                    StepKey::Cancel(turn.clone(), call.clone()),
                    Effect::Cancel {
                        child: child.clone(),
                    },
                )
            })
        })
        .collect();
    let Some(live) = state.live.as_ref() else {
        return set;
    };
    if live.pending.is_empty() {
        // The budget gate is fold state (`spend` and `budget` are both folded),
        // so it belongs here: a run past its budget calls for no further model
        // call, and the tick ends it instead of launching one.
        if live.spend.allows_call(&live.budget, config.budget_floor) {
            set.push(Pending::reexecute(
                StepKey::Model(live.turn.clone(), live.spend.own_steps),
                Effect::Model {
                    request: build_request(kind, state, live),
                },
            ));
        }
        return set;
    }
    // The lineage root a delegation carries (§8.1). `SessionCreated` is journaled
    // with the very first turn, so a live run always has one; a delegation
    // without it is unreachable rather than defaulted, and drops out of the set
    // the way any unlaunchable member does.
    let root = state.created.as_ref().map(|c| c.root.clone());
    set.extend(live.pending.iter().filter_map(|(call, pending)| {
        // Each member carries its recovery policy (workflow §4.1, agent §5.4):
        // a sandboxed tool follows its declaration; `delegate` re-executes by
        // construction (its re-submission dedups, §8.1); a synthesized outcome
        // executes nothing, so an orphaned one has no fate to be unsure of and
        // the successor re-synthesizes it (§5.2).
        let (effect, on_orphan) = if pending.name == DELEGATE {
            // Not yet launchable: the `ChildRun` intent this call needs is
            // still to be journaled (§8.1 step 1), so the set omits it.
            let child = pending.child.clone()?;
            let root = root.clone()?;
            match serde_json::from_value::<DelegateInput>(pending.input.clone()) {
                // Validated once when the intent was journaled, so a failure
                // here is a drifted schema; resolve rather than strand.
                Err(e) => (
                    Effect::Fail {
                        error: ToolError::InvalidArguments(e.to_string()),
                    },
                    OnOrphan::Reexecute,
                ),
                Ok(request) => (
                    Effect::Delegate {
                        turn: live.turn.clone(),
                        prompt: request.prompt,
                        child,
                        root,
                    },
                    OnOrphan::Reexecute,
                ),
            }
        } else {
            match kind.tools.get(&pending.name) {
                None => (
                    Effect::Fail {
                        error: ToolError::UnknownTool {
                            name: pending.name.clone(),
                        },
                    },
                    OnOrphan::Reexecute,
                ),
                Some(_) if !pending.input.is_object() => (
                    Effect::Fail {
                        error: ToolError::InvalidArguments(
                            "tool input must be a JSON object".into(),
                        ),
                    },
                    OnOrphan::Reexecute,
                ),
                Some(decl) => (
                    Effect::Tool {
                        call: call.clone(),
                        tier: decl.tier,
                        name: pending.name.clone(),
                        input: pending.input.clone(),
                        timeout: decl.timeout.unwrap_or(config.tool_timeout),
                    },
                    decl.on_orphan,
                ),
            }
        };
        Some(Pending {
            key: StepKey::Call(live.turn.clone(), call.clone()),
            effect,
            on_orphan,
        })
    }));
    set
}

/// The tier a desired call needs, or `None` for one that runs in the loop or
/// synthesizes its own outcome: what `reconcile` acquires before the launch
/// (§5.5).
pub(crate) fn tier_of(effect: &Effect) -> Option<Tier> {
    match effect {
        Effect::Tool { tier, .. } => Some(*tier),
        Effect::Model { .. }
        | Effect::Delegate { .. }
        | Effect::Cancel { .. }
        | Effect::Fail { .. } => None,
    }
}

/// The model request for the run's current step (§3.1), its `max_tokens` the
/// kind's ceiling clamped to the remaining budget (§9.1 item 2). Part of the pure
/// function: a request is a projection of the transcript and the kind, and
/// `perform` is given no state to build one from.
fn build_request(kind: &Kind, state: &SessionState, live: &LiveRun) -> ModelRequest {
    let max_tokens = kind
        .model
        .max_tokens
        .min(live.spend.remaining_tokens(&live.budget));
    let mut tools: Vec<ToolSpec> = kind.tools.specs();
    if !kind.delegates.is_empty() {
        tools.push(delegate_spec(kind));
    }
    ModelRequest {
        system_prompt: kind.system_prompt.clone(),
        params: ModelParams {
            max_tokens,
            ..kind.model.clone()
        },
        tools,
        transcript: Arc::clone(&state.transcript),
    }
}

/// The interface of the built-in `delegate` tool (§8.1), synthesized so the
/// model can call it like any declared tool.
fn delegate_spec(kind: &Kind) -> ToolSpec {
    let kinds: Vec<&str> = kind.delegates.iter().map(|k| k.as_str()).collect();
    ToolSpec {
        name: DELEGATE.to_string(),
        description: format!(
            "Delegate a task to a sub-agent. Allowed kinds: {}.",
            kinds.join(", ")
        ),
        input_schema: DelegateInput::input_schema(&kinds),
    }
}
