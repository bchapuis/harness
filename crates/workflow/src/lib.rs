//! The autonomous entity (workflow spec): an entity that drives itself.
//!
//! An entity waits. It holds durable state, admits a command, decides records,
//! commits them, and replies (entity §4.2, §6). An **autonomous** entity does
//! not wait: its activation re-derives the work its committed state calls for,
//! launches what is not yet in flight, folds each outcome back, and stops when
//! there is nothing left. The agent runtime builds that mechanism by hand,
//! welded to one agentic policy (agent §3); this crate is the mechanism on
//! its own, with no model, tool, or prompt in it.
//!
//! The whole of it rests on one decision: **the work outstanding is a pure
//! function of committed state**, [`Workflow::pending`], never a journaled queue.
//! A set re-derived from the journal needs no reconciliation after a crash,
//! because recovery computes the same function over the same records — so
//! resume stops being a special case, and the desired set is at once the
//! dispatch list (§3.2), the recovery set (§4.4), and the straggler filter
//! (§4.5).
//!
//! Everything beneath is entity's and is not re-implemented here: this crate
//! defines **no journal, no fence, no placement function, and no resume
//! protocol**, and names no durable type of its own. Every record a workflow
//! entity writes is the consumer's [`Entity::Event`]; the workflow's own state —
//! claims, backoff, subscribers — is ephemeral by specification (§3.3, §3.4,
//! §7.1).
//!
//! # Wiring a workflow entity
//!
//! A consumer implements [`Workflow`] beside its [`Entity`], holds an [`Engine`] in
//! its behavior, and forwards four hooks. There is deliberately no blanket
//! `EntityHandler` impl (the orphan rule forbids one, and entity's stance is
//! that the hand-written form is normative), so the two commands are two small
//! handlers:
//!
//! ```ignore
//! impl Entity for Fanout {
//!     async fn on_activate(&mut self, ctx: &EntityCtx<Self>) -> Result<(), BoxError> {
//!         self.engine.on_activate(ctx);
//!         Ok(())
//!     }
//!     fn can_hibernate(&self, _state: &Self::State) -> bool { self.engine.quiescent() }
//!     fn register(r: &mut EntityRegistry<Self>) { /* the workflow's commands are local-only */ }
//! }
//!
//! impl EntityHandler<Tick> for Fanout {
//!     async fn handle(&self, state: &Self::State, _: Tick, ctx: &EntityCtx<Self>)
//!         -> (Vec<Self::Event>, ()) { (self.engine.advance(self, state, ctx), ()) }
//! }
//!
//! impl EntityHandler<Settled<Self>> for Fanout {
//!     async fn handle(&self, state: &Self::State, msg: Settled<Self>, ctx: &EntityCtx<Self>)
//!         -> (Vec<Self::Event>, ()) { (self.engine.settled(self, state, msg, ctx).await, ()) }
//! }
//! ```
//!
//! and every handler of the consumer's own whose records call for work calls
//! [`Engine::tick`] with the state it was given — the cycle's entry point after a
//! commit (§3.2 item 1), and, on the activation's first handler, what classifies
//! the orphan set (§4.4). The workflow re-enters after its own settles by itself,
//! and arms the backoff timer after one that committed nothing (§3.2 item 3).

pub mod backoff;
pub mod binding;
pub mod cycle;
pub mod event;
pub mod orphan;
pub mod settle;
pub mod subscribers;
#[cfg(feature = "testing")]
pub mod testing;

use std::future::Future;

use entity::Entity;
use entity::EntityCtx;
use entity::EntityName;
use serde::Deserialize;
use serde::Serialize;

pub use backoff::Backoff;
pub use backoff::BackoffPolicy;
pub use binding::Binding;
pub use binding::BindingState;
pub use cycle::Engine;
pub use cycle::Tick;
pub use event::WorkflowEvent;
pub use settle::Settled;
pub use subscribers::Subscribers;

/// One unit of work the committed state calls for, and how to recover it
/// (spec §3.1).
///
/// `key` is the effect's identity within its entity, `effect` the consumer's own
/// opaque description of the work, and `on_orphan` the policy the workflow
/// applies if a crash leaves the effect's fate unknown (§4.4). None of the three
/// is journaled by the workflow: a `Pending` is re-derived, in full, on every
/// activation that folds the same journal prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending<K, E> {
    pub key: K,
    pub effect: E,
    /// The declared recovery policy. There is deliberately **no default**: an
    /// effect whose policy went unstated would be a silent decision about
    /// whether to repeat a side effect, and there is no safe guess (§4.4).
    pub on_orphan: OnOrphan,
}

impl<K, E> Pending<K, E> {
    /// A pending effect that is safe to re-run after an interruption (§4.4).
    pub fn reexecute(key: K, effect: E) -> Self {
        Pending {
            key,
            effect,
            on_orphan: OnOrphan::Reexecute,
        }
    }

    /// A pending effect whose repetition the consumer will not risk blindly: a
    /// orphaned one resolves as [`EffectError::Interrupted`] and the consumer
    /// decides (§4.4).
    pub fn interrupt(key: K, effect: E) -> Self {
        Pending {
            key,
            effect,
            on_orphan: OnOrphan::Interrupt,
        }
    }
}

/// Resume's policy for a **orphaned** effect — one whose intent committed and
/// whose outcome did not, so nothing says whether it ran (spec §4.4).
///
/// This is the workflow's type, and the home of the decision. The agent runtime declares
/// the same two variants per tool (agent §5.2) and re-exports this rather than
/// keeping a second copy: a tool declaration's recovery policy is exactly what
/// its [`Pending`] carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum OnOrphan {
    /// Launch it again, as an ordinary launch. The consumer has declared the
    /// effect idempotent, self-deduplicating, or cheap enough that a second run
    /// is preferable to an unknown one.
    Reexecute,
    /// Settle it as [`EffectError::Interrupted`] without launching anything, and
    /// let the consumer decide what to do about a side effect of unknown fate.
    Interrupt,
}

/// Why an effect produced no outcome (spec §4.3). A failing effect does not fail
/// the entity: the workflow settles it as a value the consumer's fold records and
/// reacts to.
///
/// **Only [`Interrupted`](EffectError::Interrupted) is the workflow's to mint**
/// (§4.4). The other three are what a consumer's [`Workflow::perform`] returns:
/// the workflow neither bounds an effect nor interprets its failure. A consumer
/// wanting a per-effect timeout takes it from the [`Clock`](actor_core::Clock)
/// on [`Perform`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EffectError {
    /// A bound the consumer's `perform` set on itself elapsed. Its effects MAY
    /// still land; only the *outcome* is bounded.
    Timeout,
    /// An orphaned effect declaring [`OnOrphan::Interrupt`] was resolved on
    /// resume (§4.4). Nothing is known about whether it ran.
    Interrupted,
    /// The environment the effect needed is gone.
    Lost(String),
    /// The effect ran and failed.
    Failed(String),
}

impl std::fmt::Display for EffectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EffectError::Timeout => write!(f, "the effect's bound elapsed"),
            EffectError::Interrupted => write!(f, "interrupted: the effect's fate is unknown"),
            EffectError::Lost(why) => write!(f, "environment lost: {why}"),
            EffectError::Failed(why) => write!(f, "failed: {why}"),
        }
    }
}

impl std::error::Error for EffectError {}

/// What an effect legitimately needs from outside itself (spec §4.1): the
/// entity's name and the system's seams (clock, entropy, task launching).
///
/// It carries **no** access to the entity's state, journal, or facets, which is
/// how "`perform` touches none of them" is enforced rather than asked for.
/// Everything else an effect needs — the consumer's own model, sandbox, or
/// network seams — the consumer clones out of `&self` before returning the
/// future.
///
/// Owned rather than borrowed, because the future `perform` returns is launched
/// as a detached task and must therefore be `'static`.
#[derive(Clone)]
pub struct Perform<S> {
    name: EntityName,
    system: S,
}

impl<S> Perform<S> {
    pub(crate) fn new(name: EntityName, system: S) -> Self {
        Perform { name, system }
    }

    /// The entity this effect belongs to.
    pub fn name(&self) -> &EntityName {
        &self.name
    }

    /// The system's seams: `now`, `sleep`, `next_random`, `launch` (core §4.6).
    /// Time and randomness inside an effect MUST come from here, or a simulated
    /// run stops reproducing.
    pub fn system(&self) -> &S {
        &self.system
    }
}

/// An entity that drives itself (spec §2.1, §3, §4).
///
/// A **supertrait of [`Entity`]**, which is what makes `Self::State`,
/// `Self::Event`, and [`EntityCtx<Self>`] below mean what they mean: a workflow
/// entity is an ordinary entity that has additionally said what work its state
/// calls for.
///
/// Three methods, but only two new *things*: [`pending`](Workflow::pending) is the
/// function and [`perform`](Workflow::perform) the seam, while
/// [`settle`](Workflow::settle) is an ordinary entity decision under the ordinary
/// rules (entity §4.2), differing from a handler only in that the workflow holds
/// the command it answers.
///
/// The two handler supertraits are the wiring, made a compile-time obligation
/// rather than a documented one: a workflow entity that has not routed [`Tick`] to
/// [`Engine::advance`] and [`Settled`] to [`Engine::settled`] does not compile,
/// which is the posture entity takes for its own calls (E10). There is
/// deliberately no blanket impl supplying them — the orphan rule forbids one, and
/// entity's stance is that the hand-written form is normative.
pub trait Workflow:
    Entity + entity::EntityHandler<Tick> + entity::EntityHandler<Settled<Self>>
{
    /// An effect's identity within this entity (§3.1).
    ///
    /// Re-derived on every activation and **never journaled by the workflow**, so
    /// it needs no stability across deployments and no registry. The bounds are
    /// the ordinary command bounds, because a key rides the [`Settled`] command
    /// through the entity's mailbox (§4.2) and orders the claim and backoff
    /// tables (§3.3, §3.4).
    ///
    /// A key MUST be **unique over the entity's whole journal**, not merely
    /// within one desired set: the straggler filter's only test is presence in
    /// the set (§4.5), so a key that leaves and later re-enters would let an
    /// outcome launched under the first occurrence be settled as the second's.
    type Key: actor_serialization::SerializationRequirement + Clone + Ord + std::fmt::Debug;

    /// One unit of work performed outside the entity: the consumer's own type,
    /// opaque to the workflow. Carried from `pending` to `perform` and never across
    /// a wire, so it needs no serialization.
    type Effect: Send + 'static;

    /// What an effect returns. Rides [`Settled`] back onto the command path, so
    /// it carries the command bounds; `Clone` because `EntityRef::tell` takes it
    /// (entity §4.3).
    type Outcome: actor_serialization::SerializationRequirement + Clone;

    /// The effects this entity's **committed** state calls for (§3.1).
    ///
    /// MUST be pure and deterministic — the same obligation [`Entity::apply`]
    /// carries (entity §4.1, E2), for the same reason: a resumed activation
    /// that disagreed with the one it replaced about what work is outstanding
    /// would either abandon work or duplicate it. It MUST NOT perform I/O, read
    /// the clock, or use entropy, and it MUST NOT read the activation's
    /// ephemeral state — the claims, the backoff table, and any binding are
    /// invisible to it by construction, because a function that saw them would
    /// no longer be a function of the journal (W1).
    fn pending(&self, state: &Self::State) -> Vec<Pending<Self::Key, Self::Effect>>;

    /// Perform one effect (§4.1). The **only** place a workflow entity performs
    /// I/O.
    ///
    /// It runs outside the activation, in a task the workflow launched, holding no
    /// lock on the entity and no claim on its mailbox — which is why the returned
    /// future is `'static` and cannot borrow `&self`. An implementation clones
    /// the seams it needs out of `&self` first, then returns a future owning
    /// them.
    fn perform(
        &self,
        effect: Self::Effect,
        cx: Perform<Self::System>,
    ) -> impl Future<Output = Result<Self::Outcome, EffectError>> + Send + 'static;

    /// Fold one effect's outcome into records (§4.2).
    ///
    /// A **decision**, under exactly the rules an [`EntityHandler`] decision
    /// follows (entity §4.2): it MUST NOT mutate state, MUST NOT perform
    /// durable I/O, and MUST NOT await anything. It returns the records that
    /// make the outcome durable; the entity's output gate commits them; the
    /// commit re-enters the workflow.
    ///
    /// Returning no records is legal and commits nothing (entity §7.5) — the
    /// way a consumer says "this outcome changed nothing" — which then leaves
    /// the key in the desired set for the backoff timer to relaunch (§3.2 item
    /// 3), since this settle committed nothing to bring the next tick.
    ///
    /// [`EntityHandler`]: entity::EntityHandler
    fn settle(
        &self,
        state: &Self::State,
        key: &Self::Key,
        outcome: Result<Self::Outcome, EffectError>,
        ctx: &EntityCtx<Self>,
    ) -> impl Future<Output = Vec<Self::Event>> + Send;
}
