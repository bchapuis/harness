//! The autonomous entity: an entity that drives itself.
//!
//! An entity waits: it admits a command, decides records, commits them, and
//! replies (entity §4.2, §6). A **workflow** entity also drives itself: its
//! activation re-derives the work its committed state calls for
//! ([`Workflow::pending`]), launches what is not yet in flight
//! ([`Workflow::perform`]), folds each outcome back into records
//! ([`Workflow::settle`]), and stops when nothing is left. The work outstanding is a
//! pure function of committed state, never a journaled queue, so one desired set is
//! at once the dispatch list, the recovery set, and the straggler filter, and resume
//! is not a special case. This crate is that mechanism with no model, tool, or
//! prompt in it; the agent runtime is its production consumer.
//!
//! # Seams
//!
//! - [`Workflow`] is the plug point a consumer implements beside its [`Entity`].
//!   Its handler supertraits make the consumer write a [`Tick`] and a [`Settled`]
//!   handler; that they forward to the engine is the consumer's obligation.
//! - [`Engine`] is what the consumer holds and forwards to:
//!   [`Engine::on_activate`], [`Engine::quiescent`] (from `can_hibernate`),
//!   [`Engine::advance`] (the `Tick` handler, after any gate of the consumer's own
//!   that must run before a launch), [`Engine::settled`] (the `Settled` handler),
//!   and [`Engine::tick`] (every handler of the consumer's own whose records call
//!   for work).
//! - [`Perform`] is all an effect may reach: the entity's name and the system's
//!   `Clock`, `Entropy`, and `Spawner`.
//! - [`Binding`] and [`Subscribers`] are passive components a consumer keeps in its
//!   activation state, for an activation-scoped resource and for callers awaiting
//!   a durable outcome; the obligations they carry are the holder's.
//! - [`WorkflowEvent`] is the checker-facing vocabulary on the actor event stream,
//!   which the checkers in `testing` (behind the `testing` feature) read.
//!
//! Beneath, the crate consumes `entity`'s seams and adds none: [`Tick`] and
//! [`Settled`] are ordinary, local-only commands self-told through the entity's
//! mailbox, and the entity's output gate is the workflow's write-ahead barrier.
//! This crate defines **no journal, no fence, no placement function, and no resume
//! protocol**, and names no durable type of its own: every record a workflow entity
//! writes is the consumer's [`Entity::Event`], and the workflow's own state
//! (claims, backoff, subscribers) is ephemeral.
//!
//! # Mechanism
//!
//! `cycle` holds the engine: its per-activation scope (claims, backoff, the orphan
//! phase, staged events) behind one mutex, a tick's four steps in
//! [`Engine::advance`], and the launch path, which hands each effect's future to
//! the `Spawner` and self-tells its outcome back as a [`Settled`]. `backoff` is the
//! per-key capped exponential schedule, timed by `Clock` and jittered from
//! `Entropy`, whose cap puts a key at rest. `orphan` classifies the orphan set at
//! the activation's first sight of committed state and catches a panicking effect
//! at the task boundary. `settle` is the outcome command, `event` the events, and
//! `binding` and `subscribers` the two components. Every module is private except
//! `testing`, and the crate root re-exports each public item once.
//!
//! # Wiring a workflow entity
//!
//! A consumer implements [`Workflow`] beside its [`Entity`], holds an [`Engine`] in
//! its behavior, and forwards to it. There is deliberately no blanket
//! `EntityHandler` impl, so the two commands are two small handlers:
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
//! [`Engine::tick`] with the state it was given; the cycle's entry point after a
//! commit (§4.2 item 1), and, when it is the activation's first entry into the
//! engine, what classifies the orphan set (§5.3). The workflow re-enters after its own settles by itself,
//! and arms the backoff timer after one that committed nothing (§4.2 item 3). The
//! crate's conformance suites run this shape as a fan-out fixture under
//! `tests/support`.
//!
//! Specified by `docs/workflow-spec.md`.

mod backoff;
mod binding;
mod cycle;
mod event;
mod orphan;
mod settle;
mod subscribers;
#[cfg(feature = "testing")]
pub mod testing;

use std::future::Future;

use entity::Entity;
use entity::EntityCtx;
use entity::EntityName;
use serde::Deserialize;
use serde::Serialize;

pub use binding::Binding;
pub use binding::BindingState;
pub use cycle::Engine;
pub use cycle::Tick;
pub use event::WorkflowEvent;
pub use settle::Settled;
pub use subscribers::Subscribers;

/// One unit of work the committed state calls for, and how to recover it
/// (spec §4.1).
///
/// `key` is the effect's identity within its entity, `effect` the consumer's own
/// opaque description of the work, and `on_orphan` the policy the workflow
/// applies if a crash leaves the effect's fate unknown (§5.3). None of the three
/// is journaled by the workflow: a `Pending` is re-derived, in full, on every
/// activation that folds the same journal prefix.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending<K, E> {
    pub key: K,
    pub effect: E,
    /// The declared recovery policy. There is deliberately **no default**: an
    /// effect whose policy went unstated would be a silent decision about
    /// whether to repeat a side effect, and there is no safe guess (§5.3).
    pub on_orphan: OnOrphan,
}

impl<K, E> Pending<K, E> {
    /// A pending effect that is safe to re-run after an interruption (§5.3).
    pub fn reexecute(key: K, effect: E) -> Self {
        Pending {
            key,
            effect,
            on_orphan: OnOrphan::Reexecute,
        }
    }
}

/// Resume's policy for an **orphaned** effect: one whose intent committed and
/// whose outcome did not, so nothing says whether it ran (spec §5.3).
///
/// This is the workflow's type, and the home of the decision. The agent runtime's
/// `ToolDecl::on_orphan` (agent §6.1) **is** this type: a tool declaration's
/// recovery policy is exactly what its [`Pending`] carries. What the agent keeps
/// of its own is the *outcome* an interrupted call synthesizes: a `ToolError` the
/// model reads, where a workflow mints an [`EffectError`] for a consumer's fold.
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

/// Why an effect produced no outcome (spec §5.2). A failing effect does not fail
/// the entity: the workflow settles it as a value the consumer's fold records and
/// reacts to.
///
/// **Two variants are the workflow's to mint**: [`Interrupted`](EffectError::Interrupted)
/// for an orphan resolved on resume (§5.3) and [`Failed`](EffectError::Failed) for a
/// panic caught at the boundary of the task running the effect (§5.2). The other
/// two are what a consumer's [`Workflow::perform`] returns: the workflow neither
/// bounds an effect nor interprets its failure. A consumer wanting a per-effect
/// timeout takes it from the [`Clock`](actor_core::Clock) on [`Perform`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EffectError {
    /// A bound the consumer's `perform` set on itself elapsed. Its effects MAY
    /// still land; only the *outcome* is bounded.
    Timeout,
    /// An orphaned effect declaring [`OnOrphan::Interrupt`] was resolved on
    /// resume (§5.3). Nothing is known about whether it ran.
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

/// What an effect legitimately needs from outside itself (spec §3.1): the
/// entity's name and the system's seams (clock, entropy, task launching).
///
/// It carries **no** access to the entity's state, journal, or facets, which is
/// how "`perform` touches none of them" is enforced rather than asked for.
/// Everything else an effect needs (the consumer's own model, sandbox, or
/// network seams) the consumer clones out of `&self` before returning the
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

    /// The system's seams: `now`, `sleep`, `next_random`, `launch` (core §11.1).
    /// Time and randomness inside an effect MUST come from here, or a simulated
    /// run stops reproducing.
    pub fn system(&self) -> &S {
        &self.system
    }
}

/// An entity that drives itself (spec §3.1, §4, §5).
///
/// **Extends [`Entity`]**, which is what makes `Self::State`,
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
/// The two handler supertraits make the wiring's *existence* a compile-time
/// obligation: a workflow entity without a [`Tick`] and a [`Settled`] handler does
/// not compile, the posture entity takes for its own calls (E10). What the
/// compiler cannot check is that those handlers forward to [`Engine::advance`] and
/// [`Engine::settled`]; that is the consumer's obligation (§3.1). There is
/// deliberately no blanket impl supplying them: the orphan rule forbids one, and
/// entity's stance is that the hand-written form is normative.
pub trait Workflow:
    Entity + entity::EntityHandler<Tick> + entity::EntityHandler<Settled<Self>>
{
    /// An effect's identity within this entity (§4.1).
    ///
    /// Re-derived on every activation and **never journaled by the workflow**, so
    /// it needs no stability across deployments and no registry. The bounds are
    /// the ordinary command bounds, because a key rides the [`Settled`] command
    /// through the entity's mailbox (§5.1) and orders the claim and backoff
    /// tables (§4.3, §4.4).
    ///
    /// A key MUST be **unique over the entity's whole journal**, not merely
    /// within one desired set: the straggler filter's only test is presence in
    /// the set (§5.4), so a key that leaves and later re-enters would let an
    /// outcome launched under the first occurrence be settled as the second's.
    /// Only the consumer knows what tells two visits to one call site apart, so
    /// a consumer whose call identifiers repeat across runs qualifies them with
    /// the run, as the agent runtime's step keys qualify each ordinal with its
    /// turn (agent §4.5).
    type Key: actor_serialization::SerializationRequirement + Clone + Ord + std::fmt::Debug;

    /// One unit of work performed outside the entity: the consumer's own type,
    /// opaque to the workflow. Carried from `pending` to `perform` and never across
    /// a wire, so it needs no serialization.
    type Effect: Send + 'static;

    /// What an effect returns. Rides [`Settled`] back onto the command path, so
    /// it carries the command bounds; `Clone` because `EntityRef::tell` takes it
    /// (entity §3.2).
    type Outcome: actor_serialization::SerializationRequirement + Clone;

    /// The effects this entity's **committed** state calls for (§4.1).
    ///
    /// MUST be pure and deterministic: the same obligation [`Entity::apply`]
    /// carries (entity §4.1, E2), for the same reason: a resumed activation
    /// that disagreed with the one it replaced about what work is outstanding
    /// would either abandon work or duplicate it. It MUST NOT perform I/O, read
    /// the clock, or use entropy, and it MUST NOT read the activation's
    /// ephemeral state: the claims, the backoff table, and any binding, because
    /// a function that saw them would no longer be a function of the journal
    /// (W1). This one is the implementer's to uphold rather than the layer's to
    /// enforce: `pending` takes `&self`, so a behavior that reached into its own
    /// binding or asked [`Engine::quiescent`] would compile. [`Perform`] is the
    /// contrast the layer does enforce: it hands an effect no route back to the
    /// entity's state, journal, or facets.
    fn pending(&self, state: &Self::State) -> Vec<Pending<Self::Key, Self::Effect>>;

    /// Perform one effect (§3.1). The **only** place a workflow entity performs
    /// I/O.
    ///
    /// It runs outside the activation, in a task the workflow launched, holding no
    /// lock on the entity and no claim on its mailbox, which is why the returned
    /// future is `'static` and cannot borrow `&self`. An implementation clones
    /// the seams it needs out of `&self` first, then returns a future owning
    /// them.
    fn perform(
        &self,
        effect: Self::Effect,
        cx: Perform<Self::System>,
    ) -> impl Future<Output = Result<Self::Outcome, EffectError>> + Send + 'static;

    /// Fold one effect's outcome into records (§5.1).
    ///
    /// A **decision**, under exactly the rules an [`EntityHandler`] decision
    /// follows (entity §4.2): it MUST NOT mutate state, MUST NOT perform
    /// durable I/O, and MUST NOT await anything. It returns the records that
    /// make the outcome durable; the entity's output gate commits them; the
    /// commit re-enters the workflow.
    ///
    /// Returning no records is legal and commits nothing (entity §7), the way
    /// a consumer says "this outcome changed nothing". The key then stays in the
    /// desired set for the backoff timer to relaunch (§4.2 item 3).
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
