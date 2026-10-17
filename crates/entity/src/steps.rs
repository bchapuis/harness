//! The steps facet (spec facets §4.6): durable **step memoization** for the
//! Cloudflare-Workflows-style `step`/`sleep`/`retry` pattern, layered on the event
//! fold, the [`Alarm`](crate::alarm::Alarm) facet, and an entity's self-driving loop.
//!
//! **The durable core is memoization.** A steps consumer is a re-entrant decision
//! the entity re-drives after every commit. Each **step** runs an external effect;
//! its result is journaled under the steps tag and folded into a
//! `step id → bytes` map. On replay (a later drive, a re-activation, a failover)
//! a completed step resolves from the map instead of re-running. The memo is
//! write-once, not the effect: a re-activation may re-launch a step whose result
//! has not committed, and the first committed result wins. `sleep` is a step
//! whose effect is a durable [`Alarm`](crate::alarm::Alarm); `retry` is a step
//! that re-launches after an alarm-backed backoff.
//!
//! **What the entity still wires.** Launching a step's effect off the command path
//! and self-`tell`ing its result back is entity-specific (the effect's type is), so
//! the facet supplies only the durable half. Not implemented: a linear `async` DSL
//! over these (§13).

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::blobs::BlobId;
use crate::entity::Entity;
use crate::entity::EntityCtx;
use crate::error::EntityError;
use crate::facet::Facet;
use crate::facet::FacetCell;
use crate::facet::FacetEnv;
use crate::facet::FacetError;
use crate::facet::HasFacet;
use crate::facet::decode_payload;
use crate::facet::encode_payload;
use crate::facet::sealed::Sealed;

/// A step's identity: its stable call-site ordinal within the consumer's step
/// sequence (spec facets §4.6). Must be assigned deterministically across re-drives and
/// replay, so a completed step resolves to the same memoized result every time.
pub type StepId = u32;

/// The steps facet marker (spec facets §4.6): declare `type Facets = (Steps, Alarm, …)`
/// and reach the memoized steps through [`EntityCtx::steps`](crate::EntityCtx::steps).
pub struct Steps;

impl Sealed for Steps {}

/// One steps record (spec facets §4.6): a step's memoized result under the steps tag.
/// `bytes` is the caller's `postcard`-encoded step output: opaque to the facet.
#[derive(Serialize, Deserialize)]
struct StepRecord {
    id: StepId,
    bytes: Vec<u8>,
}

/// The committed form: each completed step's memoized result.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct StepsForm {
    steps: BTreeMap<StepId, Vec<u8>>,
}

/// The per-command stage (spec facets §3): the step results recorded this command,
/// draining to one record each. A step recorded twice in one command keeps the last
/// (a re-drive within a command should not happen, but last-write matches the fold).
#[derive(Default)]
pub struct StepsStage {
    staged: BTreeMap<StepId, Vec<u8>>,
}

impl Facet for Steps {
    const TAG: u8 = 5;

    type Form = StepsForm;
    type Stage = StepsStage;

    fn drain(stage: StepsStage) -> Vec<Vec<u8>> {
        stage
            .staged
            .into_iter()
            .map(|(id, bytes)| encode_payload(&StepRecord { id, bytes }))
            .collect()
    }

    fn fold(form: &mut StepsForm, payload: &[u8]) -> Result<(), FacetError> {
        let rec: StepRecord = decode_payload("steps record", payload)?;
        form.steps.insert(rec.id, rec.bytes);
        Ok(())
    }

    async fn snapshot(form: &StepsForm, _env: &FacetEnv) -> Result<Vec<u8>, FacetError> {
        Ok(encode_payload(form))
    }

    async fn restore(part: Option<&[u8]>, _env: &FacetEnv) -> Result<StepsForm, FacetError> {
        match part {
            Some(bytes) => decode_payload("steps restore", bytes),
            None => Ok(StepsForm::default()),
        }
    }

    fn roots(_form: &StepsForm) -> BTreeSet<BlobId> {
        BTreeSet::new()
    }
}

/// The handler-facing step-memo accessor (spec facets §4.6), obtained from
/// [`EntityCtx::steps`](crate::EntityCtx::steps).
pub struct StepsHandle<'a, G: Entity, I>
where
    G::Facets: HasFacet<Steps, I>,
{
    cell: &'a std::sync::Arc<FacetCell<G::Facets>>,
    _index: std::marker::PhantomData<I>,
}

impl<G: Entity> EntityCtx<G> {
    /// The entity's step memo (spec facets §4.6). Compiles exactly when the entity
    /// declares the [`Steps`] facet (`type Facets = (Steps, …)`). Recording is
    /// valid only inside a command handler (facets §3); reads are valid anywhere.
    pub fn steps<I>(&self) -> StepsHandle<'_, G, I>
    where
        G::Facets: HasFacet<Steps, I>,
    {
        StepsHandle {
            cell: self.facet_cell(),
            _index: std::marker::PhantomData,
        }
    }
}

impl<G: Entity, I> StepsHandle<'_, G, I>
where
    G::Facets: HasFacet<Steps, I>,
{
    /// The memoized result of step `id`, decoded, or `None` if the step has not yet
    /// completed. Resolves a result recorded earlier this command over the committed
    /// map (read-your-staged-writes, facets §3). A completed step returns `Some` on every
    /// re-drive and after any replay.
    pub fn result<T: DeserializeOwned>(&self, id: StepId) -> Result<Option<T>, EntityError> {
        let bytes = self.cell.with_overlay::<Steps, I, _>(|form, stage| {
            stage
                .and_then(|s| s.staged.get(&id))
                .or_else(|| form.steps.get(&id))
                .cloned()
        });
        match bytes {
            None => Ok(None),
            Some(bytes) => decode_payload("steps result", &bytes)
                .map(Some)
                .map_err(|e| {
                    EntityError::Call(actor_core::CallError::Serialization(e.to_string()))
                }),
        }
    }

    /// Whether step `id` has completed (committed or staged this command).
    pub fn is_done(&self, id: StepId) -> bool {
        self.cell.with_overlay::<Steps, I, _>(|form, stage| {
            stage.is_some_and(|s| s.staged.contains_key(&id)) || form.steps.contains_key(&id)
        })
    }

    /// Record step `id`'s result, staged into this command's atomic batch (facets §3).
    /// Called from the handler that receives a step effect's outcome (typically the
    /// [`StepDone`] handler via [`complete_step`]).
    pub fn record<T: Serialize>(&self, id: StepId, value: &T) {
        self.record_bytes(id, encode_payload(value));
    }

    /// Record a step's already-encoded result bytes (the generic path, when the
    /// value arrived as opaque `postcard` bytes over a [`StepDone`]).
    pub fn record_bytes(&self, id: StepId, bytes: Vec<u8>) {
        self.cell
            .with_stage::<Steps, I, _>(|stage| stage.staged.insert(id, bytes));
    }
}

/// The generic command a step's off-path effect self-`tell`s back with its encoded
/// result (spec facets §4.6). The entity accepts it (`r.accept::<StepDone>()`) and its
/// handler calls [`complete_step`] to journal the result, after which a re-drive
/// resolves the step from the memo. A local self-`tell`, like the alarm and idle
/// ticks.
#[derive(Clone, Serialize, Deserialize)]
pub struct StepDone {
    pub id: StepId,
    /// The step's `postcard`-encoded output (see [`StepsHandle::record`]).
    pub bytes: Vec<u8>,
}

impl StepDone {
    /// Build a `StepDone` from a step id and its result value, `postcard`-encoding
    /// the value the same way [`StepsHandle::result`] decodes it.
    pub fn new<T: Serialize>(id: StepId, value: &T) -> StepDone {
        StepDone {
            id,
            bytes: encode_payload(value),
        }
    }
}

impl Message for StepDone {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("entity.StepDone");
}

/// Journal a returned step result: the body of an entity's `EntityHandler<StepDone>`.
/// Stages the memo record so the step resolves on the next drive, unless it is
/// already recorded (a duplicate `StepDone` after a re-launch commits nothing, the
/// idempotency §6 gives for free). Returns the empty event list the handler
/// journals: the steps facet carries the durable change.
pub fn complete_step<G, I>(ctx: &EntityCtx<G>, msg: StepDone) -> Vec<G::Event>
where
    G: Entity,
    G::Facets: HasFacet<Steps, I>,
{
    if !ctx.steps::<I>().is_done(msg.id) {
        ctx.steps::<I>().record_bytes(msg.id, msg.bytes);
    }
    Vec::new()
}

/// An ephemeral, per-activation guard tracking which steps this activation has
/// already launched (spec facets §4.6), the launch-once half of the step pattern.
/// Never journaled: a re-activation rebuilds it and re-launches any step still
/// unresolved in the memo (the memo stays write-once because the *result*, not
/// the launch, is the durable fact).
///
/// `K` is the consumer's step key. The default [`StepId`] fits ordinal steps;
/// an entity whose steps carry richer identity substitutes its own `Ord` key.
pub struct LaunchGuard<K: Ord = StepId> {
    launched: BTreeSet<K>,
}

// Hand-written rather than `#[derive(Default)]`: deriving would impose a
// spurious `K: Default` bound, which a claim key never needs.
impl<K: Ord> Default for LaunchGuard<K> {
    fn default() -> Self {
        LaunchGuard {
            launched: BTreeSet::new(),
        }
    }
}

impl<K: Ord> LaunchGuard<K> {
    /// Claim the right to launch step `id` this activation. Returns `true` the first
    /// time (the caller then spawns the effect), `false` if already launched, so an
    /// in-flight step is not launched twice while its `StepDone` is outstanding.
    pub fn claim(&mut self, id: K) -> bool {
        self.launched.insert(id)
    }

    /// Whether step `id` is already claimed, without claiming it, for a drive that
    /// scans its incomplete steps and must skip the in-flight ones.
    pub fn is_claimed(&self, id: &K) -> bool {
        self.launched.contains(id)
    }

    /// Whether no claim is outstanding: nothing launched since the last
    /// [`reset`](LaunchGuard::reset). The deactivation veto reads this: an
    /// activation holding an unswept claim may still receive its effect's
    /// outcome and must not hibernate under it.
    pub fn is_idle(&self) -> bool {
        self.launched.is_empty()
    }

    /// Forget every claim: a fresh activation starts clean, and a failed step's
    /// retry path resets so the next drive re-launches it.
    pub fn reset(&mut self) {
        self.launched.clear();
    }
}
