//! The workflow facet (spec §16): durable **step memoization** for the
//! Cloudflare-Workflows-style `step`/`sleep`/`retry` pattern, layered on the event
//! fold, the [`Alarm`](crate::Alarm) facet, and a grain's self-driving loop.
//!
//! **The durable core is memoization.** A workflow is a re-entrant decision the
//! grain re-drives after every commit (the agentic harness's `advance` loop is the
//! reference, harness §3). Each **step** runs an external effect *once*; its result
//! is journaled under the workflow tag and folded into a `step id → bytes` map. On
//! replay — a later drive, a re-activation, a failover — a completed step resolves
//! from the map instead of re-running, so effects are at-most-once across crashes
//! (the property manual bookkeeping gives the agent today, made a facet). `sleep`
//! is a step whose effect is a durable [`Alarm`](crate::Alarm); `retry` is a step
//! that re-launches after an alarm-backed backoff.
//!
//! **What the grain still wires.** Launching a step's effect off the command path
//! and self-`tell`ing its result back is grain-specific (the effect's type is), so
//! the facet supplies the durable half — [`result`](WorkflowHandle::result) and
//! [`record`](WorkflowHandle::record) — and [`StepDone`] plus
//! [`complete_step`](complete_step) supply the generic command that journals a
//! returned result. The ephemeral "already launched this activation" guard is a
//! [`LaunchGuard`]. A linear `async` DSL over these is a later layer (§16).
//!
//! Step ids are the workflow's call-site ordinals: stable across re-drives and
//! replay, exactly as the agent tags a model call with `live.spend.own_steps`.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use actor_core::Manifest;
use actor_core::Message;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::blobs::BlobId;
use crate::error::GrainError;
use crate::facet::Facet;
use crate::facet::FacetCell;
use crate::facet::FacetEnv;
use crate::facet::FacetError;
use crate::facet::HasFacet;
use crate::facet::decode_payload;
use crate::facet::encode_payload;
use crate::facet::sealed::Sealed;
use crate::grain::Grain;
use crate::grain::GrainCtx;

/// A workflow step's identity: its stable call-site ordinal within the workflow
/// (spec §16). Must be assigned deterministically across re-drives and replay, so
/// a completed step resolves to the same memoized result every time.
pub type StepId = u32;

/// The workflow facet marker (spec §16): declare `type Facets = (Workflow, Alarm, …)`
/// and reach the memoized steps through [`GrainCtx::workflow`](crate::GrainCtx::workflow).
pub struct Workflow;

impl Sealed for Workflow {}

/// One workflow record (spec §16): a step's memoized result under the workflow tag.
/// `bytes` is the caller's `postcard`-encoded step output — opaque to the facet.
#[derive(Serialize, Deserialize)]
struct StepRecord {
    id: StepId,
    bytes: Vec<u8>,
}

/// The committed form: each completed step's memoized result.
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct WorkflowForm {
    steps: BTreeMap<StepId, Vec<u8>>,
}

/// The per-command stage (spec §7.12): the step results recorded this command,
/// draining to one record each. A step recorded twice in one command keeps the last
/// (a re-drive within a command should not happen, but last-write matches the fold).
#[derive(Default)]
pub struct WorkflowStage {
    staged: BTreeMap<StepId, Vec<u8>>,
}

impl Facet for Workflow {
    const TAG: u8 = 5;

    type Form = WorkflowForm;
    type Stage = WorkflowStage;

    fn drain(stage: WorkflowStage) -> Vec<Vec<u8>> {
        stage
            .staged
            .into_iter()
            .map(|(id, bytes)| encode_payload(&StepRecord { id, bytes }))
            .collect()
    }

    fn fold(form: &mut WorkflowForm, payload: &[u8]) -> Result<(), FacetError> {
        let rec: StepRecord = decode_payload("workflow record", payload)?;
        form.steps.insert(rec.id, rec.bytes);
        Ok(())
    }

    async fn snapshot(form: &WorkflowForm, _env: &FacetEnv) -> Result<Vec<u8>, FacetError> {
        Ok(encode_payload(form))
    }

    async fn restore(part: Option<&[u8]>, _env: &FacetEnv) -> Result<WorkflowForm, FacetError> {
        match part {
            Some(bytes) => decode_payload("workflow restore", bytes),
            None => Ok(WorkflowForm::default()),
        }
    }

    fn roots(_form: &WorkflowForm) -> BTreeSet<BlobId> {
        BTreeSet::new()
    }
}

/// The handler-facing workflow accessor (spec §16), obtained from
/// [`GrainCtx::workflow`](crate::GrainCtx::workflow). Reads a step's memoized result
/// (committed-plus-staged); recording a result stages it into the command's atomic
/// batch (§7.12).
pub struct WorkflowHandle<'a, G: Grain, I>
where
    G::Facets: HasFacet<Workflow, I>,
{
    cell: &'a std::sync::Arc<FacetCell<G::Facets>>,
    _index: std::marker::PhantomData<I>,
}

impl<G: Grain> GrainCtx<G> {
    /// The grain's workflow step memo (spec §16). Compiles exactly when the grain
    /// declares the [`Workflow`] facet (`type Facets = (Workflow, …)`). Recording is
    /// valid only inside a command handler (§7.12); reads are valid anywhere.
    pub fn workflow<I>(&self) -> WorkflowHandle<'_, G, I>
    where
        G::Facets: HasFacet<Workflow, I>,
    {
        WorkflowHandle {
            cell: self.facet_cell(),
            _index: std::marker::PhantomData,
        }
    }
}

impl<G: Grain, I> WorkflowHandle<'_, G, I>
where
    G::Facets: HasFacet<Workflow, I>,
{
    /// The memoized result of step `id`, decoded, or `None` if the step has not yet
    /// completed. Resolves a result recorded earlier this command over the committed
    /// map (read-your-staged-writes, §7.12). A completed step returns `Some` on every
    /// re-drive and after any replay — the memoization that makes an effect
    /// at-most-once.
    pub fn result<T: DeserializeOwned>(&self, id: StepId) -> Result<Option<T>, GrainError> {
        let bytes = self
            .cell
            .with_form_and_stage::<Workflow, I, _>(|form, stage| {
                stage
                    .staged
                    .get(&id)
                    .or_else(|| form.steps.get(&id))
                    .cloned()
            });
        match bytes {
            None => Ok(None),
            Some(bytes) => decode_payload("workflow result", &bytes)
                .map(Some)
                .map_err(|e| GrainError::Call(actor_core::CallError::Serialization(e.to_string()))),
        }
    }

    /// Whether step `id` has completed (committed or staged this command).
    pub fn is_done(&self, id: StepId) -> bool {
        self.cell
            .with_form_and_stage::<Workflow, I, _>(|form, stage| {
                stage.staged.contains_key(&id) || form.steps.contains_key(&id)
            })
    }

    /// Record step `id`'s result, staged into this command's atomic batch (§7.12).
    /// Called from the handler that receives a step effect's outcome (typically the
    /// [`StepDone`] handler via [`complete_step`]); the result is then memoized and
    /// every later drive resolves it through [`result`](WorkflowHandle::result).
    pub fn record<T: Serialize>(&self, id: StepId, value: &T) {
        let bytes = encode_payload(value);
        self.cell
            .with_stage::<Workflow, I, _>(|stage| stage.staged.insert(id, bytes));
    }

    /// Record a step's already-encoded result bytes (the generic path, when the
    /// value arrived as opaque `postcard` bytes over a [`StepDone`]).
    pub fn record_bytes(&self, id: StepId, bytes: Vec<u8>) {
        self.cell
            .with_stage::<Workflow, I, _>(|stage| stage.staged.insert(id, bytes));
    }
}

/// The generic command a step's off-path effect self-`tell`s back with its encoded
/// result (spec §16): the workflow analogue of the agent's `ModelDone`/`ToolDone`.
/// The grain accepts it (`r.accept::<StepDone>()`) and its handler calls
/// [`complete_step`] to journal the result, after which a re-drive resolves the step
/// from the memo. A local self-`tell`, like the alarm and idle ticks.
#[derive(Clone, Serialize, Deserialize)]
pub struct StepDone {
    /// The step this result completes.
    pub id: StepId,
    /// The step's `postcard`-encoded output (see [`WorkflowHandle::record`]).
    pub bytes: Vec<u8>,
}

impl StepDone {
    /// Build a `StepDone` from a step id and its result value, `postcard`-encoding
    /// the value the same way [`WorkflowHandle::result`] decodes it. Use this from a
    /// step's off-path effect so it need not encode by hand.
    pub fn new<T: Serialize>(id: StepId, value: &T) -> StepDone {
        StepDone {
            id,
            bytes: encode_payload(value),
        }
    }
}

impl Message for StepDone {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("granary.StepDone");
}

/// Journal a returned step result: the body of a grain's `GrainHandler<StepDone>`.
/// Stages the memo record so the step resolves on the next drive, unless it is
/// already recorded (a duplicate `StepDone` after a re-launch commits nothing, the
/// idempotency §7.5 gives for free). Returns the empty event list the handler
/// journals — the workflow facet carries the durable change.
pub fn complete_step<G, I>(ctx: &GrainCtx<G>, msg: StepDone) -> Vec<G::Event>
where
    G: Grain,
    G::Facets: HasFacet<Workflow, I>,
{
    if !ctx.workflow::<I>().is_done(msg.id) {
        ctx.workflow::<I>().record_bytes(msg.id, msg.bytes);
    }
    Vec::new()
}

/// An ephemeral, per-activation guard tracking which steps this activation has
/// already launched (spec §16): the launch-once half of the workflow pattern,
/// used directly by the harness agent's run loop (harness §3). Never journaled —
/// a re-activation rebuilds it and re-launches any step still unresolved in the
/// memo (at-most-once holds because the *result*, not the launch, is the durable
/// fact). Kept beside the grain's other ephemeral activation state.
///
/// `K` is the consumer's step key. The default [`StepId`] fits an ordinal
/// workflow; a grain whose steps carry richer identity substitutes its own
/// `Ord` key (the agent keys tool steps by the model's call id).
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
    /// time (the caller then spawns the effect), `false` if already launched — so an
    /// in-flight step is not launched twice while its `StepDone` is outstanding.
    pub fn claim(&mut self, id: K) -> bool {
        self.launched.insert(id)
    }

    /// Whether step `id` is already claimed, without claiming it: the read for
    /// a drive that scans its incomplete steps and must skip the in-flight ones
    /// but may not launch the rest this round (e.g. when it journals intents
    /// first and re-drives after the commit).
    pub fn is_claimed(&self, id: &K) -> bool {
        self.launched.contains(id)
    }

    /// Whether no claim is outstanding: nothing launched since the last
    /// [`reset`](LaunchGuard::reset). The passivation veto reads this — an
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
