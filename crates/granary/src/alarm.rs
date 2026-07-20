//! The alarm facet (spec §16, "durable alarms"): a stored timer that
//! re-activates a grain to run its [`on_alarm`](crate::Grain::on_alarm) handler
//! with no caller present — the basis for retries, timeouts, batch flushes, and
//! the durable-step [`workflow`](crate::workflow) API.
//!
//! **Storage is a logical facet (§7.12).** A grain holds at most one pending
//! alarm — the Durable Objects model (DO §5). Its due time is durable in the
//! grain's own journal under the alarm tag, folded into a single `Option<Instant>`;
//! that folded form is the **source of truth** for whether and when the grain
//! fires. A handler arms or cancels it through [`GrainCtx::alarm`](crate::GrainCtx::alarm),
//! staged into the command's atomic batch exactly like a [`Kv`](crate::Kv) write.
//!
//! **Firing is the runtime half.** The host reads the folded due time
//! ([`FacetSet::alarm_due`](crate::FacetSet::alarm_due)) after each commit and on
//! activation, and arms an in-activation timer that delivers the callerless
//! `AlarmDue` command when the deadline passes (host.rs). Firing that survives the
//! grain's own hibernation and a node failover is layered above, through the
//! per-shard alarm index ([`AlarmIndex`](crate::AlarmIndex)).
//!
//! The due time is stored as raw nanoseconds since the clock epoch
//! ([`Instant::as_nanos`]) — [`actor_core::Instant`] is deliberately not
//! serializable (a virtual clock manufactures instants freely), so the facet form
//! carries the wire-friendly `u64` and converts at the accessor boundary.

use std::collections::BTreeSet;
use std::time::Duration;

use actor_core::Instant;
use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
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
use crate::system::GranarySystem;

/// The alarm facet marker (spec §16): declare `type Facets = (Alarm, …)` and
/// reach the timer through [`GrainCtx::alarm`](crate::GrainCtx::alarm), pairing it
/// with the [`on_alarm`](crate::Grain::on_alarm) handler the runtime invokes when
/// the alarm fires.
pub struct Alarm;

impl Sealed for Alarm {}

/// One alarm record (spec §16): the unit of durable change under the alarm tag.
/// Encoded with `postcard` — facet payloads are runtime-internal (§7.12).
#[derive(Serialize, Deserialize)]
enum AlarmOp {
    /// Arm (or re-arm) the alarm for this deadline, in nanoseconds since the
    /// clock epoch. A later `Set` supersedes an earlier one (last write wins).
    Set(u64),
    /// Cancel any pending alarm.
    Clear,
}

/// The committed form: the single pending deadline, or `None`. Nanoseconds since
/// the clock epoch, so the form stays serializable (see the module note).
#[derive(Clone, Default, Serialize, Deserialize)]
pub struct AlarmForm {
    due: Option<u64>,
}

impl AlarmForm {
    /// The pending deadline as an [`Instant`], or `None`.
    pub(crate) fn due(&self) -> Option<Instant> {
        self.due.map(Instant::from_nanos)
    }
}

/// The per-command stage (spec §7.12): the last arm/cancel staged this command,
/// giving the handler read-your-staged-writes. `None` means the command left the
/// alarm untouched; it drains to a single record when set.
#[derive(Default)]
pub struct AlarmStage {
    op: Option<AlarmOp>,
}

impl Facet for Alarm {
    const TAG: u8 = 4;

    type Form = AlarmForm;
    type Stage = AlarmStage;

    fn drain(stage: AlarmStage) -> Vec<Vec<u8>> {
        match stage.op {
            Some(op) => vec![encode_payload(&op)],
            None => Vec::new(),
        }
    }

    fn fold(form: &mut AlarmForm, payload: &[u8]) -> Result<(), FacetError> {
        match decode_payload("alarm record", payload)? {
            AlarmOp::Set(nanos) => form.due = Some(nanos),
            AlarmOp::Clear => form.due = None,
        }
        Ok(())
    }

    async fn snapshot(form: &AlarmForm, _env: &FacetEnv) -> Result<Vec<u8>, FacetError> {
        Ok(encode_payload(form))
    }

    async fn restore(part: Option<&[u8]>, _env: &FacetEnv) -> Result<AlarmForm, FacetError> {
        match part {
            Some(bytes) => decode_payload("alarm restore", bytes),
            None => Ok(AlarmForm::default()),
        }
    }

    fn roots(_form: &AlarmForm) -> BTreeSet<BlobId> {
        BTreeSet::new()
    }

    fn alarm_due(form: &AlarmForm) -> Option<u64> {
        form.due
    }

    fn stage_clear_alarm(stage: &mut AlarmStage) {
        stage.op = Some(AlarmOp::Clear);
    }
}

/// The handler-facing alarm accessor (spec §16), obtained from
/// [`GrainCtx::alarm`](crate::GrainCtx::alarm). Reads see committed-plus-staged;
/// arming or cancelling stages into the current command's atomic batch (§7.12).
pub struct AlarmHandle<'a, G: Grain, I>
where
    G::Facets: HasFacet<Alarm, I>,
{
    cell: &'a std::sync::Arc<FacetCell<G::Facets>>,
    now: Instant,
    _index: std::marker::PhantomData<I>,
}

impl<G: Grain> GrainCtx<G> {
    /// The grain's durable alarm (spec §16). Compiles exactly when the grain
    /// declares the [`Alarm`] facet (`type Facets = (Alarm, …)`) — the G10
    /// discipline applied to the timer. Arming is valid only inside a command or
    /// [`on_alarm`](crate::Grain::on_alarm) handler (§7.12); reads are valid
    /// anywhere the form is folded.
    pub fn alarm<I>(&self) -> AlarmHandle<'_, G, I>
    where
        G::Facets: HasFacet<Alarm, I>,
    {
        AlarmHandle {
            cell: self.facet_cell(),
            now: self.system().now(),
            _index: std::marker::PhantomData,
        }
    }
}

impl<G: Grain, I> AlarmHandle<'_, G, I>
where
    G::Facets: HasFacet<Alarm, I>,
{
    /// The pending deadline, resolving a staged arm/cancel over the committed
    /// form (read-your-staged-writes, §7.12).
    pub fn pending(&self) -> Option<Instant> {
        self.cell.with_form_and_stage::<Alarm, I, _>(|form, stage| match &stage.op {
            Some(AlarmOp::Set(nanos)) => Some(Instant::from_nanos(*nanos)),
            Some(AlarmOp::Clear) => None,
            None => form.due(),
        })
    }

    /// Arm the alarm to fire at `at`. A deadline already past fires on the next
    /// runtime tick. Supersedes any pending alarm (one alarm per grain, DO §5).
    pub fn set_at(&self, at: Instant) {
        self.cell
            .with_stage::<Alarm, I, _>(|stage| stage.op = Some(AlarmOp::Set(at.as_nanos())));
    }

    /// Arm the alarm to fire `after` from now (the activation's logical clock).
    pub fn set_after(&self, after: Duration) {
        self.set_at(self.now + after);
    }

    /// Cancel any pending alarm.
    pub fn clear(&self) {
        self.cell
            .with_stage::<Alarm, I, _>(|stage| stage.op = Some(AlarmOp::Clear));
    }
}
