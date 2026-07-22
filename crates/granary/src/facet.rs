//! The facet seam (spec §7.12): one substrate, many storage features.
//!
//! A **facet** is a durable storage feature of one grain defined entirely as an
//! interpretation over the two storage primitives — ordered, term-fenced records
//! (§7.2) and immutable content-addressed blobs (§7.10). A facet answers three
//! questions and nothing else: what its *records* mean, what it keeps in *blobs*,
//! and what it contributes to the *snapshot*. It gets no replication path, no
//! fence, and no recovery protocol of its own; those exist once, in the substrate.
//! Nothing at or below the [`GrainJournal`](crate::GrainJournal) seam changes —
//! the journal carries the same opaque bytes as ever.
//!
//! **Tagged records, one order, one barrier (G19).** Every record a grain
//! journals carries a one-byte facet tag; tag [`EVENT_TAG`] (0) is *facet 0*, the
//! grain's own event fold. All records a command produces — its events plus every
//! facet's staged operations — append as one atomic batch (§6), so a command that
//! touches state, the KV map, and the filesystem commits everywhere or nowhere.
//! Replay dispatches each record to its facet by tag; an unrecognized tag aborts
//! activation rather than being skipped, so a grain's history is never silently
//! misread by a runtime missing one of its facets.
//!
//! **Two facet classes (§7.12).** A *logical* facet folds: its records are
//! semantic operations applied by a pure, deterministic [`Facet::fold`] (F1), on
//! replay and after a live commit alike. A *physical* facet
//! (`PHYSICAL = true`) materializes: its live form mutates locally during the
//! command (inside [`Facet::begin`]/[`Facet::seal`]) and its records are captured
//! deltas, so the live path skips the fold and a non-committed outcome
//! [`Facet::discard`]s the materialization outright (G20) — the form is a
//! rebuildable cache, exactly as §1 demands.
//!
//! **Staging.** Handlers write through per-command stages surfaced by the
//! [`GrainCtx`](crate::GrainCtx) accessors; the host arms a fresh stage before the
//! handler runs and drains it into the command's tagged records afterwards. A
//! stage dropped on failure was never observable (§4.2: committed state changes
//! only at the commit point).
//!
//! The seam is **internal** (§7.12): [`Facet`], [`FacetSet`], and [`HasFacet`]
//! are sealed. A grain composes the built-in facets by declaring
//! `type Facets = (Kv, Ws);` — a tuple, giving each accessor a compile-time
//! containment proof (the G10 discipline applied to storage).

use std::collections::BTreeSet;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;

use crate::blobs::BlobId;
use crate::blobs::GrainBlobs;

/// The record tag of facet 0 — the grain's own event fold (spec §7.12).
pub(crate) const EVENT_TAG: u8 = 0;

/// A facet's interpretation of its durable input failed: a record that will not
/// decode, a snapshot contribution that will not restore, or — the load-bearing
/// case (G19) — a record tag no declared facet claims. The host aborts the
/// activation rather than misread the grain's history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FacetError(pub String);

impl std::fmt::Display for FacetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "facet error: {}", self.0)
    }
}

impl std::error::Error for FacetError {}

/// Prefix `payload` with its facet `tag` — the record envelope every journaled
/// record wears (spec §7.12).
pub(crate) fn tag_record(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payload.len() + 1);
    bytes.push(tag);
    bytes.extend_from_slice(payload);
    bytes
}

/// Split a journaled record into its facet tag and payload. An empty record is
/// corrupt (every record wears the envelope); the caller aborts activation.
pub(crate) fn split_record(bytes: &[u8]) -> Result<(u8, &[u8]), FacetError> {
    bytes
        .split_first()
        .map(|(tag, payload)| (*tag, payload))
        .ok_or_else(|| FacetError("empty record (missing facet tag)".into()))
}

/// Postcard-encode a facet payload — an op, a delta, a manifest, a form. Facet
/// payloads are runtime-internal plain owned data (§7.12), so encoding cannot
/// fail.
pub(crate) fn encode_payload<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("facet payload postcard encoding is infallible")
}

/// Postcard-decode a facet payload, labeling a failure with `what`
/// (e.g. `"kv record"`).
pub(crate) fn decode_payload<T: serde::de::DeserializeOwned>(
    what: &str,
    bytes: &[u8],
) -> Result<T, FacetError> {
    postcard::from_bytes(bytes).map_err(|e| FacetError(format!("{what}: {e}")))
}

/// The composite snapshot (spec §7.12): facet 0's codec-encoded `State` plus one
/// contribution per declared facet, all at one `Seq`. G4 applies to the composite
/// as a whole. Encoded with `postcard` — facet payloads and this envelope are
/// runtime-internal, deliberately independent of the deployment's user codec.
#[derive(Serialize, Deserialize)]
pub(crate) struct CompositeSnapshot {
    /// Facet 0's contribution: the grain's `State`, encoded with the system codec
    /// (it is a user type; the codec is the system's, §4.1).
    pub state: Vec<u8>,
    /// One `(tag, contribution)` per declared facet, in facet-set order.
    pub facets: Vec<(u8, Vec<u8>)>,
}

impl CompositeSnapshot {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, FacetError> {
        postcard::to_allocvec(self).map_err(|e| FacetError(format!("snapshot encode: {e}")))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<CompositeSnapshot, FacetError> {
        postcard::from_bytes(bytes).map_err(|e| FacetError(format!("snapshot decode: {e}")))
    }
}

/// What a facet's snapshot/restore work may reach (spec §7.12): the grain's
/// colocated blob area (bulk snapshot bytes, §7.10), the grain's name, and a
/// node-local scratch directory for **physical** materializations (§7.14 — the
/// SQL facet's database file lives under it, keyed by the grain). The directory
/// holds rebuildable caches only, never a source of truth (§1).
pub struct FacetEnv {
    blobs: GrainBlobs,
    dir: std::path::PathBuf,
}

impl FacetEnv {
    pub(crate) fn new(blobs: GrainBlobs, dir: std::path::PathBuf) -> FacetEnv {
        FacetEnv { blobs, dir }
    }

    /// The grain's colocated blob area (§7.10).
    pub(crate) fn blobs(&self) -> &GrainBlobs {
        &self.blobs
    }

    /// A stable node-local path for a physical facet's materialization of this
    /// grain, under the configured scratch directory: unique per grain (the
    /// name's content hash, so arbitrary keys need no path sanitizing) and per
    /// `suffix` (one materialization kind per facet).
    pub(crate) fn scratch_path(&self, suffix: &str) -> std::path::PathBuf {
        let hash = BlobId::of(self.blobs.grain().to_string().as_bytes());
        self.dir.join(format!("{hash}.{suffix}"))
    }
}

pub(crate) mod sealed {
    /// The facet seam is internal (spec §7.12): the built-in set proves it, and
    /// publishing it for out-of-crate facets is deferred until the tag registry
    /// and compatibility rules have settled (§16).
    pub trait Sealed {}
}

/// One durable storage feature of a grain (spec §7.12). **Sealed**: the built-in
/// facets — the KV map (§7.13), the workspace filesystem (§7.11), the SQL
/// database (§7.14), the disk image (§7.15) — are the only implementations;
/// third-party facets are a deferred policy decision (§16).
///
/// The obligations are the facet contract (§7.12): [`fold`](Facet::fold) is
/// deterministic (**F1**), [`restore`](Facet::restore)-then-replay equals the full
/// fold (**F2**), [`roots`](Facet::roots) covers every blob the restored form can
/// reference (**F3**), and a physical facet survives [`discard`](Facet::discard)
/// plus rehydration identically (**F4**).
pub trait Facet: sealed::Sealed + Send + Sync + 'static {
    /// The facet's stable record tag (nonzero; 0 is the event fold). Also the key
    /// of its composite-snapshot contribution. Never reused across facets.
    const TAG: u8;

    /// Whether this facet materializes (a physical facet, §7.12): its live form
    /// mutates during the command, so the live path skips [`fold`](Facet::fold)
    /// and a failed commit [`discard`](Facet::discard)s the form (G20).
    const PHYSICAL: bool = false;

    /// The committed, materialized form — a logical facet's folded value, a
    /// physical facet's handle to its materialization. `Default` is the empty
    /// form; `Clone` lets the host snapshot without holding its lock across an
    /// await (a physical form is a cheap `Arc`d handle); `Sync` because the
    /// snapshot future borrows the cloned forms across its blob puts.
    type Form: Default + Clone + Send + Sync + 'static;

    /// The per-command stage: a logical facet's overlay of staged operations, a
    /// physical facet's captured delta. Armed fresh for each command; dropped
    /// for free on any non-committed outcome.
    type Stage: Default + Send + 'static;

    /// Called before the handler runs: a physical facet opens its per-command
    /// transaction. Logical facets need nothing (a fresh `Stage` suffices).
    fn begin(_form: &mut Self::Form, _stage: &mut Self::Stage) -> Result<(), FacetError> {
        Ok(())
    }

    /// Called after the handler returns, before the append: a physical facet
    /// commits its local transaction and captures the delta into the stage
    /// (§7.14). Logical facets need nothing.
    fn seal(_form: &mut Self::Form, _stage: &mut Self::Stage) -> Result<(), FacetError> {
        Ok(())
    }

    /// Drain the stage into record payloads, in order. An empty result means the
    /// facet contributes nothing to this command's batch (a read, §7.5).
    fn drain(stage: Self::Stage) -> Vec<Vec<u8>>;

    /// Interpret one committed record into the form. MUST be deterministic
    /// (**F1**): it runs on replay and — for a logical facet — after a live
    /// commit, and the two MUST agree.
    fn fold(form: &mut Self::Form, payload: &[u8]) -> Result<(), FacetError>;

    /// Resolve blob-referencing replayed records into the materialization
    /// (spec §7.15). Called once by the host after restore + replay, before the
    /// first command: a facet whose records carry blob ids rather than bytes —
    /// the disk facet's capture manifests — fetches and applies them here, so
    /// [`fold`](Facet::fold) stays synchronous and pure (**F1** holds on the
    /// recorded bytes) while the blob fetches ride the async [`FacetEnv`] path.
    /// Every other facet keeps the no-op default. Mutation goes through the
    /// form's own interior mutability (a physical form is an `Arc`d handle).
    fn rehydrate(
        _form: &Self::Form,
        _env: &FacetEnv,
    ) -> impl Future<Output = Result<(), FacetError>> + Send {
        async { Ok(()) }
    }

    /// Drop the materialization on a non-committed outcome (G20). Logical facets
    /// need nothing (their stage was never folded); a physical facet deletes its
    /// local materialization, which the next activation rebuilds.
    fn discard(_form: &mut Self::Form) {}

    /// The facet's composite-snapshot contribution at the committed head (§7.12).
    /// Bulk bytes go to `blobs`; the returned payload holds manifests of ids,
    /// keeping the snapshot record small (§7.14).
    fn snapshot(
        form: &Self::Form,
        env: &FacetEnv,
    ) -> impl Future<Output = Result<Vec<u8>, FacetError>> + Send;

    /// Rebuild the form from a snapshot contribution (**F2**), or the empty form
    /// when the composite carries none (a grain that declared the facet after its
    /// last snapshot).
    fn restore(
        part: Option<&[u8]>,
        env: &FacetEnv,
    ) -> impl Future<Output = Result<Self::Form, FacetError>> + Send;

    /// Every blob the form references (**F3**) — the facet's contribution to the
    /// grain's unioned live set (§7.12). Only the host sweeps; a facet never
    /// issues `retain_blobs` itself, so one facet's GC can never drop another's.
    fn roots(form: &Self::Form) -> BTreeSet<BlobId>;

    /// The runtime hook for durable alarms (spec §16): the pending deadline this
    /// facet holds, in nanoseconds since the clock epoch, or `None`. Only the
    /// [`Alarm`](crate::Alarm) facet returns a deadline; every other facet keeps
    /// the default. The host reads it through [`FacetSet::alarm_due`] to arm the
    /// callerless timer without a compile-time [`HasFacet`] bound.
    ///
    /// This pair — `alarm_due`/[`stage_clear_alarm`](Facet::stage_clear_alarm),
    /// echoed on [`FacetSet`] and `FacetCell` — is a deliberate one-client
    /// exception: the Alarm facet's runtime hook, and nothing else's. It MUST
    /// NOT be extended per-facet; a second facet needing a runtime hook must
    /// instead generalize the pair into a facet-agnostic deadline hook.
    fn alarm_due(_form: &Self::Form) -> Option<u64> {
        None
    }

    /// Stage the consumption of a fired alarm (spec §16): before invoking
    /// `on_alarm`, the host stages a cancel so a fired alarm clears atomically
    /// unless the handler re-arms it (last write wins in the shared stage, the DO
    /// consume-on-fire semantic). Only the [`Alarm`](crate::Alarm) facet acts;
    /// every other facet keeps the no-op default.
    fn stage_clear_alarm(_stage: &mut Self::Stage) {}
}

/// A grain's declared facet set (spec §7.12): the unit tuple `()` (no facets) or
/// a tuple of distinct [`Facet`]s, e.g. `(Kv, Ws)`. **Sealed**: implemented
/// for tuples up to arity 4, in-crate only.
///
/// The set statically fixes the grain type's record-tag registry — which is what
/// makes the unknown-tag rule (G19) checkable — and generates the composed
/// forms/stages the host holds.
pub trait FacetSet: sealed::Sealed + Send + Sync + 'static {
    /// The composed committed forms, one per facet.
    type Forms: Default + Clone + Send + Sync + 'static;
    /// The composed per-command stages, one per facet.
    type Stages: Default + Send + 'static;

    /// The declared record tags, in tuple order. Distinctness is asserted at
    /// host construction.
    const TAGS: &'static [u8];

    /// Arm each facet's per-command work ([`Facet::begin`]).
    fn begin(forms: &mut Self::Forms, stages: &mut Self::Stages) -> Result<(), FacetError>;

    /// Close each facet's per-command work ([`Facet::seal`]).
    fn seal(forms: &mut Self::Forms, stages: &mut Self::Stages) -> Result<(), FacetError>;

    /// Drain every stage into `(tag, payload)` records, in facet-set order.
    fn drain(stages: Self::Stages) -> Vec<(u8, Vec<u8>)>;

    /// Fold one committed record, dispatched by tag. On **replay**
    /// (`live = false`) every facet folds, physical included (rebuilding its
    /// materialization from deltas). On the **live** path a physical facet
    /// skips — its form already mutated locally (§7.14); folding again would
    /// double-apply. Logical facets fold identically on both paths (F1). An
    /// unclaimed tag is the G19 abort.
    fn fold(forms: &mut Self::Forms, tag: u8, payload: &[u8], live: bool)
    -> Result<(), FacetError>;

    /// Resolve each facet's replayed blob-referencing records
    /// ([`Facet::rehydrate`]), in facet-set order.
    fn rehydrate(
        forms: &Self::Forms,
        env: &FacetEnv,
    ) -> impl Future<Output = Result<(), FacetError>> + Send;

    /// Discard every physical materialization (G20).
    fn discard(forms: &mut Self::Forms);

    /// Every facet's composite-snapshot contribution, in facet-set order.
    fn snapshot(
        forms: &Self::Forms,
        env: &FacetEnv,
    ) -> impl Future<Output = Result<Vec<(u8, Vec<u8>)>, FacetError>> + Send;

    /// Rebuild every form from the composite's contributions (absent parts
    /// restore to the empty form).
    fn restore(
        parts: &[(u8, Vec<u8>)],
        env: &FacetEnv,
    ) -> impl Future<Output = Result<Self::Forms, FacetError>> + Send;

    /// The union of every facet's blob roots (§7.12).
    fn roots(forms: &Self::Forms) -> BTreeSet<BlobId>;

    /// The pending alarm deadline of whichever facet holds one (spec §16), or
    /// `None`. At most one facet in a set is the [`Alarm`](crate::Alarm) facet, so
    /// the first non-`None` is the grain's single alarm.
    fn alarm_due(forms: &Self::Forms) -> Option<u64>;

    /// Stage the consumption of a fired alarm across the set (spec §16). Only the
    /// [`Alarm`](crate::Alarm) facet's stage is touched.
    fn stage_clear_alarm(stages: &mut Self::Stages);
}

impl sealed::Sealed for () {}

/// The empty facet set: the grain is facet 0 alone. Every nonzero tag is
/// unrecognized (G19).
impl FacetSet for () {
    type Forms = ();
    type Stages = ();

    const TAGS: &'static [u8] = &[];

    fn begin(_forms: &mut (), _stages: &mut ()) -> Result<(), FacetError> {
        Ok(())
    }

    fn seal(_forms: &mut (), _stages: &mut ()) -> Result<(), FacetError> {
        Ok(())
    }

    fn drain(_stages: ()) -> Vec<(u8, Vec<u8>)> {
        Vec::new()
    }

    fn fold(_forms: &mut (), tag: u8, _payload: &[u8], _live: bool) -> Result<(), FacetError> {
        Err(FacetError(format!("unrecognized facet tag {tag}")))
    }

    async fn rehydrate(_forms: &(), _env: &FacetEnv) -> Result<(), FacetError> {
        Ok(())
    }

    fn discard(_forms: &mut ()) {}

    async fn snapshot(_forms: &(), _env: &FacetEnv) -> Result<Vec<(u8, Vec<u8>)>, FacetError> {
        Ok(Vec::new())
    }

    async fn restore(_parts: &[(u8, Vec<u8>)], _env: &FacetEnv) -> Result<(), FacetError> {
        Ok(())
    }

    fn roots(_forms: &()) -> BTreeSet<BlobId> {
        BTreeSet::new()
    }

    fn alarm_due(_forms: &()) -> Option<u64> {
        None
    }

    fn stage_clear_alarm(_stages: &mut ()) {}
}

/// Implement [`FacetSet`] for a facet tuple. Hand-listed per arity because tuple
/// field access (`.0`, `.1`, …) needs the index as a literal token.
macro_rules! facet_set_tuple {
    ($(($T:ident, $i:tt)),+) => {
        impl<$($T: Facet),+> sealed::Sealed for ($($T,)+) {}

        impl<$($T: Facet),+> FacetSet for ($($T,)+) {
            type Forms = ($($T::Form,)+);
            type Stages = ($($T::Stage,)+);

            const TAGS: &'static [u8] = &[$($T::TAG),+];

            fn begin(forms: &mut Self::Forms, stages: &mut Self::Stages) -> Result<(), FacetError> {
                $($T::begin(&mut forms.$i, &mut stages.$i)?;)+
                Ok(())
            }

            fn seal(forms: &mut Self::Forms, stages: &mut Self::Stages) -> Result<(), FacetError> {
                $($T::seal(&mut forms.$i, &mut stages.$i)?;)+
                Ok(())
            }

            fn drain(stages: Self::Stages) -> Vec<(u8, Vec<u8>)> {
                let mut out = Vec::new();
                $(for payload in $T::drain(stages.$i) {
                    out.push(($T::TAG, payload));
                })+
                out
            }

            fn fold(
                forms: &mut Self::Forms,
                tag: u8,
                payload: &[u8],
                live: bool,
            ) -> Result<(), FacetError> {
                $(if tag == $T::TAG {
                    // On the live path a physical facet's form already mutated at
                    // local commit (§7.14); folding the captured delta again
                    // would double-apply.
                    if live && $T::PHYSICAL {
                        return Ok(());
                    }
                    return $T::fold(&mut forms.$i, payload);
                })+
                Err(FacetError(format!("unrecognized facet tag {tag}")))
            }

            fn rehydrate(
                forms: &Self::Forms,
                env: &FacetEnv,
            ) -> impl Future<Output = Result<(), FacetError>> + Send {
                async move {
                    $($T::rehydrate(&forms.$i, env).await?;)+
                    Ok(())
                }
            }

            fn discard(forms: &mut Self::Forms) {
                $($T::discard(&mut forms.$i);)+
            }

            fn snapshot(
                forms: &Self::Forms,
                env: &FacetEnv,
            ) -> impl Future<Output = Result<Vec<(u8, Vec<u8>)>, FacetError>> + Send {
                async move {
                    let mut parts = Vec::new();
                    $(parts.push(($T::TAG, $T::snapshot(&forms.$i, env).await?));)+
                    Ok(parts)
                }
            }

            fn restore(
                parts: &[(u8, Vec<u8>)],
                env: &FacetEnv,
            ) -> impl Future<Output = Result<Self::Forms, FacetError>> + Send {
                async move {
                    Ok(($(
                        {
                            let part = parts
                                .iter()
                                .find(|(tag, _)| *tag == $T::TAG)
                                .map(|(_, bytes)| bytes.as_slice());
                            $T::restore(part, env).await?
                        },
                    )+))
                }
            }

            fn roots(forms: &Self::Forms) -> BTreeSet<BlobId> {
                let mut roots = BTreeSet::new();
                $(roots.extend($T::roots(&forms.$i));)+
                roots
            }

            fn alarm_due(forms: &Self::Forms) -> Option<u64> {
                // At most one facet is the Alarm facet, so the first non-`None`
                // is the grain's single alarm (spec §16).
                $(if let Some(due) = $T::alarm_due(&forms.$i) {
                    return Some(due);
                })+
                None
            }

            fn stage_clear_alarm(stages: &mut Self::Stages) {
                $($T::stage_clear_alarm(&mut stages.$i);)+
            }
        }
    };
}

facet_set_tuple!((A, 0));
facet_set_tuple!((A, 0), (B, 1));
facet_set_tuple!((A, 0), (B, 1), (C, 2));
facet_set_tuple!((A, 0), (B, 1), (C, 2), (D, 3));

/// Type-level index of the first tuple position (see [`HasFacet`]).
pub struct Here(());

/// Type-level index one position deeper than `I` (see [`HasFacet`]).
pub struct There<I>(PhantomData<I>);

/// A compile-time containment proof: the facet set holds `F` at position `I`
/// (spec §7.12). The index parameter exists only so the per-position impls do
/// not overlap; call sites leave it to inference — `ctx.kv()` compiles exactly
/// when the grain's set contains [`Kv`](crate::Kv) once (the G10 discipline).
pub trait HasFacet<F: Facet, I>: FacetSet {
    /// Project `F`'s committed form out of the composed forms.
    fn form(forms: &Self::Forms) -> &F::Form;
    /// Project `F`'s per-command stage out of the composed stages.
    fn stage_mut(stages: &mut Self::Stages) -> &mut F::Stage;
}

macro_rules! has_facet {
    // ($T target, $idx tuple index, $I index type) over tuple ($($All),+)
    (($($All:ident),+), $T:ident, $i:tt, $I:ty) => {
        impl<$($All: Facet),+> HasFacet<$T, $I> for ($($All,)+) {
            fn form(forms: &Self::Forms) -> &$T::Form {
                &forms.$i
            }
            fn stage_mut(stages: &mut Self::Stages) -> &mut $T::Stage {
                &mut stages.$i
            }
        }
    };
}

has_facet!((A), A, 0, Here);
has_facet!((A, B), A, 0, Here);
has_facet!((A, B), B, 1, There<Here>);
has_facet!((A, B, C), A, 0, Here);
has_facet!((A, B, C), B, 1, There<Here>);
has_facet!((A, B, C), C, 2, There<There<Here>>);
has_facet!((A, B, C, D), A, 0, Here);
has_facet!((A, B, C, D), B, 1, There<Here>);
has_facet!((A, B, C, D), C, 2, There<There<Here>>);
has_facet!((A, B, C, D), D, 3, There<There<There<Here>>>);

/// The host-owned facet cell: the committed forms and the per-command stages,
/// shared with [`GrainCtx`](crate::GrainCtx) accessors through an `Arc`.
///
/// The locks are uncontended in practice — the host actor is a serial executor —
/// and are **never held across an await**: async work (snapshot's blob puts)
/// operates on a [`forms`](FacetCell::forms) clone. `stages` is `Some` only while
/// a command is being decided; a facet write outside a command has no stage and
/// panics, which is the honest surface for "facet staging is command-scoped"
/// (§4.2).
pub(crate) struct FacetCell<FS: FacetSet> {
    forms: Mutex<FS::Forms>,
    stages: Mutex<Option<FS::Stages>>,
}

impl<FS: FacetSet> FacetCell<FS> {
    /// A fresh cell with empty forms and no armed stage. Asserts the declared
    /// tags are distinct and nonzero (a duplicated tag would make record
    /// dispatch ambiguous; tag 0 is facet 0's).
    pub(crate) fn new() -> FacetCell<FS> {
        let mut seen = BTreeSet::new();
        for &tag in FS::TAGS {
            assert!(
                tag != EVENT_TAG,
                "facet tag 0 is reserved for the event fold"
            );
            assert!(seen.insert(tag), "duplicate facet tag {tag} in facet set");
        }
        FacetCell {
            forms: Mutex::new(FS::Forms::default()),
            stages: Mutex::new(None),
        }
    }

    /// Arm a fresh stage for a command and run each facet's `begin` hook.
    pub(crate) fn begin(&self) -> Result<(), FacetError> {
        let mut forms = self.forms.lock().expect("facet forms lock");
        let mut stages = self.stages.lock().expect("facet stages lock");
        let mut fresh = FS::Stages::default();
        FS::begin(&mut forms, &mut fresh)?;
        *stages = Some(fresh);
        Ok(())
    }

    /// Close the command's stage (physical facets commit-and-capture, §7.14) and
    /// drain it into `(tag, payload)` records. Always disarms the stage, so a
    /// facet write outside a command can never leak into a later batch.
    pub(crate) fn seal_and_drain(&self) -> Result<Vec<(u8, Vec<u8>)>, FacetError> {
        let mut forms = self.forms.lock().expect("facet forms lock");
        let mut stages = self.stages.lock().expect("facet stages lock");
        let Some(mut stage) = stages.take() else {
            return Ok(Vec::new());
        };
        FS::seal(&mut forms, &mut stage)?;
        Ok(FS::drain(stage))
    }

    /// Disarm the stage without draining (the command failed before the append).
    pub(crate) fn abandon(&self) {
        *self.stages.lock().expect("facet stages lock") = None;
    }

    /// Fold one committed record on the live path (§7.12).
    pub(crate) fn fold_live(&self, tag: u8, payload: &[u8]) -> Result<(), FacetError> {
        let mut forms = self.forms.lock().expect("facet forms lock");
        FS::fold(&mut forms, tag, payload, true)
    }

    /// Fold one record on replay (§9).
    pub(crate) fn fold_replay(&self, tag: u8, payload: &[u8]) -> Result<(), FacetError> {
        let mut forms = self.forms.lock().expect("facet forms lock");
        FS::fold(&mut forms, tag, payload, false)
    }

    /// Replace the forms wholesale (snapshot restore, §9).
    pub(crate) fn install(&self, forms: FS::Forms) {
        *self.forms.lock().expect("facet forms lock") = forms;
    }

    /// Resolve blob-referencing replayed records after restore + replay
    /// ([`Facet::rehydrate`], spec §7.15). Runs against a forms clone — cheap
    /// `Arc`d handles sharing the same materializations — so no lock spans the
    /// blob fetches.
    pub(crate) async fn rehydrate(&self, env: &FacetEnv) -> Result<(), FacetError> {
        let forms = self.forms();
        FS::rehydrate(&forms, env).await
    }

    /// A clone of the committed forms, for lock-free async work (snapshot).
    pub(crate) fn forms(&self) -> FS::Forms {
        self.forms.lock().expect("facet forms lock").clone()
    }

    /// Discard every physical materialization (G20, §7.14).
    pub(crate) fn discard(&self) {
        let mut forms = self.forms.lock().expect("facet forms lock");
        FS::discard(&mut forms);
    }

    /// The union of every facet's blob roots (§7.12) — what the host adds to any
    /// [`GrainBlobs::gc`](crate::GrainBlobs::gc) sweep.
    pub(crate) fn roots(&self) -> BTreeSet<BlobId> {
        FS::roots(&self.forms.lock().expect("facet forms lock"))
    }

    /// The grain's pending alarm deadline (spec §16), in nanoseconds since the
    /// clock epoch, or `None`. Read by the host after each commit and on
    /// activation to arm the callerless timer.
    pub(crate) fn alarm_due(&self) -> Option<u64> {
        FS::alarm_due(&self.forms.lock().expect("facet forms lock"))
    }

    /// Stage the consumption of a fired alarm into the armed command (spec §16).
    /// Called by the host before `on_alarm`, so the deadline clears atomically
    /// unless the handler re-arms it. Panics outside a command, exactly as the
    /// other stage writers — the host only calls it inside the alarm protocol.
    pub(crate) fn clear_alarm_stage(&self) {
        let mut stages = self.stages.lock().expect("facet stages lock");
        let stage = stages
            .as_mut()
            .expect("clear_alarm_stage is only valid inside the alarm protocol");
        FS::stage_clear_alarm(stage);
    }

    /// Run `read` against `F`'s committed form (a facet accessor's read path).
    pub(crate) fn with_form<F, I, R>(&self, read: impl FnOnce(&F::Form) -> R) -> R
    where
        F: Facet,
        FS: HasFacet<F, I>,
    {
        let forms = self.forms.lock().expect("facet forms lock");
        read(<FS as HasFacet<F, I>>::form(&forms))
    }

    /// Run `write` against `F`'s armed stage (a facet accessor's write path).
    /// Panics outside a command handler: staging is command-scoped (§4.2), and a
    /// write from `on_activate`/`on_passivate` would otherwise vanish silently.
    pub(crate) fn with_stage<F, I, R>(&self, write: impl FnOnce(&mut F::Stage) -> R) -> R
    where
        F: Facet,
        FS: HasFacet<F, I>,
    {
        let mut stages = self.stages.lock().expect("facet stages lock");
        let stage = stages
            .as_mut()
            .expect("facet writes are only valid inside a command handler (spec §7.12)");
        write(<FS as HasFacet<F, I>>::stage_mut(stage))
    }

    /// Run `write` against `F`'s committed form and its armed stage together (a
    /// facet whose stage derives from the form, e.g. a scratch overlay cloned on
    /// first write). Panics outside a command handler, exactly as
    /// [`with_stage`](FacetCell::with_stage) — staging is command-scoped (§4.2).
    pub(crate) fn with_form_and_stage<F, I, R>(
        &self,
        write: impl FnOnce(&F::Form, &mut F::Stage) -> R,
    ) -> R
    where
        F: Facet,
        FS: HasFacet<F, I>,
    {
        let forms = self.forms.lock().expect("facet forms lock");
        let mut stages = self.stages.lock().expect("facet stages lock");
        let stage = stages
            .as_mut()
            .expect("facet writes are only valid inside a command handler (spec §7.12)");
        write(
            <FS as HasFacet<F, I>>::form(&forms),
            <FS as HasFacet<F, I>>::stage_mut(stage),
        )
    }

    /// Run `read` against `F`'s committed form AND its armed stage, if any — the
    /// read-your-staged-writes overlay (§7.12). The stage is `None` outside a
    /// command, in which case only the committed form is consulted.
    pub(crate) fn with_overlay<F, I, R>(
        &self,
        read: impl FnOnce(&F::Form, Option<&mut F::Stage>) -> R,
    ) -> R
    where
        F: Facet,
        FS: HasFacet<F, I>,
    {
        let forms = self.forms.lock().expect("facet forms lock");
        let mut stages = self.stages.lock().expect("facet stages lock");
        let stage = stages
            .as_mut()
            .map(|s| <FS as HasFacet<F, I>>::stage_mut(s));
        read(<FS as HasFacet<F, I>>::form(&forms), stage)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_envelope_round_trips() {
        let record = tag_record(3, &[1, 2, 3]);
        assert_eq!(record, vec![3, 1, 2, 3]);
        let (tag, payload) = split_record(&record).unwrap();
        assert_eq!(tag, 3);
        assert_eq!(payload, &[1, 2, 3]);
    }

    #[test]
    fn empty_record_is_corrupt() {
        assert!(split_record(&[]).is_err());
    }

    #[test]
    fn empty_set_rejects_every_nonzero_tag() {
        let mut forms = ();
        assert!(<() as FacetSet>::fold(&mut forms, 1, &[], false).is_err());
        assert!(<() as FacetSet>::fold(&mut forms, 7, &[], true).is_err());
    }

    #[test]
    fn composite_snapshot_round_trips() {
        let composite = CompositeSnapshot {
            state: vec![9, 9],
            facets: vec![(1, vec![4]), (2, vec![])],
        };
        let bytes = composite.encode().unwrap();
        let back = CompositeSnapshot::decode(&bytes).unwrap();
        assert_eq!(back.state, vec![9, 9]);
        assert_eq!(back.facets, vec![(1, vec![4]), (2, vec![])]);
    }
}
