//! The facet seam (spec §7.12): one substrate, many storage features.
//!
//! A **facet** is a durable storage feature of one grain defined entirely as an
//! interpretation over the two storage primitives — ordered, term-fenced records
//! (§7.2) and immutable content-addressed blobs (§7.10). It gets no replication
//! path, no fence, and no recovery protocol of its own; those exist once, in the
//! substrate.
//!
//! **Tagged records, one order, one barrier (G19).** Every record a grain
//! journals carries a one-byte facet tag; tag [`EVENT_TAG`] (0) is *facet 0*, the
//! grain's own event fold. Tags run to [`MAX_TAG`], the high bit being reserved as
//! the escape into a later envelope revision ([`RECORD`]). All records a command
//! produces — its events plus every facet's staged operations — append as one
//! atomic batch (§6), so a command that touches state, the KV map, and the
//! filesystem commits everywhere or nowhere. Replay dispatches each record to its
//! facet by tag; an unrecognized tag aborts activation rather than being skipped,
//! so a grain's history is never silently misread by a runtime missing one of its
//! facets.
//!
//! **Two facet classes (§7.12).** A *logical* facet folds: its records are
//! semantic operations applied by a pure, deterministic [`Facet::fold`] (F1), on
//! replay and after a live commit alike. A *physical* facet
//! (`PHYSICAL = true`) materializes: its live form mutates locally during the
//! command (inside [`Facet::begin`]/[`Facet::seal`]) and its records are captured
//! deltas, so the live path skips the fold and a non-committed outcome
//! [`Facet::discard`]s the materialization outright (G20); the form is a
//! rebuildable cache (§1).
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
use crate::facet_blobs::RootSet;

/// The record tag of facet 0 — the grain's own event fold (spec §7.12).
pub(crate) const EVENT_TAG: u8 = 0;

/// The record envelope's revisions, and the one this build writes (spec §7.12,
/// compatibility spec §3).
///
/// Revision 1 is `[tag][payload]` with `tag <= MAX_TAG`, so the window costs **no
/// bytes on disk**. The boundary is stamped through the tag space rather than
/// through a version byte because a version *prefix* could not be retrofitted
/// unambiguously: `[version 1][tag 1][payload]` and `[tag 1][payload starting
/// 0x01]` are the same bytes.
///
/// A future revision announces itself by setting the high bit of the leading byte
/// (see [`MAX_TAG`]), which no revision-1 record can do.
pub(crate) const RECORD: compat::Window = compat::Window::at("granary.record", 1);

/// The largest facet tag a record envelope may carry.
///
/// The high bit of the leading byte is **reserved**: a byte at or above `0x80`
/// is not a tag but the escape into a later envelope revision (`[0x80 | revision]`
/// followed by that revision's own header), which is what lets the envelope change
/// later without a migration.
///
/// This does not version a facet's **payload** schema: `postcard` appends enum
/// variants safely, so a facet's op enum grows at its end indefinitely. This
/// escape is for changing the *envelope*.
pub(crate) const MAX_TAG: u8 = 0x7F;

/// The largest tag this crate will ever assign to a built-in facet.
///
/// The tag space is split so it can be allocated by two parties without either
/// having to consult the other: `0..=MAX_BUILTIN_TAG` is granary's, and
/// `MAX_BUILTIN_TAG+1..=MAX_TAG` belongs to facets defined outside this crate. A
/// tag is permanent — it is the dispatch key for every record ever written under it
/// — so a collision is not a compile error somewhere, it is two different readings
/// of the same durable byte.
pub(crate) const MAX_BUILTIN_TAG: u8 = 0x3F;

/// A facet's interpretation of its durable input failed: a record that will not
/// decode, a snapshot contribution that will not restore, or a record tag no
/// declared facet claims (G19). The host aborts the activation rather than
/// misread the grain's history.
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
///
/// Panics on a tag above [`MAX_TAG`], mirroring the refusal in [`split_record`]:
/// the high bit means "a later envelope revision" to every reader, so a record
/// carrying it must never be written by a build whose envelope is revision 1.
pub(crate) fn tag_record(tag: u8, payload: &[u8]) -> Vec<u8> {
    assert!(
        tag <= MAX_TAG,
        "facet tag {tag} sets the reserved envelope-escape bit; tags are 0..={MAX_TAG}"
    );
    let mut bytes = Vec::with_capacity(payload.len() + 1);
    bytes.push(tag);
    bytes.extend_from_slice(payload);
    bytes
}

/// Split a journaled record into its facet tag and payload. An empty record is
/// corrupt (every record wears the envelope); the caller aborts activation.
pub(crate) fn split_record(bytes: &[u8]) -> Result<(u8, &[u8]), FacetError> {
    let (&lead, payload) = bytes
        .split_first()
        .ok_or_else(|| FacetError("empty record (missing facet tag)".into()))?;
    if lead > MAX_TAG {
        // The high bit escapes into a later envelope revision (§7.12), which
        // revision 1 never sets — so these bytes come from a newer build. Refuse
        // by name: "unrecognized facet tag 129" would send an operator hunting for
        // a missing facet, when the fault is a version skew at the other end.
        return Err(FacetError(
            RECORD
                .admit(compat::Version(u16::from(lead & MAX_TAG)))
                .map(|v| format!("record envelope {v} has no reader in this build"))
                .unwrap_or_else(|err| err.to_string()),
        ));
    }
    Ok((lead, payload))
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

/// The composite snapshot's stamp (spec §7.12, compatibility spec §3).
///
/// A snapshot gets a full magic and revision rather than the record envelope's
/// reserved bit ([`RECORD`]): `postcard` gives its leading bytes no structure a
/// reader could use to recognize the format otherwise. Reading the revision
/// **before** the body is what lets a snapshot from another revision be refused
/// instead of misparsed.
pub(crate) const SNAPSHOT: compat::Stamp =
    compat::Stamp::new(b"GRSNAP", compat::Window::at("granary.snapshot", 1));

/// The composite snapshot's on-disk body (spec §7.12), inside the [`SNAPSHOT`]
/// stamp.
#[derive(Serialize, Deserialize)]
struct SnapshotBody {
    /// The codec that encoded facet 0's state.
    ///
    /// Recorded because facet 0's contribution is the only part of a snapshot that
    /// is **not** codec-independent: it is a user type encoded with the
    /// deployment's codec (§4.1, §5), while facet payloads are deliberately
    /// `postcard`. Without it, a codec change reads as a corrupt grain.
    codec: String,
    parts: SnapshotParts,
    /// Room to grow without a revision bump (compatibility spec §2.1).
    ///
    /// This body is `postcard`, which is positional: it cannot gain a field, so
    /// without this area every future addition would be a new revision carrying a
    /// second decoder forever. A change that reinterprets bytes already here is
    /// still a revision; this is only for carrying *more*.
    ext: compat::Extensions,
}

/// The body's positional payload: facet 0's inline state and the facet
/// contributions, in facet-set order. The wire shape, fixed by revision 1; how the
/// host handles the same snapshot is [`CompositeSnapshot`].
#[derive(Serialize, Deserialize)]
struct SnapshotParts {
    /// Facet 0's state when it travels inline, **empty** when
    /// [`EXT_STATE_CHUNKS`] carries it instead.
    state: Vec<u8>,
    facets: Vec<(u8, Vec<u8>)>,
}

/// Facet 0's state travels as content-addressed chunks, and this entry is the
/// [`StateManifest`] naming them ([`StatePayload`]).
///
/// **Critical.** A reader that skipped it would find [`SnapshotParts::state`]
/// empty and rebuild the grain with a default `State` — a silent, total misread of
/// a grain's history, which is precisely the case the criticality bit exists for
/// (compatibility spec §2.1). It is an extension rather than a revision because it
/// reinterprets no byte already defined: a snapshot without the entry means what it
/// has always meant.
const EXT_STATE_CHUNKS: u16 = compat::Extensions::CRITICAL | 0x0001;

/// The critical extension keys this build implements.
///
/// A snapshot carrying a critical key outside this list is refused (**V2**): its
/// writer marked that entry as one a reader must understand, so skipping it would be
/// exactly the silent misread the stamp exists to prevent.
const SNAPSHOT_EXT_KNOWN: &[u16] = &[EXT_STATE_CHUNKS];

/// Where facet 0's encoded `State` lives in a snapshot (spec §7.12).
///
/// Small states ride inline in the snapshot record, as they always have. Past
/// [`INLINE_MAX`] the state is cut into content-defined chunks ([`crate::cdc`]),
/// stored in the grain's blob area (§7.10), and the record carries only their ids —
/// so a snapshot of a grain that grew by one turn puts one or two chunks instead of
/// broadcasting the whole folded transcript to every replica (`docs/hardware-envelope.md`
/// §3.9).
pub(crate) enum StatePayload {
    Inline(Vec<u8>),
    Chunked(StateManifest),
}

/// The chunked form of facet 0's state: the ids to concatenate, and the length the
/// result must have.
#[derive(Serialize, Deserialize)]
pub(crate) struct StateManifest {
    /// Total encoded length. The chunks reproduce it exactly; carrying it lets a
    /// restore refuse a manifest that does not, rather than handing a truncated
    /// value to the codec.
    bytes: u64,
    chunks: Vec<BlobId>,
}

/// Below this many bytes facet 0's state stays inline.
///
/// The chunked form trades bytes for round trips: the chunk puts are one quorum
/// round of their own, ahead of the snapshot record's. That is the right trade only
/// when the bytes are worth a round trip. Under 64 KiB they are not — a state that
/// size costs about half a millisecond of a 125 MB/s uplink even re-sent whole
/// (hw §3.3) — and staying inline also keeps a small grain's snapshot free of blobs
/// to root, fetch, and sweep.
const INLINE_MAX: usize = 64 * 1024;

impl StatePayload {
    /// Choose a carriage for `state` and make it durable: inline under
    /// [`INLINE_MAX`], else chunked into the grain's blob area.
    ///
    /// `rooted` is the ids this grain already holds durably — the host's live root
    /// set (**F3**). A chunk already in it is *not* put again, and that skip is the
    /// whole mechanism: without it every snapshot re-sends every chunk and the
    /// chunked form is strictly worse than inline. The ids are pure functions of
    /// the bytes, so the check needs no round trip.
    ///
    /// The puts complete before this returns, so the snapshot record the caller
    /// writes next can never reference a chunk that is not already durable.
    pub(crate) async fn store(
        state: Vec<u8>,
        blobs: &GrainBlobs,
        rooted: &BTreeSet<BlobId>,
    ) -> Result<StatePayload, FacetError> {
        if state.len() <= INLINE_MAX {
            return Ok(StatePayload::Inline(state));
        }
        let bytes = state.len() as u64;
        let parts = crate::cdc::split(&state);
        let chunks: Vec<BlobId> = parts.iter().map(|part| BlobId::of(part)).collect();
        let fresh: Vec<Vec<u8>> = parts
            .iter()
            .zip(&chunks)
            .filter(|(_, id)| !rooted.contains(*id))
            .map(|(part, _)| part.to_vec())
            .collect();
        crate::facet_blobs::put_chunked(blobs, fresh, "snapshot state").await?;
        Ok(StatePayload::Chunked(StateManifest { bytes, chunks }))
    }

    /// The blob ids this payload depends on — empty when it travels inline. The
    /// host keeps them alive for as long as the snapshot naming them is the durable
    /// one (**F3**).
    pub(crate) fn chunks(&self) -> Vec<BlobId> {
        match self {
            StatePayload::Inline(_) => Vec::new(),
            StatePayload::Chunked(manifest) => manifest.chunks.clone(),
        }
    }

    /// Facet 0's state bytes, fetching and reassembling the chunks if that is how
    /// they travelled. A chunk no replica can serve fails here rather than yielding
    /// a short state: the composite restores whole or aborts the activation (**G4**).
    pub(crate) async fn load(self, blobs: &GrainBlobs) -> Result<Vec<u8>, FacetError> {
        match self {
            StatePayload::Inline(bytes) => Ok(bytes),
            StatePayload::Chunked(manifest) => {
                let state =
                    crate::facet_blobs::get_concat(blobs, &manifest.chunks, "snapshot state")
                        .await?;
                if state.len() as u64 != manifest.bytes {
                    return Err(FacetError(format!(
                        "granary.snapshot: state manifest names {} bytes, its {} chunks \
                         reassemble to {}",
                        manifest.bytes,
                        manifest.chunks.len(),
                        state.len()
                    )));
                }
                Ok(state)
            }
        }
    }
}

/// The composite snapshot (spec §7.12): facet 0's codec-encoded `State` plus one
/// contribution per declared facet, all at one `Seq`. G4 applies to the composite
/// as a whole. Encoded with `postcard` — facet payloads and this envelope are
/// runtime-internal, deliberately independent of the deployment's user codec.
pub(crate) struct CompositeSnapshot {
    /// Facet 0's contribution: the grain's `State`, encoded with the system codec
    /// (it is a user type; the codec is the system's, §4.1), and carried either
    /// inline or by blob ([`StatePayload`]).
    pub state: StatePayload,
    /// One `(tag, contribution)` per declared facet, in facet-set order.
    pub facets: Vec<(u8, Vec<u8>)>,
}

impl CompositeSnapshot {
    /// Stamp and encode the composite. `codec` is the name of the codec that
    /// encoded [`state`](CompositeSnapshot::state), recorded so a later read can
    /// tell a codec change from a corrupt grain.
    pub(crate) fn encode(self, codec: &str) -> Result<Vec<u8>, FacetError> {
        let mut ext = compat::Extensions::new();
        let state = match self.state {
            StatePayload::Inline(bytes) => bytes,
            StatePayload::Chunked(manifest) => {
                ext.insert(EXT_STATE_CHUNKS, encode_payload(&manifest));
                Vec::new()
            }
        };
        let body = SnapshotBody {
            codec: codec.to_string(),
            parts: SnapshotParts {
                state,
                facets: self.facets,
            },
            ext,
        };
        postcard::to_allocvec(&body)
            .map(|bytes| SNAPSHOT.stamp(&bytes))
            .map_err(|e| FacetError(format!("snapshot encode: {e}")))
    }

    /// Admit the stamp, decode the body, and confirm it was encoded with `codec` —
    /// in that order, so nothing downstream sees bytes from a revision or a codec
    /// this build cannot read (compatibility **V2**).
    ///
    /// Facet 0's state comes back as a [`StatePayload`]; the caller resolves it
    /// against the grain's blob area, which is why that is not done here.
    pub(crate) fn decode(bytes: &[u8], codec: &str) -> Result<CompositeSnapshot, FacetError> {
        let (_revision, body) = SNAPSHOT.unstamp(bytes).map_err(|e| FacetError(e.to_string()))?;
        let body: SnapshotBody =
            postcard::from_bytes(body).map_err(|e| FacetError(format!("snapshot decode: {e}")))?;
        body.ext
            .admit(SNAPSHOT.window().boundary(), SNAPSHOT_EXT_KNOWN)
            .map_err(|e| FacetError(e.to_string()))?;
        if body.codec != codec {
            return Err(FacetError(format!(
                "granary.snapshot: encoded with codec '{}', but this node runs '{codec}' \
                 — facet 0's state is codec-encoded (§4.1), so it cannot be decoded here",
                body.codec
            )));
        }
        let state = match body.ext.get(EXT_STATE_CHUNKS) {
            None => StatePayload::Inline(body.parts.state),
            Some(entry) => {
                // Both forms at once is a writer this build cannot reconcile: it
                // would have to guess which one is the state.
                if !body.parts.state.is_empty() {
                    return Err(FacetError(
                        "granary.snapshot: carries both an inline state and a chunk manifest"
                            .into(),
                    ));
                }
                StatePayload::Chunked(decode_payload("snapshot state manifest", entry)?)
            }
        };
        Ok(CompositeSnapshot {
            state,
            facets: body.parts.facets,
        })
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

    /// The runtime hook for durable alarms (spec §7.16): the pending deadline this
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

    /// Stage the consumption of a fired alarm (spec §7.16): before invoking
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

    /// The pending alarm deadline of whichever facet holds one (spec §7.16), or
    /// `None`. At most one facet in a set is the [`Alarm`](crate::Alarm) facet, so
    /// the first non-`None` is the grain's single alarm.
    fn alarm_due(forms: &Self::Forms) -> Option<u64>;

    /// Stage the consumption of a fired alarm across the set (spec §7.16). Only the
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
/// panics (staging is command-scoped, §4.2).
pub(crate) struct FacetCell<FS: FacetSet> {
    forms: Mutex<FS::Forms>,
    stages: Mutex<Option<FS::Stages>>,
    /// Facet 0's blob roots: the chunks the latest snapshot carried its state in
    /// ([`StatePayload`]), union-kept under the same **F3** discipline the
    /// checkpointing facets keep theirs under. They live here rather than beside
    /// the host's other activation state because this cell is what supplies
    /// [`GrainBlobs::gc`](crate::GrainBlobs::gc) its roots — a set kept anywhere
    /// else would be swept by the grain's own sweep.
    state_roots: Mutex<RootSet>,
}

impl<FS: FacetSet> FacetCell<FS> {
    /// A fresh cell with empty forms and no armed stage. Asserts the declared
    /// tags are distinct, nonzero, and within the tag space (a duplicated tag
    /// would make record dispatch ambiguous; tag 0 is facet 0's; the high bit is
    /// the envelope-revision escape, [`MAX_TAG`]).
    pub(crate) fn new() -> FacetCell<FS> {
        let mut seen = BTreeSet::new();
        for &tag in FS::TAGS {
            assert!(
                tag != EVENT_TAG,
                "facet tag 0 is reserved for the event fold"
            );
            assert!(
                tag <= MAX_TAG,
                "facet tag {tag} sets the reserved envelope-escape bit; tags are 1..={MAX_TAG}"
            );
            assert!(
                tag <= MAX_BUILTIN_TAG,
                "facet tag {tag} is in the range reserved for facets defined outside \
                 this crate; a built-in facet takes 1..={MAX_BUILTIN_TAG}"
            );
            assert!(seen.insert(tag), "duplicate facet tag {tag} in facet set");
        }
        FacetCell {
            forms: Mutex::new(FS::Forms::default()),
            stages: Mutex::new(None),
            state_roots: Mutex::new(RootSet::default()),
        }
    }

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

    pub(crate) fn fold_live(&self, tag: u8, payload: &[u8]) -> Result<(), FacetError> {
        let mut forms = self.forms.lock().expect("facet forms lock");
        FS::fold(&mut forms, tag, payload, true)
    }

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

    /// The union of every facet's blob roots **and facet 0's** (§7.12) — what the
    /// host adds to any [`GrainBlobs::gc`](crate::GrainBlobs::gc) sweep.
    pub(crate) fn roots(&self) -> BTreeSet<BlobId> {
        let mut roots = FS::roots(&self.forms.lock().expect("facet forms lock"));
        roots.extend(self.state_roots());
        roots
    }

    /// Facet 0's blob roots alone — the chunk ids the durable snapshot's state was
    /// carried in. Read on the snapshot path to skip re-putting a chunk this grain
    /// already holds ([`StatePayload::store`]).
    pub(crate) fn state_roots(&self) -> BTreeSet<BlobId> {
        self.state_roots.lock().expect("state roots lock").ids()
    }

    /// Union `ids` into facet 0's roots — a snapshot's chunks, kept from the moment
    /// they are durable and never pruned mid-activation (**F3**).
    pub(crate) fn keep_state_chunks(&self, ids: impl IntoIterator<Item = BlobId>) {
        self.state_roots
            .lock()
            .expect("state roots lock")
            .extend(ids);
    }

    /// Adopt the restored snapshot's chunks as facet 0's roots, discarding the
    /// previous activation's — the one place the set may shrink, because a fresh
    /// activation starts from the durable manifest (**F3**).
    pub(crate) fn adopt_state_chunks(&self, ids: impl IntoIterator<Item = BlobId>) {
        self.state_roots
            .lock()
            .expect("state roots lock")
            .reset(ids);
    }

    /// The grain's pending alarm deadline (spec §7.16), in nanoseconds since the
    /// clock epoch, or `None`. Read by the host after each commit and on
    /// activation to arm the callerless timer.
    pub(crate) fn alarm_due(&self) -> Option<u64> {
        FS::alarm_due(&self.forms.lock().expect("facet forms lock"))
    }

    /// Stage the consumption of a fired alarm into the armed command (spec §7.16).
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
    fn every_revision_1_record_is_already_stamped() {
        for tag in [EVENT_TAG, 1, 6, MAX_TAG] {
            let record = tag_record(tag, &[7]);
            assert_eq!(record.len(), 2, "revision 1 adds one byte, the tag");
            assert_eq!(split_record(&record).unwrap(), (tag, &[7][..]));
        }
    }

    #[test]
    fn a_later_envelope_revision_is_refused_as_a_version_skew() {
        let err = split_record(&[0x80 | 2, 9, 9]).expect_err("revision 2 is unreadable here");
        assert!(
            err.0.contains("granary.record") && err.0.contains("v2"),
            "the refusal must name the boundary and the revision: {err}"
        );
    }

    #[test]
    #[should_panic(expected = "reserved envelope-escape bit")]
    fn writing_an_escape_tag_panics_instead_of_forging_a_revision() {
        tag_record(0x80, &[1]);
    }

    #[test]
    fn empty_set_rejects_every_nonzero_tag() {
        let mut forms = ();
        assert!(<() as FacetSet>::fold(&mut forms, 1, &[], false).is_err());
        assert!(<() as FacetSet>::fold(&mut forms, 7, &[], true).is_err());
    }

    fn composite() -> CompositeSnapshot {
        CompositeSnapshot {
            state: StatePayload::Inline(vec![9, 9]),
            facets: vec![(1, vec![4]), (2, vec![])],
        }
    }

    /// The inline bytes of a payload that did not travel by blob.
    fn inline(payload: StatePayload) -> Vec<u8> {
        match payload {
            StatePayload::Inline(bytes) => bytes,
            StatePayload::Chunked(_) => panic!("expected an inline state"),
        }
    }

    fn parts() -> SnapshotParts {
        SnapshotParts {
            state: vec![9, 9],
            facets: vec![(1, vec![4]), (2, vec![])],
        }
    }

    #[test]
    fn composite_snapshot_round_trips() {
        let bytes = composite().encode("json").unwrap();
        assert!(bytes.starts_with(b"GRSNAP"), "the stamp leads the body");
        let back = CompositeSnapshot::decode(&bytes, "json").unwrap();
        assert_eq!(inline(back.state), vec![9, 9]);
        assert_eq!(back.facets, vec![(1, vec![4]), (2, vec![])]);
    }

    #[test]
    fn a_chunked_state_round_trips_as_a_manifest() {
        let ids = vec![BlobId::of(b"one"), BlobId::of(b"two")];
        let composite = CompositeSnapshot {
            state: StatePayload::Chunked(StateManifest {
                bytes: 7,
                chunks: ids.clone(),
            }),
            facets: vec![(1, vec![4])],
        };
        let bytes = composite.encode("json").unwrap();
        let back = CompositeSnapshot::decode(&bytes, "json").unwrap();
        assert_eq!(back.state.chunks(), ids);
        assert_eq!(back.facets, vec![(1, vec![4])]);
    }

    #[test]
    fn a_chunked_state_costs_the_record_only_its_manifest() {
        // The point of the whole mechanism: the snapshot record's size follows the
        // number of chunks, not the size of the state.
        let ids: Vec<BlobId> = (0u8..8).map(|i| BlobId::of(&[i])).collect();
        let chunked = CompositeSnapshot {
            state: StatePayload::Chunked(StateManifest {
                bytes: 8 * 64 * 1024,
                chunks: ids,
            }),
            facets: Vec::new(),
        }
        .encode("json")
        .unwrap();
        assert!(
            chunked.len() < 512,
            "a manifest for 512 KiB of state took {} bytes",
            chunked.len()
        );
    }

    #[test]
    fn a_snapshot_carrying_both_state_forms_is_refused() {
        // Neither form can be preferred without guessing, so the reader refuses
        // rather than rebuilding a grain from the wrong one.
        let mut ext = compat::Extensions::new();
        ext.insert(
            EXT_STATE_CHUNKS,
            encode_payload(&StateManifest {
                bytes: 3,
                chunks: vec![BlobId::of(b"x")],
            }),
        );
        let body = SnapshotBody {
            codec: "json".into(),
            parts: parts(),
            ext,
        };
        let bytes = SNAPSHOT.stamp(&postcard::to_allocvec(&body).unwrap());

        let err = CompositeSnapshot::decode(&bytes, "json")
            .err()
            .expect("an ambiguous state carriage must not decode");
        assert!(
            err.0.contains("both an inline state and a chunk manifest"),
            "the refusal must say what is ambiguous: {err}"
        );
    }

    #[test]
    fn a_build_without_the_chunk_extension_refuses_a_chunked_snapshot() {
        // What a downgrade sees. `EXT_STATE_CHUNKS` is critical, so a reader whose
        // known-key list predates it refuses the whole snapshot instead of
        // rebuilding the grain from the empty inline state beside the manifest.
        let bytes = CompositeSnapshot {
            state: StatePayload::Chunked(StateManifest {
                bytes: 3,
                chunks: vec![BlobId::of(b"x")],
            }),
            facets: Vec::new(),
        }
        .encode("json")
        .unwrap();
        let (_revision, body) = SNAPSHOT.unstamp(&bytes).unwrap();
        let body: SnapshotBody = postcard::from_bytes(body).unwrap();

        let err = body
            .ext
            .admit(SNAPSHOT.window().boundary(), &[])
            .expect_err("a build that predates the key must refuse");
        assert!(
            format!("{err}").contains("0x8001"),
            "the refusal must name the key: {err}"
        );
    }

    #[test]
    fn a_snapshot_from_a_later_revision_is_refused_by_name() {
        let mut bytes = composite().encode("json").unwrap();
        // A revision above the window, as a future release would write.
        bytes[6..8].copy_from_slice(&9u16.to_le_bytes());
        let err = CompositeSnapshot::decode(&bytes, "json")
            .err()
            .expect("an unreadable revision must not decode");
        assert!(
            err.0.contains("granary.snapshot") && err.0.contains("v9"),
            "the refusal must name the boundary and the revision: {err}"
        );
    }

    #[test]
    fn unstamped_bytes_are_refused_rather_than_misparsed() {
        // What a pre-stamp snapshot, or another format's bytes, look like. The
        // magic check runs before any decode, so nothing tries to read this as a
        // composite (**V2**).
        let err = CompositeSnapshot::decode(&[2, 9, 9, 0], "json")
            .err()
            .expect("unstamped bytes must not decode");
        assert!(
            err.0.contains("granary.snapshot"),
            "the refusal must name the boundary: {err}"
        );
    }

    #[test]
    fn a_codec_change_is_reported_as_a_codec_change() {
        // Facet 0's state is codec-encoded (§4.1), so a snapshot written under one
        // codec cannot be read under another.
        let bytes = composite().encode("json").unwrap();
        let err = CompositeSnapshot::decode(&bytes, "postcard")
            .err()
            .expect("a codec change must not decode");
        assert!(
            err.0.contains("'json'") && err.0.contains("'postcard'"),
            "the refusal must name both codecs: {err}"
        );
    }

    #[test]
    fn an_unknown_ancillary_snapshot_extension_is_ignored() {
        let mut body = SnapshotBody {
            codec: "json".into(),
            parts: parts(),
            ext: compat::Extensions::new(),
        };
        body.ext.insert(0x0001, vec![1, 2, 3]);
        let bytes = SNAPSHOT.stamp(&postcard::to_allocvec(&body).unwrap());

        let back = CompositeSnapshot::decode(&bytes, "json").expect("an ancillary entry is skipped");
        assert_eq!(inline(back.state), vec![9, 9]);
    }

    #[test]
    fn an_unknown_critical_snapshot_extension_is_refused() {
        let mut body = SnapshotBody {
            codec: "json".into(),
            parts: parts(),
            ext: compat::Extensions::new(),
        };
        body.ext.insert(compat::Extensions::CRITICAL | 0x7, vec![]);
        let bytes = SNAPSHOT.stamp(&postcard::to_allocvec(&body).unwrap());

        let err = CompositeSnapshot::decode(&bytes, "json")
            .err()
            .expect("a critical entry this build does not know must be refused");
        assert!(
            err.0.contains("granary.snapshot") && err.0.contains("0x8007"),
            "the refusal must name the boundary and the key: {err}"
        );
    }

    #[test]
    fn the_extension_area_costs_one_byte_while_empty() {
        let stamped = composite().encode("json").unwrap();
        let bare =
            SNAPSHOT.stamp(&postcard::to_allocvec(&(String::from("json"), parts())).unwrap());
        assert_eq!(stamped.len(), bare.len() + 1);
    }
}
