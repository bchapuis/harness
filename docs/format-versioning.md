# Format versioning: stamping the compatibility boundaries

A design for making the tree's durable and wire formats safe to evolve, and the plan
that lands it. The three changes it covers — versioned durable envelopes in granary, a
header on every WAL file, and a negotiated protocol version in the association
handshake — look like three unrelated edits. They are one problem seen from three
places, and the point of this document is to name it once so it is not solved three
ways.

This file is a plan, not a spec: it should be deleted once its steps are done, with the
resulting rules stated as current-state text in the specs each step touches (§9).

**Steps 1–5 are done** (§9), followed by a **sustainability revision** of the formats
they defined. That pass changed five things, and they are worth recording because
three of them were defects in this document rather than in the code:

- **Durable envelopes now carry an extension area** (`compat::Extensions`, §2.1 of the
  compatibility spec). Everything here was stamped so a change is *detectable*, but
  under `postcard` every added field was still a revision with a second decoder kept
  for the support window — the same ossification this document set out to avoid,
  moved one level up. An area costs one byte while empty, and the critical/ancillary
  key rule is what keeps it from becoming a way to smuggle meaning past an old reader.
- **V5 was unimplementable.** It read "storage accepts *N−2..N*", but *N* is a release
  number and a revision is per-boundary: a boundary at revision 1 across ten releases
  has no *N−2*. Restated as a duration — how long a retired revision stays readable.
- **`actor.wire` cannot actually be bumped yet**, and §7's write-up did not say so.
  Negotiation is half of a rolling upgrade; the send-side gate is the other half, and
  it needs `Transport::peer_version`, which is deferred. Now stated where someone
  planning a wire change will read it.
- **Facet tags gained an allocation policy** (`0..=0x3F` built-in, `0x40..=0x7F`
  out-of-crate). A tag is permanent, so a collision is two readings of one durable
  byte, and this was half of what granary §16 was waiting on.
- **`Window::at`** for the single-revision case, replacing three bare `u16`s in a row
  at all eight boundaries.

What landed for steps 1–3 also differs from §2 and §7 below in two places, both
improvements found while writing the code:

- **`Window` split into `Window` + `Stamp`.** A boundary has two separable concerns —
  the version *policy* and the *byte layout* that carries it — and the handshake has
  only the first. Pairing the magic with the window inside one `Stamp` value also makes
  a writer/reader magic mismatch unwritable rather than merely unlikely. `Announced`
  became `Accepted`, which describes the thing (a range of accepted revisions) rather
  than how it arrived, and so can also be reused in the refusal.
- **`Hello.proto_version` was removed, not kept.** §7 designed a fallback for peers
  predating the window fields. The clean break makes it dead weight: there are no such
  peers, and a stale binary meeting a `Hello` without the field fails to deserialize,
  which is already fail-closed.

---

## 1. The problem, stated once

Every format in the tree is produced by one build and consumed by another. That is true
across the network during a rolling upgrade, and it is true across time on disk: a
journal written by today's binary is read by next quarter's. Wherever bytes cross a
build boundary, exactly one question decides whether the system is safe: **can the
reader tell what it is looking at, and does it refuse when it cannot?**

Today, at every such boundary, the answer is no.

- A granary journal record is `[tag u8][postcard payload]`
  (`crates/granary/src/facet.rs:72`). Nothing says which envelope layout produced it.
- A composite snapshot is a bare `postcard` struct (`facet.rs:107`). Postcard is
  positional and has no field names, so `CompositeSnapshot` can never gain a field:
  adding one makes every existing snapshot fail to decode with
  `DeserializeUnexpectedEnd`, and there is no version to branch on.
- A WAL file is a bare concatenation of frames with **no header, no format version**
  (`wal-spec.md` §2 rule 3). The framing cannot change, and the pluggable checksum the
  spec's deferred list contemplates has nowhere to declare itself.
- A grain's `State` and its event payloads are encoded with the *deployment's* codec
  (`host.rs:699`, `:523`), and nothing on disk records which codec that was. Swapping
  `JsonCodec` for a binary one — the change most likely to happen next — silently turns
  every existing journal into bytes that fail to decode, reported as a corrupt grain
  rather than as a misconfiguration.
- The handshake *does* carry a version (`Hello.proto_version`,
  `crates/actor-runtime/src/wire.rs:39`) and compares it for **strict equality**
  (`transport.rs:199`). This is worse than carrying nothing: the first bump partitions
  the cluster into two halves that refuse each other, so the field exists but cannot be
  used for the one thing it is for.

The common shape is a **compatibility boundary**, and each one needs three things and
no more:

1. a **stamp** — the bytes, or the peer, say which revision they are;
2. a **window** — this build declares which revisions it accepts, and which it writes;
3. a **verdict** — outside the window, the boundary refuses *by name* and never parses.

The verdict is the part that carries the safety. A misparse is the failure mode worth
engineering against: it turns a version skew, which an operator can diagnose and fix in
a minute, into silent data corruption, which they cannot.

### Invariants

| | |
|---|---|
| **V1** | Every durable byte sequence and every association is stamped at exactly one boundary. No format is stamped twice, and none is unstamped. |
| **V2** | A build MUST refuse bytes or peers outside its window, naming the boundary, what it found, and what it accepts. It MUST NOT attempt to parse them. |
| **V3** | For every window, `writes` lies within `reads`. A build must be able to read what it wrote, or it cannot be rolled back onto its own data. |
| **V4** | No release writes revision *V* until every release that may still read those bytes accepts *V*. Read-new first, write-new later (§7). |
| **V5** | Support window: storage accepts *N−2..N*, the wire accepts *N−1..N*. Storage is wider because a journal outlives a process. |
| **V6** | Boundaries version independently. A wire bump does not invalidate a journal, and a snapshot bump does not invalidate a WAL file. |

V3 is checkable at compile time, and §2 makes it so. V4 and V5 are policy that only
tests can hold up, and §8 is how.

---

## 2. One vocabulary: the `compat` crate

The three verdicts are the same comparison written three times unless the comparison
lives somewhere shared. Where it can live is fixed by the dependency graph: `wal`
depends on nothing in the tree by design (`wal-spec.md` §1), so the vocabulary must sit
at or below it. That rules out `actor-serialization` and argues for a new crate with no
in-tree dependencies.

A crate for ~150 lines needs a justification, and it has one: four consumers today
(`wal`, `granary`, `actor-runtime`, `blob-store`), and the alternative is either four
copies of the negotiation rule or `actor-cluster` depending on a write-ahead-log crate
to borrow a version type. It is a clean sub-task behind a small interface, which is the
test the design principles set for splitting.

```rust
// crates/compat/src/lib.rs

/// A format's revision at one compatibility boundary (V6): boundaries are
/// numbered independently, so this is meaningless without the `Window` that
/// names it.
pub struct Version(pub u16);

/// The revisions one build accepts at one boundary, and the single revision it
/// writes. Constructed `const`, so V3 is a compile error rather than a
/// production surprise:
///
/// ```
/// const SNAPSHOT: Window = Window::new("granary.snapshot", 1, 1, 1);
/// ```
pub struct Window { boundary: &'static str, lo: Version, hi: Version, writes: Version }

impl Window {
    /// Panics unless `lo <= writes <= hi` (**V3**). In a `const` initializer that
    /// panic is a build failure, which is the point: a build that cannot read
    /// what it writes must not link.
    pub const fn new(boundary: &'static str, lo: u16, hi: u16, writes: u16) -> Window;

    /// The revision this build stamps onto new bytes.
    pub const fn writes(&self) -> Version;

    /// One-sided verdict, for bytes at rest: admit `found` or refuse it (**V2**).
    pub fn admit(&self, found: Version) -> Result<Version, Incompatible>;

    /// Two-sided verdict, for a live peer: the highest revision both ends accept,
    /// or a refusal when the windows are disjoint. This is the negotiation rule,
    /// defined once.
    pub fn negotiate(&self, peer: Announced) -> Result<Version, Incompatible>;
}

/// What a peer announced it accepts. Distinct from `Window` because a peer's own
/// `writes` choice is neither ours to know nor ours to trust.
pub struct Announced { pub lo: Version, pub hi: Version }

/// A refusal. Carries both sides, so the message tells an operator which end to
/// move rather than only that something is wrong.
pub enum Incompatible {
    /// Not this format at all: the magic did not match. A wrong file, or bytes
    /// predating the first stamped revision.
    Unstamped { boundary: &'static str },
    /// Stamped, but outside the window.
    Version { boundary: &'static str, found: Version, lo: Version, hi: Version },
    /// Two windows with no overlap (the handshake case).
    Disjoint { boundary: &'static str, ours: (Version, Version), theirs: Announced },
}
```

The `boundary` name lives in the `Window` rather than in each call, so an error cannot
be raised without it and no caller has to remember to supply it. `admit` and
`negotiate` are the whole interface: two functions hiding the entire policy, and one
place that owns the wording an operator will read at 3am.

`compat` also carries the one shared byte-level helper, because three boundaries want
the same prefix:

```rust
/// Write `magic ++ version` ahead of `body`, and its inverse. The stamp sits
/// *outside* the body's own encoding so a wrong-format input is refused before any
/// decoder touches it (**V2**) — which is exactly what a positional format like
/// postcard cannot do for itself.
pub fn stamp(magic: &[u8], v: Version, body: &[u8]) -> Vec<u8>;
pub fn unstamp<'a>(magic: &[u8], w: &Window, bytes: &'a [u8]) -> Result<(Version, &'a [u8]), Incompatible>;
```

---

## 3. Stamp granularity is a cost decision

A uniform rule would be simpler to state and worse to live with. The right granularity
for a stamp is **the coarsest one that still catches the error**, because a stamp's cost
is paid per stamped item and its value is paid once per mistake.

| Boundary | Stamp | Granularity | Why here |
|---|---|---|---|
| Store identity (§4) | magic + format + codec name, in a sidecar | once per store | Catches a codec swap across every record and snapshot in the store at once. Free. |
| WAL file (§5) | 16-byte header | once per file | Read at `open`; amortized over the whole log. Also where the checksum kind belongs. |
| Snapshot (§6) | 6-byte magic + `u16` | once per snapshot | Snapshots are large and cold; eight bytes is noise and detection is unambiguous. |
| Journal record (§6) | reserved escape bit | per record | Records are small and hot. Costs **zero bytes** (§6.1). |
| Association (§7) | announced window in `Hello` | per association | Already JSON with named fields, so this is additive. |

The record envelope is the interesting one, and it is where this design departs from
the obvious approach.

---

## 4. Store identity: the codec stamp

`FileGrainStore::open` (`crates/granary/src/file_store.rs:186`) creates a store's
directory layout and knows nothing about the codec whose output it will hold. But a
grain's `State` and its event payloads are codec-encoded (`host.rs:699`, `:523`), so the
codec is part of the store's compatibility identity even though the store never names
it.

Add a `store.meta` sidecar, written at creation through the existing
`wal::atomic_replace` and checked on every subsequent open:

```
compat::stamp(b"GRSTOR", store_format, postcard(StoreMeta { codec: String }))
```

`open` gains the expected codec name and refuses a mismatch as
`Incompatible::Version`-adjacent policy, naming both codecs. This is the change that
makes the JSON-to-binary codec swap a diagnosable configuration error instead of a
storm of corrupt-grain activation aborts, which is worth having *before* that swap, not
after.

The same `compat::stamp` wraps the other sidecars, which are durable formats with no
stamp today: the Raft `term` and `snapshot` pointers
(`crates/actor-runtime/src/storage.rs:217`, `:268`), the granary shardmap
(`file_store.rs:421`, `:494`), and the blob-store tombstones
(`crates/blob-store/src/local.rs:117`, `:187`). `atomic_replace` keeps its
opaque-bytes interface — the encoding stays the caller's choice, as its doc comment
insists — and each caller stamps its own bytes before handing them over. One helper,
six adoptions, no change to the primitive.

---

## 5. The WAL file header

Prepend a fixed 16-byte header, and make `open` check it:

```
[magic 8][frame_format u16][checksum_kind u16][record_schema u16][reserved u16]
```

Three fields owned by two different parties, which is the design's whole content here:

- **`frame_format`** and **`checksum_kind`** are the `wal` crate's own secrets. The
  first versions the `[len][payload][checksum]` layout; the second is the
  pluggable-checksum extension the spec's deferred list already contemplates, reserved
  at value 1 (FNV-1a) with no second algorithm implemented. Reserve, do not build.
- **`record_schema`** is the *caller's* opaque `u16`. `wal` never interprets it. This is
  the field that matters most, because it is the only way a caller of a positional
  format can version its records at all: `Wal<T>`'s `T` is postcard-encoded, so `T`
  cannot gain a field, and the version has to live outside the payload.

`open` therefore takes the caller's window and does the refusing itself:

```rust
pub fn open(path: ..., max_record: u32, records: &compat::Window)
    -> Result<(Wal<T>, Vec<T>), OpenError>;
```

Making the check part of `open` rather than an `header()` accessor the caller may
consult is deliberate: an optional check is a check someone forgets, and the forgotten
case is the misparse V2 exists to prevent. Pull the complexity down; leave the caller
the one choice that is genuinely theirs, which is the window.

The return type has to change. The crate's stated failure policy is that every method
returns `io::Result` because it refuses to decide what an I/O failure *means* — and an
incompatible file is not an I/O failure, it is a policy refusal. Folding it into
`io::ErrorKind::InvalidData` would conflate exactly the two kinds of error the design
principles insist on keeping apart, so `open` grows a two-variant `wal::OpenError { Io,
Incompatible }`. It is the only method that gains one.

**Magic.** `\x89WAL\r\n\x1a\n`, the PNG convention: a high-bit byte, then the name, then
a CRLF/EOF trap that catches a text-mode transfer. It also has a property worth keeping
even under a clean break: its first four bytes read as a little-endian `u32` are
`0x4C415789`, which exceeds any `max_record` in the tree (granary's is `1 << 30`), so a
headerless file from an older build can never be mistaken for a headered one, and a
headered file can never be mistaken for a frame. Dev machines with stale files get a
named refusal instead of a scan that finds one plausible record.

`open` should assert `max_record < 0x4C41_5789` so that property cannot be configured
away.

Call sites: `file_store.rs:343`, `:362`, and `storage.rs:152`.

---

## 6. Granary's durable envelopes

### 6.1 The record envelope: zero bytes, by reserving the escape

The obvious move is a version byte beside the facet tag. It is the wrong one, for two
reasons. It costs a byte on every record forever, on the hottest durable path in the
tree. And it is unstamped-by-construction: `[version=1][tag=1][payload]` is
byte-identical to a `[tag=1][payload]` record whose payload begins with `0x01`, so the
version cannot actually be distinguished from the thing it is meant to version.

Reserve the tag byte's **high bit** as an escape instead. Tags in use are 0 (the event
fold), 1 `Kv`, 2 `Ws`, 3 `Sql`, 4 `Alarm`, 5 `Workflow`, 6 `Disk` — all far below
`0x80`. Define:

- **envelope revision 1** is `[tag][payload]` with `tag < 0x80`. This is exactly today's
  layout, so every existing record is already a validly stamped revision-1 record;
- `tag >= 0x80` is reserved. A future revision reads it as an escape into an extended
  envelope (`[0x80 | rev][tag][…]`), and until one exists it is a named refusal, not an
  unknown tag.

Today's changes are small and none of them touch a byte on disk:

- constrain tags to `0..=0x7F` in `FacetCell::new`'s existing distinctness assert
  (`facet.rs:581`), beside the two asserts already there;
- have `split_record` (`facet.rs:81`) return `Incompatible::Version` for a high-bit tag
  rather than letting it reach `FacetSet::fold` and surface as `unrecognized facet tag
  128`;
- document the reservation in the tag registry.

That buys full headroom for an envelope change — per-record compression, a record-level
digest, a wider tag space — for zero bytes and zero migration. Note what is *not* being
solved here: a facet's **payload** schema, which is a different lever. Postcard appends
enum variants safely, so a facet's op enum can grow variants at the end indefinitely,
and that is the mechanism facets should use day to day. The envelope revision is for
changing the envelope; the append-only variant rule is for changing a payload. Two
levers, two kinds of change, no overlap.

### 6.2 The composite snapshot: magic and version

`CompositeSnapshot` (`facet.rs:107`) gets the treatment the record envelope does not,
because snapshots are large and cold, so a stamp is free, and because postcard gives
their leading bytes no structure a sniff could rely on:

```rust
const SNAPSHOT_MAGIC: &[u8] = b"GRSNAP";
// on disk: [magic 6][revision u16 LE][postcard(CompositeSnapshot)]

pub(crate) struct CompositeSnapshot {
    /// The codec that encoded `state` (§4.1, §5). Redundant with the store stamp
    /// (§4) and worth the bytes anyway: a snapshot is the one durable artifact
    /// that travels between stores, so it carries its own identity.
    codec: String,
    state: Vec<u8>,
    facets: Vec<(u8, Vec<u8>)>,
}
```

The stamp sits outside the postcard body, so a wrong-revision snapshot is refused
before any decoder runs — which is the whole reason a positional format needs an
external stamp. `codec` sits inside it, which is safe because admitting the revision is
what establishes the body's layout in the first place.

Both refusals abort the activation through the existing path (`host.rs:319`,
`decode` → `boxed`), exactly as a facet contribution that will not restore does today.
G4 applies to the composite as a whole, and a wrong-format composite is a composite
that will not restore.

---

## 7. The association handshake

The mechanism is `compat::Window::negotiate`; the work is replacing an equality check
with it, and doing so without breaking the peers that only understand the old check.

`Hello` is framed as **JSON with named fields**, deliberately, so that codec agreement
can be read before any codec-specific decoding happens (`wire.rs:112`). That decision,
made for a different reason, is what makes this change free. `serde_json` ignores
unknown fields unless `deny_unknown_fields` is set, and `Hello` does not set it. So:

```rust
pub struct Hello {
    /// Kept for peers that predate the window fields, which compare it for
    /// equality. Equals `accepts_hi`.
    pub proto_version: u32,
    /// The window this build accepts (spec §7.1).
    #[serde(default = "…")] pub accepts_lo: u16,
    #[serde(default = "…")] pub accepts_hi: u16,
    // … node, advertised, codec_name, cluster_secret unchanged
}
```

An old peer sees two fields it does not know, ignores them, and compares
`proto_version` as it always did. A new peer sees the fields missing and defaults them
to `proto_version..=proto_version`, negotiating the single revision the old peer speaks.
Both directions work with no phase gate, which makes this the cheapest of the three
changes and the first to land.

`accept_hello` (`transport.rs:198`) then replaces its version arm with
`WIRE.negotiate(peer.announced())?` and keeps the codec, secret, allowlist, and
expected-identity checks unchanged. `PROTO_VERSION` (`transport.rs:58`) becomes
`pub const WIRE: compat::Window`, and the constant that was a landmine becomes a dial.

**Where the negotiated revision goes.** Nowhere, yet, and that is deliberate. The live
behavior this step buys is the *refusal*: a future bump becomes a controlled rejection
of one peer instead of a cluster-wide mutual partition. Consuming the negotiated
revision — to downgrade a frame, or to gate a new behavior until the whole cluster can
handle it — needs a first consumer, and there is none. Adding
`Transport::peer_version` now would be a method that only relays, which is a red flag
the principles name outright.

Two extension points are worth *designing* now and building when they have a caller:

- `Transport::peer_version(NodeId) -> Option<Version>`, so the cluster layer decides
  what to send while the transport keeps owning negotiation. The simulator implementing
  it is what makes mixed-version simulation possible (§8), so the first consumer is
  likely to be a test.
- the member's announced window in `MemberDigest`, so the cluster's version spread is
  observable during a rolling upgrade and a future min-version gate has data to read.
  This is what turns V4 from a policy into a mechanism: a behavior enables itself only
  when the cluster-wide minimum has caught up.

---

## 8. What holds the policy up

Stamps are mechanism. V4 and V5 are policy, and policy that nothing checks decays.

**A golden corpus.** Per boundary, checked-in bytes produced by each revision, and a
test that decodes every one of them with the current build. This is what actually
catches the postcard footguns, because no type system will: adding a field to
`CompositeSnapshot` compiles cleanly and breaks every stored snapshot. A
`--bless`-style helper regenerates them, following the trybuild-snapshot habit already
in the tree.

**Mixed-version simulation.** The high-leverage one, and the reason to route
everything through one `Window` type: give simulated nodes *different* windows, keep
prior record and message definitions in a `compat` module behind the revision, and
assert the granary invariants and the linearizability checks hold across a rolling
upgrade and a rollback. A deterministic simulator that can run a heterogeneous cluster
is a capability almost nothing has, and it tests the thing that actually breaks in
production. Rank this above every remaining item here.

**Const-checked windows.** V3 needs no test: `Window::new` is `const` and panics, so a
window whose `writes` sits outside its `reads` fails the build.

---

## 9. Plan

Ordered by leverage over cost. Steps 1–4 are a clean break with no migration, no
sniffing, and no dual-read phase, which is the whole reason to do this now rather than
after there is data to preserve. Steps 5–6 are what make the *next* change cheap, and
they are the ones to resist deferring.

| # | Step | Touches | Spec |
|---|---|---|---|
| 1 | **Done.** The `compat` crate: `Version`, `Accepted`, `Window`, `Stamp`, `Incompatible`. No behavior change anywhere. | new `crates/compat` | new `docs/compatibility-spec.md` |
| 2 | **Done.** Handshake window. `Hello` carries `accepts: Accepted`; `accept_hello` negotiates before the codec and secret checks; `PROTO_VERSION` became `WIRE: Window`. | `wire.rs`, `transport.rs` | actor §7 rule 1 |
| 3 | **Done.** Record-envelope escape bit. Tags constrained to `0..=0x7F` at both the declaration and the write, a high-bit lead byte refused as a revision. No bytes changed. | `facet.rs` | granary §7.12, glossary |
| 4 | **Done.** Snapshot stamp: `GRSNAP` + revision outside the body, `codec` inside it, both checked before the composite is decoded. | `facet.rs`, `host.rs` | granary §7.12, compatibility §3.3 |
| 5 | **Done.** WAL header, `OpenError`, the `max_record` bound assert, and the prefix rule that keeps a crash mid-creation recoverable. | `wal/src/lib.rs`, `crash_points.rs`, `file_store.rs`, `storage.rs` | wal §2.1 (new), §3.1, §Deferred; compatibility §3.2.1 |
| 6 | Store and sidecar stamps: `store.meta` with the codec name, and `compat::stamp` on the six `atomic_replace` sidecars. | `file_store.rs:186` and callers, `storage.rs`, `blob-store/src/local.rs` | granary §7, blob-store |
| 7 | Golden corpus per boundary, with a bless helper. | `crates/*/tests/corpus/` | verification-and-validation |
| 8 | Mixed-version simulation: per-node windows, `Transport::peer_version`, invariants across a rolling upgrade and a rollback. | `actor-simulation`, `transport.rs` | simulation-testing |

Steps 2 and 3 are independent of everything else and cost almost nothing; land them
first. Step 5 is the largest single diff, because `OpenError` ripples to three call
sites. Step 8 is the one that repays the rest.

**Not in scope.** Changing the wire codec (JSON to a self-describing binary format) is
a separate decision that this design makes safe rather than makes for us: once the
store stamp records the codec (step 6) and the handshake negotiates (step 2), the swap
is a configuration change with a named failure instead of a silent one. Publishing the
facet seam for out-of-crate facets stays deferred (granary §16) — it needs the tag
registry this design reserves, so §6.1 is a prerequisite, not a substitute.
