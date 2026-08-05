# Format Compatibility: Specification

**Status:** Draft v1
**Scope:** How a format in this workspace declares which revision it is, which revisions a build accepts, and what happens outside that range. This document owns the *compatibility vocabulary* and the **registry of boundaries** (§3). It owns no format: each format's layout stays in the spec of the crate that writes it.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `compat` is the crate and namespace name. Sections of this document are cited as plain **§N**. Invariants defined here are numbered **V1, V2, …**.

> **Design stance.** Every format in the tree is produced by one build and consumed by another — across the network during a rolling upgrade, and across time on disk, where a journal written today is read by next quarter's binary. At each such place one question decides whether the system is safe: can the reader tell what it is looking at, and does it refuse when it cannot? A **misparse** converts a version skew, which an operator diagnoses in a minute, into silent corruption, which they cannot. So the machinery is small and lives in one place: a revision, a range, and a named refusal. It is not a schema-evolution framework: field-level compatibility rules tell you that v1 bytes parse in v2, and say nothing about when it is safe to start writing v2 or whether v1 can still read what v2 wrote.

---

## 1. Compatibility boundaries

A **compatibility boundary** is a place where bytes produced by one build are consumed by another. Each boundary has a stable dotted name (§3), and at each one a build declares:

- the inclusive range of revisions it **accepts**, and
- the single revision it **writes**.

A boundary needs three things, and needs nothing else:

1. a **stamp** — the bytes, or the peer, say which revision they are;
2. a **window** — the accepted range and the written revision, above;
3. a **verdict** — outside the window, the boundary refuses.

### Invariants

| | |
|---|---|
| **V1** | Every durable byte sequence and every association is stamped at exactly one boundary. No format is stamped twice, and none is unstamped. |
| **V2** | A build MUST refuse input or a peer outside its window, naming the boundary, what it found, and what it accepts. It MUST NOT attempt to parse it. |
| **V3** | For every window, the written revision lies within the accepted range. A build that cannot read what it wrote cannot be rolled back onto its own data. |
| **V4** | No release writes revision *V* until every release that may still read those bytes accepts *V*. **Read-new first, write-new later:** a format change widens the accepted range in one release, and moves the written revision in a later one. |
| **V5** | A revision stays accepted for at least **two releases** after the release that stopped writing it (durable boundaries) or **one** (wire boundaries). Durable is longer because a journal outlives the process that wrote it, while an association ends with the peers that formed it. |
| **V6** | Boundaries are revisioned independently. A wire bump does not invalidate a journal, and a durable bump does not invalidate an association. |

**V3 is enforced at compile time.** `compat::Window` is constructed `const` and asserts the property, so a window whose written revision falls outside its accepted range fails the build rather than surfacing on a rollback.

**V5 counts releases, not revisions.** A revision is per-boundary and does not advance when a release ships that leaves the format alone, so a boundary sitting at revision 1 across ten releases has no "N−2" to accept. The obligation is a *duration*: how long a retired revision must still be readable. The ranges in this tree are mostly a single revision wide because nothing has been retired yet.

**V4 and V5 are policy**, and only tests hold policy up: a golden corpus per boundary, and mixed-version simulation, are how (§4).

---

## 2. The vocabulary

The `compat` crate is the vocabulary and nothing else: it owns no format, reads no file, and depends on nothing else in the tree, so it can sit below `wal` (itself below its callers, [wal](wal-spec.md) §1) and every boundary in the workspace shares one definition of "compatible" and one wording for the refusal.

- **`Version`** — a revision at one boundary. Meaningless alone (**V6**): a wire revision 3 and a snapshot revision 3 are unrelated, which is why every refusal carries the boundary's name.
- **`Accepted`** — an inclusive range of revisions, one side of a check. Kept apart from `Window` because a peer announces what it can *read* and never what it will *write*: that choice is neither knowable nor trustworthy across a boundary.
- **`Window`** — one build's policy at one boundary: the accepted range, the written revision, and the boundary's name. The name lives here rather than at each call so that no refusal can be raised without it.
- **`Stamp`** — a magic and a revision written ahead of a body, for a boundary with bytes at rest. The stamp sits **outside** the body's own encoding: a positional format such as `postcard` cannot version itself, having no field name to make optional and no discriminant to grow, so reading the revision before any decoder runs is what makes refusal-instead-of-misparse possible. The magic travels with the window in one value, so a writer and a reader cannot disagree about it.
- **`Incompatible`** — the refusal, in three shapes: `Unstamped` (not this format at all), `Version` (stamped, outside the range), and `Disjoint` (two ranges with nothing in common). No caller branches on the shape — they propagate and report it — so the wording lives in one place instead of at each boundary.

Two verdicts, and they are the same comparison:

- **`admit`** is one-sided, for bytes at rest: accept the found revision, or refuse it.
- **`negotiate`** is two-sided, for a live peer: the highest revision both ends accept, or a refusal when the ranges are disjoint. Both ends compute it from the same two ranges and reach the same answer, so a handshake needs no confirmation round.

---

## 2.1 Negotiated and durable boundaries need different things

A **negotiated** boundary — the association handshake — can afford a strict format: both ends are present when they speak, each states what it accepts, and either may refuse.

A **durable** boundary has no counterparty. Its reader is a build that does not exist yet, and by the time it runs the writer is gone, so the bytes must carry everything needed to interpret them or be refused. Under a positional encoding such as `postcard` — no field names, no discriminants to grow — every added field is otherwise a new revision, with a second decoder to keep for the life of the support window.

So a durable envelope MUST carry an **extension area** (`compat::Extensions`), under the criticality rule:

- a key with the **critical** bit (`0x8000`) set MUST be understood; a reader that does not know it refuses the whole input (**V2**);
- any other key MAY be ignored, so a reader predating it behaves exactly as it did before.

This is PNG's ancillary/critical chunk rule. A **revision** says *these bytes are a different format*; an **extension** says *these bytes are the same format, carrying more*. A change that reinterprets bytes already defined MUST be a revision, never an extension.

An empty area costs one byte, so the headroom is free until used. Entries are held in key order, so an envelope's bytes depend on its content and not on the order its fields were written — durable bytes that vary run to run are a poor foundation for content addressing and for the byte-comparison a golden corpus does (§4).

**Where this does not apply.** Not every durable format needs an area of its own:

- A **`postcard` enum** grows variants at its end safely, an old reader failing closed on a variant it does not know. That is the extension mechanism for facet payloads, and it costs nothing.
- A **record** on a hot path may be too small to carry one. `granary.record` reserves an escape *bit* instead, spending zero bytes and accepting that a change there is a revision.

The rule is therefore: a durable envelope carries an extension area unless it has a cheaper mechanism or cannot afford the byte, and the boundary's entry in §3 says which.

**A decoder MUST admit the area before using the body.** The check is a call (`Extensions::admit`) rather than something the encoding enforces, so skipping it compiles and ignores a critical entry — the misread the criticality bit exists to prevent. Every boundary with an area names its known keys next to its window, and the golden corpus (§4) is what should catch a decoder that forgets.

---

## 3. The boundary registry

Every boundary in the tree, its name, and where its layout is specified. A new durable or wire format MUST appear here.

| Boundary | Rev | Stamp | How it grows (§2.1) | Layout owner |
|---|---|---|---|---|
| `actor.wire` | 1 | The accepted range, announced in the `Hello` preamble | negotiated — no area needed | [actor](distributed-actor-spec.md) §7.1 |
| `granary.record` | 1 | The envelope's leading byte, whose high bit is the revision escape | enum variants in the payload; the envelope itself by revision | [granary](granary-spec.md) §7.12 |
| `granary.snapshot` | 1 | `GRSNAP` and a `u16`, ahead of the composite's body | **extension area** | [granary](granary-spec.md) §7.12 |
| `wal.frame` | 1 | The log file header's magic and frame revision | by revision; the header layout is revision-scoped | [wal](wal-spec.md) §2.1 |
| `wal.checksum` | — | The header's checksum-kind field (an identity, not a revision) | a new value for the field | [wal](wal-spec.md) §2.1 |
| `wal.reserved` | — | The header's reserved field, zero at frame revision 1 | by revision | [wal](wal-spec.md) §2.1 |
| `actor.raft.log` | 1 | The record-schema field of its log's header | the caller's own record type | actor §9.4.3 |
| `granary.store.manifest` | 1 | The record-schema field of its log's header | the caller's own record type | [granary](granary-spec.md) §7 |
| `granary.store.segment` | 1 | The record-schema field of its log's header | the caller's own record type | [granary](granary-spec.md) §7 |

The three `Wal` record schemas have **no extension area**, because their record types belong to their callers and `wal` must not reach into `T`. A caller whose records will need additive evolution should carry a `compat::Extensions` field in `T` itself; otherwise every field it adds is a revision bump on that boundary. This is the one place §2.1's rule has to be honoured by convention rather than by construction.

**Granularity is a cost decision.** The right granularity for a stamp is the coarsest one that still catches the error, because a stamp's cost is paid per stamped item and its value is paid once per mistake. An association is stamped once for its whole lifetime and can afford a full announced range; a snapshot is large and cold, so eight bytes and a codec name are noise; a journal record is the hottest durable path in the tree and is stamped for **zero bytes** by reserving a bit of a byte it already carries.

### 3.1 `actor.wire`

The association handshake announces `Accepted` and negotiates (actor §7, rule 1). A peer whose range does not overlap is refused before its codec name, cluster secret, or identity is examined, so a version skew is never reported as a security failure — the two have very different operator responses.

The negotiated revision is currently **discarded** after the verdict, so **this boundary cannot yet be bumped.** A rolling upgrade also needs a *send-side gate* — a node that speaks revision 2 must not send a revision-2 frame to a peer that settled on 1 — and there is nothing to gate on, because the negotiated value reaches no caller. `Frame` is a serde enum, so a peer receiving a variant it does not know fails to decode and the association is torn down; negotiation lets that be *detected* early, not *avoided*.

What exists today is the refusal: a future bump is a controlled rejection of one peer rather than the cluster-wide mutual partition an equality check guarantees. Running two wire revisions at once needs the negotiated revision surfaced to the cluster layer (`Transport::peer_version`, §5). **Anyone planning a wire-format change should expect to build it first.**

### 3.2 `granary.record`

Revision 1 is `[tag][payload]` with the tag in `0..=0x7F`. A leading byte at or above `0x80` is the escape into a later revision, which revision 1 never writes, so:

- every record ever written is already a stamped revision-1 record, and the stamp costs nothing;
- a record from a later revision is refused *as a revision*, not reported as a facet this build is missing;
- the write path panics on an escape byte, mirroring the read path's refusal. The two paths must agree on what a valid record is, which is the same discipline `wal` applies to an oversized frame (wal §2, §4).

This versions the **envelope**, not a facet's payload schema. Those evolve separately: `postcard` appends enum variants safely, so a facet's operation enum grows at its end.

### 3.2.1 A boundary owned by two parties

The write-ahead log's header (wal §2.1) is the one place a single stamp is split between the crate that owns the *layout* and the caller that owns the *records*. `wal.frame` and `wal.checksum` are the crate's: they say how a frame is shaped and which digest closes it. `actor.raft.log`, `granary.store.manifest`, and `granary.store.segment` are the callers': one per log, because the logs evolve independently — adding a field to a segment op says nothing about a manifest entry.

`Wal<T>`'s records are `postcard`, so a caller's revision has nowhere to live but outside the payload (wal §2.1 rule 5). `wal` stores the field and returns it without interpreting it, so one crate need not know every caller's schema.

`actor.raft.log` is the strictest boundary in the tree: its records are the consensus history, and a node that cannot read its own log cannot rejoin without losing committed state. **V4** admits no exception there.

### 3.3 `granary.snapshot`

A magic and a revision ahead of the composite's `postcard` body (granary §7.12). The revision is admitted before the body is decoded, so a composite from another revision is refused rather than misparsed.

The body also records the **codec that encoded facet 0's contribution**, which is the one part of a snapshot that is not codec-independent (granary §4.1, §5). A mismatch is refused naming both codecs. This is not a revision check and does not use `Version`: the codec is an identity, not an ordering. A `granary.store` stamp (§5) would close the same gap for a whole store; a snapshot carries its own copy because it is the one durable artifact that can travel between stores.

The body carries an **extension area** (§2.1). The composite is `postcard`, so without one a single added field — a compression marker, a provenance note, a per-facet digest — would be a revision bump.

| Key | Criticality | Meaning |
| --- | --- | --- |
| `0x8001` | critical | Facet 0's state travelled as content-addressed chunks; the value is the manifest naming them, and the body's inline state is empty (granary §7.12). |

The one entry defined so far shows why the criticality bit is carried per key rather than per format. Chunking the state adds a *carriage* and reinterprets no byte already defined, so it is an extension, not a revision — a snapshot without the entry means exactly what it always meant, and a build that predates the key still reads every snapshot written before it. But a reader that *skipped* the entry would find the inline state empty and rebuild the grain from a default `State`: total, silent data loss that no later check would catch. So the key is critical, and a build without it refuses the whole snapshot and aborts the activation. That is the shape every future entry should be judged against: ancillary if ignoring it leaves the reader where it was, critical if ignoring it changes what the bytes mean.

---

## 4. What holds the policy up

Stamps are mechanism; **V4** and **V5** are policy, and policy nothing checks decays.

**A golden corpus** — per boundary, checked-in bytes from each revision and a test that decodes every one with the current build. It catches the failure a type system cannot see: adding a field to a `postcard` struct, or reordering two of them, compiles cleanly and breaks every stored copy at once, with no diagnostic anywhere — a reordered struct makes a log recover as *empty*, which every layer above reads as a grain that was never written to.

Fixtures live beside the format that owns them, at `crates/<owner>/corpus/<boundary>/v<revision>.bin`, and each is decoded by a test next to that format's own code: `compat` owns no format and reads no file (§2), so it cannot do the decoding itself. What it owns is the **completeness gate** (`crates/compat/tests/golden_corpus.rs`), which parses the §3 table above and holds the tree to it in both directions — a boundary in the registry with no corpus at its current revision fails, and so does a fixture naming a boundary the registry does not. That is what makes §3's "a new durable or wire format MUST appear here" carry an obligation a later build can act on, rather than only a place to write the name down.

**The fixtures are evidence, not output.** A file records what a revision's bytes meant, so regenerating one turns a caught format break into a green run — the one way a corpus can fail silently. `GOLDEN_UPDATE=1` therefore writes only a fixture that is *absent*, the case of adding a revision, and never rewrites one that exists. A fixture that stops decoding is the corpus working, and the fix is **V4**'s rather than the file's: widen the window, keep the old decoder, add the new revision's bytes beside it.

What the corpus deliberately does **not** assert is that the current build re-encodes a fixture byte-for-byte. Under **V4** a build reads revisions it no longer writes, so byte equality would fail on precisely the upgrade the policy prescribes; and a boundary may change its bytes compatibly without changing their meaning at all (a log records which digest wrote it and reads both, wal §2.1). Decoding old bytes to the right *value* is the property. Reproducing them is not.

**Mixed-version simulation** — simulated nodes given *different* windows, prior record and message definitions kept behind their revision, and the granary invariants and linearizability checks asserted across a rolling upgrade and a rollback. This is the reason to route every boundary through one `Window` type. *Not yet implemented.*

---

## 5. Deferred

Boundaries that exist as formats but are **not yet stamped**, in priority order. Each is a place where a format change today would be a migration rather than an edit.

- **`granary.store`** — a grain's **event payloads** are encoded with the deployment's codec, and nothing on disk records which codec that was. §3.3 closes this for snapshots, but a grain with records past its last snapshot — or none at all — is still unguarded, so swapping the codec turns those records into a storm of corrupt-grain activation aborts rather than one diagnosable configuration error. A store-level stamp catches every record and snapshot at once, and is worth having *before* a codec swap rather than after.
- **Sidecars** — the Raft term and snapshot pointers, the shardmap, and the blob-store tombstones are durable formats written through `wal::atomic_replace` with no stamp. `compat::Stamp` wraps them without changing the primitive, keeping its opaque-bytes interface intact.
- **`Transport::peer_version`** — the negotiated wire revision, surfaced so the cluster layer can gate what it sends. Until it exists, `actor.wire` can refuse an incompatible peer but cannot run two revisions at once, and so cannot be bumped (§3.1). First in priority order, because it also unblocks the mixed-version simulation of §4.
- **Raising a log's record-schema stamp in place** — a `Wal` appends frames at the revision its build writes without updating the header, so once a caller's window spans two revisions the stamp understates until a compaction restamps it (wal §2.1 rule 5). Fail-closed, and a caller can work around it by compacting; removing the caveat means a second, non-append handle to rewrite two bytes of the header on the first append after a bump.
- **A cluster-wide minimum revision** — carrying each member's announced range in the membership digest, so a behavior can enable itself only once the whole cluster accepts it. This turns **V4** from a policy into a mechanism.
