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
| `granary.store` | 1 | `GRSTOR` and a `u16`, in the store directory's `store` file | **extension area** | [granary](granary-spec.md) §7.4 |
| `wal.frame` | 1 | The log file header's magic and frame revision | by revision; the header layout is revision-scoped | [wal](wal-spec.md) §2.1 |
| `wal.checksum` | — | The header's checksum-kind field (an identity, not a revision) | a new value for the field | [wal](wal-spec.md) §2.1 |
| `wal.reserved` | — | The header's reserved field, zero at frame revision 1 | by revision | [wal](wal-spec.md) §2.1 |
| `actor.raft.log` | 1 | The record-schema field of its log's header | the caller's own record type | actor §9.4.3 |
| `granary.store.manifest` | 1 | The record-schema field of its log's header | the caller's own record type | [granary](granary-spec.md) §7 |
| `granary.store.segment` | 1 | The record-schema field of its log's header | the caller's own record type | [granary](granary-spec.md) §7 |
| `actor.raft.term` | 1 | `\x89RFTERM\n` and a `u16`, ahead of the JSON body in the node's `term` file | **extension area** | actor §9.4.3 |
| `actor.raft.snapshot` | 1 | `\x89RFSNAP\n` and a `u16`, ahead of the postcard body in the node's `snapshot` file | **extension area** | actor §9.4.3 |
| `granary.store.fence` | 1 | `GRFNCE` and a `u16`, ahead of each `fences/<shard>` file | **extension area** | [granary](granary-spec.md) §8 |
| `granary.store.seal` | 1 | `GRSEAL` and a `u16`, ahead of each `seals/<shard>` file | **extension area** | [granary](granary-spec.md) §7.7 |
| `blob.tombstone` | 1 | `BSTOMB` and a `u16`, ahead of each `tombstones/<ns>` file | **extension area** | [blob-store](blob-store-spec.md) §5.3 |

**The `Rev` column is the newest revision *defined* at a boundary, which is not always the one a build *writes*.** The two differ for exactly as long as a bump is in flight, because **V4** widens the accepted range in one release and moves the written revision in a later one. The column tracks the definition rather than the window, because that is what a corpus must hold — a revision nothing has bytes for is a revision nothing can be shown to still read (§4) — and the window itself is stated where it lives, in the boundary's `Window`. No boundary is mid-bump today, so every row's revision is also what its build writes.

The three `Wal` record schemas have **no extension area**, because their record types belong to their callers and `wal` must not reach into `T`. A caller whose records will need additive evolution should carry a `compat::Extensions` field in `T` itself; otherwise every field it adds is a revision bump on that boundary. This is the one place §2.1's rule has to be honoured by convention rather than by construction.

**Two durable formats are deliberately unstamped**, and their absence from this table is a decision rather than an omission: a blob file (`blob-store` `local.rs`) and a grain-colocated blob (`granary` `file_store.rs`) are named by the BLAKE3 hash of their own bytes. A prefix would make the path disagree with what it addresses, and the reader that re-hashes on every read would refuse what the writer wrote. There the stamp *is* the name, and it is checked on every read rather than once per format change.

**Granularity is a cost decision.** The right granularity for a stamp is the coarsest one that still catches the error, because a stamp's cost is paid per stamped item and its value is paid once per mistake. An association is stamped once for its whole lifetime and can afford a full announced range; a snapshot is large and cold, so eight bytes and a codec name are noise; a journal record is the hottest durable path in the tree and is stamped for **zero bytes** by reserving a bit of a byte it already carries.

The rule says *coarsest that still catches the error*, and a fence file is where the second half does the work. Folding `granary.store.fence` into `granary.store`'s revision would cost nothing in bytes — one stamp already covers the directory, and it is admitted before the fences load — but it fails three ways. It couples two formats that evolve apart to one number (**V6**). It puts a fence file's identity in a *different file*, so a fence restored from a backup or left by a half-finished upgrade no longer says what it is, which is the case **V1** exists for. And §3.4's adoption arm stamps an unstamped directory without inspecting anything, so the store's revision would assert a fence layout nothing had looked at — on the one boundary whose misread costs a safety property rather than a decode. The per-item cost that would argue the other way rounds to nothing here: one file per shard, rewritten only when the term advances, on a write whose expense is an fsync of a block it already occupies.

### 3.1 `actor.wire`

The association handshake announces `Accepted` and negotiates (actor §7, rule 1). A peer whose range does not overlap is refused before its codec name, cluster secret, or identity is examined, so a version skew is never reported as a security failure — the two have very different operator responses.

The negotiated revision reaches a caller as **`Transport::peer_version`**, which is the *send-side gate* a rolling upgrade needs: a node that speaks revision 2 must not send a revision-2 frame to a peer that settled on 1, and this is what it asks before composing one. `Frame` is a serde enum, so a peer receiving a variant it does not know fails to decode and the association is torn down; negotiation lets that be *detected* early, and the gate is what *avoids* it.

The answer belongs to the **association**, not to the peer. A revision is what two ends settled on when they handshook, so it is stored on the connection frames travel over and dies with it — a value cached per peer would outlive that peer being restarted onto a narrower window, which is the case the gate exists for. Two consequences follow, and a caller must handle both:

- **`None` means *not yet known*, never *anything goes*.** A caller that reads `None` writes what the oldest revision in its own accepted range could read. Guessing upward is the misparse **V2** exists to prevent.
- **The first send to a peer always reads `None`**, because establishing the association is what a send does. A gate is therefore a *steady-state* optimization: the first frame to a peer goes out conservatively, and everything after it is gated.

The TCP transport stores the revision beside the outbound queue and reports it once the dial has handshook; the accepted (receive-only) connection's revision is discarded, since it governs what the peer may write to *us*. The simulator has no handshake, so it negotiates the two nodes' windows on demand and treats a partition as no association — `SimNetwork::set_wire_window` is what gives a simulated node a window of its own, and so what makes a mixed-version run possible at all (§4).

**A frame family grows by appending a variant, never by editing one.** Under a positional encoding an edited variant changes the layout of everything after it, and a peer a release behind reads every subsequent frame as a different one — it compiles, it round-trips against itself, and both ends agree right up until they are not the same build. An appended variant leaves each prior definition exactly as the release that shipped it saw it, which is what makes the prior definition something a peer can still be sent. `actor-cluster`'s `frame_discriminants_are_stable` is what notices an insertion, since nothing in the type system can and no round-trip test can either — a round-trip only ever asks one build to agree with itself. It is worth noting what the golden corpus does *not* cover here: `actor.wire`'s fixture pins the `Hello` preamble, so the frame vocabulary's own layout has no checked-in bytes and that test stands in their place.

Three rules will meet at such a variant, and each is a different question:

- **May this build write it?** `Window::writes()`. A release writes one revision whatever a peer would tolerate, so a build mid-bump composes the newer form for nobody.
- **May this peer read it?** `Transport::peer_version`. The association must have settled at or above the frame's revision, and `None` withholds.
- **May this cluster read it?** Nothing answers this yet; §5 is where it is deferred. Neither of the first two can speak for a member this node holds no association with.

A withheld frame is **dropped**, not queued or downgraded, and that is only sound for a frame whose loss is already tolerated. A frame carrying something that must arrive needs a revision-1 form to fall back to — which is what **V4**'s two releases buy, and why the ordering is not optional.

**A design note for whoever takes the first bump: only first-hand evidence can retract.** Any scheme where a node *announces* something about its own release — a range, a capability, a feature flag — has a hole at the rollback. A node that rolls back to a release predating the announcement runs a build that cannot announce anything at all, so its peers keep believing whatever they last heard, and anything gated on that belief stays on for a member that can no longer read what it writes. No announcement protocol fixes it, because the node that would have to send the correction is exactly the one that no longer speaks the revision the correction would ride on. The association does: negotiation settles at the lower of the two ceilings, so an association settling *below* this build's own proves where the peer's ends, without the peer's cooperation. A design that propagates such a claim must therefore carry two beliefs per member — what it was told and what it has seen — and relay the narrowed result, or it will be correct on the way up and wrong on the way back down.

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

The body also records the **codec that encoded facet 0's contribution**, which is the one part of a snapshot that is not codec-independent (granary §4.1, §5). A mismatch is refused naming both codecs. This is not a revision check and does not use `Version`: the codec is an identity, not an ordering. The `granary.store` stamp (§3.4) closes the same gap for a whole store and catches it earlier — at open, rather than when a grain activates — so in a store this check is the second line. A snapshot keeps its own copy regardless, because it is the one durable artifact that can travel *between* stores, where the store's answer does not follow it.

The body carries an **extension area** (§2.1). The composite is `postcard`, so without one a single added field — a compression marker, a provenance note, a per-facet digest — would be a revision bump.

| Key | Criticality | Meaning |
| --- | --- | --- |
| `0x8001` | critical | Facet 0's state travelled as content-addressed chunks; the value is the manifest naming them, and the body's inline state is empty (granary §7.12). |

The one entry defined so far shows why the criticality bit is carried per key rather than per format. Chunking the state adds a *carriage* and reinterprets no byte already defined, so it is an extension, not a revision — a snapshot without the entry means exactly what it always meant, and a build that predates the key still reads every snapshot written before it. But a reader that *skipped* the entry would find the inline state empty and rebuild the grain from a default `State`: total, silent data loss that no later check would catch. So the key is critical, and a build without it refuses the whole snapshot and aborts the activation. That is the shape every future entry should be judged against: ancillary if ignoring it leaves the reader where it was, critical if ignoring it changes what the bytes mean.

### 3.4 `granary.store`

A magic and a revision in a `store` file at the root of a node's grain-store directory, holding the name of the **deployment codec that encoded everything under it** (granary §7.4). It is admitted at `open`, before the fences, the manifest, or any grain's records are read.

The boundary it closes is a gap §3.3 leaves. A grain's *event payloads* are user types under the deployment's codec (granary §4.1, §5) — facet payloads are `postcard` by construction, and a snapshot carries its own copy of the codec that wrote facet 0 — so a grain with records past its last snapshot, or with no snapshot at all, has nothing that would notice the codec changing. Every one of those records would fail to decode, and each failure would surface as its own corrupt-grain activation abort. The store stamp turns that storm into **one refusal at startup naming both codecs**, which is what the situation actually is: a configuration change, not a corrupt store.

The granularity is the directory, not the record (§3, *granularity is a cost decision*). Every record under one store is written by one deployment's codec, so the question is answered once per store rather than once per record on the hottest durable path in the tree.

**An unstamped directory is adopted, not refused.** A store predating the stamp opens, and the codec running at that moment is written down. Adoption cannot verify what it records, so a store whose codec was *already* swapped is stamped with the wrong answer and its records still fail one grain at a time. That is the honest limit: the stamp guards every swap after it and cannot retroactively guard one that already happened. Refusing instead would make the check a migration for every existing directory, which is the cost a stamp exists to avoid.

### 3.5 `actor.raft.term`

A magic and a revision ahead of the JSON body in a voter's `term` file (actor §9.4.3). It is the one boundary in the registry where the stamp sits in front of a body that could already detect a problem on its own, and the two checks are deliberately kept apart.

**The body stays JSON, and the stamp does not replace what that buys.** JSON's structural redundancy is the *corruption* check: a damaged byte fails to parse rather than decoding to a different valid term, which is why the term file stayed JSON while the snapshot beside it went `postcard`. Election safety rests on that — a term read wrongly is a second vote in a term already voted in. The stamp answers a different question: whether these bytes are this format at all.

**The two refusals must not read alike**, because their fixes are opposite. A corrupt term tells the operator to restore or remove the node's state and rejoin it as a new member, which throws away its vote history. A version skew tells them to run the node on a build that accepts the file, and to *keep* that state. Before the stamp, a build skew would have been reported as corruption, and an operator following that advice would have destroyed recoverable state to fix a binary they only had to roll back.

**The extension area is not redundant with JSON's own tolerance.** JSON already ignores a field a reader does not know, which is the ancillary half of §2.1 and comes free. The half it cannot express is criticality — *a reader that does not know this MUST refuse* — and the term file is precisely where a silently-ignored field costs safety: a pre-vote term or a lease written here and skipped by an older build is a double vote waiting to happen.

**An unstamped file is adopted**, and unlike §3.4's adoption this one verifies nothing and claims nothing: the predecessor is read by the same JSON decoder it always was, and the stamp appears on the next ordinary write.

---

## 4. What holds the policy up

Stamps are mechanism; **V4** and **V5** are policy, and policy nothing checks decays.

**A golden corpus** — per boundary, checked-in bytes from each revision and a test that decodes every one with the current build. It catches the failure a type system cannot see: adding a field to a `postcard` struct, or reordering two of them, compiles cleanly and breaks every stored copy at once, with no diagnostic anywhere — a reordered struct makes a log recover as *empty*, which every layer above reads as a grain that was never written to.

Fixtures live beside the format that owns them, at `crates/<owner>/corpus/<boundary>/v<revision>.bin`, and each is decoded by a test next to that format's own code: `compat` owns no format and reads no file (§2), so it cannot do the decoding itself. What it owns is the **completeness gate** (`crates/compat/tests/golden_corpus.rs`), which parses the §3 table above and holds the tree to it in both directions — a boundary in the registry with no corpus at its current revision fails, and so does a fixture naming a boundary the registry does not. That is what makes §3's "a new durable or wire format MUST appear here" carry an obligation a later build can act on, rather than only a place to write the name down.

**The fixtures are evidence, not output.** A file records what a revision's bytes meant, so regenerating one turns a caught format break into a green run — the one way a corpus can fail silently. `GOLDEN_UPDATE=1` therefore writes only a fixture that is *absent*, the case of adding a revision, and never rewrites one that exists. A fixture that stops decoding is the corpus working, and the fix is **V4**'s rather than the file's: widen the window, keep the old decoder, add the new revision's bytes beside it.

What the corpus deliberately does **not** assert is that the current build re-encodes a fixture byte-for-byte. Under **V4** a build reads revisions it no longer writes, so byte equality would fail on precisely the upgrade the policy prescribes; and a boundary may change its bytes compatibly without changing their meaning at all (a log records which digest wrote it and reads both, wal §2.1). Decoding old bytes to the right *value* is the property. Reproducing them is not.

**Mixed-version simulation** — simulated nodes given *different* windows, prior record and message definitions kept behind their revision, and the granary invariants and linearizability checks asserted across a rolling upgrade and a rollback. This is the reason to route every boundary through one `Window` type.

Two pieces of it exist. `crates/actor-simulation/tests/conformance_compatibility.rs` pins the negotiation node by node — an upgrade, a mixed cluster, a rollback, and a refusal — against widened windows standing in for a bump not yet taken. Above it, `conformance_mixed_version_swarm.rs` sweeps a `Rollout`: the nemesis walks nodes one release at a time, forward and back, while partitions, crashes, freezes and loss run underneath, and the asserted property is that **no node is ever sent a form of a message its build cannot read**. A `Rollout` is checked when it is built, so a stage sequence that is not a legal upgrade path — adjacent releases that share no revision, or one that does not accept what its neighbour writes — is refused rather than failing a sweep for the wrong reason.

**The honest limit is that no boundary in the tree has a second revision.** The revision-varying behavior in that sweep is therefore the *workload's own* — a synthetic `Form` on its own message — which makes the send-side gate falsifiable and catches a build that ignores it, while nothing in `crates/` is itself kept behind a revision. What that leaves is a rolling upgrade whose *churn* is real and whose *bytes* are not: releases, restarts and renegotiation all happen, and no format changes underneath them. No granary invariant or linearizability check is asserted across the transition either.

This is a deliberate position rather than an unfinished one. A revision invented to have something to exercise is a revision every later build carries under **V5**, for a rollout nobody is performing; the tree has no production deployment to de-risk. What is built ahead of need is only what costs nothing until used — the window type, the corpus and its completeness gate, `Transport::peer_version`, `SimNetwork::set_wire_window` and the `Rollout` vocabulary — so that the release which first changes a format inherits them rather than writing them under the pressure of the change. Machinery that would have to be *maintained* in the meantime, and could not fire until that release, is deferred with the revision it serves (§5).

---

## 5. Deferred

Boundaries that exist as formats but are **not yet stamped**, in priority order. Each is a place where a format change today would be a migration rather than an edit.

- **The shard map's commands** — `ShardMapCommand` (granary §7.6) is `serde_json` inside `EntryPayload::App`, riding the already-stamped `actor.raft.log`. That log's revision covers the envelope, not the payload, so the shard map has no boundary of its own. The consequence is worse than an unstamped format: `decode` swallows every failure with `.ok()`, so a command a build does not recognize is **silently skipped at apply time on that node alone** — one node applies a split commit and its peer does not, and the two disagree about which node owns a range. That is state divergence on a consensus apply path, which running the shard map through consensus was supposed to make impossible, and it violates **V2** directly: input a build cannot read is being skipped rather than refused. Fail-closed first (a node that cannot apply a committed entry must stop, as `actor.raft.log` already reasons), a `granary.shardmap` boundary around the payload second. This needs the mixed-version machinery of §4 to be believable, not a corpus fixture, which is why it is not format work.
- **Raising a log's record-schema stamp in place** — a `Wal` appends frames at the revision its build writes without updating the header, so once a caller's window spans two revisions the stamp understates until a compaction restamps it (wal §2.1 rule 5). Fail-closed, and a caller can work around it by compacting; removing the caveat means a second, non-append handle to rewrite two bytes of the header on the first append after a bump.
- **A cluster-wide minimum revision** — carrying each member's announced range in the membership digest, so a behavior can enable itself only once the whole cluster accepts it. This turns **V4** from a policy into a mechanism. It is deferred rather than built because it is a mechanism with no caller until a boundary has a second revision, and because the design has a trap worth reading §3.1's note about before starting: only first-hand evidence can retract, so a node that rolls back cannot report its own retraction.
