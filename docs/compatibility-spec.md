# Format Compatibility: Specification

**Status:** Draft v1
**Scope:** How a format in this workspace says which version it is, and what a build does with bytes or a peer at any other version. This document owns the *stamp vocabulary* and the **registry of formats** (§3). It owns no format: each format's layout stays in the spec of the crate that writes it.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Throughout, `compat` is the crate and namespace name. Sections of this document are cited as plain **§N**. Invariants defined here are numbered **V1, V2, …**.

> **Design stance.** Every format in the tree is produced by one build and consumed by another: across the network between two nodes, and across time on disk, where a journal written today is read by a later binary. At each such place one question decides safety: can the reader tell what it is looking at, and does it refuse when it cannot? A **misparse** turns a version skew, which an operator diagnoses in a minute, into silent corruption, which they cannot. So the machinery is small and lives in one place: a version, a comparison, and a named refusal. A build reads exactly the version it writes; there is no accepted range, no negotiation, and no second decoder for an older form. A format change is a new version that every node and every data directory takes at once.

---

## 1. Formats and stamps

A **format** is a byte layout produced by one build and consumed by another. Each has a stable dotted name (§3) and, in each build, exactly one **version**.

A format needs three things and nothing else:

1. a **stamp**: the bytes, or the peer, say which version they are;
2. a **version**: the one this build reads and writes;
3. a **verdict**: anything else, the format refuses.

### Invariants

| | |
|---|---|
| **V1** | Every durable byte sequence and every association is stamped at exactly one format. No format is stamped twice, and none is unstamped. |
| **V2** | A build MUST refuse input or a peer at any version but its own, naming the format, what it found, and what it reads. It MUST NOT attempt to parse it. |

---

## 2. The vocabulary

The `compat` crate is the vocabulary and nothing else: it owns no format, reads no file, and depends on nothing else in the tree. So it can sit below `wal` (itself below its callers, [wal](wal-spec.md) §1), and every format in the workspace shares one definition of "readable" and one wording for the refusal.

- **`Version`**: a version at one format. Meaningless alone: a wire version 3 and a snapshot version 3 are unrelated, which is why every refusal carries the format's name.
- **`Format`**: one build's policy at one format, its name and the version it reads and writes. The name lives here rather than at each call so that no refusal can be raised without it. `admit` is the verdict: the found version is this build's, or it is refused.
- **`Stamp`**: a magic and a version written ahead of a body, for a format with bytes at rest. The stamp sits **outside** the body's own encoding: a positional format such as `postcard` cannot version itself, having no field name to make optional and no discriminant to grow, so reading the version before any decoder runs is what makes refusal-instead-of-misparse possible. The magic travels with the format in one value, so a writer and a reader cannot disagree about it. `stamp` writes the prefix; `unstamp` admits it and returns the body.
- **`Incompatible`**, the refusal, in two shapes: `Unstamped` (not this format: the magic is missing, or the input is too short to carry one) and `Version` (this format, at another version). No caller branches on the shape; they propagate and report it, so the wording lives in one place instead of at each format.

**A decoder MUST admit the stamp before decoding the body.** The check is a call rather than something the encoding enforces, so a reader that decodes first compiles and misparses. Every format with a stamp admits it as the first thing it does with the bytes.

---

## 3. The registry

Every format in the tree, its name, and where its layout is specified. A new durable or wire format MUST appear here.

| Format | Version | Stamp | Layout owner |
|---|---|---|---|
| `actor.wire` | 1 | The version announced in the `Hello` preamble | [actor](distributed-actor-spec.md) §7.1 |
| `entity.record` | 1 | The envelope's leading byte, whose high bit marks another version | [entity](entity-spec.md) §9 |
| `entity.snapshot` | 1 | `GRSNAP` and a `u16`, ahead of the composite's body | [entity](entity-spec.md) §7.12 |
| `entity.store` | 1 | `GRSTOR` and a `u16`, in the store directory's `store` file | [entity](entity-spec.md) §7.4 |
| `wal.frame` | 1 | The log file header's magic and frame version | [wal](wal-spec.md) §2.1 |
| `actor.raft.log` | 1 | The record-schema field of its log's header | actor §9.4.3 |
| `entity.store.manifest` | 1 | The record-schema field of its log's header | [entity](entity-spec.md) §7 |
| `entity.store.segment` | 1 | The record-schema field of its log's header | [entity](entity-spec.md) §7 |
| `actor.raft.term` | 1 | `\x89RFTERM\n` and a `u16`, ahead of the JSON body in the node's `term` file | actor §9.4.3 |
| `actor.raft.snapshot` | 1 | `\x89RFSNAP\n` and a `u16`, ahead of the postcard body in the node's `snapshot` file | actor §9.4.3 |
| `entity.store.fence` | 1 | `GRFNCE` and a `u16`, ahead of each `fences/<shard>` file | [entity](entity-spec.md) §8 |
| `entity.store.seal` | 1 | `GRSEAL` and a `u16`, ahead of each `seals/<shard>` file | [entity](entity-spec.md) §7.7 |
| `blob.tombstone` | 1 | `BSTOMB` and a `u16`, ahead of each `tombstones/<ns>` file | [blob-store](blob-store-spec.md) §5.3 |
| `entity.shardmap` | 1 | `GRSMAP` and a `u16`, ahead of the JSON payload in each committed `EntryPayload::App` | [entity](entity-spec.md) §7.6 |
| `actor.membership` | 1 | `ACMEMB` and a `u16`, ahead of the tag-plus-node payload in each committed `EntryPayload::App` | actor §9.4.3 |

`entity.shardmap` and `actor.membership` are payloads riding the already-stamped `actor.raft.log`, whose version covers the envelope and not them, so each carries a stamp of its own.

**Two durable formats are deliberately unstamped**: a blob file (`blob-store` `local.rs`) and an entity-colocated blob (`entity` `file_store.rs`) are named by the BLAKE3 hash of their own bytes. A prefix would make the path disagree with what it addresses, and the reader that re-hashes on every read would refuse what the writer wrote. There the stamp *is* the name, and it is checked on every read rather than once per format change.

**Granularity is a cost decision.** A stamp should be the coarsest one that still catches the error, because its cost is paid per stamped item and its value once per mistake. An association is stamped once for its whole lifetime; a snapshot is large and cold, so eight bytes and a codec name are noise; a journal record is the hottest durable path in the tree and is stamped for **zero bytes** by reserving a bit of a byte it already carries. A fence file keeps a stamp of its own rather than borrowing the store directory's, because a fence restored from a backup or copied between directories is a durable byte sequence in its own right (**V1**), and one file per shard, rewritten only when the term advances, rounds the cost to nothing.

### 3.1 `actor.wire`

The association handshake announces the wire version, and a peer at any other version is refused before its codec name, cluster secret, or identity is examined (actor §7, rule 1), so a version skew is never reported as a security failure; the two have different operator responses.

The frame vocabulary is `postcard`-positional: inserting, removing, or editing a variant changes the layout of every frame after it. Any such change is a new wire version, which every node takes at once.

### 3.2 `entity.record`

Version 1 is `[tag][payload]` with the tag in `0..=0x7F`. A leading byte at or above `0x80` marks another envelope version, which version 1 never writes, so:

- every record ever written is already a stamped version-1 record, and the stamp costs nothing;
- a record from another version is refused *as a version*, not reported as a facet this build is missing;
- the write path panics on such a byte, mirroring the read path's refusal. The two paths must agree on what a valid record is, which is the same discipline `wal` applies to an oversized frame (wal §2, §4).

This versions the **envelope**, not a facet's payload schema. Those evolve separately: `postcard` appends enum variants safely, so a facet's operation enum grows at its end.

### 3.2.1 A stamp owned by two parties

The write-ahead log's header (wal §2.1) is the one place a single stamp is split between the crate that owns the *layout* and the caller that owns the *records*. `wal.frame` is the crate's: it says how a frame is shaped. `actor.raft.log`, `entity.store.manifest`, and `entity.store.segment` are the callers', one per log, because the logs evolve independently: adding a field to a segment op says nothing about a manifest entry.

`Wal<T>`'s records are `postcard`, so a caller's version has nowhere to live but outside the payload (wal §2.1 rule 5). `wal` admits the field against the caller's `Format` and never interprets it, so one crate need not know every caller's schema.

### 3.3 `entity.snapshot`

A magic and a version ahead of the composite's `postcard` body (entity §9). The version is admitted before the body is decoded, so a composite from another version is refused rather than misparsed.

The body also records the **codec that encoded facet 0's contribution**, which is the one part of a snapshot that is not codec-independent (entity §4.1, §5). A mismatch is refused naming both codecs. This is not a version check and does not use `Version`: the codec is an identity, not an ordering. The `entity.store` stamp (§3.4) closes the same gap for a whole store and catches it earlier, at open rather than when an entity activates, so in a store this check is the second line. A snapshot keeps its own copy regardless, because it is the one durable artifact that can travel *between* stores, where the store's answer does not follow it.

### 3.4 `entity.store`

A magic and a version in a `store` file at the root of a node's entity-store directory, holding the name of the **deployment codec that encoded everything under it** (entity §7.4). It is admitted at `open`, before the fences, the manifest, or any entity's records are read; a directory with no `store` file is a fresh store, and the codec running at that moment is written down.

The gap it closes is §3.3's other half. An entity's *event payloads* are user types under the deployment's codec (entity §4.1, §5), while facet payloads are `postcard` by construction and a snapshot carries its own copy of the codec that wrote facet 0, so an entity with records past its last snapshot, or with no snapshot at all, has nothing that would notice the codec changing. Every one of those records would fail to decode, and each failure would surface as its own corrupt-entity activation abort. The store stamp turns that storm into **one refusal at startup naming both codecs**, which is what the situation is: a configuration change, not a corrupt store.

The granularity is the directory, not the record (§3, *granularity is a cost decision*). Every record under one store is written by one deployment's codec, so the question is answered once per store rather than once per record on the hottest durable path in the tree.

### 3.5 `actor.raft.term`

A magic and a version ahead of the JSON body in a voter's `term` file (actor §9.4.3). It is the one format in the registry where the stamp sits in front of a body that could already detect a problem on its own, and the two checks are deliberately kept apart.

**The body stays JSON, and the stamp does not replace what that buys.** JSON's structural redundancy is the *corruption* check: a damaged byte fails to parse rather than decoding to a different valid term, which is why the term file stayed JSON while the snapshot beside it went `postcard`. Election safety rests on that: a term read wrongly is a second vote in a term already voted in. The stamp answers a different question: whether these bytes are this format at all.

**The two refusals must not read alike**, because their fixes are opposite. A corrupt term tells the operator to restore or remove the node's state and rejoin it as a new member, which throws away its vote history. A version skew tells them to run the node on the build that wrote the file, and to *keep* that state. Without the stamp, a build skew would be reported as corruption, and an operator following that advice would destroy recoverable state to fix a binary they only had to swap.

The `snapshot` file beside it is where the stamp matters most: a bare `postcard` body has no first byte a reader can reject, so a wrong-format file decodes to a plausible index, after which the log prefix below it is discarded. The stamp is admitted first, so that decoder never sees foreign bytes.
