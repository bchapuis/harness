# Format Compatibility: Specification

The key words **MUST**, **MUST NOT**, and **MAY** carry the meanings defined in RFC 2119.

## 1. Scope

A **format** is a byte layout one build produces and another consumes: between nodes on the network, and across time on disk. This document owns the convention every format follows and the registry of formats (§4), but no layout: each stays with the component that writes it. The vocabulary is the `compat` crate, which depends on nothing in the tree.

## 2. Seams

### 2.1 The vocabulary

A convention, not a plug point: `compat` defines the types, and each format's owner uses them to write and to read.

- `Format`: a stable dotted name and the one `Version` this build reads and writes; `Format::admit` is the verdict.
- `Stamp`: a magic and a version ahead of a body at rest; `Stamp::stamp` writes it, `Stamp::unstamp` admits it and returns the body.
- `Incompatible`: the refusal, `Unstamped` or `Version`, naming the format and both sides.

An owner MUST declare each format once, as one `Format`, and list it in §4. A reader MUST reach the verdict through `Format::admit` or `Stamp::unstamp` and report the `Incompatible` with its wording intact; it MAY add the path and a remedy.

## 3. The convention

### 3.1 One version, refused by name

A format needs a **stamp** saying which version the bytes or the peer are, the one **version** this build reads and writes, and a **verdict**. A build MUST refuse input or a peer at any other version, naming the format, what it found, and what it reads, and MUST NOT attempt to parse it (V2). There is no range and no negotiation: a format change is a new version every node and data directory takes at once, because a misparse turns a skew into silent corruption.

### 3.2 Admission

A decoder MUST admit the stamp before decoding the body, as the first thing it does with the bytes; nothing in the encoding enforces this.

**Magic:** No stamp's magic may be a prefix of another's, and no unstamped bytes a reader can meet where it expects a stamp (a headerless predecessor, a bare body of the same role) may begin with that magic.

### 3.3 Positional bodies

A `postcard` body cannot version itself, so any change to its type, an enum variant appended at the end included, MUST be a new version of the format that carries it. Otherwise an older build reads the new body as damage (a log's torn-tail rule drops it silently) instead of refusing it by name. A wire vocabulary is the same. A payload is versioned by its carrier unless the carrier versions only an envelope: `entity.shardmap` and `actor.membership` ride `actor.raft.log` entries and carry their own stamps.

### 3.4 Refusal versus corruption

For every format, a version refusal MUST differ from a corruption report in its wording (V3): it carries the `Incompatible` wording and MUST NOT read as damage. The remedies are opposite (run the matching build and keep the state, versus restore or discard it), and a skew reported as corruption leads an operator to destroy recoverable state.

### 3.5 Granularity and exemptions

Each owner stamps at the coarsest grain that still catches the error: once per association, once per store directory, zero bytes per journal record (a reserved high bit). V1 does not reach:

- **Content-addressed blobs**, named by the BLAKE3 hash of their bytes and re-hashed on every read: the name is the stamp, and a prefix would break it.
- **Node-local materializations** (facet scratch files, the disk facet's raw image), rebuilt per activation.
- **Process control files** (a microVM's configuration, pid, and console files), owned by one process.

## 4. The registry

A new durable or wire format MUST appear here. The `compat` crate's registry test holds this table equal to the formats the code declares.

| Format | Version | Stamp | Owner |
|---|---|---|---|
| `actor.wire` | 1 | in the association handshake | actor §2.14 |
| `actor.membership` | 1 | `ACMEMB`, per committed payload | actor §6.4.3 |
| `actor.raft.log` | 1 | log header's record schema | actor §6.4.3 |
| `actor.raft.term` | 1 | `\x89RFTERM\n`, ahead of the JSON `term` file | actor §6.4.3 |
| `actor.raft.snapshot` | 1 | `\x89RFSNAP\n`, ahead of the `snapshot` file | actor §6.4.3 |
| `entity.record` | 1 | the envelope byte's high bit | storage §8 |
| `entity.snapshot` | 1 | `GRSNAP`, ahead of the composite | storage §8 |
| `entity.store` | 1 | `GRSTOR`, in the directory's `store` file | storage §2.2 |
| `entity.store.manifest` | 1 | log header's record schema | storage §1 |
| `entity.store.segment` | 1 | log header's record schema | storage §1 |
| `entity.store.fence` | 1 | `GRFNCE`, per shard fence file | storage §5 |
| `entity.store.seal` | 1 | `GRSEAL`, per shard seal file | storage §7 |
| `entity.shardmap` | 1 | `GRSMAP`, per committed command | storage §6 |
| `wal.frame` | 1 | log header's magic and frame version | the `wal` crate |

Each magic is followed by a little-endian `u16` version. A log header is one stamp with two owners: the frame layout is the `wal` crate's, the record schema the caller's, one format per log.

## 5. Invariants

No Rust catalogue copies these; each format's tests hold its stamp to them.

| # | Invariant | Defined in |
|---|---|---|
| V1 | Every durable byte sequence and every association is stamped at exactly one format listed in §4; none is stamped twice or left unstamped, except as §3.5 lists. | §3.5, §4 |
| V2 | A build refuses input or a peer at any version but its own, by name, and never parses it. | §3.1, §3.2 |
| V3 | A version refusal differs from a corruption report in its wording. | §3.4 |
