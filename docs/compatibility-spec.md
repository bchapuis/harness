# Format Compatibility: Specification

The key words **MUST**, **MUST NOT**, and **MAY** carry the meanings defined in RFC 2119.

## 1. Scope

This document is the convention by which a build refuses, by name, bytes or a peer it cannot read, and the registry of the formats that follow it (§4). It builds on no other specification; the actor framework, entity storage, and the write-ahead log build on it.

Non-goals:

- **Version ranges and negotiation.** A build reads no range of versions, negotiates no common version with a peer, and holds no decoder for another version.
- **Rolling format changes.** Every node and data directory takes a new version of a format at once.
- **Layouts.** Each format's layout belongs to its owner, named in §4.

Conformance classes:

- An *owner* conforms by meeting §3.1, §3.2, §3.3, and §3.5, and by listing each of its formats in §4.
- A *reader* conforms by meeting §3.1, §3.2, and §3.4.

## 2. Model

- **Format:** a byte layout one build produces and another consumes, between nodes on the network or across time on disk. A format has a stable dotted **name** (`entity.record`).
- **Owner:** the component that defines a format's layout and writes it. **Reader:** any build that consumes a format's bytes, or admits a peer that speaks it.
- **Version:** an unsigned 16-bit number naming one layout of one format. Versions of different formats are unrelated. A build has one **own version** per format, the one it reads and writes.
- **Stamp:** what tells a reader the version of the bytes or the peer before it. At rest, a stamp is a **magic** (a fixed byte string identifying the format) followed by the version as an unsigned 16-bit little-endian integer, written ahead of the **body**. Elsewhere it is carried by an association handshake, a log header, or a reserved bit, as §4 lists.
- **Verdict:** a reader's decision on a stamp: **admit** when it is the format's stamp and carries the reader's own version, **refuse** otherwise.
- **Refusal:** the report of a refusing verdict, naming the format, what the reader found, and the version it reads. It is **unstamped** when the magic does not match or the input is too short to hold a stamp, and **version** when the format's stamp carries another version.
- **Corruption report:** a reader's report that bytes it admitted are damaged.
- **Positional body:** a body whose encoding carries no field names or tags a reader could skip, so its bytes do not reveal a change to its layout.

## 3. The convention

The convention binds the owner and every reader of each format. It is neither a plug point nor a boundary: each owner applies it on its own write and read paths.

### 3.1 One version, refused by name

An owner MUST declare each format exactly once, with one name and one own version. A reader MUST refuse, by name, input or a peer at any version but its own, and MUST NOT attempt to parse it (V2). A format change is a new version of that format.

### 3.2 Admission

A reader MUST reach its verdict before it decodes any part of the body.

**Magic:** A magic MUST NOT be a prefix of another stamp's magic, and MUST NOT begin any unstamped bytes a reader can meet in its place (a headerless predecessor, a bare body of the same role).

### 3.3 Positional bodies

Any change to a positional body's layout, a variant appended at the end included, MUST be a new version of the format that carries it. The same holds for a wire vocabulary. A payload inside another format's body is versioned by that carrier format, unless the carrier versions only an envelope: `entity.shardmap` and `actor.membership` ride `actor.raft.log` entries and carry their own stamps.

### 3.4 Refusal versus corruption

For every format, a version refusal MUST differ from a corruption report in its wording (V3), and MUST NOT read as damage: neither it nor a remedy added to it suggests restoring or discarding the state. A reader MUST report every refusal with its wording intact, and MAY add the path and a remedy. The wordings are:

- version: `<format>: found v<found>, this build reads v<own>`
- unstamped: `<format>: not a stamped <format> record (this build reads v<own>)`

### 3.5 Granularity and exemptions

Each owner stamps at the coarsest grain that still identifies everything it writes: once per association, once per store directory, and zero bytes per journal record (a reserved high bit). V1 does not reach:

- **Content-addressed blobs**, named by the BLAKE3 hash of their bytes, which a reader re-hashes on every read: the name is the stamp.
- **Node-local materializations** (facet scratch files, the disk facet's raw image), rebuilt per activation.
- **Process control files** (a microVM's configuration, pid, and console files), owned by one process.

## 4. The registry

Every durable or wire format MUST appear in this table.

| Format | Version | Stamp | Owner |
|---|---|---|---|
| `actor.wire` | 1 | in the association handshake | cluster §9.1 |
| `actor.membership` | 1 | `ACMEMB`, per committed payload | cluster §5.4.3 |
| `actor.raft.log` | 1 | log header's record schema | cluster §3.4 |
| `actor.raft.term` | 1 | `\x89RFTERM\n`, ahead of the JSON `term` file | cluster §3.4 |
| `actor.raft.snapshot` | 1 | `\x89RFSNAP\n`, ahead of the `snapshot` file | cluster §3.4 |
| `entity.record` | 1 | the envelope byte's high bit | storage §12 |
| `entity.snapshot` | 1 | `GRSNAP`, ahead of the composite | storage §12 |
| `entity.store` | 1 | `GRSTOR`, in the directory's `store` file | storage §12 |
| `entity.store.manifest` | 1 | log header's record schema | storage §12 |
| `entity.store.segment` | 1 | log header's record schema | storage §12 |
| `entity.store.fence` | 1 | `GRFNCE`, per shard fence file | storage §12 |
| `entity.store.seal` | 1 | `GRSEAL`, per shard seal file | storage §12 |
| `entity.shardmap` | 1 | `GRSMAP`, per committed command | storage §12 |
| `wal.frame` | 1 | log header's magic and frame version | the `wal` crate |

A log header is one stamp serving two formats: the frame layout (`wal.frame`) and the record schema of the log's caller, one format per log.

## 5. Invariants

| # | Invariant | Defined in |
|---|---|---|
| V1 | Every durable byte sequence and every association is stamped at exactly one format listed in §4; none is stamped twice or left unstamped, except as §3.5 lists. | §3.5, §4 |
| V2 | A build refuses input or a peer at any version but its own, by name, and never parses it. | §3.1, §3.2 |
| V3 | A version refusal differs from a corruption report in its wording. | §3.4 |

## 6. Security considerations

A stamp identifies a format and a version; it is neither an integrity check nor an authenticator. Detecting damage to an admitted body is its owner's concern, and authenticating a peer is the association handshake's (actor §14). The convention's own guarantee is that no reader decodes a body it has not admitted (§3.2).
