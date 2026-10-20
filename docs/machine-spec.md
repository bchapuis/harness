# Persistent Machines: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** carry the meanings defined in RFC 2119. Other specifications are cited with a prefix; this document's sections are plain §N, and its invariants are M1–M6.

## 1. Scope

A machine is a persistent, name-addressed lightweight VM that a person reaches over SSH and whose whole root filesystem survives disconnection, idle hibernation, migration, and node loss. It is an entity whose identity, fence, placement, activation, hibernation, and failover are the entity layer's, unchanged (entity §1). This specification adds the machine's state and commands (§4), a network seam of ingress and egress (§5), a session-grained durability discipline for its disk (§6), and its lifecycle (§7). It builds on the actor and entity specifications (entity, storage, facets), shares its guest mechanism with the agent sandbox's `Native` tier (sandbox §5.4), and only deployments and operator tools build on it.

### 1.1 Non-goals

- **An agent:** a machine has no run loop, model seam, or autonomy, and does nothing until a person attaches (agent §1).
- **Per-keystroke durability:** the disk is durable to the last capture, with the crash consistency a real VM has across power loss (§6).
- **Live process migration:** a migration or a crash reboots the guest against the last captured image (§7).
- **A new consensus or isolation technology:** durability and the fence are the entity layer's, confinement the mechanism's (sandbox §5.4).
- **Network access for the agent sandbox:** the machine's network seam sits outside the sandbox's tier model (sandbox §1.1) and grants it nothing.

### 1.2 Conformance

- A *machine* conforms by meeting §4, §6, §7, and the machine's side of §3.1 and §5.1. With its node's provider and front doors it upholds M1–M6, its facets meet F1–F4 (facets §7), and it meets the deterministic profile (actor §11).
- A *provider* conforms by meeting §5.2 and the provider's side of §3.1.
- A *mechanism* conforms by meeting §3.2.
- A *machine image*, with the guest agent it ships, conforms by meeting §8.2 and the image's side of §3.3.
- A *front door* conforms by meeting §5.1, the host's side of §3.3, and §8.2.
- A deployment's *authority* and *channel backend* conform by meeting §3.4.

## 2. Model

- **Machine:** an entity of type `machine`, keyed by its name, with a disk facet holding its image (facets §5.4), a workspace facet holding its workspace (facets §5.1), and an alarm facet holding its one timer (facets §5.5).
- **Image:** the root filesystem as a fixed-size raw block image, first imported from a **base image** the owner supplies, which ships the guest agent.
- **Workspace:** the tree the guest sees at `/workspace`, durable only in the workspace facet's directory.
- **Guest:** the live VM running the image. The guest and a client's connection are disposable: stopping either loses no committed block.
- **Guest agent:** the process inside the guest that answers the host (§3.3).
- **Provider** and **mechanism:** a node's runtime binding (§3.1), and what it holds guests with (§3.2). A mechanism's **isolation grade** is hardware-virtualized or shared-kernel (sandbox §5.4).
- **Quiescent:** paused or stopped, so writing nothing to the image.
- **Capture:** a command that reads the image while the guest is quiescent and stages its changed blocks as one disk manifest (facets §5.4), with the workspace's delta in the same batch (§6).
- **Checkpoint interval** and **lease interval:** durations fixed at provisioning (§6.2, §6.3).
- **Front door:** a cluster member that terminates SSH for a machine (§5.1). Its **front-door member** is the actor it names as the holder of its attachments.
- **Attachment:** a journaled record that a **principal**, derived from the authenticating key, is connected through a front-door member, live from its `Attached` event to its `Detached` event.
- **Authorized key set:** the public keys that may attach, keyed by fingerprint.
- **Host-key seed:** 32 random bytes drawn at provisioning. The SSH host key is the Ed25519 key whose private key is the seed (RFC 8032).
- **Egress policy:** what the guest may reach: open (the default), an allowlist of destination addresses or prefixes, or none.

## 3. Seams

The machine implements the entity layer's authoring plug point (entity §3.1) and is called through entity references (entity §3.2); those seams are the entity specification's. The seams below are the machine's.

### 3.1 The runtime binding: `MachineRuntimeProvider`

A plug point between the machine (the caller) and a node's provider (the implementer). A node gives every machine activation it hosts the same provider, which boots one runtime per live guest.

```rust
pub struct BootSpec {
    pub image: PathBuf,
    pub vcpus: u8,
    pub mem_mib: u32,
    pub machine: EntityName,
    pub egress: EgressPolicy,
}

pub enum EgressPolicy {
    Open,
    Allowlist(Vec<String>),
    None,
}

pub struct RuntimeError(pub String);

pub trait MachineRuntimeProvider {
    async fn boot(&self, spec: BootSpec) -> Result<Arc<dyn MachineRuntime>, RuntimeError>;
}

pub trait MachineRuntime {
    async fn pause(&self) -> Result<(), RuntimeError>;
    async fn resume(&self) -> Result<(), RuntimeError>;
    async fn kill(&self);
    async fn push_ws(&self, ws: PathBuf) -> Result<(), RuntimeError>;
    async fn pull_ws(&self, ws: PathBuf) -> Result<(), RuntimeError>;
}
```

The machine boots with the disk facet's image and its journaled sizing and egress policy. Every failure is a `RuntimeError` carrying a message. The provider's side:

- `boot` MUST run the guest on `BootSpec::image` in place, so that captures read the guest's writes (§6.1).
- `boot` MUST realize no more egress than `BootSpec::egress` grants (§5.2).
- Once `pause` resolves, the guest MUST issue no write to the image until `resume`.
- `kill` MUST be idempotent, and a runtime discarded without `kill` MUST still stop its guest and release its egress path.
- `push_ws` MUST replace the guest's workspace with the contents of the directory `ws`. `pull_ws` MUST flush the guest's filesystem and replace the contents of `ws` with the guest's workspace; a failed pull MUST leave `ws` unchanged.

The machine's side:

- It MUST push the workspace facet's directory once per boot, before it answers the first attach, and on a failed push MUST kill the guest and fail the command.
- It MUST pull only while the guest runs, and MUST NOT capture a guest it failed to pause: it kills the guest instead (§6.1).

### 3.2 The guest mechanism: `MachineHost`, `MachineGuest`

A plug point between a guest's consumer (a provider, or the agent sandbox's `Native` tier) and a node's mechanism. `MachineHost` holds a node's guests and `MachineGuest` is one live guest. A `GuestSpec` names the guest (`GuestKey`), its image (`GuestSpec::disk`), its sizing, and its network device if any; a listener inside the guest is reached as a `Duplex` byte stream.

```rust
pub enum MachineIsolation {
    MicroVm,
    Container,
}

pub enum GuestError {
    Gone(String),
    Host(String),
}

pub trait MachineHost {
    fn isolation(&self) -> MachineIsolation;
    fn accepts_tap(&self) -> bool;
    async fn start(&self, spec: GuestSpec) -> Result<Arc<dyn MachineGuest>, GuestError>;
    async fn connect_by_key(&self, key: &GuestKey, port: u32) -> Result<Box<dyn Duplex>, GuestError>;
}

pub trait MachineGuest {
    async fn alive(&self) -> bool;
    async fn connect(&self, port: u32) -> Result<Box<dyn Duplex>, GuestError>;
    async fn pause(&self) -> Result<(), GuestError>;
    async fn resume(&self) -> Result<(), GuestError>;
    async fn stop(&self);
    async fn console_tail(&self) -> String;
}
```

`MicroVm` is the hardware-virtualized grade, `Container` the shared-kernel one. `GuestError::Gone` means the guest is not there; `GuestError::Host` means the mechanism failed and the guest may be live. `start` resolves once the guest is up, not once its agent serves; `console_tail` is the guest's diagnostic output. The mechanism's side:

- It MUST run the guest on `GuestSpec::disk` in place.
- `connect` MUST reach the numbered listener inside the guest the same way under every mechanism, so one guest agent serves all of them unchanged.
- `pause` and `stop` carry §3.1's pause and kill obligations.
- `connect_by_key` MUST answer `GuestError::Gone` for a guest this node does not hold.
- `accepts_tap` MUST be true only where a guest can take a host network device; it is false by default.

A deployment MUST choose each node's mechanism, and so its grade, explicitly; a node MUST NOT select one by probing the host (sandbox §5.4). Every property of §6 holds under either grade; confinement does not (§10).

### 3.3 The guest-agent wire

A boundary between the host (a provider's runtime, or a front door's channel backend) and the guest agent, and an interop surface: every host, and every guest agent a machine image ships, MUST speak it as this section and §8.2 define. The host opens one channel per exchange to the guest listener `AGENT_PORT` (§3.2) and sends a header frame naming the channel's kind; then both ends exchange frames. A frame is a 32-bit little-endian payload length and the payload; after the header, each payload is a one-byte tag and a body. `Pty`, `Exec`, and `Sftp` channels serve the front door; `Sync`, `WsPush`, and `WsPull` serve the workspace and captures.

The machine image's side:

- It MUST serve the protocol on `AGENT_PORT`.
- It MUST mount `/workspace` as a volatile filesystem, never a directory of the image (§4.3).
- On `Sync`, the agent MUST flush the guest's filesystem to its disk before it answers.

The host's side, where the agent is the owner's untrusted code and a compromise persists in the image:

- It MUST refuse a frame over `MAX_FRAME`, or a workspace stream over `MAX_TAR`, before allocating for it, and MUST bound in time every exchange a boot or a capture waits on.
- It MUST treat what the agent sends as data that widens nothing the host bridges or grants. Holding the channel is the host's authority: the guest authenticates nothing, and no authorized-key material enters it.
- A failed or missing agent MUST cost only front-door access and workspace captures, never disk captures.

### 3.4 The front door's plug points: `MachineAuthority`, `ChannelBackend`

Two plug points a deployment supplies to a front door: `MachineAuthority`, the cluster half, over the machine's `machine.DoorPolicy`, `machine.Attach`, and `machine.Detach` commands on its leader; and `ChannelBackend`, the guest half, over the node's mechanism (§3.2).

```rust
pub trait MachineAuthority {
    async fn host_key(&self, machine: &EntityName) -> Result<PrivateKey, FrontDoorError>;
    async fn authorizes(&self, machine: &EntityName, key: &PublicKey) -> bool;
    async fn attach(&self, machine: &EntityName, principal: &str) -> Result<u64, FrontDoorError>;
    async fn detach(&self, machine: &EntityName, attachment: u64);
}

pub trait ChannelBackend {
    async fn open(&self, machine: &EntityName, kind: ChannelKind) -> std::io::Result<Box<dyn Duplex>>;
}
```

The deployment's side:

- `host_key` MUST return the host key derived from the machine's journaled host-key seed (§2).
- `authorizes` MUST answer true only for a key in the machine's current authorized key set, read per call, and MUST answer false when the machine is unreachable.
- `attach` MUST journal the attachment through `machine.Attach`, with `principal` as given and the front-door member as holder, and return its id. The door's leadership refusal (§5.1) is `attach` failing, and the failure's message reaches the client.
- `detach` journals the end through `machine.Detach`, best-effort: the machine's death watch reconciles a lost detach (§5.1).
- `open` MUST return a stream to the machine's guest agent with the channel's header sent, and only to a guest this node holds.

## 4. State and commands

### 4.1 Commands

A machine accepts these commands from the network (entity §5.5). Front doors use `machine.DoorPolicy`, `machine.Attach`, and `machine.Detach`; operator tools use the rest.

| Command | Takes | Effect |
|---|---|---|
| `machine.Provision` | owner, base image path on the leader's node, sizing, checkpoint and lease intervals, authorized keys | journals `Provisioned` (§4.2) |
| `machine.Attach` | principal, front-door member | journals `Attached`, replies its id (§5.1) |
| `machine.Detach` | attachment id | journals `Detached` with reason `Closed` if the attachment is live |
| `machine.DoorPolicy` | nothing | replies the host-key seed and the authorized key set |
| `machine.AddKey` | fingerprint, public key | journals `KeyAdded` |
| `machine.RevokeKey` | fingerprint | journals `KeyRevoked` |
| `machine.SetEgress` | egress policy | journals `EgressChanged` (§5.2) |
| `machine.Status` | nothing | replies the owner, egress policy, live attachments, capture counts, image size and digest, and whether it is provisioned and a guest runs |
| `machine.WsWrite` | path, bytes | writes a workspace file (§4.3) |
| `machine.WsRead` | path | replies a workspace file's bytes |
| `machine.WsList` | path, empty for the root | replies one directory level: name, length, and kind |
| `machine.WsRemove` | path | removes a workspace file or tree |

Every command but `machine.Detach` and `machine.Status` can answer an error instead: `NotProvisioned` (no provisioning has committed), `AlreadyProvisioned` (a second provisioning), `Running` (a workspace file command while a guest runs), or `Disk`, `Runtime`, or `Ws` with a message (the disk facet, the provider, or a workspace file operation failed).

### 4.2 State and provisioning

- **State is metadata:** a machine's state holds its owner, authorized key set, host-key seed, egress policy, sizing (vCPU count and memory), base image, checkpoint and lease intervals, and live attachments; never image or workspace bytes. Its idle window is the entity type's (entity §8).
- **Provisioning:** `machine.Provision` imports the base image as the disk's first manifest (facets §5.4) and journals `Provisioned` with a fresh host-key seed, which MUST be drawn from the actor system's randomness (actor §11.1). Until a provisioning commits, every command but `machine.Provision`, `machine.Status`, and `machine.Detach` answers `NotProvisioned`; a second provisioning answers `AlreadyProvisioned`.
- **Access policy:** `machine.AddKey` and `machine.RevokeKey` journal ordinary events. A revocation takes effect at the next authentication (§5.1) and does not end a live connection.

### 4.3 The workspace

- **One durable home:** the workspace facet's directory is the workspace's only durable copy. The guest's volatile `/workspace` is filled from it at boot (§3.1) and drained into it at every capture (§6.1).
- **File commands need no guest:** the four workspace file commands act on the facet directory directly, and a write or removal commits with its delta in the command's batch (facets §5.1). While a guest runs it owns `/workspace`, and these commands answer `Running`; otherwise the next boot's push delivers every committed write. Their paths MUST be confined by capability handle (sandbox §5.1), and one command carries at most `MAX_WS_FILE` bytes in either direction.

## 5. The network seam

A machine's network seam has two halves: ingress through front doors (§5.1, M4) and egress from the guest (§5.2, M6).

### 5.1 Ingress: the front door

A front door is a cluster member inside the cluster's trust boundary, as the HTTP edge is (edge §4.1). Each connection serves one machine, fixed before the SSH handshake. The bridge is node-local: a channel ends at a guest the door's own node holds.

- **Authentication:** the door MUST authenticate by public key only, verifying possession itself, MUST NOT bridge a session for a key its authority does not authorize (§3.4), and MUST present the machine's host key, the same across hibernation, migration, and failover.
- **Leadership:** a door whose node does not lead the machine MUST refuse during authentication, journaling and booting nothing, and SHOULD say so distinctly: a key refusal is a public-key failure, a leadership refusal a disconnect carrying the reason.
- **Attribution:** the principal an attachment journals MUST derive from the authenticating key (its fingerprint); a client-asserted name MAY sit beside it.
- **Attachments:** the door MUST journal the attachment before bridging any channel, SHOULD journal its end when the connection closes, and MUST open channels only to its own machine's guest. `machine.Attach` MUST journal `Attached` (id, principal, front-door member, time), boot the guest if none runs, and start the capture cadence (§6.2); `machine.Detach` is idempotent.
- **Holder liveness:** the machine MUST death-watch the front-door member of every live attachment (actor §10) and journal `Detached` with reason `FrontDoorLost` for one that terminates without detaching, so an attachment pins the activation only while its holder lives.
- **Host-key material** MUST stay inside the cluster's trust boundary, never reaching the guest; only `machine.DoorPolicy` returns it.
- **Channels:** an SSH `shell` or `exec` request (RFC 4254) opens `Pty` after a `pty-req` and `Exec` otherwise, an `exec` command running under `/bin/sh -c`; the `sftp` subsystem opens `Sftp`, and other subsystems are refused. Data, end of input, window changes, and signals go to the guest, and output, error output (extended data type 1), and exit status come back, as the frames of §8.2.

Another ingress protocol MAY replace SSH where its door meets this section's authentication, attribution, and isolation requirements (M4).

### 5.2 Egress

A machine's guest has a standing outbound path, governed by its journaled egress policy and realized by the provider at boot (§3.1).

- **Policy-bound:** the provider MUST give the guest no outbound path beyond what the journaled policy grants. An owner or operator MAY narrow it.
- **Attributable:** the provider MUST be able to attribute egress flows to their machine, joinable with the attachment journal.
- **Isolated:** whatever the policy grants, the path MUST NOT reach another machine's guest, an agent-sandbox environment, the host's services, or the cluster's control-plane and node-internal addresses, link-local and metadata endpoints included.
- A policy change MUST apply no later than the machine's next boot; a provider MAY apply it sooner.
- A node that cannot realize egress (no egress configuration, a mechanism that takes no network device, an exhausted address pool, failed setup) MUST boot the machine with no network device rather than fail the boot.

## 6. Durability: the session is the command

A machine's disk is durable to its last committed capture. Between captures the guest writes the image in place: those writes are visible to whoever is attached, not committed, and discarded by any outcome but a commit (E20). This is the disk facet's declared departure from the physical discipline (facets §4.2, §5.4), and M3 bounds it.

### 6.1 Capture

- A capture is a command the machine's alarm delivers (facets §5.5). It stages one disk manifest through the ordinary barrier (entity §6) and MUST read the image only while the guest is quiescent.
- **Sequence:** with a guest running, a capture flushes the guest (`Sync`), pulls the workspace, stages its delta, makes the guest quiescent (a pause mid-session, a stop at the final capture), captures the disk, and resumes a paused guest. Workspace and disk commit in one batch (E19), the workspace as of the pull and the disk as of the quiescent point.
- A failed pull journals `WsCaptureSkipped`, and the disk capture proceeds.
- A failed pause stops the guest and skips the disk capture, while the pulled delta still commits: the workspace is then one cadence ahead of the disk, and the image keeps its uncaptured writes.
- Apart from provisioning's import (§4.2), disk state MUST be staged only by captures, one manifest per capture, and never at deactivation (facets §4.3).

### 6.2 Quiescent points

1. **The final capture:** when the last attachment ends, a capture runs at once with the guest stopped, and is retried on each later alarm fire until it commits. Once it has, the machine may hibernate (entity §8).
2. **Checkpoints:** from the first attach, the machine periodically pauses the guest, captures, and resumes; the period is its **capture cadence**. A machine SHOULD checkpoint once per checkpoint interval while attached; without checkpoints, M3's window is the whole session.

### 6.3 The session lease

While attached, the activation MUST commit a fenced append at least once per lease interval. Any outcome but a contiguous commit ends the activation (entity §6), which stops the guest and severs its attachments (§7, M5).

### 6.4 Windows

- **The relaxed barrier:** a capture counts only once it commits on a quorum; only the frequency relaxes. Disk state is appended only by captures (§6.1), under the term fence (storage §6), each as one record in one batch, so a crash falls back to a consistent prior capture, never a torn composite (F4, E19).
- **Lost writes:** the window is the capture cadence (M3).
- **Doomed sessions:** a deposed activation's window is the lease interval plus one bounded capture and the append timeout (M5).
- **Planned migration:** a cooperative handoff (storage §6.3) is a forced step-down, which stages nothing (facets §4.3): lossless for a machine already captured, M3's window for an attached machine with uncaptured writes.

## 7. Lifecycle and failure

- **Activation:** activating a machine MUST NOT boot a guest. An attach to a hibernated machine activates it on its shard leader, paying at most one quorum head recovery (storage §9, entity §8, E8), and boots a guest against the rehydrated image.
- **Hibernation:** the machine MUST refuse idle hibernation while an attachment is live or the image may hold uncaptured writes (facets §5.4).
- **Deactivation:** every end of an activation, whatever its cause, MUST stop the guest.
- **Crash and failover:** the new leader recovers the committed head from a write quorum (storage §6, E14) and boots the last captured image on the next attach. Later writes are lost and the connection dropped; the disk never forks or corrupts (storage §11, E11).
- **The deposed side:** until the lease interval runs out, a deposed leader's guest can run beside the new leader's, unable to commit. The deposed activation MUST stop its guest and sever its attachments within the lease interval (§6.3, M5), and its image is discarded (E20).
- **Routing:** each connection resolves the current leader. A leadership change severs the connection rather than rehoming it, and the client reconnects to the new leader's door.
- **Reporting loss:** a severed connection MUST reach the client as a disconnect, and a channel whose guest end vanished as exit status 255, never as a hang (sandbox §6). A reconnect after a crash MAY see the disk rewound to the last capture (M3), never a mixture of histories (M1).

## 8. Registries

### 8.1 Names

- **Entity type:** `machine`.
- **Command manifests:** the twelve of §4.1, each `machine.` and the command's name.
- **Journal events:** `Provisioned`, `KeyAdded`, `KeyRevoked`, `EgressChanged`, `Attached`, `Detached` (reason `Closed` or `FrontDoorLost`), `Captured`, `WsCaptured`, and `WsCaptureSkipped`.
- **Workspace file cap:** `MAX_WS_FILE`, 1 MiB (1,048,576 bytes).

### 8.2 The guest-agent wire

`AGENT_PORT` is 62. `MAX_FRAME` is 1 MiB (1,048,576 bytes). `MAX_TAR` is 64 MiB (67,108,864 bytes), summed over a workspace stream's frames, and equals the workspace facet's tree cap (facets §5.1).

A header's payload is the kind's JSON encoding: the JSON string of its name for a kind without fields (`"Sftp"`), else an object whose one member, named for the kind, holds an object of its fields.

| Kind | Fields | Exchange |
|---|---|---|
| `Pty` | `term`, `cols`, `rows`, `argv` | a process on a pseudo-terminal; an empty `argv` is the login shell |
| `Exec` | `argv`, `env` (name and value pairs) | `argv` with piped standard streams |
| `Sftp` | none | the image's SFTP server |
| `Sync` | none | the agent flushes the guest's filesystem to its disk |
| `WsPush` | none | the host sends a tar as `Data` frames then `Eof`, and the agent replaces the contents of `/workspace` with it |
| `WsPull` | none | the agent sends a tar of `/workspace` as `Data` frames then `Eof` |

`Sync`, `WsPush`, and `WsPull` end with `ExitStatus` 0, or on failure with `Stderr` then `ExitStatus` 1 (a failed pull sends no data).

| Tag | Frame | Body |
|---|---|---|
| 0 | `Data` | bytes, either direction |
| 1 | `Stderr` | bytes from the guest; never on a `Pty` channel |
| 2 | `WindowChange` | columns then rows, each 16-bit little-endian; `Pty` only |
| 3 | `Signal` | a signal name in UTF-8, for the channel's processes |
| 4 | `Eof` | empty, either direction: no more data follows |
| 5 | `ExitStatus` | a 32-bit little-endian signed exit code from the guest, 255 when a signal ended the process; ends the channel |

## 9. Invariants

These add to E1–E21 and F1–F4.

| # | Invariant | Defined in |
|---|---|---|
| **M1** | **Single disk, never forked.** Only the shard leader appends disk state, term-fenced (E1), each capture one atomic record (E19), so the rootfs never forks or mixes histories. | §6, §7; storage §6, facets §5.4 |
| **M2** | **Lossless across idle hibernation.** An idle machine re-activates with the disk as of the final capture, run when the last attachment goes (E12, E14). A cooperative handoff runs no capture, so it is lossless only for a machine already captured. | §6, §7 |
| **M3** | **Bounded crash window.** On node loss mid-session the disk rewinds to the last committed capture, never a fork or a torn image (E19, F4, E11); the window is the capture cadence. | §6, §7 |
| **M4** | **Authenticated, attributable, isolated ingress.** A session is bridged only for a key the current journaled policy authorizes; every attachment and detachment is journaled with a principal derived from that key; a session reaches only its own machine's guest. | §5.1 |
| **M5** | **A deposed activation self-fences within the lease.** While attached it commits a fenced append once per lease interval, or stops its guest and severs its attachments; the deposed guest never commits (M1). | §6, §7 |
| **M6** | **Policy-bound, attributable, isolated egress.** A guest's outbound path is at most what its policy grants, attributable per machine, and never reaches another guest, a sandbox, the host's services, or cluster-internal addresses. | §5.2 |

The workspace adds no invariant: its delta commits in the disk's captures (§6.1), so M1–M3 cover it. A failed pause leaves the workspace one cadence ahead of the disk: two consistent points, never a torn image.

## 10. Security considerations

- **Untrusted guests:** the image and its guest agent are the owner's code, and a compromise persists in the image across hibernation and failover; the host's defenses are §3.3's. The shared-kernel grade runs the guest privileged and carries sandbox §5.4's caveats: it is a development mechanism, not a boundary for untrusted guests.
- **Front doors** are cluster members: they read the host-key seed and journal attachments, and clients reach them only over SSH (§5.1, M4). Revocation does not end a live connection (§4.2).
- **No caller identity on commands:** the entity layer consults no authorizer (entity §11), so any cluster member that can address a machine can provision it, change its keys or egress policy, or read its host-key seed. The cluster's transport security and authorizer (cluster §3.6) bound who that is.
- **The host-key seed at rest** is in the machine's journal and snapshots, held in the clear by every replica of its shard: its confidentiality is that of the cluster's storage.
