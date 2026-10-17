# Persistent Machines: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119. Other specifications are cited with a prefix; this document's sections are plain §N, and its invariants are M1–M6.

## 1. Scope

A **machine** is a persistent, name-addressed lightweight VM that a person reaches over SSH and whose whole root filesystem survives disconnection, idle hibernation, migration, and node loss. It is an entity ([`entity-spec.md`](entity-spec.md)) of type `MACHINE_TYPE`, keyed by the machine's name, whose durable state is its journal, a disk facet holding the rootfs as a raw block image (facets §4.4), and a workspace facet holding `/workspace` (facets §4.1). Identity, the fence, placement, activation, hibernation, and failover are the entity's, unchanged. The live guest and the client's connection are disposable: stopping either loses no committed block. A machine adds a session-grained durability discipline (§4) and a network seam, ingress (§2.5) and egress (§2.6). Its guest runs under the mechanisms the agent sandbox's `Native` tier also uses (sandbox §4), but where the sandbox confines an autonomous model, a machine is reactive: it does nothing until a person attaches.

### 1.1 Non-goals

- **An agent:** no run loop, model seam, or autonomy; agents belong to the agent runtime ([`agent-spec.md`](agent-spec.md)).
- **Per-keystroke durability:** the disk is durable to the last capture: snapshot crash-consistency, what a real VM gives across power loss (§4).
- **Live process migration:** a migration or a crash reboots the guest against the last captured image (§5).
- **A new consensus or isolation technology:** durability and the fence are the entity's, confinement the mechanism's (sandbox §4.4).
- **Network access for the agent sandbox:** the machine's network seam sits outside the sandbox's tier model (sandbox §1.1) and grants it nothing.

## 2. Seams

### 2.1 The machine entity

`Machine` implements the entity's `Entity` trait, the entity layer's plug point, with `Facets = (Disk, Alarm, Ws)`: the disk facet owns the rootfs image (facets §4.4), the alarm facet supplies §4's one timer (facets §4.5), and the workspace facet is `/workspace` (facets §4.1). Its state is `MachineState`, its journal `MachineEvent`, its application failures `MachineError`, and its command surface `Provision`, `Attach`, `Detach`, `DoorPolicy`, `AddKey`, `RevokeKey`, `SetEgress`, `Status`, `WsWrite`, `WsRead`, `WsList`, and `WsRemove` (§3), used by front doors (§2.5) and operator tools.

- The machine MUST stage disk state only from capture commands (§4), one capture manifest each, and MUST NOT stage from `on_deactivate` (facets §3).
- `can_hibernate` MUST refuse while an attachment is live or the image may hold uncaptured writes.
- `on_deactivate` MUST stop the guest, whatever ended the activation.
- Activation MUST NOT boot a guest; only `Attach` does.
- The facets MUST meet F1–F4 (facets §3).

### 2.2 The runtime binding

The entity drives its guest through `MachineRuntimeProvider` (one per node, injected into the entity factory), which boots one `MachineRuntime` per live guest from a `BootSpec`: the disk facet's image, the journaled sizing, the machine's `EntityName`, and its `EgressPolicy`. It is a plug point: simulation binds the deterministic `FakeRuntimeProvider`, production binds `HostedRuntimeProvider` over the node's mechanism (§2.3). Every failure is one `RuntimeError`, because the entity answers every runtime failure the same way.

- `boot` MUST use `BootSpec::image` in place: the guest writing that file between captures is the persistence model.
- `pause` MUST NOT resolve while the guest can still write the image, and the guest issues no write until `resume`.
- `kill` MUST be idempotent, and dropping a runtime MUST kill its guest and release its egress plumbing, so a dropped activation leaks neither.
- `push_ws` MUST replace the guest's `/workspace` with the facet directory; `pull_ws` MUST flush the guest's filesystem and replace the facet directory with the guest's `/workspace`. A failed pull MUST leave the facet directory unchanged.
- The provider MUST realize no more egress than `BootSpec::egress` grants (§2.6).
- The entity MUST push once per boot before answering the first attach, and on a failed push MUST kill the guest and fail the command. It MUST pull while the guest runs, and MUST NOT capture a guest it failed to pause: it kills it instead (§4).

### 2.3 The guest mechanism

`MachineHost` holds a node's guests and `MachineGuest` is one live guest, both in `machine-host` and shared with the agent sandbox's `Native` tier (sandbox §4). A microVM and a container that loop-mounts the same disk image implement them; which one a node runs is configuration, never probed, because the two confinement grades are not interchangeable (sandbox §4.4). A guest is described by a `GuestSpec`, reports its grade as `MachineIsolation`, and fails with `GuestError::Gone` (not here) or `GuestError::Host` (the mechanism failed; the guest may be live).

- A mechanism MUST use `GuestSpec::disk` in place.
- `MachineGuest::connect` MUST reach the numbered listener inside the guest with one handshake under every mechanism, so one guest agent serves either.
- `MachineGuest::pause` and `MachineGuest::stop` carry §2.2's pause and kill obligations.
- `MachineHost::connect_by_key` MUST answer `GuestError::Gone` for a guest this node does not hold.
- `MachineHost::accepts_tap` MUST be true only where a guest can take a host tap.

Every property of §4 holds under either mechanism; confinement does not. The container grade is shared-kernel and privileged (sandbox §4.4's SHOULD grade) and takes no network device: a development mechanism, not a boundary for untrusted guests.

### 2.4 The guest-agent wire

The user's own rootfs, a plug point the machine's owner supplies, ships the **guest agent**, which answers the host from inside the guest; `machine-proto` is the single definition of their wire, an interop boundary. One stream carries one channel: the host sends a header frame naming a `ChannelKind` (`Pty`, `Exec`, `Sftp` for the front door; `Sync`, `WsPush`, `WsPull` for the workspace sync), then both ends exchange `Frame`s (a `u32` little-endian length, a tag byte, a payload). The interop constants are `AGENT_PORT` (distinct from the sandbox agent's), `MAX_FRAME` (1 MiB), and `MAX_TAR` (64 MiB per workspace stream, the workspace facet's cap).

- A machine image MUST serve the protocol on `AGENT_PORT`, and MUST mount `/workspace` as a volatile filesystem, never a rootfs directory, so the facet is its single durable home (§3).
- `Sync` MUST flush the guest's page cache to its disk before the agent answers; the host issues it before every capture's pause.
- The agent is untrusted user code, and a compromise persists in the rootfs. The host MUST refuse a frame or stream past its cap before allocating, MUST bound in time every exchange a boot or capture waits on, and MUST treat what the agent sends as data that widens nothing the host bridges or grants.
- Holding the stream is the host's authority: the guest performs no second authentication, and no authorized-key material enters it.
- A broken or removed agent costs front-door access and the workspace cadence, never disk captures.

### 2.5 Ingress: the front door

A **front door** (`serve_connection`) is a cluster member, like the agent runtime's HTTP gateway (edge §2.4), that terminates SSH and bridges authenticated sessions to its machine's guest. The deployment supplies its two plug points: `MachineAuthority`, the cluster half, over the machine's `DoorPolicy`, `Attach`, and `Detach` commands on its leader, and `ChannelBackend`, the guest half, over the node's `MachineHost`. The bridge is node-local: a channel ends at a guest the door's own node holds.

- The door MUST authenticate by public key, verifying possession itself, against the machine's current journaled key set read per connection, and MUST NOT bridge a session for a key that set does not authorize.
- The door MUST present the machine's journaled host key, so a client's pin survives hibernation, migration, and failover.
- The principal an attachment journals MUST derive from the authenticating key; a client-asserted name MAY sit beside it.
- The door MUST journal the attachment (`Attach`) before bridging any channel, SHOULD journal its end (`Detach`) when the connection closes, and MUST open channels only to its own machine's guest.
- A door whose node does not lead the machine MUST refuse during authentication, journaling and booting nothing. A door refusing for leadership SHOULD say so, distinct from a key refusal.
- `MachineAuthority::authorizes` MUST fail closed when the machine is unreachable.
- `Attach` MUST journal `Attached` (id, principal, the holding front-door member, time), boot the guest if none runs, and start the capture cadence (§4). `Detach` journals `Detached` with a `DetachReason` and is idempotent.
- The machine MUST death-watch the front-door member of every live attachment (actor §2.5) and fold `DetachReason::FrontDoorLost` for one that dies without detaching, so an attachment pins the activation only while something live holds it.
- Host-key material MUST stay inside the cluster's trust boundary and never reach the guest: `DoorPolicy` alone returns it.

Ingress is thus authenticated, attributable, and isolated (M4). SSH is the realization in the tree; a provider MAY bridge another protocol that meets the three properties.

### 2.6 Egress

Unlike the agent sandbox's per-call default-deny (sandbox §4.3), a machine's guest has a standing outbound path: its egress is a person's, authenticated at attach and attributed per machine. The journaled `EgressPolicy` (`Open` by default, `Allowlist`, or `None`, changed by `SetEgress`) says what the guest may reach; a node's `EgressConfig` (uplink, guest address pool, cluster ranges) is the plug point through which the hosted binding realizes it.

- **Policy-bound:** the provider MUST give the guest no outbound path beyond what the journaled policy grants. An owner or operator MAY narrow it.
- **Attributable:** the provider MUST be able to attribute egress flows to their machine, joinable with the attachment journal.
- **Isolated:** whatever the policy grants, the path MUST NOT reach another machine's guest, an agent-sandbox environment, the host's services, or the cluster's control-plane and node-internal addresses, link-local and metadata endpoints included.
- A policy change MUST apply no later than the machine's next boot; a provider MAY apply it sooner.
- A node that cannot realize egress (no configuration, no network device at this grade, an exhausted pool, failed plumbing) MUST boot the machine with no network device rather than fail the boot.

These three properties are M6.

## 3. State, provisioning, and the workspace

- **State is metadata:** `MachineState` holds the owner, authorized keys by fingerprint, the host-key seed, the egress policy, sizing, the base image, the checkpoint and lease intervals, and the live attachments, never image or workspace bytes. The idle window is the entity type's `EntityConfig::idle_after` (entity §7).
- **Provisioning:** `Provision` imports the base image as the disk's first manifest (facets §4.4) and folds `Provisioned` with a host-key seed that MUST come from the system's `Entropy` (actor §2.12.1): secret in production, reproducible in simulation. Until it commits, every command but `Status` and `Detach` answers `MachineError::NotProvisioned`; a second one answers `MachineError::AlreadyProvisioned`.
- **Access policy:** `AddKey` and `RevokeKey` fold ordinary events. A revocation stops the next attach and does not end a live connection (§7).
- **The workspace:** the facet directory is the workspace's single durable home; the guest's volatile `/workspace` is filled at boot and drained at every capture (§4), so a crash rewind cannot split two histories.
- **File commands need no guest:** `WsWrite`, `WsRead`, `WsList`, and `WsRemove` act on the facet directly, a write committing with its delta record. While a guest runs it owns `/workspace` and they answer `MachineError::Running`; otherwise the next boot's push delivers every write. Paths are confined by capability handle (sandbox §4.1), and one command carries at most `MAX_WS_FILE` bytes.

## 4. Durability: the session is the command

The output gate holds a reply until its events are durable (entity §5), and a terminal cannot wait for a quorum per keystroke. So a machine's command unit is a session, and its disk is durable to the last **capture**.

- **Capture:** the disk facet's capture command (facets §4.4), delivered by the checkpoint alarm (facets §4.5), stages one capture manifest through the ordinary barrier, and MUST read the image only with the guest paused or stopped. Between captures the guest writes the image in place: visible to whoever is attached, not committed, and discarded by a non-committed outcome (E20). That is the disk facet's one departure from the physical discipline, and M3 bounds it.
- **Sequence:** flush (`Sync`), pull the workspace, stage its delta, pause (mid-session) or stop (final), capture the disk, resume. Workspace and disk commit in one batch (E19), the workspace as of the pull and the disk as of the pause. A failed pull journals `WsCaptureSkipped` and the disk capture proceeds. A failed pause stops the guest and skips the disk capture while the pulled delta still commits, leaving the workspace one cadence ahead and the image dirty.
- **Quiescent points:** (1) when the last attachment goes, the final capture runs at once with the guest stopped, retrying each cadence tick until it commits, after which the entity can hibernate (entity §7); (2) while attached, the checkpoint alarm pauses, captures, and resumes. A machine SHOULD checkpoint at a configured interval while attached, or M3's window is the whole session.
- **Session lease:** a deposed activation that appends nothing never learns it is deposed and may keep serving (entity §6), here a live terminal. So while attached, the activation MUST commit a fenced append at least once per lease interval. Every alarm fire stages its re-arm record (facets §4.5), so an alarm firing once per `min(checkpoint, lease)` meets it. Any outcome but a contiguous commit ends the activation (entity §5), stopping the guest and severing its attachments (M5).
- **Planned migration:** a cooperative handoff (storage §5.3) is a forced step-down, which stages nothing (facets §3): lossless for a machine already captured, M3's window for an attached, dirty one (§7).
- **The relaxed barrier:** a capture counts only once it commits on a quorum; only the frequency relaxes. Only captures append disk state, under the term fence (storage §5), each one record in one batch, so a crash falls back to a consistent prior capture, never a torn composite (F4, E19). The lost-write window is the capture cadence (M3); the doomed-session window is the lease plus one bounded capture and the append timeout (M5).

## 5. Lifecycle and failure

- **Activation on attach:** the first `Attach` activates the entity on its shard leader, paying at most one quorum head-recovery (storage §8, entity §7, E8), and boots a guest against the rehydrated image.
- **Crash and failover:** the new leader recovers the committed head from a write quorum (storage §5, E14) and boots the last captured image on the next attach. Later writes are lost and the connection dropped; the disk never forks or corrupts (storage §10, E11).
- **The deposed side:** until the lease expires, a deposed leader's guest can live beside the new leader's, unable to commit. The deposed activation MUST stop its guest and sever its attachments within the lease (§4, M5), and its image is discarded (E20).
- **Routing:** each connection resolves the current leader; a leadership change severs it rather than rehoming it, and the client reconnects to the new leader's door.
- **Reporting loss:** a severed connection MUST reach the client as a disconnect, and a channel whose guest end vanished as exit status 255, never as a hang (cf. sandbox §5). A reconnect after a crash MAY see the disk rewound to the last capture (M3), never a mixture of histories (M1).

## 6. Invariants

These hold under the faults of entity §11 and add to E1–E21 and F1–F4. A machine conforms iff M1–M6 hold and its facets meet F1–F4 (facets §3). The machine entity takes time and randomness only from its system (actor §2.12.1), so one simulation seed reproduces an attach, a crash, a partition, a failover, and a reconnect (actor §2.12.2). The Rust copy of this table, with the tests that verify each row, is `machine-entity`'s conformance catalogue.

| # | Invariant | Defined in |
|---|---|---|
| **M1** | **Single disk, never forked.** Only the shard leader appends disk state, term-fenced (E1), each capture one atomic record (E19), so the rootfs never forks or mixes histories. | §4, §5; storage §5, facets §4.4 |
| **M2** | **Lossless across idle hibernation.** An idle machine re-activates with the disk as of the final capture, run when the last attachment goes (E12, E14). A cooperative handoff runs no capture, so it is lossless only for a machine already captured. | §4, §5 |
| **M3** | **Bounded crash window.** On node loss mid-session the disk rewinds to the last committed capture, never a fork or a torn image (E19, F4, E11); the window is the capture cadence. | §4, §5 |
| **M4** | **Authenticated, attributable, isolated ingress.** A session is bridged only for a key the current journaled policy authorizes; every attachment and detachment is journaled with a principal derived from that key; a session reaches only its own machine's guest. | §2.5 |
| **M5** | **A deposed activation self-fences within the lease.** While attached it commits a fenced append once per lease interval, or stops its guest and severs its attachments; the deposed guest never commits (M1). | §4, §5 |
| **M6** | **Policy-bound, attributable, isolated egress.** A guest's outbound path is at most what its policy grants, attributable per machine, and never reaches another guest, a sandbox, the host's services, or cluster-internal addresses. | §2.6 |

The workspace adds no invariant: its delta rides the disk's captures (§4), so M1–M3 cover it. A failed pause leaves the workspace one cadence ahead, two consistent points, never a torn image.

## 7. Deferred

- **Warm resume:** a hibernated or migrated machine reboots its guest; resuming processes needs memory snapshots as blobs.
- **Lazy block hydration:** an attach materializes the whole image, so first-attach latency grows with image size (entity §13).
- **An external blob store:** capture blobs live on every shard replica; an object store behind the entity's blob seam (entity §13) would cut idle cost and needs lazy hydration.
- **Fleet-wide base-image sharing:** each machine's import stores its own copy of the base blocks.
- **Restore and fork:** checkpoints make both metadata operations, and restore is the only recovery for a compromised rootfs; both need a policy first.
- **Capture before a planned handoff:** a rolling restart costs an attached, dirty machine M3's window.
- **Within-connection migration:** a leadership change severs established connections.
- **A cross-node front-door relay:** doors serve only their node's machines, so clients reconnect after a move; a relay also needs transport flow control.
- **Ending live connections:** no command ends an attached connection; revocation acts on the next attach.
- **Caller-scoped host-key reads:** the entity answers `DoorPolicy` to any cluster member, not only front doors.
- **Richer ingress:** port forwarding and non-SSH protocols (sandbox §7).
- **Quotas:** per-owner counts, disk, CPU, egress budgets, and idle reaping (agent §1.1, §11).
