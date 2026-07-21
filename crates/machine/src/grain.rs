//! The machine grain (machine spec §1, §3, §4, §6).
//!
//! `Facets = (Disk, Alarm, Ws)`: the disk facet (grain §7.15) owns the rootfs
//! image's capture, checkpoint, and rehydration; the alarm facet (grain
//! §7.16) supplies the one timer machine §4 needs; the workspace facet (grain
//! §7.11) is the machine's `/workspace` volume — a host-side directory the
//! guest sees as a tmpfs, pushed at boot and pulled at every capture
//! quiescent point, so the ws delta and the disk manifest commit in **one
//! atomic batch** (G19). Facet-0 state is metadata only (machine §3): keys,
//! host key, egress policy, sizing, intervals, and the live attachment set —
//! never image or workspace bytes.
//!
//! **File commands without a VM (machine §3).** [`WsWrite`], [`WsRead`],
//! [`WsList`], and [`WsRemove`] operate on the workspace facet directly —
//! activating the grain activates *no* microVM (boot is attach-driven) — so
//! an agent or tool reads and writes a hibernated machine's `/workspace`
//! durably without booting it. While a VM is live the guest owns
//! `/workspace`, and mutating commands refuse with
//! [`MachineError::VmLive`]; the next boot's push delivers host-side writes.
//!
//! **The session is the command (machine §4).** Between captures the guest
//! writes the image out of band; durability is capture-grained. The
//! **checkpoint alarm** is the whole cadence: it fires every
//! `min(checkpoint, lease)` while anything needs it, runs the capture command
//! when a full checkpoint interval has elapsed, and — because a fired alarm
//! always stages its consume/re-arm record — every fire is a **fenced
//! append**, which makes the alarm the session lease too (M5): on a deposed
//! or partitioned activation the append cannot commit, the host steps down,
//! and `on_passivate` kills the microVM, bounding the doomed-session window
//! to one alarm interval plus the append timeout.
//!
//! **Quiescent points (machine §4).** Mid-session the alarm pauses the guest,
//! captures, resumes (M3's window is the checkpoint interval). When the last
//! attachment detaches the alarm is re-armed to fire immediately: the final
//! fire stops the guest, runs the last capture, and leaves the grain clean, so
//! `can_passivate` (which refuses while attached or dirty) lets the §10 idle
//! path hibernate a machine whose disk is fully durable (M2).

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_core::ActorId;
use actor_core::Instant;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::TerminationReason;
use granary::Alarm;
use granary::BlobId;
use granary::Disk;
use granary::Grain;
use granary::GrainCtx;
use granary::GrainHandler;
use granary::GrainRegistry;
use granary::GranarySystem;
use granary::Ws;
use granary::WsError;
use serde::Deserialize;
use serde::Serialize;

use crate::vm::MachineVm;
use crate::vm::MachineVmProvider;
use crate::vm::VmSpec;

/// The machine's stable grain type (machine §1: `MachineId` is a `GrainName`
/// with this type and the machine's name as the key).
pub const MACHINE_TYPE: &str = "machine";

/// A machine operation failed — an application-level outcome carried in the
/// reply, distinct from a durability failure (grain §12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MachineError {
    AlreadyProvisioned,
    NotProvisioned,
    Disk(String),
    Vm(String),
    /// A workspace file command arrived while the microVM is running: the
    /// guest owns `/workspace` while live (machine §3), and a host write
    /// would be clobbered by the next pull. Detach (or wait for hibernation)
    /// and retry.
    VmLive,
    /// A workspace file operation failed (a bad path, a cap, an IO error).
    Ws(String),
}

impl std::fmt::Display for MachineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineError::AlreadyProvisioned => write!(f, "machine already provisioned"),
            MachineError::NotProvisioned => write!(f, "machine not provisioned"),
            MachineError::Disk(e) => write!(f, "machine disk: {e}"),
            MachineError::Vm(e) => write!(f, "machine vm: {e}"),
            MachineError::VmLive => {
                write!(
                    f,
                    "machine vm is running: the guest owns /workspace while live"
                )
            }
            MachineError::Ws(e) => write!(f, "machine workspace: {e}"),
        }
    }
}

impl std::error::Error for MachineError {}

/// The journaled egress policy (machine §5.2, M6). `Open` is the fresh
/// machine's default — the development-machine posture; an owner MAY narrow
/// it, and the provider realizes exactly what it grants.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EgressPolicy {
    #[default]
    Open,
    Allowlist(Vec<String>),
    None,
}

/// Why an attachment ended (machine §5.1, §6).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DetachReason {
    /// The connection closed and the front door detached it.
    Closed,
    /// The front-door member holding it died; the death watch folded the
    /// detach (machine §5.1's attachment liveness).
    FrontDoorLost,
}

/// One live attachment (machine §5.1): who reached the machine, through which
/// front-door member, when — the journaled, attributable half of M4.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub principal: String,
    /// The front-door actor holding the connection — the death-watch target.
    pub front_door: ActorId,
    pub at_nanos: u64,
}

/// Facet-0 state (machine §3): the machine's shape and policy, nothing bulk.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MachineState {
    pub provisioned: bool,
    pub owner: String,
    /// `fingerprint → public key` (machine §3): the front door authenticates
    /// against the current folded set (M4).
    pub authorized_keys: BTreeMap<String, String>,
    /// The machine's SSH host-key material (machine §3): one identity across
    /// hibernation, migration, and failover. Lives in folded state, inside the
    /// cluster's own trust boundary, which already holds the journal it
    /// travels in.
    pub host_key: Vec<u8>,
    pub egress: EgressPolicy,
    pub vcpus: u8,
    pub mem_mib: u32,
    pub base_image: String,
    /// The capture cadence while attached (machine §4, M3).
    pub checkpoint_nanos: u64,
    /// The self-fence bound (machine §4, M5): the alarm — the activation's
    /// fenced append — fires at least once per `min(checkpoint, lease)`.
    pub lease_nanos: u64,
    pub attachments: BTreeMap<u64, Attachment>,
    pub next_attachment: u64,
    /// Committed captures folded so far (observability; the disk facet holds
    /// the image itself).
    pub captures: u64,
    /// Committed workspace captures that staged a delta (observability; the
    /// ws facet holds the files themselves).
    pub ws_captures: u64,
}

impl MachineState {
    fn alarm_cadence(&self) -> Duration {
        Duration::from_nanos(self.checkpoint_nanos.min(self.lease_nanos).max(1))
    }
}

/// The journal record (machine §3, §5.1): provisioning, policy, attachment,
/// and capture history — an operator answers "who reached this machine, and
/// when" from the journal alone (M4).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MachineEvent {
    Provisioned {
        owner: String,
        base_image: String,
        vcpus: u8,
        mem_mib: u32,
        checkpoint_nanos: u64,
        lease_nanos: u64,
        host_key: Vec<u8>,
        authorized_keys: BTreeMap<String, String>,
    },
    KeyAdded {
        fingerprint: String,
        key: String,
    },
    KeyRevoked {
        fingerprint: String,
    },
    EgressChanged {
        policy: EgressPolicy,
    },
    Attached {
        id: u64,
        principal: String,
        front_door: ActorId,
        at_nanos: u64,
    },
    Detached {
        id: u64,
        at_nanos: u64,
        reason: DetachReason,
    },
    Captured {
        at_nanos: u64,
        blocks: u32,
        bytes: u64,
    },
    /// A workspace pull staged a delta into the same batch as its disk
    /// capture (machine §4): the two commit together or not at all (G19).
    WsCaptured {
        at_nanos: u64,
        written: u32,
        removed: u32,
        tree_bytes: u64,
    },
}

/// Ephemeral activation state (machine §1: the live microVM is one of the two
/// disposable things). Reset on every activation (**G3**).
#[derive(Default)]
struct Activation {
    vm: Option<Arc<dyn MachineVm>>,
    /// The guest has run since the last committed capture — the conservative
    /// dirty flag of grain §7.15: the host cannot observe guest block writes,
    /// so "has run" over-approximates "has written", which is safe (an extra
    /// capture, never a stranded dirty image).
    dirty: bool,
    /// When the last capture committed (activation clock), so the alarm — the
    /// lease heartbeat — captures only once per checkpoint interval.
    last_capture: Option<Instant>,
    /// Front-door members already death-watched by this activation, so the
    /// re-watch sweep (below) registers each once.
    watched: std::collections::BTreeSet<ActorId>,
}

/// The machine grain (machine §1). `P` is the node's VM binding, injected per
/// activation through the `granary_named` factory; `S` is the hosting system.
pub struct Machine<S: GranarySystem, P: MachineVmProvider> {
    provider: Arc<P>,
    act: Mutex<Activation>,
    _system: PhantomData<fn() -> S>,
}

impl<S: GranarySystem, P: MachineVmProvider> Machine<S, P> {
    pub fn new(provider: Arc<P>) -> Machine<S, P> {
        Machine {
            provider,
            act: Mutex::new(Activation::default()),
            _system: PhantomData,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Activation> {
        self.act.lock().expect("machine activation lock")
    }

    /// Boot the microVM if it is not running (machine §6: activation on
    /// attach; boot touches no consensus), then push the workspace facet's
    /// directory into the guest's `/workspace` (machine §4). A failed push is
    /// hard: the guest must never serve against a silently stale workspace,
    /// so the VM is killed and the command fails.
    async fn ensure_vm(
        &self,
        state: &MachineState,
        ctx: &GrainCtx<Self>,
    ) -> Result<(), MachineError> {
        if self.lock().vm.is_some() {
            return Ok(());
        }
        let image = ctx
            .disk()
            .path()
            .map_err(|e| MachineError::Disk(e.to_string()))?;
        let ws_dir = ctx
            .ws()
            .dir_path()
            .map_err(|e| MachineError::Ws(e.to_string()))?;
        let vm = self
            .provider
            .boot(VmSpec {
                image,
                vcpus: state.vcpus,
                mem_mib: state.mem_mib,
                machine: ctx.name().clone(),
            })
            .await
            .map_err(|e| MachineError::Vm(e.to_string()))?;
        if let Err(e) = vm.push_ws(ws_dir).await {
            vm.kill().await;
            return Err(MachineError::Vm(format!("workspace push: {e}")));
        }
        let mut act = self.lock();
        act.vm = Some(vm);
        act.dirty = true;
        Ok(())
    }

    /// Stop the microVM (idempotent; machine §1: stopping it loses no
    /// committed block).
    async fn kill_vm(&self) {
        let vm = self.lock().vm.take();
        if let Some(vm) = vm {
            vm.kill().await;
        }
    }

    /// Death-watch the front-door member of every folded attachment not yet
    /// watched by this activation (machine §5.1). `on_activate` cannot see
    /// state, so a rehydrated machine re-watches on its first state-bearing
    /// entry point — the pending alarm's fire, or any command; watch-after-
    /// death then folds `Detached { FrontDoorLost }` for holders that died
    /// while the grain was down.
    fn watch_attachments(&self, state: &MachineState, ctx: &GrainCtx<Self>) {
        for attachment in state.attachments.values() {
            self.watch_door(&attachment.front_door, ctx);
        }
    }

    /// Death-watch one front-door member idempotently (the `watched` set makes
    /// a re-watch a no-op), so a rehydrated machine and a fresh attach share
    /// one registration path.
    fn watch_door(&self, door: &ActorId, ctx: &GrainCtx<Self>) {
        if self.lock().watched.insert(door.clone()) {
            ctx.watch(door.clone());
        }
    }

    /// Record a committed capture (advance the capture clock) and map its
    /// stats to the journal event — nothing for a clean image (§7.5). Shared
    /// by the mid-session and final capture paths.
    fn capture_event(&self, stats: granary::DiskCaptureStats, now: Instant) -> Vec<MachineEvent> {
        self.lock().last_capture = Some(now);
        if stats.blocks == 0 {
            vec![]
        } else {
            vec![MachineEvent::Captured {
                at_nanos: now.as_nanos(),
                blocks: stats.blocks,
                bytes: stats.bytes,
            }]
        }
    }

    /// Pull the guest's `/workspace` into the facet's directory and stage the
    /// delta into the *current* command's batch, so it commits atomically
    /// with the disk manifest that follows (G19). Must run while the guest
    /// can still answer — before pause or kill (machine §4).
    ///
    /// Degrades gracefully (machine §5.1: a broken agent severs access, not
    /// the machine): a failed pull skips ws staging this cadence and the disk
    /// capture proceeds — the ws delta simply waits for the next quiescent
    /// point. A [`WsError::TooLarge`] likewise stages nothing and self-heals
    /// at the next under-cap capture.
    ///
    /// Cost note: a pull rewrites every workspace file (fresh mtimes), so the
    /// capture's stat-prune never applies on this path — each checkpoint
    /// re-hashes the full pulled tree, accepted at the facet's 64 MiB cap.
    async fn pull_and_capture_ws(
        &self,
        vm: Option<&Arc<dyn MachineVm>>,
        ctx: &GrainCtx<Self>,
        now: Instant,
    ) -> Vec<MachineEvent> {
        let Some(vm) = vm else { return vec![] };
        let ws_dir = match ctx.ws().dir_path() {
            Ok(dir) => dir,
            Err(_) => return vec![],
        };
        if vm.pull_ws(ws_dir).await.is_err() {
            return vec![];
        }
        match ctx.ws().capture() {
            Ok(stats) if stats.written > 0 || stats.removed > 0 => {
                vec![MachineEvent::WsCaptured {
                    at_nanos: now.as_nanos(),
                    written: stats.written as u32,
                    removed: stats.removed as u32,
                    tree_bytes: stats.tree_bytes,
                }]
            }
            _ => vec![],
        }
    }
}

impl<S: GranarySystem, P: MachineVmProvider> Grain for Machine<S, P> {
    type System = S;
    type State = MachineState;
    type Event = MachineEvent;
    type Facets = (Disk, Alarm, Ws);
    const GRAIN_TYPE: &'static str = MACHINE_TYPE;

    fn apply(state: &mut MachineState, event: &MachineEvent) {
        match event {
            MachineEvent::Provisioned {
                owner,
                base_image,
                vcpus,
                mem_mib,
                checkpoint_nanos,
                lease_nanos,
                host_key,
                authorized_keys,
            } => {
                state.provisioned = true;
                state.owner = owner.clone();
                state.base_image = base_image.clone();
                state.vcpus = *vcpus;
                state.mem_mib = *mem_mib;
                state.checkpoint_nanos = *checkpoint_nanos;
                state.lease_nanos = *lease_nanos;
                state.host_key = host_key.clone();
                state.authorized_keys = authorized_keys.clone();
            }
            MachineEvent::KeyAdded { fingerprint, key } => {
                state
                    .authorized_keys
                    .insert(fingerprint.clone(), key.clone());
            }
            MachineEvent::KeyRevoked { fingerprint } => {
                state.authorized_keys.remove(fingerprint);
            }
            MachineEvent::EgressChanged { policy } => {
                state.egress = policy.clone();
            }
            MachineEvent::Attached {
                id,
                principal,
                front_door,
                at_nanos,
            } => {
                state.attachments.insert(
                    *id,
                    Attachment {
                        principal: principal.clone(),
                        front_door: front_door.clone(),
                        at_nanos: *at_nanos,
                    },
                );
                state.next_attachment = state.next_attachment.max(*id + 1);
            }
            MachineEvent::Detached { id, .. } => {
                state.attachments.remove(id);
            }
            MachineEvent::Captured { .. } => {
                state.captures += 1;
            }
            MachineEvent::WsCaptured { .. } => {
                state.ws_captures += 1;
            }
        }
    }

    fn register(r: &mut GrainRegistry<Self>) {
        r.accept::<Provision>();
        r.accept::<Attach>();
        r.accept::<Detach>();
        r.accept::<AddKey>();
        r.accept::<RevokeKey>();
        r.accept::<SetEgress>();
        r.accept::<Status>();
        r.accept::<WsWrite>();
        r.accept::<WsRead>();
        r.accept::<WsList>();
        r.accept::<WsRemove>();
    }

    async fn on_activate(&mut self, _ctx: &GrainCtx<Self>) -> Result<(), actor_core::BoxError> {
        // The VM, dirty flag, and watch set are activation-local (G3): a fresh
        // activation has a freshly rehydrated — clean — image and no guest.
        // Folded attachments are re-watched by `watch_attachments` on the first
        // state-bearing entry point (the pending alarm's fire, or a command).
        *self.lock() = Activation::default();
        Ok(())
    }

    fn can_passivate(&self, state: &MachineState) -> bool {
        // A live attachment pins the activation (machine §5.1), and an image
        // holding uncaptured writes must not be stranded by the idle path
        // (grain §7.15): the final capture command clears the flag first
        // (machine §4, quiescent point 1).
        let act = self.lock();
        state.attachments.is_empty() && !act.dirty
    }

    async fn on_passivate(&mut self, _ctx: &GrainCtx<Self>) {
        // Every deactivation — idle hibernation after the final capture, or a
        // forced step-down (the self-fence of M5: a failed fenced append ends
        // the activation, and the guest must not outlive it) — kills the VM.
        self.kill_vm().await;
    }

    async fn on_alarm(&self, state: &MachineState, ctx: &GrainCtx<Self>) -> Vec<MachineEvent> {
        // A rehydrated machine's first alarm re-watches its folded
        // attachments' holders (machine §5.1); a no-op otherwise.
        self.watch_attachments(state, ctx);
        let now = ctx.system().now();
        if state.attachments.is_empty() {
            // Quiescent point 1 (machine §4): the final capture. Pull the
            // workspace while the guest can still answer, stop the guest,
            // capture the settled image, and leave the grain clean so idle
            // hibernation can proceed (M2). The ws delta and the disk
            // manifest ride this one command's batch (G19). No re-arm: the
            // alarm is consumed and `can_passivate` now permits eviction.
            let vm = self.lock().vm.clone();
            let mut ws_events = self.pull_and_capture_ws(vm.as_ref(), ctx, now).await;
            self.kill_vm().await;
            match ctx.disk().capture().await {
                Ok(stats) => {
                    self.lock().dirty = false;
                    ws_events.extend(self.capture_event(stats, now));
                    return ws_events;
                }
                Err(_) => {
                    // The capture could not complete (blob quorum, IO). Keep
                    // the dirty pin and retry on the next cadence tick. The
                    // staged ws delta still commits with this batch — it
                    // reflects real pulled bytes.
                    ctx.alarm().set_after(state.alarm_cadence());
                    return ws_events;
                }
            }
        }

        // Quiescent point 3 (machine §4): the mid-session checkpoint. Every
        // fire re-arms, and the consume/re-arm record makes this a fenced
        // append — the session lease (M5) — even when no capture is due.
        ctx.alarm().set_after(state.alarm_cadence());
        let checkpoint_due = match self.lock().last_capture {
            Some(last) => now.duration_since(last) >= Duration::from_nanos(state.checkpoint_nanos),
            None => true,
        };
        if !checkpoint_due {
            return vec![];
        }
        // Pull before pause — a paused guest cannot answer vsock — then
        // capture both facets in this command: one atomic batch (G19). The
        // ws delta is as-of-pull, the disk as-of-pause; the skew is bounded
        // by the pull duration (machine §4).
        let vm = self.lock().vm.clone();
        let mut ws_events = self.pull_and_capture_ws(vm.as_ref(), ctx, now).await;
        if let Some(vm) = &vm
            && vm.pause().await.is_err()
        {
            // A guest that cannot pause cannot be captured consistently
            // (grain §7.15: never capture a running guest's image). Treat the
            // VM as lost; the image keeps its writes and stays dirty until the
            // next capture, exactly the M3 window. The staged ws delta still
            // commits — it reflects real pulled bytes.
            self.kill_vm().await;
            return ws_events;
        }
        let out = ctx.disk().capture().await;
        if let Some(vm) = &vm {
            let _ = vm.resume().await;
        }
        match out {
            Ok(stats) => {
                ws_events.extend(self.capture_event(stats, now));
                ws_events
            }
            Err(_) => ws_events,
        }
    }

    async fn on_peer_terminated(
        &self,
        state: &MachineState,
        ctx: &GrainCtx<Self>,
        peer: &ActorId,
        _reason: TerminationReason,
    ) -> Vec<MachineEvent> {
        // A front-door member died without detaching (machine §5.1): fold
        // `Detached { FrontDoorLost }` for its attachments, releasing their
        // pin; the idle window then runs from this release.
        let now_nanos = ctx.system().now().as_nanos();
        let dropped: Vec<u64> = state
            .attachments
            .iter()
            .filter(|(_, a)| &a.front_door == peer)
            .map(|(id, _)| *id)
            .collect();
        if dropped.is_empty() {
            return vec![];
        }
        if dropped.len() == state.attachments.len() {
            // The last attachments are going: schedule the final capture
            // (machine §4, quiescent point 1) immediately.
            ctx.alarm().set_after(Duration::ZERO);
        }
        dropped
            .into_iter()
            .map(|id| MachineEvent::Detached {
                id,
                at_nanos: now_nanos,
                reason: DetachReason::FrontDoorLost,
            })
            .collect()
    }
}

// --- Commands -----------------------------------------------------------------

/// Provision the machine (machine §3): a journaled `Provisioned` event whose
/// disk begins as the base image's full-coverage manifest (grain §7.15).
///
/// The host key here is a **placeholder** derived from the machine's name —
/// deterministic, not secret — until the front door (machine §5.1) supplies
/// real SSH keypair generation.
#[derive(Clone, Serialize, Deserialize)]
pub struct Provision {
    pub owner: String,
    /// Node-local path of the base image to import.
    pub base_image: String,
    pub vcpus: u8,
    pub mem_mib: u32,
    pub checkpoint: Duration,
    pub lease: Duration,
    pub authorized_keys: BTreeMap<String, String>,
}

impl Message for Provision {
    type Reply = Result<(), MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.Provision");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<Provision> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: Provision,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<(), MachineError>) {
        if state.provisioned {
            return (vec![], Err(MachineError::AlreadyProvisioned));
        }
        if let Err(e) = ctx
            .disk()
            .import(std::path::Path::new(&msg.base_image))
            .await
        {
            return (vec![], Err(MachineError::Disk(e.to_string())));
        }
        let host_key = BlobId::of(format!("machine-host-key:{}", ctx.name()).as_bytes()).as_bytes()
            [..]
            .to_vec();
        (
            vec![MachineEvent::Provisioned {
                owner: msg.owner,
                base_image: msg.base_image,
                vcpus: msg.vcpus,
                mem_mib: msg.mem_mib,
                checkpoint_nanos: msg.checkpoint.as_nanos() as u64,
                lease_nanos: msg.lease.as_nanos() as u64,
                host_key,
                authorized_keys: msg.authorized_keys,
            }],
            Ok(()),
        )
    }
}

/// What a successful [`Attach`] returns to the front door (machine §5.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachReply {
    pub attachment: u64,
    /// The machine's host-key material, so the front door presents one SSH
    /// identity across hibernation, migration, and failover (M4).
    pub host_key: Vec<u8>,
}

/// Attach a connection (machine §5.1): journaled with its principal (M4),
/// boots the microVM lazily, death-watches the holding front-door member, and
/// starts the checkpoint/lease cadence (machine §4).
#[derive(Clone, Serialize, Deserialize)]
pub struct Attach {
    pub principal: String,
    /// The front-door actor holding this connection — the death-watch target
    /// (machine §5.1's attachment liveness).
    pub front_door: ActorId,
}

impl Message for Attach {
    type Reply = Result<AttachReply, MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.Attach");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<Attach> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: Attach,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<AttachReply, MachineError>) {
        if !state.provisioned {
            return (vec![], Err(MachineError::NotProvisioned));
        }
        if let Err(e) = self.ensure_vm(state, ctx).await {
            return (vec![], Err(e));
        }
        self.watch_attachments(state, ctx);
        self.watch_door(&msg.front_door, ctx);
        // The capture cadence while attached (machine §4): the checkpoint
        // alarm, which doubles as the session lease's fenced append (M5).
        ctx.alarm().set_after(state.alarm_cadence());
        let id = state.next_attachment;
        (
            vec![MachineEvent::Attached {
                id,
                principal: msg.principal,
                front_door: msg.front_door,
                at_nanos: ctx.system().now().as_nanos(),
            }],
            Ok(AttachReply {
                attachment: id,
                host_key: state.host_key.clone(),
            }),
        )
    }
}

/// Detach a connection (machine §5.1): journaled; when the last attachment
/// goes, the final capture is scheduled immediately (machine §4, quiescent
/// point 1) so the idle path finds a clean disk.
#[derive(Clone, Serialize, Deserialize)]
pub struct Detach {
    pub attachment: u64,
}

impl Message for Detach {
    type Reply = bool;
    const MANIFEST: Manifest = Manifest::new("machine.Detach");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<Detach> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: Detach,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, bool) {
        if !state.attachments.contains_key(&msg.attachment) {
            return (vec![], false);
        }
        if state.attachments.len() == 1 {
            ctx.alarm().set_after(Duration::ZERO);
        }
        (
            vec![MachineEvent::Detached {
                id: msg.attachment,
                at_nanos: ctx.system().now().as_nanos(),
                reason: DetachReason::Closed,
            }],
            true,
        )
    }
}

/// Authorize a key (machine §3): an ordinary journaled event; the next attach
/// sees the folded set (M4).
#[derive(Clone, Serialize, Deserialize)]
pub struct AddKey {
    pub fingerprint: String,
    pub key: String,
}

impl Message for AddKey {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("machine.AddKey");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<AddKey> for Machine<S, P> {
    async fn handle(
        &self,
        _state: &MachineState,
        msg: AddKey,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, ()) {
        (
            vec![MachineEvent::KeyAdded {
                fingerprint: msg.fingerprint,
                key: msg.key,
            }],
            (),
        )
    }
}

/// Revoke a key (machine §3): stops authorizing the next attach; it does not
/// tear down a live connection (that is a separate administrative action).
#[derive(Clone, Serialize, Deserialize)]
pub struct RevokeKey {
    pub fingerprint: String,
}

impl Message for RevokeKey {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("machine.RevokeKey");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<RevokeKey> for Machine<S, P> {
    async fn handle(
        &self,
        _state: &MachineState,
        msg: RevokeKey,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, ()) {
        (
            vec![MachineEvent::KeyRevoked {
                fingerprint: msg.fingerprint,
            }],
            (),
        )
    }
}

/// Change the egress policy (machine §5.2): a journaled event, effective from
/// the next activation or applied live where the provider can.
#[derive(Clone, Serialize, Deserialize)]
pub struct SetEgress {
    pub policy: EgressPolicy,
}

impl Message for SetEgress {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("machine.SetEgress");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<SetEgress> for Machine<S, P> {
    async fn handle(
        &self,
        _state: &MachineState,
        msg: SetEgress,
        _ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, ()) {
        (vec![MachineEvent::EgressChanged { policy: msg.policy }], ())
    }
}

/// A read of the machine's shape (grain §7.5: commits nothing).
#[derive(Clone, Serialize, Deserialize)]
pub struct Status;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatusReply {
    pub provisioned: bool,
    pub owner: String,
    pub egress: EgressPolicy,
    pub attachments: Vec<(u64, String)>,
    pub captures: u64,
    pub ws_captures: u64,
    pub image_bytes: u64,
    pub vm_running: bool,
    /// BLAKE3 of the live image (verification surface for tests and
    /// operators; reads the activation-local materialization).
    pub image_digest: Option<BlobId>,
}

impl Message for Status {
    type Reply = StatusReply;
    const MANIFEST: Manifest = Manifest::new("machine.Status");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<Status> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        _msg: Status,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, StatusReply) {
        let image_bytes = ctx.disk().image_bytes().unwrap_or(0);
        // The committed-image digest, derived from the block index — no
        // multi-MiB read on this read command (grain §7.15).
        let image_digest = ctx.disk().content_digest().ok().flatten();
        (
            vec![],
            StatusReply {
                provisioned: state.provisioned,
                owner: state.owner.clone(),
                egress: state.egress.clone(),
                attachments: state
                    .attachments
                    .iter()
                    .map(|(id, a)| (*id, a.principal.clone()))
                    .collect(),
                captures: state.captures,
                ws_captures: state.ws_captures,
                image_bytes,
                vm_running: self.lock().vm.is_some(),
                image_digest,
            },
        )
    }
}

// --- Workspace file commands (machine §3) --------------------------------------

/// Cap on one file command's bytes, both directions: a [`WsWrite`]'s record
/// and a [`WsRead`]'s reply must stay bounded (the ws facet's records carry
/// bytes inline, grain §7.11).
pub const MAX_WS_FILE: usize = 1024 * 1024;

/// Validate a workspace-relative path: non-empty, normal components only.
/// The subsequent operations go through a capability handle over the facet's
/// directory, so an escape is unrepresentable even past this check (S1's
/// belt-and-suspenders, as in the sandbox's Workspace tier).
fn ws_rel_path(path: &str) -> Result<&std::path::Path, MachineError> {
    let p = std::path::Path::new(path);
    if p.as_os_str().is_empty()
        || !p
            .components()
            .all(|c| matches!(c, std::path::Component::Normal(_)))
    {
        return Err(MachineError::Ws(format!(
            "invalid workspace path: {path:?}"
        )));
    }
    Ok(p)
}

/// Open the workspace facet's directory as a capability handle.
fn ws_open<S: GranarySystem, P: MachineVmProvider>(
    ctx: &GrainCtx<Machine<S, P>>,
) -> Result<cap_std::fs::Dir, MachineError> {
    let root = ctx
        .ws()
        .dir_path()
        .map_err(|e| MachineError::Ws(e.to_string()))?;
    cap_std::fs::Dir::open_ambient_dir(&root, cap_std::ambient_authority())
        .map_err(|e| MachineError::Ws(format!("open workspace: {e}")))
}

impl<S: GranarySystem, P: MachineVmProvider> Machine<S, P> {
    /// The shared guard of every workspace file command: the machine must be
    /// provisioned, and no microVM may be live — while one runs the guest
    /// owns `/workspace` (machine §3) and a host mutation would be clobbered
    /// by the next pull.
    fn ws_command_guard(&self, state: &MachineState) -> Result<(), MachineError> {
        if !state.provisioned {
            return Err(MachineError::NotProvisioned);
        }
        if self.lock().vm.is_some() {
            return Err(MachineError::VmLive);
        }
        Ok(())
    }

    /// Stage a mutating file command's delta into its own batch: the write
    /// and its durability record are one commit. A capture failure fails the
    /// command — the caller must not believe an unstaged write durable; the
    /// materialized file is picked up by the next successful capture (the
    /// facet diffs against the committed index, self-healing).
    fn ws_stage(&self, ctx: &GrainCtx<Self>) -> Result<(), MachineError> {
        match ctx.ws().capture() {
            Ok(_) => Ok(()),
            Err(WsError::TooLarge { bytes, cap }) => Err(MachineError::Ws(format!(
                "durable workspace is {bytes} bytes, over the {cap}-byte cap"
            ))),
            Err(e) => Err(MachineError::Ws(e.to_string())),
        }
    }
}

/// Write one file into the machine's `/workspace` without booting it
/// (machine §3): the bytes land in the workspace facet's directory and their
/// delta commits in this command's batch. The next boot pushes them into the
/// guest.
#[derive(Clone, Serialize, Deserialize)]
pub struct WsWrite {
    pub path: String,
    pub bytes: Vec<u8>,
}

impl Message for WsWrite {
    type Reply = Result<(), MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.WsWrite");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<WsWrite> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: WsWrite,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<(), MachineError>) {
        let outcome = (|| {
            self.ws_command_guard(state)?;
            if msg.bytes.len() > MAX_WS_FILE {
                return Err(MachineError::Ws(format!(
                    "file is {} bytes, over the {MAX_WS_FILE}-byte command cap",
                    msg.bytes.len()
                )));
            }
            let rel = ws_rel_path(&msg.path)?;
            let dir = ws_open(ctx)?;
            if let Some(parent) = rel.parent()
                && !parent.as_os_str().is_empty()
            {
                dir.create_dir_all(parent)
                    .map_err(|e| MachineError::Ws(e.to_string()))?;
            }
            dir.write(rel, &msg.bytes)
                .map_err(|e| MachineError::Ws(e.to_string()))?;
            self.ws_stage(ctx)
        })();
        (vec![], outcome)
    }
}

/// Read one file from the machine's `/workspace` without booting it: the last
/// committed state while hibernated. Stages nothing (grain §7.5).
#[derive(Clone, Serialize, Deserialize)]
pub struct WsRead {
    pub path: String,
}

impl Message for WsRead {
    type Reply = Result<Vec<u8>, MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.WsRead");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<WsRead> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: WsRead,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<Vec<u8>, MachineError>) {
        let outcome = (|| {
            self.ws_command_guard(state)?;
            let rel = ws_rel_path(&msg.path)?;
            let dir = ws_open(ctx)?;
            let len = dir
                .metadata(rel)
                .map_err(|e| MachineError::Ws(e.to_string()))?
                .len();
            if len as usize > MAX_WS_FILE {
                return Err(MachineError::Ws(format!(
                    "file is {len} bytes, over the {MAX_WS_FILE}-byte command cap"
                )));
            }
            dir.read(rel).map_err(|e| MachineError::Ws(e.to_string()))
        })();
        (vec![], outcome)
    }
}

/// One [`WsList`] entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WsFileInfo {
    pub name: String,
    pub len: u64,
    pub is_dir: bool,
}

/// List one level of the machine's `/workspace` without booting it. An empty
/// `path` lists the root. Stages nothing (grain §7.5).
#[derive(Clone, Serialize, Deserialize)]
pub struct WsList {
    pub path: String,
}

impl Message for WsList {
    type Reply = Result<Vec<WsFileInfo>, MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.WsList");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<WsList> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: WsList,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<Vec<WsFileInfo>, MachineError>) {
        let outcome = (|| {
            self.ws_command_guard(state)?;
            let dir = ws_open(ctx)?;
            let listed = if msg.path.is_empty() {
                dir
            } else {
                dir.open_dir(ws_rel_path(&msg.path)?)
                    .map_err(|e| MachineError::Ws(e.to_string()))?
            };
            let mut entries = Vec::new();
            for entry in listed
                .entries()
                .map_err(|e| MachineError::Ws(e.to_string()))?
            {
                let entry = entry.map_err(|e| MachineError::Ws(e.to_string()))?;
                let meta = entry
                    .metadata()
                    .map_err(|e| MachineError::Ws(e.to_string()))?;
                entries.push(WsFileInfo {
                    name: entry.file_name().to_string_lossy().into_owned(),
                    len: meta.len(),
                    is_dir: meta.is_dir(),
                });
            }
            entries.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(entries)
        })();
        (vec![], outcome)
    }
}

/// Remove a file or directory tree from the machine's `/workspace` without
/// booting it; the deletion commits in this command's batch.
#[derive(Clone, Serialize, Deserialize)]
pub struct WsRemove {
    pub path: String,
}

impl Message for WsRemove {
    type Reply = Result<(), MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.WsRemove");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<WsRemove> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: WsRemove,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<(), MachineError>) {
        let outcome = (|| {
            self.ws_command_guard(state)?;
            let rel = ws_rel_path(&msg.path)?;
            let dir = ws_open(ctx)?;
            let meta = dir
                .metadata(rel)
                .map_err(|e| MachineError::Ws(e.to_string()))?;
            let removed = if meta.is_dir() {
                dir.remove_dir_all(rel)
            } else {
                dir.remove_file(rel)
            };
            removed.map_err(|e| MachineError::Ws(e.to_string()))?;
            self.ws_stage(ctx)
        })();
        (vec![], outcome)
    }
}
