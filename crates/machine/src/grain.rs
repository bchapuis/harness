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
    /// The machine's SSH host-key material (machine §3): the raw 32-byte
    /// ed25519 seed drawn from system entropy at provisioning, which the
    /// front door expands into the host key it presents at KEX (machine §5.1)
    /// — one identity across hibernation, migration, and failover. Lives in
    /// folded state, inside the cluster's own trust boundary, which already
    /// holds the journal it travels in.
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
    /// Quiescent points whose workspace pull failed (observability): a
    /// climbing counter with flat `ws_captures` reads as a broken guest
    /// agent.
    pub ws_capture_skips: u64,
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
    /// A workspace pull (or its staging) failed at a quiescent point, so the
    /// disk capture proceeded without a ws delta — machine §5.1's blessed
    /// degrade, journaled so "who lost workspace cadence, and since when" is
    /// answerable from the journal alone (M4's stance).
    WsCaptureSkipped {
        at_nanos: u64,
        reason: String,
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
    /// the machine): a failed pull skips ws staging this cadence — journaled
    /// as [`MachineEvent::WsCaptureSkipped`] — and the disk capture proceeds;
    /// the ws delta simply waits for the next quiescent point. A
    /// [`granary::WsError::TooLarge`] likewise stages nothing and self-heals at the
    /// next under-cap capture.
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
        let skipped = |reason: String| {
            vec![MachineEvent::WsCaptureSkipped {
                at_nanos: now.as_nanos(),
                reason,
            }]
        };
        let ws_dir = match ctx.ws().dir_path() {
            Ok(dir) => dir,
            Err(e) => return skipped(format!("workspace facet: {e}")),
        };
        if let Err(e) = vm.pull_ws(ws_dir).await {
            return skipped(e.to_string());
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
            Ok(_) => vec![],
            Err(e) => skipped(format!("workspace capture: {e}")),
        }
    }

    /// The capture spine shared by the final and mid-session paths (machine
    /// §4): pull the workspace while the guest can still answer, quiesce,
    /// capture the disk facet in the same command's batch (G19), un-quiesce.
    /// Returns the batch and whether the disk capture committed.
    async fn run_capture(
        &self,
        ctx: &GrainCtx<Self>,
        now: Instant,
        quiesce: Quiesce,
    ) -> (Vec<MachineEvent>, bool) {
        let vm = self.lock().vm.clone();
        let mut events = self.pull_and_capture_ws(vm.as_ref(), ctx, now).await;
        match quiesce {
            Quiesce::Stop => self.kill_vm().await,
            Quiesce::Pause => {
                if let Some(vm) = &vm
                    && vm.pause().await.is_err()
                {
                    // A guest that cannot pause cannot be captured
                    // consistently (grain §7.15: never capture a running
                    // guest's image). Treat the VM as lost; the image keeps
                    // its writes and stays dirty until the next capture,
                    // exactly the M3 window. The staged ws delta still
                    // commits — it reflects real pulled bytes.
                    self.kill_vm().await;
                    return (events, false);
                }
            }
        }
        let out = ctx.disk().capture().await;
        if quiesce == Quiesce::Pause
            && let Some(vm) = &vm
        {
            let _ = vm.resume().await;
        }
        match out {
            Ok(stats) => {
                events.extend(self.capture_event(stats, now));
                (events, true)
            }
            Err(_) => (events, false),
        }
    }
}

/// How [`Machine::run_capture`] quiesces the guest (machine §4): the final
/// capture stops it for good; a mid-session checkpoint pauses and resumes
/// around the disk scan.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Quiesce {
    Stop,
    Pause,
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
            MachineEvent::WsCaptureSkipped { .. } => {
                state.ws_capture_skips += 1;
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
            // Quiescent point 1 (machine §4): the final capture. Stop the
            // guest, capture the settled image, and leave the grain clean so
            // idle hibernation can proceed (M2). No re-arm on success: the
            // alarm is consumed and `can_passivate` now permits eviction.
            let (events, committed) = self.run_capture(ctx, now, Quiesce::Stop).await;
            if committed {
                self.lock().dirty = false;
            } else {
                // The capture could not complete (blob quorum, IO). Keep the
                // dirty pin and retry on the next cadence tick. The staged ws
                // delta still commits with this batch — it reflects real
                // pulled bytes.
                ctx.alarm().set_after(state.alarm_cadence());
            }
            return events;
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
        // Pause-and-resume around the capture; the ws delta is as-of-pull,
        // the disk as-of-pause, the skew bounded by the pull duration
        // (machine §4). The guest stays dirty — it keeps running.
        let (events, _committed) = self.run_capture(ctx, now, Quiesce::Pause).await;
        events
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
/// disk begins as the base image's full-coverage manifest (grain §7.15), and
/// which carries the machine's freshly generated host key — 32 bytes of
/// system entropy, journaled as the ed25519 seed the front door expands
/// (machine §5.1).
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
        // The host key is born here and nowhere else: 32 bytes drawn from the
        // system's entropy stream, journaled as a raw ed25519 seed — any 32
        // bytes are a valid seed (RFC 8032) — which the front door expands
        // into the presented host key (machine §5.1). Drawing from the
        // Entropy seam (actor §4.6) keeps both worlds right: production
        // entropy is OS-seeded, so the key is fresh and secret (machine §3);
        // the simulator's is run-seeded, so a replayed seed reproduces the
        // same key (actor §18.1).
        let host_key: Vec<u8> = (0..4)
            .flat_map(|_| ctx.system().next_random().to_le_bytes())
            .collect();
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
/// point 1) so the idle path finds a clean disk. Idempotent — detaching an
/// unknown or already-detached id is a no-op, so there is no failure to
/// report and the reply is `()`.
#[derive(Clone, Serialize, Deserialize)]
pub struct Detach {
    pub attachment: u64,
}

impl Message for Detach {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("machine.Detach");
}

impl<S: GranarySystem, P: MachineVmProvider> GrainHandler<Detach> for Machine<S, P> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: Detach,
        ctx: &GrainCtx<Self>,
    ) -> (Vec<MachineEvent>, ()) {
        if !state.attachments.contains_key(&msg.attachment) {
            return (vec![], ());
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
            (),
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
    /// Quiescent points whose workspace pull failed (see
    /// [`MachineEvent::WsCaptureSkipped`]).
    pub ws_capture_skips: u64,
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
                ws_capture_skips: state.ws_capture_skips,
                image_bytes,
                vm_running: self.lock().vm.is_some(),
                image_digest,
            },
        )
    }
}

mod ws_cmds;

pub use ws_cmds::MAX_WS_FILE;
pub use ws_cmds::WsFileInfo;
pub use ws_cmds::WsList;
pub use ws_cmds::WsRead;
pub use ws_cmds::WsRemove;
pub use ws_cmds::WsWrite;
