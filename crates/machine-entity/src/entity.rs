//! The machine entity (machine spec §1, §3, §4, §6).
//!
//! `Facets = (Disk, Alarm, Ws)`: the disk facet (entity §7.15) owns the rootfs
//! image's capture, checkpoint, and rehydration; the alarm facet (entity
//! §7.16) supplies the one timer machine §4 needs; the workspace facet (entity
//! §7.11) is the machine's `/workspace` volume; a host-side directory the
//! guest sees as a tmpfs, pushed at boot and pulled at every capture
//! quiescent point, so the ws delta and the disk manifest commit in **one
//! atomic batch** (E19). Facet-0 state is metadata only (machine §3): keys,
//! host key, egress policy, sizing, intervals, and the live attachment set,
//! never image or workspace bytes.
//!
//! **File commands without a guest (machine §3).** [`WsWrite`], [`WsRead`],
//! [`WsList`], and [`WsRemove`] operate on the workspace facet directly, and
//! activating the entity activates *no* guest (boot is attach-driven). While
//! a guest is live it owns `/workspace`, and all four refuse with
//! [`MachineError::Running`]; the next boot's push delivers host-side writes.
//!
//! **The session is the command (machine §4).** Between captures the guest
//! writes the image out of band; durability is capture-grained. The
//! **checkpoint alarm** is the whole cadence: it fires every
//! `min(checkpoint, lease)` while anything needs it, and captures when a full
//! checkpoint interval has elapsed. Because a fired alarm always stages its
//! consume/re-arm record, every fire is a **fenced append**, which makes the
//! alarm the session lease too (M5): on a deposed or partitioned activation
//! the append cannot commit, the host steps down, and `on_deactivate` kills the
//! guest, bounding the doomed-session window to one alarm interval plus the
//! append timeout.
//!
//! **Quiescent points (machine §4).** Mid-session the alarm pauses the guest,
//! captures, resumes (M3's window is the checkpoint interval). When the last
//! attachment detaches the alarm re-arms to fire immediately: the final fire
//! stops the guest, runs the last capture, and leaves the entity clean, so
//! `can_hibernate` (which refuses while attached or dirty) lets the §10 idle
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
use entity::Entity;
use entity::EntityCtx;
use entity::EntityHandler;
use entity::EntityRegistry;
use entity::EntitySystem;
use entity::facet::Alarm;
use entity::facet::BlobId;
use entity::facet::Disk;
use entity::facet::Ws;
use serde::Deserialize;
use serde::Serialize;

use crate::runtime::BootSpec;
use crate::runtime::MachineRuntime;
use crate::runtime::MachineRuntimeProvider;

/// The machine's stable entity type (machine §1: `MachineId` is an `EntityName`
/// with this type and the machine's name as the key).
pub const MACHINE_TYPE: &str = "machine";

/// A machine operation failed: an application-level outcome carried in the
/// reply, distinct from a durability failure (entity §12).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MachineError {
    AlreadyProvisioned,
    NotProvisioned,
    Disk(String),
    Runtime(String),
    /// A workspace file command arrived while the machine is running: the
    /// guest owns `/workspace` while live (machine §3), and a host write
    /// would be clobbered by the next pull. Detach (or wait for hibernation)
    /// and retry.
    Running,
    /// A workspace file operation failed (a bad path, a cap, an IO error).
    Ws(String),
}

impl std::fmt::Display for MachineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineError::AlreadyProvisioned => write!(f, "machine already provisioned"),
            MachineError::NotProvisioned => write!(f, "machine not provisioned"),
            MachineError::Disk(e) => write!(f, "machine disk: {e}"),
            MachineError::Runtime(e) => write!(f, "machine runtime: {e}"),
            MachineError::Running => {
                write!(
                    f,
                    "machine is running: the guest owns /workspace while live"
                )
            }
            MachineError::Ws(e) => write!(f, "machine workspace: {e}"),
        }
    }
}

impl std::error::Error for MachineError {}

/// The journaled egress policy (machine §5.2, M6). `Open` is the fresh
/// machine's default: the development-machine posture; an owner MAY narrow
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
/// front-door member, when; the journaled, attributable half of M4.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Attachment {
    pub principal: String,
    /// The front-door actor holding the connection: the death-watch target.
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
    /// front door expands into the host key it presents at KEX (machine §5.1);
    /// one identity across hibernation, migration, and failover.
    pub host_key: Vec<u8>,
    pub egress: EgressPolicy,
    pub vcpus: u8,
    pub mem_mib: u32,
    pub base_image: String,
    /// The capture cadence while attached (machine §4, M3).
    pub checkpoint_nanos: u64,
    /// The self-fence bound (machine §4, M5): the alarm (the activation's
    /// fenced append) fires at least once per `min(checkpoint, lease)`.
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
/// and capture history; an operator answers "who reached this machine, and
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
    /// capture (machine §4): the two commit together or not at all (E19).
    WsCaptured {
        at_nanos: u64,
        written: u32,
        removed: u32,
        tree_bytes: u64,
    },
    /// A workspace pull (or its staging) failed at a quiescent point, so the
    /// disk capture proceeded without a ws delta: machine §5.1's blessed
    /// degrade, journaled so "who lost workspace cadence, and since when" is
    /// answerable from the journal alone (M4).
    WsCaptureSkipped {
        at_nanos: u64,
        reason: String,
    },
}

/// Ephemeral activation state (machine §1: the live guest is one of the two
/// disposable things). Reset on every activation (**E3**).
#[derive(Default)]
struct Activation {
    runtime: Option<Arc<dyn MachineRuntime>>,
    /// The guest has run since the last committed capture, the conservative
    /// dirty flag of entity §7.15: the host cannot observe guest block writes,
    /// so "has run" over-approximates "has written", which is safe (an extra
    /// capture, never a stranded dirty image).
    dirty: bool,
    /// When the last capture committed (activation clock), so the alarm (the
    /// lease heartbeat) captures only once per checkpoint interval.
    last_capture: Option<Instant>,
}

/// The machine entity (machine §1). The provider is the node's runtime binding,
/// injected per activation through the `entities_named` factory; `S` is the
/// hosting system.
pub struct Machine<S: EntitySystem> {
    provider: Arc<dyn MachineRuntimeProvider>,
    act: Mutex<Activation>,
    _system: PhantomData<fn() -> S>,
}

impl<S: EntitySystem> Machine<S> {
    pub fn new(provider: Arc<dyn MachineRuntimeProvider>) -> Machine<S> {
        Machine {
            provider,
            act: Mutex::new(Activation::default()),
            _system: PhantomData,
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Activation> {
        self.act.lock().expect("machine activation lock")
    }

    /// Boot the guest if it is not running (machine §6: activation on
    /// attach; boot touches no consensus), then push the workspace facet's
    /// directory into the guest's `/workspace` (machine §4). A failed push is
    /// hard: the guest must never serve against a silently stale workspace,
    /// so the guest is killed and the command fails.
    async fn ensure_vm(
        &self,
        state: &MachineState,
        ctx: &EntityCtx<Self>,
    ) -> Result<(), MachineError> {
        if self.lock().runtime.is_some() {
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
        let runtime = self
            .provider
            .boot(BootSpec {
                image,
                vcpus: state.vcpus,
                mem_mib: state.mem_mib,
                machine: ctx.name().clone(),
                egress: state.egress.clone(),
            })
            .await
            .map_err(|e| MachineError::Runtime(e.to_string()))?;
        if let Err(e) = runtime.push_ws(ws_dir).await {
            runtime.kill().await;
            return Err(MachineError::Runtime(format!("workspace push: {e}")));
        }
        let mut act = self.lock();
        act.runtime = Some(runtime);
        act.dirty = true;
        Ok(())
    }

    /// Stop the guest (idempotent; machine §1: stopping it loses no
    /// committed block).
    async fn kill_vm(&self) {
        let runtime = self.lock().runtime.take();
        if let Some(runtime) = runtime {
            runtime.kill().await;
        }
    }

    /// Death-watch the front-door member of every folded attachment (machine
    /// §5.1). `on_activate` cannot see state, so a rehydrated machine
    /// re-watches on its first state-bearing entry point: the pending alarm's
    /// fire, or any command; watch-after-death then folds
    /// `Detached { FrontDoorLost }` for holders that died while the entity was
    /// down. Re-watching is idempotent: the host dedups watches by watcher id,
    /// so a door yields at most one `Terminated` per activation.
    fn watch_attachments(&self, state: &MachineState, ctx: &EntityCtx<Self>) {
        for attachment in state.attachments.values() {
            ctx.watch(attachment.front_door.clone());
        }
    }

    /// Record a committed capture (advance the capture clock) and map its
    /// stats to the journal event: `None` for a clean image (§7.5).
    fn capture_event(
        &self,
        stats: entity::facet::DiskCaptureStats,
        now: Instant,
    ) -> Option<MachineEvent> {
        self.lock().last_capture = Some(now);
        if stats.blocks == 0 {
            None
        } else {
            Some(MachineEvent::Captured {
                at_nanos: now.as_nanos(),
                blocks: stats.blocks,
                bytes: stats.bytes,
            })
        }
    }

    /// Pull the guest's `/workspace` into the facet's directory and stage the
    /// delta into the *current* command's batch, so it commits atomically
    /// with the disk manifest that follows (E19). Must run while the guest
    /// can still answer: before pause or kill (machine §4).
    ///
    /// Degrades gracefully (machine §5.1): a failed pull, or a
    /// [`entity::facet::WsError::TooLarge`], skips ws staging this cadence;
    /// journaled as [`MachineEvent::WsCaptureSkipped`], and the disk capture
    /// proceeds; the ws delta waits for the next quiescent point.
    ///
    /// Cost: a pull rewrites every workspace file (fresh mtimes), so the
    /// capture's stat-prune never applies on this path; each checkpoint
    /// re-hashes the full pulled tree, accepted at the facet's 64 MiB cap.
    async fn pull_and_capture_ws(
        &self,
        runtime: Option<&Arc<dyn MachineRuntime>>,
        ctx: &EntityCtx<Self>,
        now: Instant,
    ) -> Vec<MachineEvent> {
        let Some(runtime) = runtime else {
            return vec![];
        };
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
        if let Err(e) = runtime.pull_ws(ws_dir).await {
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
    /// capture the disk facet in the same command's batch (E19), un-quiesce.
    /// Returns the batch and whether the disk capture committed.
    async fn run_capture(
        &self,
        ctx: &EntityCtx<Self>,
        now: Instant,
        quiesce: Quiesce,
    ) -> (Vec<MachineEvent>, bool) {
        let runtime = self.lock().runtime.clone();
        let mut events = self.pull_and_capture_ws(runtime.as_ref(), ctx, now).await;
        match quiesce {
            Quiesce::Stop => self.kill_vm().await,
            Quiesce::Pause => {
                if let Some(runtime) = &runtime
                    && runtime.pause().await.is_err()
                {
                    // Never capture a running guest's image (entity §7.15).
                    // Treat the guest as lost; the image keeps its writes and
                    // stays dirty until the next capture, the M3 window. The
                    // staged ws delta still commits: it is real pulled bytes.
                    self.kill_vm().await;
                    return (events, false);
                }
            }
        }
        let out = ctx.disk().capture().await;
        if quiesce == Quiesce::Pause
            && let Some(runtime) = &runtime
        {
            let _ = runtime.resume().await;
        }
        match out {
            Ok(stats) => {
                events.extend(self.capture_event(stats, now));
                if runtime.is_none() {
                    // No guest is writing, so the committed image is the
                    // whole truth: nothing is left for a later capture.
                    self.lock().dirty = false;
                }
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

impl<S: EntitySystem> Entity for Machine<S> {
    type System = S;
    type State = MachineState;
    type Event = MachineEvent;
    type Facets = (Disk, Alarm, Ws);
    const ENTITY_TYPE: &'static str = MACHINE_TYPE;

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

    fn register(r: &mut EntityRegistry<Self>) {
        r.accept::<Provision>();
        r.accept::<Attach>();
        r.accept::<Detach>();
        r.accept::<AddKey>();
        r.accept::<RevokeKey>();
        r.accept::<SetEgress>();
        r.accept::<DoorPolicy>();
        r.accept::<Status>();
        r.accept::<WsWrite>();
        r.accept::<WsRead>();
        r.accept::<WsList>();
        r.accept::<WsRemove>();
    }

    async fn on_activate(&mut self, _ctx: &EntityCtx<Self>) -> Result<(), actor_core::BoxError> {
        // The runtime and dirty flag are activation-local (E3): a fresh
        // activation has a freshly rehydrated (clean) image and no guest.
        *self.lock() = Activation::default();
        Ok(())
    }

    fn can_hibernate(&self, state: &MachineState) -> bool {
        // A live attachment pins the activation (machine §5.1), and an image
        // holding uncaptured writes must not be stranded by the idle path
        // (entity §7.15): the final capture command clears the flag first
        // (machine §4, quiescent point 1).
        let act = self.lock();
        state.attachments.is_empty() && !act.dirty
    }

    async fn on_deactivate(&mut self, _ctx: &EntityCtx<Self>) {
        // Every deactivation kills the guest: idle hibernation after the final
        // capture, or the forced step-down of M5, whose guest must not outlive
        // the activation.
        self.kill_vm().await;
    }

    async fn on_alarm(&self, state: &MachineState, ctx: &EntityCtx<Self>) -> Vec<MachineEvent> {
        // A rehydrated machine's first alarm re-watches its folded
        // attachments' holders (machine §5.1); a no-op otherwise.
        self.watch_attachments(state, ctx);
        let now = ctx.system().now();
        if state.attachments.is_empty() {
            // Quiescent point 1 (machine §4): the final capture. Stop the
            // guest, capture the settled image, and leave the entity clean so
            // idle hibernation can proceed (M2). No re-arm on success: the
            // alarm is consumed and `can_hibernate` now permits eviction.
            let (events, committed) = self.run_capture(ctx, now, Quiesce::Stop).await;
            if committed {
                self.lock().dirty = false;
            } else {
                // The capture could not complete (blob quorum, IO): keep the
                // dirty pin and retry on the next cadence tick.
                ctx.alarm().set_after(state.alarm_cadence());
            }
            return events;
        }

        // Quiescent point 2 (machine §4): the mid-session checkpoint. Every
        // fire re-arms, which is the session lease's fenced append (M5), even
        // when no capture is due.
        ctx.alarm().set_after(state.alarm_cadence());
        let (checkpoint_due, idle) = {
            let act = self.lock();
            let due = match act.last_capture {
                Some(last) => {
                    now.duration_since(last) >= Duration::from_nanos(state.checkpoint_nanos)
                }
                None => true,
            };
            (due, act.runtime.is_none() && !act.dirty)
        };
        // An attached machine with no guest (attachments folded on a node that
        // never booted one, as after a failover while the door lives on) has
        // nothing to capture: a scan would hash the whole image to find no
        // dirty block. The re-arm above is still the lease's append.
        if !checkpoint_due || idle {
            return vec![];
        }
        // Pause-and-resume around the capture; the ws delta is as-of-pull,
        // the disk as-of-pause, the skew bounded by the pull duration
        // (machine §4). The guest stays dirty: it keeps running.
        let (events, _committed) = self.run_capture(ctx, now, Quiesce::Pause).await;
        events
    }

    async fn on_peer_terminated(
        &self,
        state: &MachineState,
        ctx: &EntityCtx<Self>,
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
/// disk begins as the base image's full-coverage manifest (entity §7.15) and
/// which carries the machine's freshly generated host key (machine §5.1).
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

impl<S: EntitySystem> EntityHandler<Provision> for Machine<S> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: Provision,
        ctx: &EntityCtx<Self>,
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
        // The host key is born here and nowhere else: 32 bytes journaled as a
        // raw ed25519 seed; any 32 bytes are a valid seed (RFC 8032). Drawing
        // from the Entropy seam (actor §4.6) keeps both worlds right:
        // production entropy is OS-seeded, so the key is fresh and secret; the
        // simulator's is run-seeded, so a replayed seed reproduces the same
        // key (actor §18.1).
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

/// What a successful [`Attach`] returns to the front door (machine §5.1). The
/// host key is not here: the door already read it; SSH presents it at KEX,
/// before any principal exists to attach ([`DoorPolicy`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttachReply {
    pub attachment: u64,
}

/// Attach a connection (machine §5.1): journaled with its principal (M4),
/// boots the guest lazily, death-watches the holding front-door member, and
/// starts the checkpoint/lease cadence (machine §4).
#[derive(Clone, Serialize, Deserialize)]
pub struct Attach {
    pub principal: String,
    /// The front-door actor holding this connection: the death-watch target.
    pub front_door: ActorId,
}

impl Message for Attach {
    type Reply = Result<AttachReply, MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.Attach");
}

impl<S: EntitySystem> EntityHandler<Attach> for Machine<S> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: Attach,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<AttachReply, MachineError>) {
        if !state.provisioned {
            return (vec![], Err(MachineError::NotProvisioned));
        }
        if let Err(e) = self.ensure_vm(state, ctx).await {
            return (vec![], Err(e));
        }
        self.watch_attachments(state, ctx);
        ctx.watch(msg.front_door.clone());
        // Start the capture cadence, which is also the session lease (M5).
        ctx.alarm().set_after(state.alarm_cadence());
        let id = state.next_attachment;
        (
            vec![MachineEvent::Attached {
                id,
                principal: msg.principal,
                front_door: msg.front_door,
                at_nanos: ctx.system().now().as_nanos(),
            }],
            Ok(AttachReply { attachment: id }),
        )
    }
}

/// Detach a connection (machine §5.1): journaled; when the last attachment
/// goes, the final capture is scheduled immediately (machine §4, quiescent
/// point 1) so the idle path finds a clean disk. Idempotent, detaching an
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

impl<S: EntitySystem> EntityHandler<Detach> for Machine<S> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: Detach,
        ctx: &EntityCtx<Self>,
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
/// sees the folded set (M4). Refused before `Provision` (machine §3): the
/// `Provisioned` fold sets the key set whole, so an earlier `KeyAdded` would
/// be journaled and then silently discarded.
#[derive(Clone, Serialize, Deserialize)]
pub struct AddKey {
    pub fingerprint: String,
    pub key: String,
}

impl Message for AddKey {
    type Reply = Result<(), MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.AddKey");
}

impl<S: EntitySystem> EntityHandler<AddKey> for Machine<S> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: AddKey,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<(), MachineError>) {
        if !state.provisioned {
            return (vec![], Err(MachineError::NotProvisioned));
        }
        (
            vec![MachineEvent::KeyAdded {
                fingerprint: msg.fingerprint,
                key: msg.key,
            }],
            Ok(()),
        )
    }
}

/// Revoke a key (machine §3): stops authorizing the next attach; it does not
/// tear down a live connection (that is a separate administrative action).
/// Refused before `Provision`, like every policy command (machine §3).
#[derive(Clone, Serialize, Deserialize)]
pub struct RevokeKey {
    pub fingerprint: String,
}

impl Message for RevokeKey {
    type Reply = Result<(), MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.RevokeKey");
}

impl<S: EntitySystem> EntityHandler<RevokeKey> for Machine<S> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: RevokeKey,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<(), MachineError>) {
        if !state.provisioned {
            return (vec![], Err(MachineError::NotProvisioned));
        }
        (
            vec![MachineEvent::KeyRevoked {
                fingerprint: msg.fingerprint,
            }],
            Ok(()),
        )
    }
}

/// Change the egress policy (machine §5.2): a journaled event, effective from
/// the next activation or applied live where the provider can. Refused before
/// `Provision`, like every policy command (machine §3).
#[derive(Clone, Serialize, Deserialize)]
pub struct SetEgress {
    pub policy: EgressPolicy,
}

impl Message for SetEgress {
    type Reply = Result<(), MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.SetEgress");
}

impl<S: EntitySystem> EntityHandler<SetEgress> for Machine<S> {
    async fn handle(
        &self,
        state: &MachineState,
        msg: SetEgress,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<(), MachineError>) {
        if !state.provisioned {
            return (vec![], Err(MachineError::NotProvisioned));
        }
        (
            vec![MachineEvent::EgressChanged { policy: msg.policy }],
            Ok(()),
        )
    }
}

/// What the front door must know *before* it can serve a connection (machine
/// §5.1), read in one command because SSH needs both at the same moment: the
/// host key is presented at KEX, and the authorized-key set decides the
/// publickey method that immediately follows; both before any `Attach`.
///
/// The reply carries the machine's private host-key material: a front door is
/// a cluster member inside the trust boundary (machine §5.1), and no key
/// material ever crosses into the guest. Reading the set per connection is
/// what makes revocation take effect on the *next* attach (M4).
#[derive(Clone, Serialize, Deserialize)]
pub struct DoorPolicy;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoorPolicyReply {
    /// See [`MachineState::host_key`]: the raw 32-byte ed25519 seed.
    pub host_key: Vec<u8>,
    /// `fingerprint → public key`, the current folded set (machine §3).
    pub authorized_keys: BTreeMap<String, String>,
}

impl Message for DoorPolicy {
    type Reply = Result<DoorPolicyReply, MachineError>;
    const MANIFEST: Manifest = Manifest::new("machine.DoorPolicy");
}

impl<S: EntitySystem> EntityHandler<DoorPolicy> for Machine<S> {
    async fn handle(
        &self,
        state: &MachineState,
        _msg: DoorPolicy,
        _ctx: &EntityCtx<Self>,
    ) -> (Vec<MachineEvent>, Result<DoorPolicyReply, MachineError>) {
        if !state.provisioned {
            return (vec![], Err(MachineError::NotProvisioned));
        }
        (
            vec![],
            Ok(DoorPolicyReply {
                host_key: state.host_key.clone(),
                authorized_keys: state.authorized_keys.clone(),
            }),
        )
    }
}

/// A read of the machine's shape (entity §7.5: commits nothing).
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
    pub running: bool,
    /// The hash of the committed block index (the disk facet's
    /// `content_digest`), no image read: the verification surface for tests
    /// and operators. `None` before an import commits.
    pub image_digest: Option<BlobId>,
}

impl Message for Status {
    type Reply = StatusReply;
    const MANIFEST: Manifest = Manifest::new("machine.Status");
}

impl<S: EntitySystem> EntityHandler<Status> for Machine<S> {
    async fn handle(
        &self,
        state: &MachineState,
        _msg: Status,
        ctx: &EntityCtx<Self>,
    ) -> (Vec<MachineEvent>, StatusReply) {
        let image_bytes = ctx.disk().image_bytes().unwrap_or(0);
        // The committed-image digest, derived from the block index, no
        // multi-MiB read on this read command (entity §7.15).
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
                running: self.lock().runtime.is_some(),
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
