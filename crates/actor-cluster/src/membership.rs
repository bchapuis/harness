//! Cluster membership and node lifecycle (spec §9, §10).
//!
//! Each node keeps a roster of its peers with a [`MemberStatus`] along the
//! lifecycle `joining → up → leaving → down → removed`, and an orthogonal
//! [`Reachability`] the SWIM detector drives (`Reachable`/`Suspect`/`Unreachable`,
//! spec §9.1). `down` is the terminal liveness decision (invariant #15): a downed
//! node never returns; `removed` is its tombstone, pruned after a while so the
//! roster does not grow without bound under churn.
//!
//! Reachability is detected by **direct and indirect probing** and disseminated
//! by **gossip** with **incarnation refutation** (spec §9.2, §10): a successful
//! probe clears a suspicion; a suspicion unrefuted for `suspect_timeout` becomes
//! `Unreachable`, which the [`DowningPolicy`] may then move to `Down`. The
//! lifecycle is driven by an elected **leader** — the lowest-id reachable member
//! — which admits joiners to `Up` and finalizes leaves to `Down` (spec §9.3).
//! Full seen-by gossip-convergence detection remains a follow-up.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::time::Duration;

use serde::Deserialize;
use serde::Serialize;

use actor_core::Entropy;
use actor_core::Event;
use actor_core::EventSink;
use actor_core::Instant;
use actor_core::NodeId;

/// Severity ordering for reachability, used by gossip merge: a more severe view
/// wins at equal incarnation (spec §9.2).
fn severity(r: Reachability) -> u8 {
    match r {
        Reachability::Reachable => 0,
        Reachability::Suspect => 1,
        Reachability::Unreachable => 2,
    }
}

/// The observability event for a reachability transition.
fn reachability_event(observer: NodeId, node: NodeId, reachability: Reachability) -> Event {
    match reachability {
        Reachability::Reachable => Event::Reachable { observer, node },
        Reachability::Suspect => Event::Suspected { observer, node },
        Reachability::Unreachable => Event::Unreachable { observer, node },
    }
}

/// The **status-axis** merge order (spec §9.2): whether an incoming
/// `(revision, status)` view supersedes the current one. A higher operator
/// revision wins; at equal revision the more-advanced rank wins (the
/// `joining → … → removed` lattice). In static/autonomous mode every revision is
/// `0`, so this is the plain monotonic lattice; in managed mode the leader's
/// latest decision carries the highest revision, so a reversible `up ⇄ draining`
/// change converges without rank ordering.
fn status_supersedes(incoming: (u64, MemberStatus), current: (u64, MemberStatus)) -> bool {
    incoming.0 > current.0 || (incoming.0 == current.0 && incoming.1.rank() > current.1.rank())
}

/// The **reachability-axis** merge order (spec §10): whether an incoming
/// `(incarnation, reachability)` view supersedes the current one. A higher
/// incarnation wins (a refutation); at equal incarnation the more severe view wins.
fn reachability_supersedes(
    incoming: (u64, Reachability),
    current: (u64, Reachability),
) -> bool {
    incoming.0 > current.0 || (incoming.0 == current.0 && severity(incoming.1) > severity(current.1))
}

/// One node's view of a member, exchanged by gossip (spec §9.2). Serializable so
/// it can be piggybacked on `Ping`/`Ack` frames over the wire (spec §10).
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MemberDigest {
    pub node: NodeId,
    pub status: MemberStatus,
    pub reachability: Reachability,
    pub incarnation: u64,
    /// The operator-decision **revision** that produced `status`, in managed mode
    /// (spec §9.4). The designated leader is the single writer, so this is a
    /// monotonic per-decision counter; the merge takes the higher revision, which
    /// lets a *reversible* `up ⇄ draining` change converge without rank ordering.
    /// `0` in static/autonomous mode, where status follows the rank lattice.
    #[serde(default)]
    pub revision: u64,
}

/// A member's lifecycle status (spec §9.1): `joining → up → leaving → down →
/// removed`. The lifecycle only moves forward, so gossip merges it monotonically
/// (the more advanced status wins). `down` is the terminal *liveness* decision
/// (irrevocable); `removed` is its tombstone, gossiped briefly and then pruned
/// from the roster entirely.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum MemberStatus {
    /// Handshake complete, not yet admitted to full participation (spec §9.3).
    Joining,
    /// Full member; may host and address actors.
    Up,
    /// **Reversible** maintenance state, set by the operator in the managed
    /// control plane (spec §9.4): the node is cordoned — callers route
    /// away — but it stays a full member and is *not* terminal. A later `resume`
    /// returns it to [`Up`](MemberStatus::Up). Because it is reversible it does
    /// not sit on the monotonic `joining → … → removed` ladder; transitions in and
    /// out of it are ordered by the operator's revision, not by rank (see
    /// [`Membership::merge`]). It therefore shares [`Up`](MemberStatus::Up)'s rank.
    Draining,
    /// Graceful shutdown initiated; draining (spec §9.3).
    Leaving,
    /// Declared dead — terminal and irrevocable (spec §9.1).
    Down,
    /// Tombstone for a long-dead member (spec §9.1): kept briefly so the removal
    /// disseminates, then pruned from the roster. The node's `NodeId` is never
    /// reused (§9.1), so a stale gossip can never resurrect it as live.
    Removed,
}

impl MemberStatus {
    /// Position in the `joining → up → leaving → down → removed` lifecycle.
    /// Higher wins on merge, so progress is monotonic (spec §9.1, §9.2).
    fn rank(self) -> u8 {
        match self {
            MemberStatus::Joining => 0,
            // `Draining` shares `Up`'s rank: it is a reversible off-ladder state
            // (ordered by operator revision, not rank), so neither dominates the
            // other on a rank tie — only a higher revision flips between them.
            MemberStatus::Up | MemberStatus::Draining => 1,
            MemberStatus::Leaving => 2,
            MemberStatus::Down => 3,
            MemberStatus::Removed => 4,
        }
    }

    /// Whether this is a terminal status — a `down` or `removed` member is no
    /// longer a participant (not addressable, not probed, not a leader).
    fn is_terminal(self) -> bool {
        matches!(self, MemberStatus::Down | MemberStatus::Removed)
    }
}

/// How long a member stays `Down` before it is tombstoned `Removed` (spec §9.1).
/// Long enough that the `down` decision has fully disseminated first.
const TOMBSTONE_AFTER: Duration = Duration::from_secs(30);

/// How long a `Removed` tombstone lingers (so the removal disseminates) before
/// the entry is pruned from the roster entirely (spec §9.1).
const PRUNE_AFTER: Duration = Duration::from_secs(30);

/// Reachability as seen by the failure detector (spec §9.1, §10). Orthogonal to
/// [`MemberStatus`] and reversible until `Down`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub enum Reachability {
    Reachable,
    Suspect,
    Unreachable,
}

/// How an `unreachable` member becomes `down` (spec §9.2). The default is
/// conservative: a partition alone never downs a node (invariant #16).
#[derive(Clone, Copy, Debug)]
pub enum DowningPolicy {
    /// Never auto-down; `unreachable` is left for an operator to resolve.
    Conservative,
    /// Down a member that has been `unreachable` for this long.
    Timeout(Duration),
}

/// SWIM detector parameters (spec §10). All MUST be configurable.
#[derive(Clone, Copy, Debug)]
pub struct SwimConfig {
    /// `T_probe`: how often to probe a random member.
    pub probe_interval: Duration,
    /// `T_rtt`: how long to await an `Ack`.
    pub rtt: Duration,
    /// `T_suspect` **base**: how long a suspicion stands before becoming
    /// `unreachable` in a small cluster. The effective timeout scales
    /// logarithmically with cluster size (spec §10); see
    /// `Membership::effective_suspect_timeout`.
    pub suspect_timeout: Duration,
    /// `k`: how many peers to enlist for indirect probing when a direct probe is
    /// missed (spec §10 #2). `0` disables indirect probing.
    pub indirect_count: usize,
    /// How `unreachable` escalates to `down`.
    pub downing: DowningPolicy,
}

impl Default for SwimConfig {
    fn default() -> Self {
        SwimConfig {
            probe_interval: Duration::from_secs(1),
            rtt: Duration::from_millis(200),
            suspect_timeout: Duration::from_secs(3),
            indirect_count: 3,
            downing: DowningPolicy::Conservative,
        }
    }
}

/// Which **control plane** governs membership — the single decision that
/// distinguishes the three membership modes (spec §9). The variants name the
/// *authority* for the member set, not the mechanism:
///
/// - [`Static`](MembershipMode::Static): the set is fixed at startup and never
///   changes. No failure detector runs; sends to a vanished node just fail.
/// - [`Autonomous`](MembershipMode::Autonomous): the cluster governs its own
///   membership — an elected leader admits joiners and the SWIM detector drives
///   `unreachable → down` (spec §9.2, §9.3, §10). Self-organizing and
///   self-healing.
/// - [`Managed`](MembershipMode::Managed): an external control plane governs the
///   member set through a **designated leader** (not elected). The detector still
///   runs, but only as a read-only reachability sensor — it never downs a node.
///   The operator admits, drains, resumes, and decommissions nodes by explicit
///   command, each stamped with a monotonic revision and disseminated by gossip
///   (the Kubernetes node-lifecycle model).
///
/// `Static`/`Autonomous` carry no revision (it stays `0`), so the gossip merge
/// falls through to the plain rank-monotonic lattice and behaves exactly as
/// before; `Managed` bumps the revision so an operator decision wins.
#[derive(Clone, Copy, Debug)]
pub enum MembershipMode {
    /// Fixed member set, no failure detection (spec §9, mode a).
    Static,
    /// Self-organizing SWIM with an elected leader (spec §9.2, §9.3, §10, mode b).
    Autonomous(SwimConfig),
    /// Operator-governed via a designated control-plane `leader` (spec §9.4).
    /// The detector observes reachability but never decides `down`.
    Managed { swim: SwimConfig, leader: NodeId },
}

impl MembershipMode {
    /// The SWIM parameters this mode runs the detector with, or `None` for
    /// [`Static`](MembershipMode::Static) (no detector loop).
    pub fn swim(&self) -> Option<SwimConfig> {
        match self {
            MembershipMode::Static => None,
            MembershipMode::Autonomous(swim) => Some(*swim),
            MembershipMode::Managed { swim, .. } => Some(*swim),
        }
    }

    /// The designated control-plane leader, set only in
    /// [`Managed`](MembershipMode::Managed) mode; `None` means the leader is
    /// elected (spec §9.2).
    fn designated_leader(&self) -> Option<NodeId> {
        match self {
            MembershipMode::Managed { leader, .. } => Some(*leader),
            _ => None,
        }
    }

    /// Whether the leader autonomously drives the lifecycle (admits joiners,
    /// finalizes leaves, downs the unreachable) — true only for
    /// [`Autonomous`](MembershipMode::Autonomous). In `Managed` mode those
    /// transitions are the operator's to make.
    fn autonomous(&self) -> bool {
        matches!(self, MembershipMode::Autonomous(_))
    }

    /// The downing policy in force. `Managed` mode forces
    /// [`Conservative`](DowningPolicy::Conservative): the detector never downs a
    /// node — only the operator does (so a maintenance outage is `unreachable`,
    /// never the terminal `down`).
    fn downing(&self) -> DowningPolicy {
        match self {
            MembershipMode::Managed { .. } => DowningPolicy::Conservative,
            MembershipMode::Autonomous(swim) => swim.downing,
            MembershipMode::Static => DowningPolicy::Conservative,
        }
    }
}

struct Member {
    status: MemberStatus,
    reachability: Reachability,
    /// The incarnation this view is tagged with; a higher incarnation wins, and
    /// only a higher one (a refutation) can clear a suspicion (spec §9.2, §10).
    incarnation: u64,
    /// The operator-decision revision behind `status` (managed mode, spec §9.4);
    /// `0` otherwise. The merge prefers the higher revision, so a reversible
    /// `up ⇄ draining` toggle converges; the rank lattice is only the tie-break.
    revision: u64,
    /// When `reachability` last changed — drives the suspect and downing timers.
    changed_at: Instant,
}

/// One node's view of the cluster (spec §9). Internally synchronized.
pub struct Membership {
    node: NodeId,
    downing: DowningPolicy,
    suspect_timeout: Duration,
    /// The designated control-plane leader in managed mode (spec §9.4), or `None`
    /// when the leader is elected (static/autonomous).
    designated_leader: Option<NodeId>,
    /// Whether the leader autonomously drives the lifecycle (autonomous mode).
    /// `false` in managed mode, where the operator drives it by command.
    autonomous: bool,
    members: Mutex<BTreeMap<NodeId, Member>>,
    /// This node's own lifecycle status, advertised in its gossip digest. A
    /// founding member starts `Up`; a joiner starts `Joining` and is admitted to
    /// `Up` by the leader (elected, or the operator in managed mode).
    self_status: Mutex<MemberStatus>,
    /// This node's own incarnation; bumped to refute a suspicion about itself
    /// (spec §10 #4).
    incarnation: AtomicU64,
    /// The revision of the operator decision behind this node's own `self_status`
    /// (managed mode). A node only *adopts* status decisions about itself — it is
    /// never the writer — so this just tracks the highest revision it has accepted.
    self_revision: AtomicU64,
    /// The designated leader's monotonic operator-decision counter (managed mode,
    /// spec §9.4): every `admit`/`drain`/`resume`/`decommission` takes the next
    /// value, making the leader the single writer of the member set. Kept ahead of
    /// any revision seen in gossip so it survives a leader restart. Unused (stays
    /// `0`) off the leader and in other modes.
    revision: AtomicU64,
    events: Arc<dyn EventSink>,
}

/// An operator command of the managed control plane (spec §9.4). The four
/// commands share one protocol — guard, find-or-create, precondition,
/// revision-stamp, announce (see [`Membership::command`]) — and differ only in
/// the data captured here: the status they drive a member to, whether they may
/// introduce a node not yet in the roster, which prior states they are valid
/// from, and the event they announce.
#[derive(Clone, Copy)]
enum OperatorCommand {
    /// Bring a node into the member set as `Up` (idempotent if already up).
    Admit,
    /// Cordon a live member for maintenance — the reversible `Draining` state.
    Drain,
    /// Return a drained member to `Up`.
    Resume,
    /// Terminally remove a member (`Down`); the caller then runs the cascade.
    Decommission,
}

impl OperatorCommand {
    /// The status the command drives its target to.
    fn target(self) -> MemberStatus {
        match self {
            OperatorCommand::Admit | OperatorCommand::Resume => MemberStatus::Up,
            OperatorCommand::Drain => MemberStatus::Draining,
            OperatorCommand::Decommission => MemberStatus::Down,
        }
    }

    /// Whether the command may introduce a node not yet in the roster. Only
    /// admission and decommission name a node the leader has not seen; `drain`
    /// and `resume` act on an existing member or are a no-op.
    fn admits_new(self) -> bool {
        matches!(self, OperatorCommand::Admit | OperatorCommand::Decommission)
    }

    /// Whether the command is valid against a member's current (non-terminal)
    /// status — the precondition that makes `drain`/`resume` a matched pair and
    /// keeps each command idempotent.
    fn valid_from(self, current: MemberStatus) -> bool {
        match self {
            OperatorCommand::Admit | OperatorCommand::Decommission => true,
            OperatorCommand::Drain => current != MemberStatus::Draining,
            OperatorCommand::Resume => current == MemberStatus::Draining,
        }
    }

    /// The observability event announcing the command (spec §16). Fixed per
    /// command — an operator action, unlike a passively-observed transition
    /// ([`Membership::transition_event`]), so it never depends on the prior state.
    fn event(self, observer: NodeId, node: NodeId) -> Event {
        match self {
            OperatorCommand::Admit => Event::MemberUp { observer, node },
            OperatorCommand::Drain => Event::MemberDraining { observer, node },
            OperatorCommand::Resume => Event::MemberResumed { observer, node },
            OperatorCommand::Decommission => Event::NodeDown { observer, node },
        }
    }
}

impl Membership {
    /// Create an empty roster for `node` under `mode`. `joining` marks this node a
    /// joiner (starts `Joining`, awaiting admission — by the elected leader, or by
    /// the operator in managed mode); otherwise it is a founding member (`Up`).
    pub fn new(
        node: NodeId,
        mode: &MembershipMode,
        events: Arc<dyn EventSink>,
        joining: bool,
    ) -> Membership {
        let self_status = if joining {
            MemberStatus::Joining
        } else {
            MemberStatus::Up
        };
        let swim = mode.swim().unwrap_or_default();
        Membership {
            node,
            downing: mode.downing(),
            suspect_timeout: swim.suspect_timeout,
            designated_leader: mode.designated_leader(),
            autonomous: mode.autonomous(),
            members: Mutex::new(BTreeMap::new()),
            self_status: Mutex::new(self_status),
            incarnation: AtomicU64::new(0),
            self_revision: AtomicU64::new(0),
            revision: AtomicU64::new(0),
            events,
        }
    }

    /// This node's own current lifecycle status.
    pub fn self_status(&self) -> MemberStatus {
        *self.self_status.lock().expect("self status mutex poisoned")
    }

    /// Announce that this node is leaving (spec §9.3): it advertises `Leaving` in
    /// its digest; the leader finalizes it to `Down` and watchers are notified.
    pub fn begin_leaving(&self) {
        let mut s = self.self_status.lock().expect("self status mutex poisoned");
        if s.rank() < MemberStatus::Leaving.rank() {
            *s = MemberStatus::Leaving;
        }
    }

    /// Add a peer as `Up`/`Reachable` (idempotent — never resurrects a `Down`).
    pub fn add_member(&self, node: NodeId, now: Instant) {
        if node == self.node {
            return;
        }
        self.members
            .lock()
            .expect("members mutex poisoned")
            .entry(node)
            .or_insert(Member {
                status: MemberStatus::Up,
                reachability: Reachability::Reachable,
                incarnation: 0,
                revision: 0,
                changed_at: now,
            });
    }

    /// Whether `node` is terminal — `down` or its `removed` tombstone (spec
    /// §9.1). Used to fail routing fast and to prune receptionist entries.
    pub fn is_down(&self, node: NodeId) -> bool {
        self.members
            .lock()
            .expect("members mutex poisoned")
            .get(&node)
            .is_some_and(|m| m.status.is_terminal())
    }

    /// Pick a live peer to probe, chosen via `entropy` (spec §10). Suspect and
    /// unreachable peers are still probed, so recovery is detected; terminal
    /// (`down`/`removed`) peers are not.
    pub fn pick_probe_target<E: Entropy>(&self, entropy: &E) -> Option<NodeId> {
        let members = self.members.lock().expect("members mutex poisoned");
        let candidates: Vec<NodeId> = members
            .iter()
            .filter(|(_, m)| !m.status.is_terminal())
            .map(|(n, _)| *n)
            .collect();
        let index = entropy.pick_index(candidates.len())?;
        Some(candidates[index])
    }

    /// Pick up to `k` distinct reachable peers to relay an indirect probe, none
    /// of them `exclude` (the probe target) — the `k` members of SWIM's indirect
    /// probing (spec §10 #2). Fewer than `k` if the cluster is small.
    pub fn pick_helpers<E: Entropy>(&self, k: usize, exclude: NodeId, entropy: &E) -> Vec<NodeId> {
        let members = self.members.lock().expect("members mutex poisoned");
        let mut candidates: Vec<NodeId> = members
            .iter()
            .filter(|(n, m)| {
                **n != exclude
                    && !m.status.is_terminal()
                    && m.reachability == Reachability::Reachable
            })
            .map(|(n, _)| *n)
            .collect();
        let mut chosen = Vec::new();
        while chosen.len() < k && !candidates.is_empty() {
            match entropy.pick_index(candidates.len()) {
                Some(i) => chosen.push(candidates.swap_remove(i)),
                None => break,
            }
        }
        chosen
    }

    /// Direct evidence that `node` is alive at `incarnation` (we exchanged a
    /// ping/ack with it): mark it reachable, clearing any suspicion, and adopt
    /// the reported incarnation. Never resurrects a terminal (`down`/`removed`)
    /// node (invariant #15, spec §9.1).
    pub fn mark_alive_direct(&self, node: NodeId, incarnation: u64, now: Instant) {
        let mut members = self.members.lock().expect("members mutex poisoned");
        // Direct evidence proves liveness, not admission: a node we have never
        // heard of enters `Joining` until its digest or the leader says `Up`.
        let member = members.entry(node).or_insert(Member {
            status: MemberStatus::Joining,
            reachability: Reachability::Reachable,
            incarnation,
            revision: 0,
            changed_at: now,
        });
        if member.status.is_terminal() {
            return;
        }
        member.incarnation = member.incarnation.max(incarnation);
        if member.reachability != Reachability::Reachable {
            member.reachability = Reachability::Reachable;
            member.changed_at = now;
            self.events.emit(Event::Reachable {
                observer: self.node,
                node,
            });
        }
    }

    /// A failed probe: mark a reachable `node` suspect (spec §10). Leaves an
    /// existing suspicion's timer running.
    pub fn mark_suspect(&self, node: NodeId, now: Instant) {
        let mut members = self.members.lock().expect("members mutex poisoned");
        if let Some(m) = members.get_mut(&node) {
            if m.status == MemberStatus::Down {
                return;
            }
            if m.reachability == Reachability::Reachable {
                m.reachability = Reachability::Suspect;
                m.changed_at = now;
                self.events.emit(Event::Suspected {
                    observer: self.node,
                    node,
                });
            }
        }
    }

    /// The cluster leader (spec §9.2). In autonomous mode it is *elected* — the
    /// lowest-id reachable, non-`down` member, this node included — and performs
    /// the `up`/`down` lifecycle transitions; election returns `None` only if this
    /// node has excluded itself by leaving or being down. In managed mode it is the
    /// *designated* control-plane node (spec §9.4), returned unconditionally.
    fn compute_leader(&self, members: &BTreeMap<NodeId, Member>) -> Option<NodeId> {
        // Managed mode (spec §9.4): the leader is *designated*, not elected — the
        // control plane is a provisioned authority, not the lowest address. It is
        // the single writer of the member set, which is what gives managed mode a
        // split-brain-free membership decision (at the cost of pausing membership
        // changes if the control-plane node is unreachable).
        if let Some(leader) = self.designated_leader {
            return Some(leader);
        }
        let mut best = None;
        let ss = self.self_status();
        if !ss.is_terminal() && ss != MemberStatus::Leaving {
            best = Some(self.node);
        }
        for (node, m) in members.iter() {
            // A leaving, draining, or terminal member is not a leader candidate, so
            // leadership fails over when the leader itself steps aside (spec §9.2, §9.3).
            if !m.status.is_terminal()
                && m.status != MemberStatus::Leaving
                && m.status != MemberStatus::Draining
                && m.reachability == Reachability::Reachable
            {
                best = Some(best.map_or(*node, |b: NodeId| b.min(*node)));
            }
        }
        best
    }

    /// The current cluster leader as this node sees it (spec §9.2).
    pub fn leader(&self) -> Option<NodeId> {
        let members = self.members.lock().expect("members mutex poisoned");
        self.compute_leader(&members)
    }

    /// Whether this node is the leader (spec §9.2).
    pub fn is_leader(&self) -> bool {
        self.leader() == Some(self.node)
    }

    /// Whether this node is the **designated** control-plane leader (managed mode,
    /// spec §9.4) — the single node whose operator commands take effect. A command
    /// issued anywhere else is a no-op, which keeps the member set single-writer.
    fn is_control_leader(&self) -> bool {
        self.designated_leader == Some(self.node)
    }

    /// The next operator-decision revision (managed mode, spec §9.4): strictly
    /// increasing, so the leader's latest decision always wins the gossip merge.
    fn next_revision(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// **Admit** `node` to the managed member set as `Up` (operator command, spec
    /// §9.4). A new or `joining` node becomes a full member; a decommissioned one
    /// is *not* revived (`down` is terminal, invariant #15) — it must rejoin with a
    /// fresh identity. Returns whether this node is the control leader and applied
    /// it; a command on any other node is a no-op (single-writer member set).
    pub(crate) fn admit(&self, node: NodeId, now: Instant) -> bool {
        self.command(node, OperatorCommand::Admit, now)
    }

    /// **Drain** `node` into the reversible maintenance state (operator command,
    /// spec §9.4): the node is cordoned but stays a member — no `down`, no death
    /// watch fires, in-flight calls are unaffected. [`resume`](Self::resume)
    /// reverses it. No-op unless this is the control leader and `node` is a live
    /// member.
    pub(crate) fn drain(&self, node: NodeId, now: Instant) -> bool {
        self.command(node, OperatorCommand::Drain, now)
    }

    /// **Resume** a drained `node` back to `Up` after maintenance (operator
    /// command, spec §9.4) — the reverse of [`drain`](Self::drain). No-op unless
    /// this is the control leader and `node` is currently `draining`.
    pub(crate) fn resume(&self, node: NodeId, now: Instant) -> bool {
        self.command(node, OperatorCommand::Resume, now)
    }

    /// **Decommission** `node`: declare it terminally `down` (operator command,
    /// spec §9.4). Unlike [`drain`](Self::drain) this is irrevocable (invariant
    /// #15): it triggers the node-down cascade (spec §8.1) — in-flight callers fail
    /// `Unreachable`, watchers get `Terminated { NodeDown }`. Returns whether the
    /// caller should run that cascade (i.e. this is the control leader and `node`
    /// was newly downed); the decision then disseminates by gossip.
    pub(crate) fn decommission(&self, node: NodeId, now: Instant) -> bool {
        self.command(node, OperatorCommand::Decommission, now)
    }

    /// Apply an operator [`command`](OperatorCommand) to `node` — the one protocol
    /// behind `admit`/`drain`/`resume`/`decommission` (spec §9.4). It enforces the
    /// single-writer rule (only the designated leader acts, never on itself),
    /// stamps the decision with the next [revision](Self::next_revision) so it wins
    /// the gossip merge, applies it under the per-command precondition, and
    /// announces it. Returns whether it was applied — which, for `decommission`, is
    /// exactly when the caller must run the node-down cascade (spec §8.1).
    fn command(&self, node: NodeId, command: OperatorCommand, now: Instant) -> bool {
        if !self.is_control_leader() || node == self.node {
            return false;
        }
        let mut members = self.members.lock().expect("members mutex poisoned");
        match members.get_mut(&node) {
            Some(member) => {
                // A terminal member is gone for good (#15); otherwise the command
                // applies only from a status it is valid for, keeping each verb
                // idempotent (re-draining, re-admitting) without a redundant event.
                if member.status.is_terminal() || !command.valid_from(member.status) {
                    return false;
                }
                member.revision = self.next_revision();
                if member.status != command.target() {
                    member.status = command.target();
                    member.changed_at = now;
                    self.events.emit(command.event(self.node, node));
                }
            }
            None => {
                // Only admission and decommission name a node the leader has not
                // seen; drain/resume on an unknown node are a no-op.
                if !command.admits_new() {
                    return false;
                }
                members.insert(
                    node,
                    Member {
                        status: command.target(),
                        reachability: Reachability::Reachable,
                        incarnation: 0,
                        revision: self.next_revision(),
                        changed_at: now,
                    },
                );
                self.events.emit(command.event(self.node, node));
            }
        }
        true
    }

    /// The single mapping from a **peer** status transition to the observability
    /// event this node emits for it (spec §16). `prev` is the status we last held
    /// for the peer, or `None` at first sight; the distinction is what keeps a
    /// passively-learned already-`up` peer silent while an observed `joining → up`
    /// or operator admission announces `MemberUp`, and a `draining → up` announces
    /// `MemberResumed`. `down` maps to `NodeDown` (the caller also drives the
    /// cascade); `leaving`/`removed` are silent. Self-status events are handled
    /// separately (a node stays quiet about its own `down`).
    fn transition_event(
        &self,
        node: NodeId,
        prev: Option<MemberStatus>,
        new: MemberStatus,
    ) -> Option<Event> {
        let observer = self.node;
        let event = match new {
            MemberStatus::Joining => Event::MemberJoining { observer, node },
            MemberStatus::Up => match prev {
                Some(MemberStatus::Draining) => Event::MemberResumed { observer, node },
                // A transition from a known prior status is an admission/promotion;
                // first sight of an already-up peer is passive and stays silent.
                Some(_) => Event::MemberUp { observer, node },
                None => return None,
            },
            MemberStatus::Draining => Event::MemberDraining { observer, node },
            MemberStatus::Down => Event::NodeDown { observer, node },
            MemberStatus::Leaving | MemberStatus::Removed => return None,
        };
        Some(event)
    }

    /// `T_suspect` scaled for a cluster of `cluster_size` members (spec §10).
    /// Logarithmic in the size and clamped to the base for small clusters: the
    /// factor is `max(1, floor(log2(cluster_size)))`, so ≤3 members keep the base
    /// timeout, 4–7 double it, 8–15 triple it, and so on.
    fn effective_suspect_timeout(&self, cluster_size: usize) -> Duration {
        let factor = (cluster_size as u32).max(1).ilog2().max(1);
        self.suspect_timeout * factor
    }

    /// Advance time-based transitions (spec §10) and, when this node is the
    /// leader, the lifecycle transitions it owns (spec §9.2, §9.3):
    ///
    /// - a suspicion older than `suspect_timeout` becomes `Unreachable`, and the
    ///   downing policy may move `Unreachable` to `Down`;
    /// - the leader admits reachable `Joining` members (itself included) to `Up`,
    ///   and finalizes `Leaving` members to `Down`;
    /// - long-`Down` members are tombstoned `Removed`, then pruned.
    ///
    /// Returns the nodes newly declared `down`, so the caller can run the cascade
    /// (spec §8.1 step 3). The three passes run under one lock, so the roster
    /// never changes underneath them within a tick.
    pub fn tick(&self, now: Instant) -> Vec<NodeId> {
        let mut downed = Vec::new();
        let mut members = self.members.lock().expect("members mutex poisoned");
        self.advance_reachability(&mut members, now, &mut downed);
        self.advance_lifecycle(&mut members, now, &mut downed);
        Self::gc_tombstones(&mut members, now);
        downed
    }

    /// Reachability timeouts and reachability-driven downing (spec §10, §9.2): a
    /// suspicion older than the (size-scaled) suspect timeout becomes
    /// `Unreachable`, and the leader moves an `Unreachable` member to `Down`
    /// under a `Timeout` policy.
    ///
    /// Declaring a member `down` is a cluster decision the leader owns (spec
    /// §9.2 #3), so only the leader runs the downing transition; other nodes
    /// adopt the `down` once it reaches them by gossip (`merge`). This downing is
    /// deliberately *not* gated on the reachable-convergence rule used for
    /// promotion: the node being downed is by definition unreachable, so that
    /// rule could never hold — leadership alone supplies the single-decider
    /// property. Reachability (`Suspect`→`Unreachable`) is a local detector
    /// state, not a cluster decision, so it stays per-node.
    fn advance_reachability(
        &self,
        members: &mut BTreeMap<NodeId, Member>,
        now: Instant,
        downed: &mut Vec<NodeId>,
    ) {
        let is_leader = self.compute_leader(members) == Some(self.node);
        // `T_suspect` scales with cluster size (spec §10): a larger cluster needs
        // a longer suspicion window to hold the false-positive rate down. The
        // scaling is logarithmic and stays at the base for small clusters
        // (≤3 members, factor 1), so detection latency is unchanged in the common
        // small case and grows only gently as the cluster does.
        let suspect_timeout = self.effective_suspect_timeout(members.len() + 1);
        for (node, m) in members.iter_mut() {
            if m.status.is_terminal() {
                continue;
            }
            if m.reachability == Reachability::Suspect
                && now.duration_since(m.changed_at) >= suspect_timeout
            {
                m.reachability = Reachability::Unreachable;
                m.changed_at = now;
                self.events.emit(Event::Unreachable {
                    observer: self.node,
                    node: *node,
                });
            }
            if is_leader && m.reachability == Reachability::Unreachable {
                if let DowningPolicy::Timeout(after) = self.downing {
                    if now.duration_since(m.changed_at) >= after {
                        let prev = m.status;
                        m.status = MemberStatus::Down;
                        m.changed_at = now;
                        if let Some(event) =
                            self.transition_event(*node, Some(prev), MemberStatus::Down)
                        {
                            self.events.emit(event);
                        }
                        downed.push(*node);
                    }
                }
            }
        }
    }

    /// Leader-driven lifecycle transitions (spec §9.2, §9.3), gated on
    /// convergence: the leader acts only when every live member is reachable, so
    /// it never makes membership decisions while the cluster is in flux or
    /// partitioned (avoiding split decisions). This is a partition-safe
    /// approximation of §9.2's "all up members agree on the set"; a full
    /// vector-clock seen-set is deeper future work. Reachability-driven downing
    /// ([`advance_reachability`](Self::advance_reachability)) is independent and
    /// still proceeds.
    ///
    /// Promotion uses a deliberately simple rule — the leader admits any
    /// reachable joiner — which is safe because the lifecycle is monotonic and
    /// admission is idempotent (a split-brain leader would reach the same `Up`).
    fn advance_lifecycle(
        &self,
        members: &mut BTreeMap<NodeId, Member>,
        now: Instant,
        downed: &mut Vec<NodeId>,
    ) {
        // Managed mode (spec §9.4): the operator drives admission and removal by
        // command, so the leader never auto-admits or auto-finalizes here.
        if !self.autonomous {
            return;
        }
        let converged = members
            .values()
            .all(|m| m.status.is_terminal() || m.reachability == Reachability::Reachable);
        if !(converged && self.compute_leader(members) == Some(self.node)) {
            return;
        }
        for (node, m) in members.iter_mut() {
            let new = match m.status {
                MemberStatus::Joining if m.reachability == Reachability::Reachable => {
                    MemberStatus::Up
                }
                MemberStatus::Leaving => MemberStatus::Down,
                _ => continue,
            };
            let prev = m.status;
            m.status = new;
            m.changed_at = now;
            if let Some(event) = self.transition_event(*node, Some(prev), new) {
                self.events.emit(event);
            }
            if new == MemberStatus::Down {
                downed.push(*node);
            }
        }
        // The leader admits itself, too (bootstrapping a fresh cluster).
        if self.self_status() == MemberStatus::Joining {
            self.set_self_status(MemberStatus::Up);
        }
    }

    /// Tombstone GC (spec §9.1): a member `Down` long enough is tombstoned
    /// `Removed`; a `Removed` tombstone that has lingered long enough is pruned
    /// from the roster, bounding its growth under churn. A pruned node's id is
    /// never reused (§9.1), so this never resurrects it.
    fn gc_tombstones(members: &mut BTreeMap<NodeId, Member>, now: Instant) {
        for m in members.values_mut() {
            if m.status == MemberStatus::Down && now.duration_since(m.changed_at) >= TOMBSTONE_AFTER
            {
                m.status = MemberStatus::Removed;
                m.changed_at = now;
            }
        }
        members.retain(|_, m| {
            m.status != MemberStatus::Removed || now.duration_since(m.changed_at) < PRUNE_AFTER
        });
    }

    /// This node's own incarnation, carried in gossip so peers can clear a stale
    /// suspicion about us (spec §10 #4).
    pub fn self_incarnation(&self) -> u64 {
        self.incarnation.load(Ordering::Relaxed)
    }

    /// This node's view of the cluster, for gossip (spec §9.2). Includes our own
    /// `alive` entry at our current incarnation.
    pub fn digest(&self) -> Vec<MemberDigest> {
        let members = self.members.lock().expect("members mutex poisoned");
        let mut out: Vec<MemberDigest> = members
            .iter()
            .map(|(node, m)| MemberDigest {
                node: *node,
                status: m.status,
                reachability: m.reachability,
                incarnation: m.incarnation,
                revision: m.revision,
            })
            .collect();
        out.push(MemberDigest {
            node: self.node,
            status: self.self_status(),
            reachability: Reachability::Reachable,
            incarnation: self.incarnation.load(Ordering::Relaxed),
            revision: self.self_revision.load(Ordering::Relaxed),
        });
        out
    }

    /// Merge a peer's gossiped view into ours (spec §9.2). Two axes merge
    /// independently:
    ///
    /// - **Status** (the operator/lifecycle axis): the **higher revision wins**;
    ///   at equal revision the **rank lattice** is the tie-break (`joining → up →
    ///   … → removed`). In static/autonomous mode every revision is `0`, so this
    ///   reduces to the plain monotonic lattice exactly as before. In managed mode
    ///   the leader's latest operator decision carries the highest revision, so a
    ///   *reversible* `up ⇄ draining` change converges without rank ordering. A
    ///   terminal member (`down`/`removed`) is sticky — it only advances toward
    ///   `removed`, never reverts, regardless of revision (invariant #15).
    /// - **Reachability** (the detector axis): a higher incarnation wins, else the
    ///   more severe view; a non-reachable claim about *ourselves* is refuted by
    ///   bumping our incarnation (spec §10 #4). An operator status *decision* about
    ///   ourselves (a strictly higher revision) we instead adopt.
    ///
    /// Returns nodes newly declared `down`, for the caller to run the cascade
    /// (spec §8.1).
    pub fn merge(&self, incoming: Vec<MemberDigest>, now: Instant) -> Vec<NodeId> {
        let mut downed = Vec::new();
        let mut to_emit: Vec<Event> = Vec::new();
        {
            let mut members = self.members.lock().expect("members mutex poisoned");
            for d in incoming {
                // Keep the leader's decision counter ahead of anything in gossip,
                // so single-writer ordering survives a control-plane restart.
                self.revision.fetch_max(d.revision, Ordering::Relaxed);

                if d.node == self.node {
                    self.merge_self(d);
                } else if let Some(node) = self.merge_peer(&mut members, d, now, &mut to_emit) {
                    downed.push(node);
                }
            }
        }
        for event in to_emit {
            self.events.emit(event);
        }
        downed
    }

    /// Merge one peer's gossiped entry into the roster — the per-entry body of
    /// [`merge`] for a node other than ourselves. Applies the two-axis precedence
    /// ([`status_supersedes`] over status, [`reachability_supersedes`] over
    /// reachability), pushing any observed [transition](Self::transition_event)
    /// onto `to_emit`, and returns the node if this merge newly declared it `down`
    /// (so the caller runs the cascade, spec §8.1).
    fn merge_peer(
        &self,
        members: &mut BTreeMap<NodeId, Member>,
        d: MemberDigest,
        now: Instant,
        to_emit: &mut Vec<Event>,
    ) -> Option<NodeId> {
        let Some(member) = members.get_mut(&d.node) else {
            // Never resurrect a tombstone we have already pruned or never knew.
            if d.status == MemberStatus::Removed {
                return None;
            }
            // First sight: adopt the incoming view verbatim and announce it.
            members.insert(
                d.node,
                Member {
                    status: d.status,
                    reachability: d.reachability,
                    incarnation: d.incarnation,
                    revision: d.revision,
                    changed_at: now,
                },
            );
            if let Some(event) = self.transition_event(d.node, None, d.status) {
                to_emit.push(event);
            }
            return (d.status == MemberStatus::Down).then_some(d.node);
        };

        // A terminal member is sticky: it only advances toward `removed` (the
        // tombstone disseminating), never reverts (invariant #15), and carries no
        // reachability.
        if member.status.is_terminal() {
            if d.status.rank() > member.status.rank() {
                member.status = d.status;
                member.changed_at = now;
            }
            return None;
        }

        // Status axis: operator-revision then rank.
        let mut downed = None;
        if status_supersedes((d.revision, d.status), (member.revision, member.status)) {
            member.revision = member.revision.max(d.revision);
            if d.status != member.status {
                let prev = member.status;
                member.status = d.status;
                member.changed_at = now;
                if let Some(event) = self.transition_event(d.node, Some(prev), d.status) {
                    to_emit.push(event);
                }
                if d.status == MemberStatus::Down {
                    downed = Some(d.node);
                }
            }
        }

        // Reachability axis: incarnation then severity, while the member is live.
        if !member.status.is_terminal()
            && reachability_supersedes(
                (d.incarnation, d.reachability),
                (member.incarnation, member.reachability),
            )
        {
            member.incarnation = member.incarnation.max(d.incarnation);
            if member.reachability != d.reachability {
                member.reachability = d.reachability;
                member.changed_at = now;
                to_emit.push(reachability_event(self.node, d.node, d.reachability));
            }
        }
        downed
    }

    /// Merge a digest entry that is about *this* node (spec §9.2, §10 #4).
    ///
    /// A strictly higher **revision** is an operator decision about us (managed
    /// mode) — admission, drain, resume, or decommission — which we adopt. Anything
    /// else is the legacy path: refute a stale non-reachable claim by bumping our
    /// incarnation, and adopt a forward rank transition (e.g. an elected leader
    /// admitting us `joining → up`).
    fn merge_self(&self, d: MemberDigest) {
        if d.revision > self.self_revision.load(Ordering::Relaxed) {
            self.self_revision.store(d.revision, Ordering::Relaxed);
            self.set_self_status(d.status);
            return;
        }
        if d.reachability != Reachability::Reachable || d.status == MemberStatus::Down {
            let cur = self.incarnation.load(Ordering::Relaxed);
            if d.incarnation >= cur {
                self.incarnation.store(d.incarnation + 1, Ordering::Relaxed);
            }
        }
        if d.status.rank() > self.self_status().rank() {
            self.set_self_status(d.status);
        }
    }

    /// Set this node's own status and announce the transition — the single
    /// mutator of `self_status`, whether the move is an operator decision about us
    /// (managed mode) or a forward lifecycle step the elected leader drove (e.g.
    /// `joining → up`). Callers gate it on a real transition (a higher rank, or a
    /// higher revision). A terminal status (decommission) is recorded so the node
    /// can observe it and shut down, but emits no self event — observers announce
    /// the `down` (spec §8.1). It never reaches `draining` from `joining` (a node
    /// cannot be a drain target before it is `up`), so that case needs no arm.
    fn set_self_status(&self, status: MemberStatus) {
        let mut s = self.self_status.lock().expect("self status mutex poisoned");
        if *s == status {
            return;
        }
        let prev = *s;
        *s = status;
        let event = match status {
            MemberStatus::Up if prev == MemberStatus::Draining => Some(Event::MemberResumed {
                observer: self.node,
                node: self.node,
            }),
            MemberStatus::Up => Some(Event::MemberUp {
                observer: self.node,
                node: self.node,
            }),
            MemberStatus::Draining => Some(Event::MemberDraining {
                observer: self.node,
                node: self.node,
            }),
            _ => None,
        };
        if let Some(event) = event {
            self.events.emit(event);
        }
    }

    /// Every live (non-terminal) peer, for broadcasting cluster-wide state
    /// (spec §13).
    pub fn members(&self) -> Vec<NodeId> {
        self.members
            .lock()
            .expect("members mutex poisoned")
            .iter()
            .filter(|(_, m)| !m.status.is_terminal())
            .map(|(n, _)| *n)
            .collect()
    }

    /// The current reachability of `node`, if known (for tests/inspection).
    pub fn reachability(&self, node: NodeId) -> Option<Reachability> {
        self.members
            .lock()
            .expect("members mutex poisoned")
            .get(&node)
            .map(|m| m.reachability)
    }

    /// The current lifecycle status of `node`, if known (for tests/inspection).
    pub fn status(&self, node: NodeId) -> Option<MemberStatus> {
        self.members
            .lock()
            .expect("members mutex poisoned")
            .get(&node)
            .map(|m| m.status)
    }
}
