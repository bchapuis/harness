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
//! `Unreachable`. Who may then declare `down` — and who drives the
//! `joining → up` / `leaving → down` lifecycle — is the **control plane**, one
//! of four configurable [`MembershipMode`]s (spec §9.4): a fixed **static**
//! roster, an external **registry**, a self-hosted Raft log behind an elected
//! **leader**, or peer-to-peer **gossip** with a deterministic coordinator.
//! Stamped authority decisions (registry revisions, Raft commit indexes) enter
//! through [`Membership::apply_stamped`] and win the view merge (spec §9.2).

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

use crate::raft::RaftConfig;
use crate::registry::RegistryClient;

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
/// `(stamp, status)` view supersedes the current one. A higher **authority
/// stamp** — the registry revision (registry-based, spec §9.4.2) or the log
/// commit index (leader-based, spec §9.4.3) — wins; at equal stamp the
/// more-advanced rank wins (the `joining → … → removed` lattice). In static and
/// gossip-based mode no stamp is ever issued (every stamp is `0`), so this
/// reduces to the plain monotonic lattice; in the stamped modes the authority's
/// latest decision carries the highest stamp, which is what lets a reversible
/// `up ⇄ draining` change converge without rank ordering.
fn status_supersedes(incoming: (u64, MemberStatus), current: (u64, MemberStatus)) -> bool {
    incoming.0 > current.0 || (incoming.0 == current.0 && incoming.1.rank() > current.1.rank())
}

/// The **reachability-axis** merge order (spec §10): whether an incoming
/// `(incarnation, reachability)` view supersedes the current one. A higher
/// incarnation wins (a refutation); at equal incarnation the more severe view wins.
fn reachability_supersedes(incoming: (u64, Reachability), current: (u64, Reachability)) -> bool {
    incoming.0 > current.0
        || (incoming.0 == current.0 && severity(incoming.1) > severity(current.1))
}

/// One node's view of a member, exchanged by gossip (spec §9.2). Serializable so
/// it can be piggybacked on `Ping`/`Ack` frames over the wire (spec §10).
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MemberDigest {
    pub node: NodeId,
    pub status: MemberStatus,
    pub reachability: Reachability,
    pub incarnation: u64,
    /// The **authority stamp** that produced `status` (spec §9.2): the registry
    /// revision (registry-based, spec §9.4.2) or the Raft commit index
    /// (leader-based, spec §9.4.3). The mode's stamps come from a single logical
    /// writer, so the merge taking the higher stamp totally orders its decisions,
    /// which lets a *reversible* `up ⇄ draining` change converge without rank
    /// ordering. `0` in static and gossip-based mode, where status follows the
    /// rank lattice.
    #[serde(default)]
    pub stamp: u64,
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
    /// **Reversible** maintenance state, available in the modes with an
    /// authoritative control plane — registry-based and leader-based (spec
    /// §9.1, §9.4): the node is cordoned — callers route away — but it stays a
    /// full member and is *not* terminal. A later `resume` returns it to
    /// [`Up`](MemberStatus::Up). Because it is reversible it does not sit on the
    /// monotonic `joining → … → removed` ladder; transitions in and out of it are
    /// ordered by the authority's stamp, not by rank (see [`Membership::merge`]).
    /// It therefore shares [`Up`](MemberStatus::Up)'s rank.
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

/// How an `unreachable` member becomes `down` (spec §9.4). A policy exists only
/// in the modes whose authority downs at all — applied by the coordinator
/// (gossip-based, spec §9.4.4) or committed by the leader (leader-based, spec
/// §9.4.3). The default is conservative: a partition alone never downs a node
/// (invariant #16).
#[derive(Clone, Copy, Debug)]
pub enum DowningPolicy {
    /// Never auto-down; `unreachable` is left for an operator to resolve.
    Conservative,
    /// Down a member that has been `unreachable` for this long.
    Timeout(Duration),
}

/// SWIM detector parameters (spec §10). All MUST be configurable. The detector
/// is a pure reachability *sensor*: what its confirmations may cause — nothing,
/// an observation, or a `down` — is the mode's [`DowningPolicy`], not part of
/// this config.
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
}

impl Default for SwimConfig {
    fn default() -> Self {
        SwimConfig {
            probe_interval: Duration::from_secs(1),
            rtt: Duration::from_millis(200),
            suspect_timeout: Duration::from_secs(3),
            indirect_count: 3,
        }
    }
}

/// The registry-based control plane (spec §9.4.2): the authoritative member set
/// lives in an external registry the cluster reads but does not operate. Every
/// member runs a sync loop against [`client`](RegistryMode::client) and applies
/// the registry's revision-stamped state to its view; the detector is
/// **observe-only** — only a registry mutation ever declares `down`.
#[derive(Clone)]
pub struct RegistryMode {
    /// Detector parameters (observe-only, spec §10).
    pub swim: SwimConfig,
    /// The registry seam (spec §9.4.2 item 7).
    pub client: Arc<dyn RegistryClient>,
    /// Sync-loop cadence: how often to fetch and apply the registry state.
    pub sync_interval: Duration,
}

/// The leader-based control plane (spec §9.4.3): membership transitions are
/// entries of a self-hosted replicated log, committed by quorum through an
/// elected leader. The detector is a sensor feeding the leader, which alone may
/// commit `down` under [`downing`](LeaderMode::downing).
#[derive(Clone)]
pub struct LeaderMode {
    /// Detector parameters (the leader's downing sensor, spec §10).
    pub swim: SwimConfig,
    /// The consensus configuration: voters, timing, storage (spec §9.4.3).
    pub raft: RaftConfig,
    /// How the leader escalates a confirmed `unreachable` to a committed `down`
    /// (spec §9.4.3 item 4). Quorum-gated by construction (invariant #22).
    pub downing: DowningPolicy,
}

/// The gossip-based control plane (spec §9.4.4): membership propagates
/// peer-to-peer through SWIM and anti-entropy gossip, and the transitions that
/// need a single actor are performed by the **coordinator** — a deterministic
/// role, the lowest-address `up`, reachable member.
#[derive(Clone, Copy)]
pub struct GossipMode {
    /// Detector parameters (full SWIM, spec §10).
    pub swim: SwimConfig,
    /// How the coordinator escalates a confirmed `unreachable` to `down`
    /// (spec §9.4.4 item 4).
    pub downing: DowningPolicy,
}

/// Which **control plane** governs membership (spec §9.4) — where the
/// authoritative member set lives, how members learn it, and who may declare a
/// member dead. One choice at node startup; every node in a cluster MUST run
/// the same mode. Everything else — the lattice (spec §9.1), the merge rule
/// (spec §9.2), the lifecycle (spec §9.3), terminal `down` — is shared.
#[derive(Clone)]
pub enum MembershipMode {
    /// Fixed roster, no lifecycle transitions ever (spec §9.4.1). By default no
    /// detector runs and no membership traffic flows; `detector` MAY enable the
    /// SWIM loop **observe-only** — reachability events, early `Unreachable`
    /// completion, discovery routing — with still no down authority.
    Static { detector: Option<SwimConfig> },
    /// An external registry is the authority (spec §9.4.2).
    Registry(RegistryMode),
    /// A self-hosted Raft log behind an elected leader is the authority
    /// (spec §9.4.3).
    Leader(LeaderMode),
    /// No authority — peer-to-peer dissemination with a deterministic
    /// coordinator (spec §9.4.4).
    Gossip(GossipMode),
}

impl MembershipMode {
    /// The SWIM parameters this mode runs the detector with, or `None` when no
    /// detector loop runs (static mode without the observe-only option,
    /// spec §9.4.1).
    pub fn detector(&self) -> Option<SwimConfig> {
        match self {
            MembershipMode::Static { detector } => *detector,
            MembershipMode::Registry(mode) => Some(mode.swim),
            MembershipMode::Leader(mode) => Some(mode.swim),
            MembershipMode::Gossip(mode) => Some(mode.swim),
        }
    }

    /// Whether the lifecycle is coordinator-driven (gossip-based mode, spec
    /// §9.4.4): the coordinator admits joiners, finalizes leaves, and applies
    /// the downing policy. In every other mode those transitions enter as
    /// stamped authority decisions ([`Membership::apply_stamped`]) or not at all.
    fn coordinator_driven(&self) -> bool {
        matches!(self, MembershipMode::Gossip(_))
    }

    /// The downing policy [`Membership`] itself applies — the gossip-based
    /// coordinator's (spec §9.4.4 item 4). Every other mode is
    /// [`Conservative`](DowningPolicy::Conservative) here: static and
    /// registry-based have no in-cluster down authority at all (spec §9.4.1,
    /// §9.4.2 item 4), and the leader-based policy is applied by the Raft
    /// leader through committed entries, not by the local view.
    fn local_downing(&self) -> DowningPolicy {
        match self {
            MembershipMode::Gossip(mode) => mode.downing,
            _ => DowningPolicy::Conservative,
        }
    }
}

struct Member {
    status: MemberStatus,
    reachability: Reachability,
    /// The incarnation this view is tagged with; a higher incarnation wins, and
    /// only a higher one (a refutation) can clear a suspicion (spec §9.2, §10).
    incarnation: u64,
    /// The authority stamp behind `status` (spec §9.2): a registry revision or a
    /// Raft commit index; `0` where no authority stamps (static/gossip). The
    /// merge prefers the higher stamp, so a reversible `up ⇄ draining` toggle
    /// converges; the rank lattice is only the tie-break.
    stamp: u64,
    /// When `reachability` last changed — drives the suspect and downing timers.
    changed_at: Instant,
}

/// A snapshot of the view the coordinator's stability gate compares across
/// detector ticks (spec §9.4.4 item 3): one `(node, status, reachability)`
/// triple per member.
type ViewSnapshot = Vec<(NodeId, MemberStatus, Reachability)>;

/// One node's view of the cluster (spec §9). Internally synchronized.
pub struct Membership {
    node: NodeId,
    /// The downing policy applied by this view itself — the gossip-based
    /// coordinator's (spec §9.4.4); `Conservative` in every other mode
    /// (see [`MembershipMode::local_downing`]).
    downing: DowningPolicy,
    suspect_timeout: Duration,
    /// Whether the coordinator drives the lifecycle here (gossip-based mode,
    /// spec §9.4.4). `false` in the other modes, where transitions enter as
    /// stamped authority decisions ([`apply_stamped`](Self::apply_stamped)).
    coordinator_driven: bool,
    /// The coordinator's stability window (spec §9.4.4 item 3): how long the
    /// view must be unchanged and fully reachable before the coordinator
    /// performs lifecycle transitions. Derived from the probe interval.
    stability_window: Duration,
    /// The last view snapshot and the instant it has been stable since — the
    /// coordinator's "locally stable" gate (spec §9.4.4 item 3). `None` until
    /// the first detector tick.
    stable_view: Mutex<Option<(ViewSnapshot, Instant)>>,
    members: Mutex<BTreeMap<NodeId, Member>>,
    /// This node's own lifecycle status, advertised in its gossip digest. A
    /// founding member starts `Up`; a joiner starts `Joining` and is admitted to
    /// `Up` by the mode's authority (spec §9.3).
    self_status: Mutex<MemberStatus>,
    /// This node's own incarnation; bumped to refute a suspicion about itself
    /// (spec §10 #4).
    incarnation: AtomicU64,
    /// The stamp of the authority decision behind this node's own `self_status`.
    /// A node only *adopts* status decisions about itself — it is never the
    /// writer — so this just tracks the highest stamp it has accepted.
    self_stamp: AtomicU64,
    events: Arc<dyn EventSink>,
}

impl Membership {
    /// Create an empty roster for `node` under `mode`. `joining` marks this node
    /// a joiner (starts `Joining`, awaiting admission by the mode's authority,
    /// spec §9.3); otherwise it is a founding member (`Up`).
    pub fn new(
        node: NodeId,
        mode: &MembershipMode,
        events: Arc<dyn EventSink>,
        joining: bool,
    ) -> Membership {
        // `joining` is meaningful only where admission is an in-cluster decision
        // (gossip- and leader-based, spec §9.1): static members always start
        // `Up`, and in registry-based mode admission *is* the registry entry
        // (spec §9.4.2 item 2) — the `Joining` state is unused.
        let joining =
            joining && matches!(mode, MembershipMode::Gossip(_) | MembershipMode::Leader(_));
        let self_status = if joining {
            MemberStatus::Joining
        } else {
            MemberStatus::Up
        };
        let swim = mode.detector().unwrap_or_default();
        Membership {
            node,
            downing: mode.local_downing(),
            suspect_timeout: swim.suspect_timeout,
            coordinator_driven: mode.coordinator_driven(),
            stability_window: 2 * swim.probe_interval,
            stable_view: Mutex::new(None),
            members: Mutex::new(BTreeMap::new()),
            self_status: Mutex::new(self_status),
            incarnation: AtomicU64::new(0),
            self_stamp: AtomicU64::new(0),
            events,
        }
    }

    /// This node's own current lifecycle status.
    pub fn self_status(&self) -> MemberStatus {
        *self.self_status.lock().expect("self status mutex poisoned")
    }

    /// Announce that this node is leaving (spec §9.3): it advertises `Leaving` in
    /// its digest; the mode's authority finalizes it to `Down` and watchers are
    /// notified. The announcement itself decides nothing (spec §9.3).
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
                stamp: 0,
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
        // heard of enters `Joining` until its digest or the mode's authority
        // says `Up`.
        let member = members.entry(node).or_insert(Member {
            status: MemberStatus::Joining,
            reachability: Reachability::Reachable,
            incarnation,
            stamp: 0,
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

    /// The gossip-based **coordinator** (spec §9.4.4 item 3): a deterministic
    /// *role*, not an election — it falls to the lowest-address `up`, reachable
    /// member of this node's view, this node included. Returns `None` only if
    /// this node has excluded itself by leaving or being down and sees no other
    /// candidate.
    fn compute_coordinator(&self, members: &BTreeMap<NodeId, Member>) -> Option<NodeId> {
        let mut best = None;
        let ss = self.self_status();
        if !ss.is_terminal() && ss != MemberStatus::Leaving {
            best = Some(self.node);
        }
        for (node, m) in members.iter() {
            // A leaving, draining, or terminal member is not a coordinator
            // candidate, so the role falls over when the coordinator itself
            // steps aside (spec §9.3, §9.4.4).
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

    /// The current coordinator as this node sees it (spec §9.4.4).
    pub fn coordinator(&self) -> Option<NodeId> {
        let members = self.members.lock().expect("members mutex poisoned");
        self.compute_coordinator(&members)
    }

    /// Whether this node is the coordinator (spec §9.4.4).
    pub fn is_coordinator(&self) -> bool {
        self.coordinator() == Some(self.node)
    }

    /// Apply one **stamped authority decision** to the view — a registry entry at
    /// its revision (spec §9.4.2) or a committed Raft entry at its commit index
    /// (spec §9.4.3). The single write path for both stamped control planes:
    /// inserts an unknown member (admission), applies the decision iff it
    /// supersedes the current view ([`status_supersedes`]), respects terminal
    /// stickiness — no stamp revives a `down` member (invariant #15) — and
    /// announces the transition (spec §16). A decision about *this* node is
    /// adopted into `self_status`.
    ///
    /// Returns whether `node` was **newly declared `down`**, in which case the
    /// caller runs the node-down cascade (spec §8.1).
    pub(crate) fn apply_stamped(
        &self,
        node: NodeId,
        status: MemberStatus,
        stamp: u64,
        now: Instant,
    ) -> bool {
        if node == self.node {
            if stamp > self.self_stamp.load(Ordering::Relaxed) {
                self.self_stamp.store(stamp, Ordering::Relaxed);
                self.set_self_status(status);
            }
            return false;
        }
        let event = {
            let mut members = self.members.lock().expect("members mutex poisoned");
            let Some(member) = members.get_mut(&node) else {
                // First sight: the decision itself introduces the member —
                // admission is the entry (spec §9.4.2 item 2) or the committed
                // command. Announce it as the authority's transition.
                members.insert(
                    node,
                    Member {
                        status,
                        reachability: Reachability::Reachable,
                        incarnation: 0,
                        stamp,
                        changed_at: now,
                    },
                );
                let event = match status {
                    MemberStatus::Up => Some(Event::MemberUp {
                        observer: self.node,
                        node,
                    }),
                    MemberStatus::Draining => Some(Event::MemberDraining {
                        observer: self.node,
                        node,
                    }),
                    MemberStatus::Down => Some(Event::NodeDown {
                        observer: self.node,
                        node,
                    }),
                    _ => None,
                };
                if let Some(event) = event {
                    self.events.emit(event);
                }
                return status == MemberStatus::Down;
            };
            // Terminal stickiness comes before the stamp rule (invariant #15): a
            // higher-stamped re-registration must not resurrect a downed member —
            // re-admission after `down` is a new identity, never a revival.
            if member.status.is_terminal() {
                if status.rank() > member.status.rank() {
                    member.status = status;
                    member.changed_at = now;
                }
                return false;
            }
            if !status_supersedes((stamp, status), (member.stamp, member.status)) {
                return false;
            }
            member.stamp = member.stamp.max(stamp);
            if member.status == status {
                return false;
            }
            let prev = member.status;
            member.status = status;
            member.changed_at = now;
            self.transition_event(node, Some(prev), status)
        };
        if let Some(event) = event {
            self.events.emit(event);
        }
        status == MemberStatus::Down
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

    /// Advance time-based transitions (spec §10) and, in gossip-based mode when
    /// this node is the coordinator, the lifecycle transitions it owns
    /// (spec §9.3, §9.4.4):
    ///
    /// - a suspicion older than `suspect_timeout` becomes `Unreachable`, and the
    ///   coordinator's downing policy may move `Unreachable` to `Down`;
    /// - the coordinator admits reachable `Joining` members (itself included) to
    ///   `Up`, and finalizes `Leaving` members to `Down`;
    /// - long-`Down` members are tombstoned `Removed`, then pruned.
    ///
    /// Returns the nodes newly declared `down`, so the caller can run the cascade
    /// (spec §8.1 step 3). The passes run under one lock, so the roster
    /// never changes underneath them within a tick.
    pub fn tick(&self, now: Instant) -> Vec<NodeId> {
        let mut downed = Vec::new();
        let mut members = self.members.lock().expect("members mutex poisoned");
        self.advance_reachability(&mut members, now, &mut downed);
        let stable = self.view_stable(&members, now);
        self.advance_lifecycle(&mut members, now, stable, &mut downed);
        Self::gc_tombstones(&mut members, now);
        downed
    }

    /// The coordinator's **stability gate** (spec §9.4.4 item 3): whether the
    /// view — member set, statuses, reachabilities, and this node's own status —
    /// has been unchanged for the [stability window](Self::stability_window).
    /// Checked once per detector tick, so stability is quantized to the probe
    /// interval; any observed change restarts the window.
    fn view_stable(&self, members: &BTreeMap<NodeId, Member>, now: Instant) -> bool {
        let mut current: ViewSnapshot = members
            .iter()
            .map(|(node, m)| (*node, m.status, m.reachability))
            .collect();
        current.push((self.node, self.self_status(), Reachability::Reachable));
        let mut stable = self.stable_view.lock().expect("stable view mutex poisoned");
        match stable.as_mut() {
            Some((view, since)) if *view == current => {
                now.duration_since(*since) >= self.stability_window
            }
            _ => {
                *stable = Some((current, now));
                false
            }
        }
    }

    /// Reachability timeouts and reachability-driven downing (spec §10,
    /// §9.4.4): a suspicion older than the (size-scaled) suspect timeout becomes
    /// `Unreachable`, and — in gossip-based mode — the coordinator moves an
    /// `Unreachable` member to `Down` under a `Timeout` policy. (In every other
    /// mode [`Membership::downing`] is `Conservative`, so the downing arm never
    /// fires; `down` enters as a stamped authority decision instead, spec §9.4.)
    ///
    /// Declaring a member `down` is a cluster decision the coordinator owns
    /// (spec §9.4.4 item 4), so only the coordinator runs the downing
    /// transition; other nodes adopt the `down` once it reaches them by gossip
    /// (`merge`). This downing is deliberately *not* gated on the stable
    /// fully-reachable view used for promotion: the node being downed is by
    /// definition unreachable, so that gate could never hold — the spec carves
    /// out exactly this exception (§9.4.4 item 3). Reachability
    /// (`Suspect`→`Unreachable`) is a local detector state, not a cluster
    /// decision, so it stays per-node.
    fn advance_reachability(
        &self,
        members: &mut BTreeMap<NodeId, Member>,
        now: Instant,
        downed: &mut Vec<NodeId>,
    ) {
        let is_coordinator = self.compute_coordinator(members) == Some(self.node);
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
            if is_coordinator && m.reachability == Reachability::Unreachable {
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

    /// Coordinator-driven lifecycle transitions (gossip-based mode, spec §9.3,
    /// §9.4.4 item 3), gated on a **locally stable, fully-reachable view**: the
    /// coordinator acts only when every live member it can see is reachable
    /// *and* the view has not changed for the [stability
    /// window](Self::stability_window), so it never transitions members while
    /// its own view is in flux or partitioned (avoiding split decisions).
    /// Reachability-driven downing
    /// ([`advance_reachability`](Self::advance_reachability)) is the spec's
    /// carved-out exception and still proceeds.
    ///
    /// Promotion uses a deliberately simple rule — the coordinator admits any
    /// reachable joiner — which is safe because the lifecycle is monotonic and
    /// admission is idempotent: two nodes that transiently both consider
    /// themselves coordinator cannot conflict, since their decisions merge
    /// through the lattice (`up` monotonic, `down` terminal, spec §9.4.4 item 3).
    fn advance_lifecycle(
        &self,
        members: &mut BTreeMap<NodeId, Member>,
        now: Instant,
        stable: bool,
        downed: &mut Vec<NodeId>,
    ) {
        // Only the gossip-based control plane transitions the lifecycle from
        // inside the cluster view (spec §9.4.4); the stamped modes apply their
        // authority's decisions, and static never transitions at all.
        if !self.coordinator_driven {
            return;
        }
        let converged = members
            .values()
            .all(|m| m.status.is_terminal() || m.reachability == Reachability::Reachable);
        if !(converged && stable && self.compute_coordinator(members) == Some(self.node)) {
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
        // The coordinator admits itself, too (bootstrapping a fresh cluster).
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
                stamp: m.stamp,
            })
            .collect();
        out.push(MemberDigest {
            node: self.node,
            status: self.self_status(),
            reachability: Reachability::Reachable,
            incarnation: self.incarnation.load(Ordering::Relaxed),
            stamp: self.self_stamp.load(Ordering::Relaxed),
        });
        out
    }

    /// Merge a peer's gossiped view into ours (spec §9.2). Two axes merge
    /// independently:
    ///
    /// - **Status** (the authority/lifecycle axis): the **higher stamp wins**;
    ///   at equal stamp the **rank lattice** is the tie-break (`joining → up →
    ///   … → removed`). In static and gossip-based mode no stamp is ever issued
    ///   (every stamp is `0`), so this reduces to the plain monotonic lattice. In
    ///   the stamped modes the authority's latest decision carries the highest
    ///   stamp, so a *reversible* `up ⇄ draining` change converges without rank
    ///   ordering — and gossiping stamped entries is a safe *accelerant*, never a
    ///   second authority (spec §9.4.2 item 1, §9.4.3 item 3). A terminal member
    ///   (`down`/`removed`) is sticky — it only advances toward `removed`, never
    ///   reverts, regardless of stamp (invariant #15).
    /// - **Reachability** (the detector axis): a higher incarnation wins, else the
    ///   more severe view; a non-reachable claim about *ourselves* is refuted by
    ///   bumping our incarnation (spec §10 #4). An authority status *decision*
    ///   about ourselves (a strictly higher stamp) we instead adopt.
    ///
    /// Returns nodes newly declared `down`, for the caller to run the cascade
    /// (spec §8.1).
    pub fn merge(&self, incoming: Vec<MemberDigest>, now: Instant) -> Vec<NodeId> {
        let mut downed = Vec::new();
        let mut to_emit: Vec<Event> = Vec::new();
        {
            let mut members = self.members.lock().expect("members mutex poisoned");
            for d in incoming {
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
                    stamp: d.stamp,
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

        // Status axis: authority stamp then rank.
        let mut downed = None;
        if status_supersedes((d.stamp, d.status), (member.stamp, member.status)) {
            member.stamp = member.stamp.max(d.stamp);
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
    /// A strictly higher **stamp** is an authority decision about us — a registry
    /// entry or a committed log entry naming this node (admission, drain, resume,
    /// removal) — which we adopt. Anything else is the unstamped path: refute a
    /// stale non-reachable claim by bumping our incarnation, and adopt a forward
    /// rank transition (e.g. the coordinator admitting us `joining → up`).
    fn merge_self(&self, d: MemberDigest) {
        if d.stamp > self.self_stamp.load(Ordering::Relaxed) {
            self.self_stamp.store(d.stamp, Ordering::Relaxed);
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
    /// mutator of `self_status`, whether the move is a stamped authority decision
    /// about us or a forward lifecycle step the coordinator drove (e.g.
    /// `joining → up`). Callers gate it on a real transition (a higher rank, or a
    /// higher stamp). A terminal status (removal) is recorded so the node
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

    /// The members the Raft leader should propose for admission (leader-based
    /// mode, spec §9.3): currently `Joining` and reachable. Read-only — the
    /// transition itself is a committed log entry applied via
    /// [`apply_stamped`](Self::apply_stamped).
    pub(crate) fn admission_candidates(&self) -> Vec<NodeId> {
        self.members
            .lock()
            .expect("members mutex poisoned")
            .iter()
            .filter(|(_, m)| {
                m.status == MemberStatus::Joining && m.reachability == Reachability::Reachable
            })
            .map(|(n, _)| *n)
            .collect()
    }

    /// The members announcing a graceful leave (spec §9.3), for the Raft leader
    /// to finalize by committed entry at the departing node's request.
    pub(crate) fn leaving_members(&self) -> Vec<NodeId> {
        self.members
            .lock()
            .expect("members mutex poisoned")
            .iter()
            .filter(|(_, m)| m.status == MemberStatus::Leaving)
            .map(|(n, _)| *n)
            .collect()
    }

    /// The members `policy` would down: confirmed `unreachable` longer than the
    /// policy allows (leader-based mode, spec §9.4.3 item 4). Read-only — the
    /// leader proposes each as a log entry, so downing stays quorum-gated
    /// (invariant #22); this never mutates the view.
    pub(crate) fn downing_candidates(&self, policy: DowningPolicy, now: Instant) -> Vec<NodeId> {
        let DowningPolicy::Timeout(after) = policy else {
            return Vec::new();
        };
        self.members
            .lock()
            .expect("members mutex poisoned")
            .iter()
            .filter(|(_, m)| {
                !m.status.is_terminal()
                    && m.reachability == Reachability::Unreachable
                    && now.duration_since(m.changed_at) >= after
            })
            .map(|(n, _)| *n)
            .collect()
    }

    /// The authority stamp behind `node`'s status, if known (for
    /// tests/inspection, spec §9.2): the registry revision or the Raft commit
    /// index that produced it; `0` where no authority has stamped it.
    pub fn stamp(&self, node: NodeId) -> Option<u64> {
        self.members
            .lock()
            .expect("members mutex poisoned")
            .get(&node)
            .map(|m| m.stamp)
    }
}
