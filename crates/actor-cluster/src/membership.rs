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

/// One node's view of a member, exchanged by gossip (spec §9.2). Serializable so
/// it can be piggybacked on `Ping`/`Ack` frames over the wire (spec §10).
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct MemberDigest {
    pub node: NodeId,
    pub status: MemberStatus,
    pub reachability: Reachability,
    pub incarnation: u64,
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
            MemberStatus::Up => 1,
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

struct Member {
    status: MemberStatus,
    reachability: Reachability,
    /// The incarnation this view is tagged with; a higher incarnation wins, and
    /// only a higher one (a refutation) can clear a suspicion (spec §9.2, §10).
    incarnation: u64,
    /// When `reachability` last changed — drives the suspect and downing timers.
    changed_at: Instant,
}

/// One node's view of the cluster (spec §9). Internally synchronized.
pub struct Membership {
    node: NodeId,
    downing: DowningPolicy,
    suspect_timeout: Duration,
    members: Mutex<BTreeMap<NodeId, Member>>,
    /// This node's own lifecycle status, advertised in its gossip digest. A
    /// founding member starts `Up`; a joiner starts `Joining` and is admitted to
    /// `Up` by the leader once it converges (spec §9.3).
    self_status: Mutex<MemberStatus>,
    /// This node's own incarnation; bumped to refute a suspicion about itself
    /// (spec §10 #4).
    incarnation: AtomicU64,
    events: Arc<dyn EventSink>,
}

impl Membership {
    /// Create an empty roster for `node`. `joining` marks this node as a joiner
    /// (starts `Joining`, awaiting admission); otherwise it is a founding member
    /// (starts `Up`).
    pub fn new(
        node: NodeId,
        config: &SwimConfig,
        events: Arc<dyn EventSink>,
        joining: bool,
    ) -> Membership {
        let self_status = if joining {
            MemberStatus::Joining
        } else {
            MemberStatus::Up
        };
        Membership {
            node,
            downing: config.downing,
            suspect_timeout: config.suspect_timeout,
            members: Mutex::new(BTreeMap::new()),
            self_status: Mutex::new(self_status),
            incarnation: AtomicU64::new(0),
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

    /// Advance this node's own status forward (monotonically), emitting a
    /// `MemberUp` when it reaches `Up`.
    fn advance_self(&self, to: MemberStatus) {
        let mut s = self.self_status.lock().expect("self status mutex poisoned");
        if to.rank() > s.rank() {
            *s = to;
            if to == MemberStatus::Up {
                self.events.emit(Event::MemberUp {
                    observer: self.node,
                    node: self.node,
                });
            }
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

    /// The cluster leader (spec §9.2): the lowest-id reachable, non-`down`
    /// member, this node included. The leader performs the `up`/`down` lifecycle
    /// transitions. Returns `None` only if this node has excluded itself by
    /// leaving or being down.
    fn compute_leader(&self, members: &BTreeMap<NodeId, Member>) -> Option<NodeId> {
        let mut best = None;
        let ss = self.self_status();
        if !ss.is_terminal() && ss != MemberStatus::Leaving {
            best = Some(self.node);
        }
        for (node, m) in members.iter() {
            // A leaving or terminal member is not a leader candidate, so
            // leadership fails over when the leader itself leaves (spec §9.2, §9.3).
            if !m.status.is_terminal()
                && m.status != MemberStatus::Leaving
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
                        m.status = MemberStatus::Down;
                        m.changed_at = now;
                        self.events.emit(Event::NodeDown {
                            observer: self.node,
                            node: *node,
                        });
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
        let converged = members
            .values()
            .all(|m| m.status.is_terminal() || m.reachability == Reachability::Reachable);
        if !(converged && self.compute_leader(members) == Some(self.node)) {
            return;
        }
        for (node, m) in members.iter_mut() {
            match m.status {
                MemberStatus::Joining if m.reachability == Reachability::Reachable => {
                    m.status = MemberStatus::Up;
                    m.changed_at = now;
                    self.events.emit(Event::MemberUp {
                        observer: self.node,
                        node: *node,
                    });
                }
                MemberStatus::Leaving => {
                    m.status = MemberStatus::Down;
                    m.changed_at = now;
                    self.events.emit(Event::NodeDown {
                        observer: self.node,
                        node: *node,
                    });
                    downed.push(*node);
                }
                _ => {}
            }
        }
        // The leader admits itself, too (bootstrapping a fresh cluster).
        if self.self_status() == MemberStatus::Joining {
            self.advance_self(MemberStatus::Up);
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
            })
            .collect();
        out.push(MemberDigest {
            node: self.node,
            status: self.self_status(),
            reachability: Reachability::Reachable,
            incarnation: self.incarnation.load(Ordering::Relaxed),
        });
        out
    }

    /// Merge a peer's gossiped view into ours (spec §9.2): a higher incarnation
    /// wins; at equal incarnation the more severe reachability wins; `down` is
    /// terminal; and a non-reachable claim about *ourselves* is refuted by
    /// bumping our incarnation above it (spec §10 #4). Returns nodes newly
    /// declared `down`, for the caller to run the cascade (spec §8.1).
    pub fn merge(&self, incoming: Vec<MemberDigest>, now: Instant) -> Vec<NodeId> {
        let mut downed = Vec::new();
        let mut to_emit: Vec<Event> = Vec::new();
        {
            let mut members = self.members.lock().expect("members mutex poisoned");
            for d in incoming {
                if d.node == self.node {
                    if d.reachability != Reachability::Reachable || d.status == MemberStatus::Down {
                        let cur = self.incarnation.load(Ordering::Relaxed);
                        if d.incarnation >= cur {
                            self.incarnation.store(d.incarnation + 1, Ordering::Relaxed);
                        }
                    }
                    // Adopt a forward self-transition (e.g. the leader admitted us
                    // to `Up`, or finalized our `Leaving` to `Down`).
                    if d.status.rank() > self.self_status().rank() {
                        self.advance_self(d.status);
                    }
                    continue;
                }
                // A node first appears with the status the digest reports — a new
                // joiner shows up as `Joining`, not silently `Up`. But never
                // resurrect a tombstone: ignore an unknown node reported `Removed`
                // (we have already pruned it, or never knew it).
                let known = members.contains_key(&d.node);
                if !known && d.status == MemberStatus::Removed {
                    continue;
                }
                let member = members.entry(d.node).or_insert_with(|| {
                    if d.status == MemberStatus::Joining {
                        to_emit.push(Event::MemberJoining {
                            observer: self.node,
                            node: d.node,
                        });
                    }
                    Member {
                        status: d.status,
                        reachability: d.reachability,
                        incarnation: d.incarnation,
                        changed_at: now,
                    }
                });
                // A `Removed` member is the most terminal — ignore further gossip.
                if member.status == MemberStatus::Removed {
                    continue;
                }
                // Status moves forward monotonically (spec §9.1): `down` triggers
                // the cascade; `removed` advances the tombstone by gossip.
                if known && d.status.rank() > member.status.rank() {
                    member.status = d.status;
                    member.changed_at = now;
                    match d.status {
                        MemberStatus::Up => to_emit.push(Event::MemberUp {
                            observer: self.node,
                            node: d.node,
                        }),
                        MemberStatus::Down => {
                            to_emit.push(Event::NodeDown {
                                observer: self.node,
                                node: d.node,
                            });
                            downed.push(d.node);
                        }
                        _ => {}
                    }
                }
                // Reachability matters only for a live member; a terminal one is
                // gone. A higher incarnation wins, else the more severe view.
                if !member.status.is_terminal() {
                    let adopt = d.incarnation > member.incarnation
                        || (d.incarnation == member.incarnation
                            && severity(d.reachability) > severity(member.reachability));
                    if adopt {
                        member.incarnation = member.incarnation.max(d.incarnation);
                        if member.reachability != d.reachability {
                            member.reachability = d.reachability;
                            member.changed_at = now;
                            to_emit.push(reachability_event(self.node, d.node, d.reachability));
                        }
                    }
                }
            }
        }
        for event in to_emit {
            self.events.emit(event);
        }
        downed
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
