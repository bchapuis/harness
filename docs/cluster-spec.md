# Actor Cluster: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, and **MAY** carry the meanings defined in RFC 2119.

## 1. Scope

A clustered system is an actor system (actor §3.3) whose nodes form one cluster: envelopes cross authenticated associations, members agree on the member set through one of four control planes, a failure detector senses reachability, and consensus groups agree on more than membership. It keeps the actor model's guarantees (actor §4 to §10) across nodes.

It builds on [actor](distributed-actor-spec.md) and on [compatibility](compatibility-spec.md) for its formats. The [cluster utilities](cluster-utilities-spec.md) use its membership view (§3.2); entity storage runs its shard map and per-shard leader election as consensus groups (§3.5; storage §7, §6).

### 1.1 Non-goals

- **Consensus on the message path.** Messaging, death watch, and discovery are eventually consistent in every mode; leader-based mode (§5.4.3) runs consensus for control-plane state only.
- **Loss repair without a detector.** Static mode without a detector (§5.4.1) runs no anti-entropy, so a lost registration, `Watch`, or `Terminated` frame is never repaired (§8).

### 1.2 Conformance

A **clustered system** conforms by meeting the actor system's class (actor §1.2) and §3.2, §3.5, and §4 to §8; a **transport**, §3.1 and §4; a **registry client**, §3.3; a **Raft log store**, §3.4; an **authorizer**, by answering every query of §3.6.

## 2. Model

- **Association:** the authenticated, version-checked channel between two nodes, established by a handshake (§4), over which frames flow.
- **Frame:** one unit of the wire vocabulary every node speaks (§9.1). The **wire version** names the vocabulary.
- **Member:** a node in the cluster's membership. Each member has a **state** on the ladder of §5.1, possibly the reversible **draining** cordon, and a **reachability**: reachable, suspect, or unreachable.
- **View:** a node's membership view, one entry per member: its node id, state, **incarnation** (a counter that orders reachability claims about the member), and authority stamp.
- **Authority stamp:** the totally ordered stamp a mode's single logical writer attaches to its decisions: the registry's revision, or the consensus log's commit index.
- **Control plane** (or **mode**): who holds the authoritative member set, how members learn it, and who may declare a member down. There are four: static, registry-based, leader-based, and gossip-based (§5.4).
- **Down authority:** the role a mode allows to declare a member down. The **downing policy** is the rule it applies to an unreachable member: **conservative** never downs automatically; **timeout** downs a member unreachable for a configured time.
- **Detector:** the SWIM-style failure detector that maintains each member's reachability (§6).
- **Consensus group:** an independent Raft instance with its own log, leadership, and terms. **Voters** elect leaders and form quorums; **learners** replicate without voting. The **control group** carries membership in leader-based mode.

## 3. Seams

### 3.1 Transport

A transport carries frames between nodes: a network transport in a deployment, a virtual one in a simulator. A plug point.

```rust
pub trait Transport: Clone + Send + Sync + 'static {
    async fn send(&self, peer: NodeId, frame: Frame) -> Result<(), TransportError>;
    fn shutdown(&self);
}
```

- `send` dials and handshakes on first use (§4) and MUST NOT retransmit (actor §8.1); an unreachable peer is `TransportError::Unreachable`, which callers see as `Unreachable`.
- A transport MUST deliver each inbound frame to the system tagged with the node identity its association authenticated, in the order that peer sent it.
- `shutdown` releases its resources on a graceful stop (§5.3); closing the inbound path ends the receive loop.
- A transport MAY carry its own address-discovery traffic, never handed up to the system.
- Under the deterministic profile (actor §11.2), the transport and the registry client (§3.3) are the system's only sources of network nondeterminism.

### 3.2 Membership view

A read-only boundary over the local view, which only the control plane writes. The utilities read it for placement, layers above to rank replicas.

| Operation | Yields |
|---|---|
| members | each member's state and reachability |
| serving set | the **serving set**: every member, this node included, that is up and not confirmed unreachable, in node id order |
| in rotation | whether a node is in rotation (actor §2): it is out exactly when the view holds it draining or down |
| failure | why this node stopped applying committed transitions (§5.4.3), if it did |

- Every operation MUST be a synchronous read of local state that generates no traffic.
- A merely suspected member stays in the serving set, and a confirmed unreachable one stays in rotation, its actors still listed (actor §3.7).

### 3.3 Registry client

The registry client connects registry-based mode (§5.4.2) to an external registry, implemented by a deployment against its platform or virtual in a simulator. A plug point.

```rust
pub trait RegistryClient: Send + Sync + 'static {
    fn fetch(&self) -> BoxFuture<'static, Result<RegistrySnapshot, RegistryError>>;
    fn register(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>>;
    fn set_state(&self, node: NodeId, state: RegistryState) -> BoxFuture<'static, Result<u64, RegistryError>>;
    fn deregister(&self, node: NodeId) -> BoxFuture<'static, Result<u64, RegistryError>>;
}
```

- `fetch` MUST return one consistent read of the full desired member set, stamped with the registry's global revision, each entry carrying the revision that last changed it. Revisions MUST be monotonic and totally order the registry's states.
- Each mutation returns the revision it produced. `register` is idempotent and admits the node up; `set_state` sets the up or draining cordon; `deregister` removes the entry, whose absence finalizes down.
- An unreachable registry MUST answer `RegistryError::Unavailable`, never a fabricated snapshot.

### 3.4 Raft log store

A Raft log store persists one voter's state for one consensus group. A deployment supplies a factory yielding one per group and node, on stable storage (in memory under simulation). A plug point.

```rust
pub trait RaftWAL: Send + Sync + 'static {
    fn load(&self) -> PersistedRaft;
    fn save_term_and_vote(&self, term: u64, voted_for: Option<NodeId>) -> WalAck;
    fn append(&self, from_index: u64, entries: &[RaftEntry]) -> WalAck;
    fn save_snapshot(&self, index: u64, term: u64, data: &[u8]) -> WalAck;
    fn poisoned(&self) -> Option<String>;
}
```

- `WalAck::Persisted` MUST mean the write is on stable storage when the call returns: after any later crash, `load` returns a state that includes it. A voter announces what a write records (a vote, an append, a snapshot) only after `Persisted`.
- A write that did not reach stable storage MUST answer `WalAck::Failed`, never `Persisted`, and MUST NOT panic. After one `Failed` the store MUST refuse every later write, and `poisoned` MUST report the reason from then on.
- `load` MUST return the fold of every write acknowledged `Persisted`. `append` truncates at an absolute index and appends there; `save_snapshot` discards the log prefix the snapshot subsumes.
- The factory MUST be stable per group and node, returning the same durable state on every call, so a restarted voter reloads the term it voted in.
- Its at-rest formats are `actor.raft.log`, `actor.raft.term`, and `actor.raft.snapshot` (compatibility §4).

### 3.5 Consensus groups

Consensus beyond membership, a boundary, live in leader-based mode and inert in the others. Entity storage consumes it.

| Operation | Takes | Yields |
|---|---|---|
| create group | a group id, voters, learners | nothing |
| reconfigure group | a group id, voters | nothing |
| remove group | a group id | nothing |
| propose | a group id, a command | nothing; the commit is observed on the committed stream |
| subscribe | a group id | the group's committed stream |
| leadership | a group id | whether this node leads it, its leader, its current term |
| transfer leadership | a group id, a voter | whether the transfer started |
| configured voters | nothing | the founding voter set |
| current voters | nothing | the control group's current voter set |

- Each group MUST provide election safety, log matching, and leader completeness. The control group is created by the cluster itself.
- A group MUST deliver to each subscriber every entry committed after it subscribed, exactly once and in commit order, on one stream that also carries snapshot installs, so an install never reorders with later commands. There is no replay from an index: a subscriber that needs the whole log subscribes right after creating the group.
- A group's term never decreases on a node and has at most one leader, so a leader's term serves a layer above as a single-writer fencing token.
- A proposal goes to the leader; a non-leader forwards it and never applies it. The proposer learns of the commit from the committed stream and bounds its own wait.
- The configured voters are identical on every node and seed every new group. Learners replicate but never vote.
- Transferring leadership follows §5.4.3 item 8.

### 3.6 Authorization

An authorizer decides, per inbound envelope, whether its peer may deliver that manifest to that recipient. A deployment supplies the policy; a plug point.

```rust
pub trait Authorizer: Send + Sync + 'static {
    fn authorize(&self, peer: NodeId, recipient: &ActorId, manifest: &str) -> bool;
}
```

- A system MUST consult it before dispatch. A denied envelope MUST be refused as `System` and never decoded into the actor's message type; the refusal is recorded like any other (actor §7).
- With no authorizer, every envelope that clears the handshake is admitted.

## 4. Associations

1. **Handshake.** Before actor traffic, two nodes MUST complete a handshake establishing an association: a wire-version check, a node-identity exchange, codec agreement (actor §3.4), and optional authentication (§11). It precedes the frame vocabulary and is encoded independently of the codec under negotiation.
2. **Version first.** A peer at a wire version other than this node's MUST be refused, naming both versions (compatibility V2), before its codec, secret, or identity is examined: a version skew MUST NOT be reported as an authentication failure.
3. **Vocabulary.** Every node of a cluster MUST speak the frame vocabulary of §9.1 at one wire version, the format `actor.wire` (compatibility §4); the version travels in the handshake, so no frame carries a stamp.
4. **Multiplexing.** Many conversations share one association; each request carries a correlation that its response references.
5. **Framing.** A malformed frame MUST tear down the association, not the node. A transport that encodes frames onto a byte stream MUST refuse a frame longer than a bound every node of the cluster shares, before allocating for it.
6. **No liveness decisions.** The detector probes independently of association state (§6). A transport MAY surface association loss as a hint, which MUST feed only the detector's refutable suspect state, never down.

## 5. Membership

States, merge rule, and lifecycle are common to every cluster; the mode decides the member set (§5.4).

### 5.1 Member states

```text
joining → up ──→ leaving ──→ down ──→ removed     (the monotonic ladder)
           ⇅        ▲           ▲
        draining ───┴───────────┘                 (reversible cordon, off the ladder)

reachability: reachable ⇄ unreachable             (orthogonal to every state above)
```

- **joining:** handshake complete, awaiting an in-cluster admission (leader-based and gossip-based modes).
- **up:** a full member, hosting and addressing actors.
- **draining** (registry-based and leader-based modes): a reversible maintenance cordon. The node stays a full member, discovery routes new work away from it (actor §3.7), and resuming returns it to up. Off the ladder, its transitions are ordered by authority stamp, not rank (§5.2).
- **leaving:** graceful shutdown has begun; the node drains its mailboxes.
- **down:** declared dead by the mode's down authority, or reached by completing a leave. Terminal: a node that was down MUST NOT rejoin under the same incarnation; it MUST rejoin as a new member with a new node id, discarding any identity its mode persists across ordinary restarts.
- **removed:** a tombstone, eventually pruned.

**Reachability** is the detector's (§6): a member is first suspect, then confirmed unreachable, both reversible by a higher incarnation. Down is the separate, terminal decision of the down authority.

### 5.2 Views and the merge rule

Views merge entry by entry, by the same rule in every mode:

1. **Authority stamp wins.** An entry with a higher authority stamp replaces one with a lower or absent stamp; a mode's stamps come from a single logical writer and totally order its decisions.
2. **Otherwise, the ladder.** At equal stamp, the higher state on the ladder `joining < up < leaving < down < removed` wins; incarnation plays no part on this axis.
3. **Down is absorbing.** No merge result, stamped or not, returns a down member to a live state under the same incarnation.

Reachability is outside this merge: the detector orders suspicion and refutation by incarnation, the higher incarnation winning and, within one, the more severe claim (§6). Static and gossip-based modes issue no stamps.

### 5.3 Joining and leaving

A node starts as a founding member or a joiner. A joiner handshakes an existing member (a configured seed, or one learned from the registry); a leaver announces leaving and drains; a detector, where one runs, confirms a crash unreachable. Each transition that changes the member set needs the authority its mode names (§5.4.1 to §5.4.4). Announcing leaving decides nothing: it starts the drain and advises peers.

### 5.4 Control-plane modes

The mode is chosen at node startup, and every node in a cluster MUST run the same one.

| | **Static** | **Registry-based** | **Leader-based** | **Gossip-based** |
|---|---|---|---|---|
| Authoritative set | local configuration | an external registry | a replicated Raft log | none; the converged view |
| Dissemination | none | each node syncs the registry | log replication, plus stamped gossip | infection-style gossip |
| Detector (§6) | none, or observe-only | observe-only | a sensor feeding the leader | full |
| Declares down | nobody | the operator, through the registry | the leader, by committed entry | the coordinator, by policy |
| Draining | no | yes | yes | no |

Where a mode has an in-cluster down authority, the downing policy decides unreachable to down; the default MUST be conservative.

#### 5.4.1 Static

Membership is fixed at deployment: every node reads the full topology from configuration, all members start up, and a topology change is a redeploy. By default no detector runs and no membership traffic flows.

- Node identity MUST be stable across restarts and derivable from configuration.
- A node that stops answering remains a member: sends to it fail with `Unreachable` or `Timeout`, no termination signal is produced for its actors, and the node-down consequences never run.
- A deployment MAY enable the detector observe-only, gaining suspect and unreachable events and nothing else: an ask to an unreachable peer runs to its deadline, and the receptionist keeps listing that peer's actors.

#### 5.4.2 Registry-based

The member set lives in an external registry (a platform API, a database, a coordination service) the cluster reads but does not operate. An operator or platform mutates it; every member syncs its state into the local view, through the registry client only (§3.3).

1. **Revisions.** The registry MUST expose a monotonic revision that totally orders its states; each applied entry carries it as its authority stamp (§5.2), so members converge whatever their sync order. Members MAY also gossip stamped entries, as an accelerant, never a second authority.
2. **Join:** a node is admitted by appearing in the registry (registered by the platform or by itself); members mark it up at that revision. Joining is unused.
3. **Leave:** whoever registered the node removes its entry, and the removal's revision finalizes down and runs the node-down consequences (§7).
4. **Down:** only a registry mutation declares down. A node that stops answering becomes unreachable, reversibly, and stays a member until its entry is removed.
5. **Drain and resume:** the cordon is a registry state. A draining or unreachable member keeps its membership across a restart, so node identity MUST be stable across restarts.
6. **Registry unavailability** pauses membership changes only; the data plane runs on the last synced view. A node MUST NOT treat its own inability to reach the registry as evidence about peer liveness.

#### 5.4.3 Leader-based

The member set is a replicated log: an elected leader serializes every change and a quorum of voters commits it. The control group MUST provide Raft's observable guarantees (election safety, log matching, leader completeness).

1. **Single writer, total order.** Every membership transition (admission, drain and resume, a leave committed at the departing node's request, anything to down) MUST be a log entry committed through the current leader. The commit index is the authority stamp: members apply committed entries in log order. A command offered to a non-leader is redirected or rejected, never applied.

   **Undecodable entries:** a committed entry a member cannot decode MUST stop that member's control plane, never be skipped, so its view stays a prefix of the log. It MUST stop applying transitions, permanently, and MUST NOT proceed as though it had applied one. The payload carries its own version (`actor.membership`, compatibility §4), telling another build's transition apart from damage. The stopped member SHOULD keep replicating and gossiping, SHOULD hand off leadership if it leads (item 8), and SHOULD expose its condition through the membership view (§3.2).
2. **Voters.** A configured subset of members are voters; only they elect leaders and form quorums. The founding voter set MUST be identical on every node. Voters MUST persist their Raft state (§3.4) and MUST keep stable identities across restarts. Voter-set changes are committed configuration entries. A voter declared down MUST be removed from the voter set by a committed change.

   **Failed persistence:** a voter whose store reports a failed write (§3.4) MUST stop voting, permanently: it steps down, grants no vote, starts no election, acknowledges no append, and MUST NOT resume on a later success. It SHOULD stay reachable and refuse explicitly, SHOULD expose its condition, and SHOULD NOT be expressed as a panic unless the panic halts the whole node.
3. **Dissemination.** Voters learn state from log replication. Non-voting members MAY replicate the log as learners or receive committed state by gossip stamped with the commit index; by the merge rule both converge.
4. **Downing.** The detector feeds the leader: on a confirmed unreachable, the leader MAY commit down under the downing policy. Downing is thus quorum-gated, and a minority partition never evicts the majority. Consensus heartbeats are internal to the group, not member reachability.
5. **Quorum loss** pauses the control plane only; members keep exchanging actor traffic on the last committed view.
6. **Beyond membership.** Further groups serve other control-plane decisions through §3.5. This specification defines only the membership entries.
7. **Transport.** Consensus traffic rides the ordinary associations, in the consensus frames of §9.1.
8. **Leadership transfer.** A group leader SHOULD be able to hand leadership to a named voter without waiting out an election timeout. It MUST offer the transfer only to a voter that has replicated its last entry, the invitation MUST be accepted only from the leader of the recipient's current term, and the transfer starts an ordinary election, so it can move leadership but never split it.

#### 5.4.4 Gossip-based

Membership spreads peer to peer through the detector's probes and anti-entropy gossip, with no election, quorum, or external authority; views converge eventually, at constant per-node load.

1. **Dissemination.** The detector's probes are the one channel: each carries a membership digest (§6 item 6) to a target drawn from the entropy source, merged by §5.2.
2. **Refutation.** A node that sees a stale suspicion or state about itself increments its incarnation and gossips the override (§6).
3. **Coordinator.** Transitions that need a single actor (admitting joining to up, finalizing leaving to down) fall to the **coordinator**: the member with the lowest node id among those reachable and neither terminal nor leaving, in each node's own view. Joining members are eligible, the deriving node included, so a fresh cluster can bootstrap. The coordinator acts only on a locally stable, fully reachable view, except when downing an unreachable member (item 4). Two transient coordinators cannot conflict: their decisions merge on the ladder.
4. **Downing.** The detector moves a member from suspect to unreachable; the downing policy, applied by the coordinator, decides unreachable to down. The default policy MUST NOT down a member across a partition.
5. **Join:** a new node handshakes a configured seed and enters joining; the coordinator admits it once views converge.

## 6. Failure detection

Every mode but static runs the detector on each node; static MAY enable it (§5.4.1). It maintains reachability (§5.1) identically wherever it runs; only its authority over down differs by mode (§5.4).

1. **Direct probing.** Every `T_probe`, a node probes one member, chosen through the entropy source, with a `Ping`; a probed node MUST answer with an `Ack` carrying its incarnation.
2. **Indirect probing.** On a missed `Ack` within `T_rtt`, the prober SHOULD ask `k` other members to probe the target (`PingReq`); a helper that hears the target's `Ack` MUST relay an `IndirectAck` carrying the target's incarnation.
3. **Suspicion.** Only when the direct and the indirect probes both fail does the prober mark the target suspect, at its incarnation.
4. **Refutation.** A node that sees itself suspected MUST increment its incarnation and disseminate the override, clearing the suspicion cluster-wide.
5. **Confirmation.** A suspicion unrefuted for `T_suspect` becomes unreachable; the mode's down authority then MAY move the member to down.
6. **Piggybacking.** Membership and suspicion updates SHOULD ride on the probes.

`T_probe`, `T_rtt`, `k`, and `T_suspect` MUST be configurable; `T_suspect` SHOULD grow with cluster size.

## 7. Node down

Declaring a node N down has six consequences:

1. **Decision** by the mode's down authority (§5.4), usually after the detector confirms N unreachable (§6). Static mode has no down authority, so the consequences never run there.
2. **Membership:** the transition propagates through the mode's channel, and down is terminal (§5.1).
3. **In-flight callers:** every pending ask to an actor on N completes with `Unreachable` (actor §8.1).
4. **Watchers:** each node produces a node-down termination signal for every locally watched actor on N, when its own view marks N down (actor §10).
5. **Receptionist:** those signals prune the registrations of actors hosted on N (actor §3.7).
6. **Routing afterward:** resolving an id on N still yields a remote reference, and sends to it fail with `Unreachable` rather than waiting.

A graceful leave (§5.3) reaches the same state and consequences 3 to 6 through leaving instead of suspicion; an operator decommission declares down directly. A drain is none of this: it is reversible and the node stays a member.

## 8. Repair under loss

**Death watch between nodes:** `Watch` registers a remote watcher's interest and `Unwatch` withdraws it; the target's node sends one `Terminated` per remote watcher, carrying the reason, which the watcher's node MUST deliver only to the watcher it addresses (actor §10). Either frame can be lost, so a node running the detector MUST re-send `Watch` for each remote watch it holds, within bounded logical time (one anti-entropy interval per peer, peers in turn), while the watch is outstanding. The target's node MUST answer a repeated `Watch` idempotently: it registers the watcher once, and once the target is gone it answers with `Terminated` carrying the recorded reason. The watcher's node MUST drop a `Terminated` for a watch it no longer holds.

**Receptionist replication:** a node sends each registration to every other node as `Receptionist` and, each anti-entropy interval, its whole registry as `ReceptionistSync`. Anti-entropy repairs a lost registration; the death-watch repair repairs a lost prune, which rides on a termination signal (actor §3.7).

A node that runs no detector runs no anti-entropy, so neither repair runs there (§1.1).

## 9. Registries

### 9.1 Frames

| Family | Frames | Carries |
|---|---|---|
| Actor traffic | `Envelope`, `Reply` | an envelope (recipient, manifest, correlation for an ask, payload); a reply (correlation, then the encoded reply or a call error) |
| Detection | `Ping`, `Ack`, `PingReq`, `IndirectAck` | probes, each with the sender's incarnation and a membership digest (§6) |
| Death watch | `Watch`, `Unwatch`, `Terminated` | a remote watcher's interest, and its withdrawal; a termination addressed to one watcher (§8) |
| Discovery | `Receptionist`, `ReceptionistSync` | one registration; a whole registry, for anti-entropy (§8) |
| Consensus | `Raft`, `RaftHeartbeats`, `RaftHeartbeatReplies`, `RaftPropose` | one group's protocol message; empty appends, and their replies, batched per peer; a command for a leader (§3.5) |

- `RaftHeartbeats` is framing only: a receiver MUST hand each beat to its group as the lone empty append it stands for.
- The handshake is not a frame (§4). Membership has no frame: each mode disseminates through a channel it already has (§5.4).

### 9.2 Formats

The cluster owns `actor.wire` (§4), `actor.membership` (§5.4.3), and `actor.raft.log`, `actor.raft.term`, and `actor.raft.snapshot` (§3.4). Their versions and stamps are registered in compatibility §4.

## 10. Invariants defined here

The actor catalogue (actor §13) holds these rows, defined in this specification: #2 in part (§5.4), #14 (§5.2, §5.4), #15 (§5.1), #16 (§5.4), #17 (§6), and #22 (§5.4.3).

## 11. Security considerations

1. **Transport security.** Associations SHOULD support mutual TLS, and node identity SHOULD bind to a verified certificate.
2. **Authentication.** The handshake SHOULD authenticate the peer and MAY enforce an allowlist of node identities. A **cluster secret** checked in the handshake prevents accidental association across clusters; it travels in the handshake, so on an unencrypted association anyone who sees one handshake can join. It guards against accident, not an on-path adversary.
3. **Encryption is a stated choice.** A deployment's configuration MUST make a plaintext association an explicit choice, never the result of omitting TLS material. Plaintext suits loopback, tests, and a network the operator controls end to end.
4. **Authorization.** A system MAY gate delivery per peer, recipient, and manifest through an authorizer (§3.6).
5. **Hostile frames.** Version refusal precedes authentication, and a malformed or oversized frame tears down its association, not the node (§4).
