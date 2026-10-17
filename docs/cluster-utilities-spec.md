# Cluster Utilities: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Invariants defined here are numbered **U1, U2, …** to keep them apart from the core catalogue (actor §10).

---

## 1. Scope

The cluster utilities are data-plane helpers layered on the [actor framework](distributed-actor-spec.md): deterministic placement (§2.1), group routers (§2.2), and the cluster singleton (§2.3), in `actor-cluster` beside `ClusterSystem`. They build on the membership view (actor §6), the receptionist (actor §2.6), and the event stream (actor §2.7), and change no core semantics. Entity ranks each shard's replicas with the placement function (storage §6).

A utility's behavior MUST be a deterministic function of the membership view, the system's seeded `Entropy`, and its `Clock` (actor §2.12.2), never of wall-clock time or unseeded randomness.

### 1.1 Non-goals

- **Quorums and leases.** Every utility is eventually consistent; none takes a quorum or a lease, or grants exclusivity.
- **Masking failure.** A utility that cannot route fails the call at once, never buffering or retrying (actor §8.2).
- **Sharding.** Entities addressed by `(entity type, entity id)` are entity's layer (storage §3.1), not a utility.

---

## 2. Seams

### 2.1 Placement

`placement::owner`, `placement::top`, and `placement::weight` answer, identically on every node, which member owns a key; `ClusterSystem::place` applies `owner` to the local serving set. The singleton, keyed routing, and entity consume them. The weight is an interop constant every node, version, and platform MUST compute identically.

1. **Serving set.** Placement candidates MUST be the local view's serving set (`Membership::serving_members`): every `up` member not confirmed `unreachable`, the local node included iff it is `up`. A merely `suspect` member MUST remain, since suspicion is refutable (actor §7) and excluding it would move ownership on every dropped probe. The set is stricter than the receptionist's rotation filter (actor §2.6 req 4) because a key owned by a node nobody can reach is unserved.
2. `owner(set, key)` and `top(set, key, n)` MUST be pure functions of the candidate set and the key, so nodes holding identical serving sets compute identical owners for every key.
3. The weight of a member for a key is normatively `mix64(fnv1a64(tag ‖ key))`, where `tag` is the member's node uid as 8 little-endian bytes (`placement::node_tag`), `fnv1a64` is FNV-1a (64-bit, offset basis `0xcbf29ce484222325`, prime `0x100000001b3`), and `mix64` is the splitmix64 finalizer. The hash MUST NOT vary across platforms, framework versions, or process runs, which rules out `std::hash`.
4. `owner` is the member with the highest weight, a tie resolving to the lowest `NodeId`. `top(n)` is the `n` distinct members in descending weight order under the same tie rule; `top(1)` equals `owner`. An empty set has no owner.
5. **Minimal movement.** Removing one member MUST reassign only the keys it owned; adding one MUST move only the keys it now owns.
6. **Honesty.** Nodes with divergent views MAY place a key differently until views converge (actor §10 #14). Placement is a routing function, not a lease: it grants no exclusivity and no fencing.

### 2.2 Group routers

A `Router<A>` is a node-local, typed view over the actors registered under one receptionist `Key` (actor §2.6). `Router::route`, `ask`, and `tell` select by the router's `RouteStrategy`; `Router::route_by`, `ask_by`, and `tell_by` select by a caller-supplied key.

1. A router MUST NOT replicate state of its own or introduce wire frames.
2. Every routing decision MUST draw from the current in-rotation listing (actor §2.6 req 4), so a routee on a node that goes `down` or `draining` stops being selected as soon as the local listing reflects it, and `resume` restores it.
3. **Strategies.** `RoundRobin` MUST cycle the listing in its deterministic order (actor §2.6), so the cycle is reproducible under a fixed seed; `Random` MUST draw from the system's seeded entropy. **Keyed** selection bypasses the strategy for one call: routees are ranked against the caller's key by the §2.1 weight over each routee's actor tag (owner-node tag ‖ path ‖ incarnation, `placement::actor_tag`), ties to the lower `ActorId`, so the same key over the same listing MUST select the same routee on every node.
4. A route over an empty listing MUST fail fast with `DeadLetter`; the router MUST NOT buffer, queue, or retry.
5. Routing adds no delivery guarantee beyond the underlying `ask` and `tell` (actor §4): a stale entry MAY route to an actor that just terminated, and the call fails as a direct call would.

### 2.3 Cluster singleton

`ClusterSystem::singleton(name, factory, stop)` runs a manager for one logical instance cluster-wide and returns a `SingletonProxy`; `ClusterSystem::singleton_proxy(name)` returns a proxy without hosting. They are inherent methods of `ClusterSystem`, not generic over `ActorSystem`, because the manager reads the serving set, which only a clustered system has.

1. Every node that may host the singleton runs a manager per name. The manager observes only its local membership view, periodically on the system clock, and MUST activate the instance iff its view's **anchor**, `owner(serving set, name)` (§2.1), is the local node; on activation it MUST register the instance under the singleton's receptionist key. The anchor is a pure function of the view in every control-plane mode (actor §6.4), so it needs no join order or coordinator.
2. When a node's view stops naming it anchor, its manager MUST hand off by delivering the configured stop message to the instance, whose handler SHOULD stop the actor promptly; there is no external kill. The manager MUST NOT activate a new local instance while a previous local activation is live (the per-node half of U2).
3. **Guarantee honesty (normative).** The singleton guarantees at most one activation per converged view. During divergence (a partition, detector lag) two nodes MAY run an instance concurrently; once views converge (actor §10 #14) the surplus manager MUST stop its instance. The singleton is not a mutual-exclusion primitive: an application requiring exclusivity MUST fence or deduplicate by its own means.
4. **Liveness.** If the instance terminates while its node remains anchor, the manager MUST re-activate it within bounded logical time.
5. A `SingletonProxy` resolves the instance from the in-rotation listing on every call, following it across handoffs. A call during a handoff gap MUST fail fast (`DeadLetter` on an empty listing; `Unreachable` or `Timeout` on a stale entry), never buffer.
6. The hosting node MUST emit `SingletonStarted` on each activation and `SingletonStopped` when it observes that activation terminated (§3).

---

## 3. Events

Placement and routers define no events. The singleton's manager emits two, on the hosting node: `SingletonStarted { name, actor }` when it activates `actor` as its node's instance of `name`, and `SingletonStopped { name, actor }` when it observes that activation terminated (handoff, supervision, or the actor's own stop). Per node, the two strictly alternate for one name. They are named variants of the core `Event` enum, not `Event::App` payloads (actor §2.7), because the core checker reads them by variant to enforce U2.

---

## 4. Invariants

The Rust copy of this table, with the suites that verify each row, is `actor-simulation`'s utilities catalogue.

| # | Invariant | Defined in |
|---|---|---|
| U1 | **Deterministic placement.** Placement is a pure, version-stable function of the serving set and key: identical serving sets yield identical owners for every key, and a single-member change reassigns only the keys that member owned or now owns. | §2.1 |
| U2 | **Singleton activation discipline.** A node never runs two live activations of one name at once; once views converge on a healed cluster, exactly one activation per name is live cluster-wide; an anchor that loses its instance re-activates it within bounded logical time. | §2.3 |

Routers define no numbered invariant: drawing from the current listing and failing fast on an empty one are local properties of a function, not emergent ones.

---

## 5. Deferred

- **Leader-anchored singleton.** No singleton commits its activation through the leader-based log, so the §2.3 item 3 convergence caveat holds in every mode.
- **Distributed pub/sub.** No topic fan-out over receptionist-discovered mediators exists.
