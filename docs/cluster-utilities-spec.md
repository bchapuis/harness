# Cluster Utilities: Specification

The key words **MUST**, **MUST NOT**, **SHOULD**, and **MAY** carry the meanings defined in RFC 2119.

## 1. Scope

The cluster utilities are three helpers layered on the actor framework: deterministic placement, which answers identically on every node which member owns a key; group routers, which spread calls over the actors registered under one receptionist key; and the cluster singleton, which keeps one instance of an actor running cluster-wide. They build on the membership view (cluster §3.2), receptionist (actor §3.7), and event stream (actor §3.6) of the [actor framework](distributed-actor-spec.md), and change no core semantics. Entity storage builds on placement to rank each shard's replicas (storage §7).

Non-goals:

- **Quorums and leases.** Every utility is eventually consistent: none takes a quorum or a lease, or grants exclusivity.
- **Masking failure.** A utility that cannot route fails the call at once, never buffering or retrying (actor §8.2).
- **Sharding.** Addressing entities by entity type and id belongs to entity storage (storage §4.1), not to a utility.

Conformance classes:

- *Placement* conforms by meeting §4.1.
- A *router* conforms by meeting §3.1.
- A *singleton*, its managers and proxies together, conforms by meeting §3.2, §4.2, and §5.

Every class MUST also meet the deterministic profile (actor §11).

## 2. Model

- **Serving set:** the members of a node's local membership view that are `up` and not confirmed `unreachable`, the local node included iff it is `up` (cluster §3.2).
- **Key:** a byte string naming what is placed or routed.
- **Tag:** the byte string a candidate is weighed by. A member's **node tag** is its node uid as 8 little-endian bytes. An actor's **actor tag** is its node's tag, then its path as UTF-8 bytes, then its incarnation as 8 little-endian bytes (actor §3.2).
- **Weight:** the 64-bit score of a tag for a key (§4.1).
- **Owner:** the member a key is placed on among a set of members; the **top n** are the n members ranked first for it (§4.1).
- **Router:** a node-local view over the actors registered under one receptionist key (actor §3.7), its **routees**, that sends each call to one of them. It selects by its **strategy**, **round-robin** or **random**, or by **keyed selection** against a key the caller supplies.
- **Singleton:** one logical instance of an actor cluster-wide, identified by a **name**.
- **Manager:** what a hosting node runs for a singleton's name. It starts and stops the node's **activations** of the singleton, each live until its actor terminates.
- **Anchor:** the owner of the singleton's name, as UTF-8 bytes, among a node's serving set. It depends on the local view alone, in every control-plane mode (cluster §5.4).
- **Stop message:** the application message a manager delivers to its activation to **hand off**.
- **Proxy:** a handle that sends calls to the singleton's current instance, wherever it runs.

## 3. Seams

### 3.1 Group routers

A boundary between a caller and a router. A router runs over any actor system, clustered or not (actor §3.3).

| Operation | Takes | Yields |
|---|---|---|
| create | a receptionist key, a strategy | a router over the key |
| routees | | the key's current in-rotation listing |
| route | a key, optionally | the routee keyed selection picks for the key, else the one the strategy selects; none on an empty listing |
| ask, tell | a message, and optionally a key | the call, sent to the routee route picks: a reply (ask) or nothing (tell), or a call error (actor §8.2) |

1. A router MUST NOT replicate state of its own or add frames to the wire protocol.
2. Every selection MUST draw from the key's in-rotation listing at the time of the call (actor §3.7): a routee on a node that goes `down` or `draining` stops being selected once the local listing omits it, and is selected again once the node resumes (cluster §5.1).
3. Round-robin MUST cycle through the listing in its order (actor §3.7). Random MUST draw from the actor system's entropy (actor §3.5).
4. Keyed selection ignores the strategy: it picks the routee whose actor tag weighs most (§4.1) for the caller's key, a tie resolving to the lower actor id (ordered by node uid, then path, then incarnation). The same key over the same listing MUST select the same routee on every node.
5. A call over an empty listing MUST fail at once with `DeadLetter` (actor §8.2); a router MUST NOT buffer, queue, or retry.
6. Routing adds no delivery guarantee beyond a direct ask or tell (actor §8.1): a stale entry MAY route to an actor that just terminated, and the call fails as a direct call would.

### 3.2 Cluster singleton

A boundary between a caller and a clustered actor system, the only kind that offers it (cluster §3.2).

| Operation | Takes | Yields |
|---|---|---|
| host | a name, a way to construct the actor, a stop message | a proxy; the node runs a manager for the name (§4.2) |
| proxy | a name | a proxy; the node runs no manager and never hosts |
| resolve (on a proxy) | | the instance as this node sees it, or none |
| ask, tell (on a proxy) | a message | the instance's reply (ask) or nothing (tell), or a call error (actor §8.2) |

1. The singleton's actor SHOULD stop promptly when it handles the stop message.
2. A proxy resolves the instance on every call from the in-rotation listing of the receptionist key named by the singleton's name (actor §3.7), following it across handoffs. A call during a handoff gap MUST fail at once, never buffer: `DeadLetter` on an empty listing, `Unreachable` or `Timeout` on a stale entry (actor §8.2).

## 4. Semantics

### 4.1 Placement

1. **Candidates:** placement candidates MUST be the local view's serving set; a member that is only suspected (cluster §6) MUST remain one.
2. **Purity:** the owner and the top n MUST be pure functions of the candidate set and the key, so nodes holding identical serving sets compute identical owners for every key.
3. **Weight:** the weight of a tag for a key is `mix64(fnv1a64(tag ‖ key))`, where `fnv1a64` is 64-bit FNV-1a (offset basis `0xcbf29ce484222325`, prime `0x100000001b3`) and `mix64` is the splitmix64 finalizer (in wrapping 64-bit arithmetic: `x ^= x >> 30; x *= 0xbf58476d1ce4e5b9; x ^= x >> 27; x *= 0x94d049bb133111eb; x ^= x >> 31`). The weight is an interop constant: it MUST NOT vary across nodes, platforms, implementation versions, or process runs.
4. **Owner and top n:** the owner of a key is the member whose node tag weighs most for it, a tie resolving to the lowest node id; an empty set has no owner. The top n are the n distinct members in descending weight order under the same tie rule (all of them when the set is smaller); the top 1 is the owner.
5. **Minimal movement:** removing one member MUST reassign only the keys it owned; adding one MUST move only the keys it now owns.
6. **No exclusivity:** nodes with divergent views MAY place a key differently until their views converge (membership convergence, actor §13). Placement is a routing function: it grants no exclusivity and no fencing.

### 4.2 Singleton activation and handoff

1. **Activation:** a manager evaluates the anchor periodically on the actor system's clock, against its local membership view only, and MUST activate the instance iff the local node is the anchor. On activation it MUST register the instance under the receptionist key named by the singleton's name (actor §3.7).
2. **Handoff:** when its view stops naming the local node anchor, the manager MUST hand off by delivering the stop message to the instance, and ends an instance by no other means. It MUST NOT activate a new local instance while a previous local activation is live.
3. **At most one per converged view:** the singleton guarantees at most one activation per converged view. While views diverge (a partition, detector lag), two nodes MAY run an instance concurrently; once views converge (membership convergence, actor §13), the surplus manager MUST stop its instance. The singleton is not a mutual-exclusion primitive: an application requiring exclusivity MUST fence or deduplicate by its own means.
4. **Liveness:** if the instance terminates while its node remains anchor, the manager MUST re-activate it within bounded logical time.
5. **Events:** the hosting node MUST emit `SingletonStarted` on each activation and `SingletonStopped` when it observes that activation terminated (§5).

## 5. Event kinds

A singleton's hosting node emits two event kinds of its own onto the actor system's event stream (actor §3.6), not application events. Placement and routers emit none.

| Kind | Carries | Emitted when |
|---|---|---|
| `SingletonStarted` | the name, the actor's id | the node's manager activates the actor as its instance of the name |
| `SingletonStopped` | the name, the actor's id | the node's manager observes that activation terminated: by handoff, by supervision, or by the actor's own stop |

Per node and name, the two strictly alternate.

## 6. Invariants

| # | Invariant | Defined in |
|---|---|---|
| U1 | **Deterministic placement.** Placement is a pure, version-stable function of the serving set and key: identical serving sets yield identical owners for every key, and a single-member change reassigns only the keys that member owned or now owns. | §4.1 |
| U2 | **Singleton activation discipline.** A node never runs two live activations of one name at once; once views converge on a healed cluster, exactly one activation per name is live cluster-wide; an anchor that loses its instance re-activates it within bounded logical time. | §4.2 |

## 7. Security considerations

The utilities add no frames and no trust boundary: routers and proxies call through ordinary references, so their calls are authenticated and authorized as any call is (actor §14). Placement and the singleton act on the local membership view and trust it, so whoever can change the view can move key ownership and the anchor; admission to the view is the actor framework's (actor §14). Neither placement nor the singleton grants exclusivity (§4.1, §4.2).
