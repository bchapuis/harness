# Cluster Utilities: Specification

**Status:** Draft v1
**Scope:** Utilities layered on top of the core framework ([`distributed-actor-spec.md`](distributed-actor-spec.md)): deterministic placement, group routers, and the cluster singleton.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the core specification are cited as **core §N**; sections of this document as plain **§N**. Invariants defined here are numbered **U1, U2, …** to keep them apart from the core catalogue (core §18.5 #1–#22).

---

## 1. Scope and layering

Everything in this document is built **on top of** the core abstractions and modifies none of them:

- the **membership view** and its merge rule (core §9) supply the node sets every utility derives from;
- the **receptionist** (core §13) supplies replicated, eventually consistent service listings;
- the **event stream** (core §16) carries the utilities' observability events — they extend the single `Event` enum, which is extensible by design.

The core non-goals (core §1.2) hold unchanged. In particular: every utility is **data-plane** machinery and therefore **eventually consistent** — none of them places a quorum on any path or acquires a lease; and none of them masks failure — a utility that cannot route fails the call immediately rather than buffering or retrying (core §14.2).

A utility's behavior MUST be a deterministic function of the virtualized runtime (core §18.1, §18.2): membership views, seeded entropy, and the virtual clock. No utility may consult wall-clock time or unseeded randomness.

---

## 2. Placement (rendezvous hashing)

Placement answers one question deterministically on every node: *given a key, which member owns it?* It is the primitive the singleton (§4) anchors on and the planned sharding layer (§7) maps shards with.

### 2.1 The serving set

1. Placement candidates MUST be derived from the local membership view as the **serving set**: every member whose status is `up` *and* whose reachability is `reachable`, **including the local node** iff its own status is `up`. Members that are `joining`, `draining`, `leaving`, `down`, `removed`, or confirmed `unreachable` are excluded.

This is deliberately stricter than the receptionist's listing filter (core §13 req 4, which only MUST exclude `draining`/`down`): placement assigns *ownership*, so it routes around a confirmed-unreachable member rather than assigning keys to a node nobody can reach.

### 2.2 The placement function

2. `owner(set, key)` and `top(set, key, n)` MUST be **pure functions** of the candidate set and the key — no internal state, no clock, no entropy — so that two nodes holding identical serving sets compute identical owners for every key (core §18.1).
3. The weight of a member for a key is normatively `mix64(fnv1a64(tag ‖ key))`, where `tag` is the member's node uid as 8 little-endian bytes, `fnv1a64` is FNV-1a (64-bit, offset basis `0xcbf29ce484222325`, prime `0x100000001b3`), and `mix64` is the splitmix64 finalizer. The hash is fixed: it MUST NOT vary across platforms, framework versions, or process runs. (`std::hash` and other unstable hashers are therefore ruled out; the finalizer compensates for FNV-1a's weak avalanche on short inputs.)
4. `owner` is the member with the highest weight; a weight tie MUST resolve to the lowest `NodeId`. `top(n)` is the `n` distinct members in descending weight order, under the same tie rule; `top(1)` equals `owner`. An empty candidate set yields no owner.
5. **Minimal movement.** Removing one member from the set MUST reassign only the keys that member owned; adding one MUST move only the keys it now owns. (This follows from per-member independent weights and is what makes the function suitable for shard placement, §7.)

### 2.3 Honesty

6. Nodes with **divergent** views MAY place a key differently; view convergence (core §18.5 #14) restores agreement. Placement is a routing function, **not a lease**: it grants no exclusivity and no fencing. Consumers that need single-activation semantics get the singleton's guarantee (§4.3) — which carries the same convergence caveat.

---

## 3. Group routers

A group router spreads calls over the actors registered under one receptionist `Key` (core §13) — the cluster-utility counterpart of addressing one `ActorRef` directly.

1. A router is a **node-local, typed view** over a receptionist `Key`. It MUST NOT replicate state of its own or introduce wire frames; the receptionist's replication is the only cluster state involved.
2. Every routing decision MUST draw from the **current serving listing** (core §13 req 4) at decision time, so a routee on a node that goes `down` or `draining` stops being selected as soon as the local listing reflects it, and a `resume` restores it without re-registration.
3. Strategies:
   - **Round-robin** MUST cycle the listing in its deterministic order (the registry is an ordered set), so the cycle is reproducible under a fixed seed (core §18.1).
   - **Random** MUST draw from the system's seeded entropy (core §4.6, §18.1), never an ambient RNG.
   - **Rendezvous-hashed** routing selects by a **caller-supplied key**: routees are ranked by the §2 weight function over the routee's actor tag (owner-node tag ‖ path ‖ incarnation), ties to the lower `ActorId`. The same key over the same listing MUST select the same routee on every node.
4. A route over an **empty listing** MUST fail fast with `DeadLetter`; the router MUST NOT buffer, queue, or retry (core §1.2, §14.2).
5. Routing adds **no delivery guarantee** beyond the underlying `ask`/`tell` (core §7.2), and no failure handling beyond theirs: a stale listing entry MAY still route to an actor that just terminated, and the call then fails like any direct call would.

---

## 4. Cluster singleton

*Reserved — specified together with its implementation in a subsequent change.*

---

## 5. Events

Utility events extend the core `Event` enum (core §16). Placement (§2) and routers (§3) define none: both are pure or node-local functions whose per-decision events would flood the stream without enabling any check their conformance tests do not already perform.

---

## 6. Conformance

The utilities catalogue mirrors the core catalogue's structure (core §17, §18.5) and is machine-readable in `actor-simulation` (`utilities_catalogue()`), guarded by the same drift test that keeps the core catalogue honest.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| U1 | **Deterministic placement.** Rendezvous placement is a pure, version-stable function of the serving set and key: nodes with identical serving sets compute identical owners for every key, and a single-member change reassigns only the keys that member owned or now owns. | §2 | property + cluster tests (`conformance_placement.rs`); pinned known-answer hash vectors |

Group routers (§3) define no numbered invariant: "selects only from the current serving listing, fails fast when empty" is a local function property, not an emergent one — it is pinned directly by `conformance_router.rs`.

---

## 7. Future work

- **Cluster sharding.** Entity actors addressed by `(entity type, entity id)`, grouped into a fixed number of shards by a static hash; shards map to nodes via §2's `owner`/`top` — hash-based placement, no coordinator, uniform across all four control-plane modes (core §9.4). A leader-committed shard-allocation table (control-plane metadata in the Raft log, core §9.4.3) is a possible leader-mode upgrade enabling load-aware allocation and graceful rebalancing.
- **Distributed pub/sub.** Topic-based fan-out over per-node topic mediators discovered through the receptionist; at-most-once, matching core §7.2.
- **Leader-anchored singleton.** A singleton whose activation is a committed log entry in the leader-based mode, trading the §4 convergence caveat for a quorum-gated activation.
