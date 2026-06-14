# Cloudflare Durable Objects: properties, and how they replicate without a global consensus bottleneck

**Status:** Research note
**Purpose:** Understand Durable Objects (DO) as a building block that fuses compute with durable, replicated storage behind a single global address, and extract the lessons that apply to grains-on-actors in this repo (the actor framework in `docs/distributed-actor-spec.md` and the replicated-journal idea).

Sources are listed at the end. Where a claim is inferred rather than stated by Cloudflare, it is marked **(inferred)**.

---

## 1. What a Durable Object is

A Durable Object is a special Cloudflare Worker that combines **one unit of single-threaded compute** with **one unit of strongly-consistent, durable storage**, addressable by a **globally-unique name**. The platform guarantees that at most one live instance exists for that name across the entire network at any time, and routes every request for the name to that one instance. Cloudflare frames DO explicitly through the Actor model and names Orleans, Akka, and Erlang as relatives.

This is the same shape as an actor in `distributed-actor-spec.md`, plus: virtual (name-based) identity, colocated durable storage, and a durability barrier on the reply path. The rest of this note is about *how the storage half works without becoming a scaling bottleneck*, because that is the part the actor spec does not yet have and the part most at risk of a naive "one big Raft log" design.

---

## 2. Key properties

1. **Globally-unique single instance per name.** A `DurableObjectId` (random, or derived from a string) names the object. At most one live activation exists network-wide. This is the single point of coordination, with no lock service or leader election in the application.
2. **Implicit lifecycle.** Created on first access, migrated among healthy servers by the platform, evicted when idle. The application never provisions or destroys one. This is Orleans's *virtual actor* / activation model.
3. **Storage colocated with compute, private to the instance.** Each object has its own SQLite database on the local disk of the machine where it runs (up to 10 GB), reachable only from inside that object. Reads and writes are effectively local — "zero-latency."
4. **Single-threaded, cooperatively scheduled.** One event at a time, browser-style. `&mut self`-equivalent safety by construction; no in-object data races.
5. **Input/output gates** give the appearance of atomic, synchronously-durable execution over an asynchronous, write-coalescing storage engine (Section 3).
6. **Durability by per-object quorum replication of the WAL** (Section 4) — the heart of this note.
7. **Wake-ups beyond requests:** durable **Alarms** (`storage.setAlarm()` → `alarm()` handler) for scheduled work with no client connected.
8. **Hibernation:** evicts from memory when idle; in-memory state is lost, persisted storage survives, rehydrates on next request. **Hibernatable WebSockets** keep many connections open without billing for idle time.
9. **RPC + fetch** as the two entrypoints — the analogue of `ask`/`tell` over an `ActorRef`.

---

## 3. The consistency model: input and output gates

This is the idea most worth lifting wholesale, because it makes "naive sequential code that is nonetheless race-free and durable" possible at almost no cost on top of a serial executor.

- **Input gate.** While a storage operation is in flight, the runtime delivers **no new events** to the object (no new request, no RPC). They queue until the object is no longer executing code and has no outstanding storage operations. This closes the race window that a plain serial executor leaves open *at `await` points*: a second request cannot observe half-applied state mid-handler.
- **Output gate.** When the object writes, outgoing messages and the HTTP response are **held back until the write is confirmed durable**. If the write fails, the response is replaced with an error and the object restarts, so no observer is ever told "stored" for data that was not stored. Writes therefore *look* synchronous while actually committing asynchronously and batched.
- **Write coalescing.** Because the gate lets writes batch, many logical writes fold into few physical commits.
- `blockConcurrencyWhile()` is the escape hatch to extend the gate across a multi-step critical section (for example, initialization-before-first-request).

Net effect: the developer writes ordinary sequential code; the gates supply atomicity-on-the-outside and durability-before-effects.

---

## 4. Durability and replication — why it is **not** a Raft bottleneck

This section answers the core concern: if every write needs a quorum, why doesn't the consensus layer become a global throughput ceiling the way a single Raft cluster would?

### 4.1 The naive worry

A single Raft (or Paxos) group serializes *all* writes through *one* elected leader and *one* log. Throughput is capped by that one leader's round-trips, and every participant pays election and heartbeat overhead. Put all your state on one Raft group and the group is your bottleneck. This is the right thing to be worried about.

### 4.2 What DO actually does: consensus sharded to one object

DO never builds one global log. The unit of replication is **one object**. The storage engine (the **Storage Relay Service**, SRS — the same engine that backs D1) works like this per object:

1. SQLite runs in **WAL mode**. Every write appends *frames* to a write-ahead log. A frame is just "write these bytes at this offset in the database file." The WAL is an already-totally-ordered sequence — ordering is decided by the single writer, before replication.
2. As frames are appended, an **SRS leader synchronously replicates them to 5 durability followers** on servers in different nearby datacenters.
3. When a **quorum (3 of 5)** of followers acknowledge they have safely stored the frames, the leader lets SQLite's write **commit**. The output gate (Section 3) holds the client response until this point.
4. Separately and asynchronously, the WAL is **streamed to object storage**, batched every **16 MB or 10 seconds**, whichever first. That archive gives **point-in-time recovery for up to 30 days** by replaying transactions.

Every object has **its own** leader and **its own** set of followers **(inferred** from "each Durable Object constantly streams its own WAL" and the single-owner model**)**. Object Y's writes never touch object X's quorum. There are millions of these tiny, independent replication domains. Aggregate write throughput scales horizontally with the number of objects; there is no shared serialization point. The bottleneck of "one global log" simply does not exist, because there is no global log — there are millions of per-object logs.

### 4.3 The deeper move: separate *ordering* from *durability*

Classic Raft does two jobs at once: it **elects a leader and agrees on an order** for entries, *and* it **replicates them durably**. DO splits these:

- **Ordering is free.** Because the object has exactly one live activation that is single-threaded (Sections 2, 4.4), there is exactly one writer. The WAL it produces is already a total order. No agreement-on-order round is needed — nobody else is proposing entries to disagree about.
- **Durability is a quorum append.** What remains is "is this already-ordered frame stored on enough machines yet?" That is a single round-trip, write-once quorum append (leader → 5 followers, wait for 3). It is the *log-replication* half of Raft without the *leader-election-per-write* half, and without multi-round agreement, because there is nothing to agree about except "stored: yes/no."

So the steady-state write path is one fan-out to nearby datacenters and a wait for a 3-of-5 ack — not a consensus protocol's multiple message rounds. This is closer to **primary-backup with quorum acknowledgment** (or Kafka's ISR model, or chain replication) than to multi-Paxos on the hot path.

### 4.4 Single-instance is a *routing/ownership* problem, off the write path

The "only one live instance" guarantee is not enforced by per-write consensus. It is a **placement and routing** decision: each object ID is owned by exactly one machine, and Cloudflare's routing layer directs every request for that ID to that machine. The lease/ownership lookup happens off the hot path; once you are talking to the owner, ordering is trivially serial because there is one owner.

This is the clean separation:
- **Who owns the object** (single-instance) — a control-plane lease/placement decision, rare, off the write path.
- **In what order operations apply** — free, because the one owner is single-threaded.
- **Whether a write is durable** — a per-object quorum append, the only thing on the write hot path that touches other machines.

A leader election *does* happen for an object — but only on **migration, eviction, region failover, or deploy**, not per write. The "leader" of the durability group is pinned to the object's current activation, so elections are infrequent reconfiguration events, not a per-request cost.

### 4.5 The takeaway for this repo

The lesson for the replicated-journal / "Raft journal groups" idea is exactly this decomposition:

- **Do not put all grains on one Raft group.** Use **one journal group per grain** (or per small shard of grains). The memory note "replicated Journal (Raft journal groups)" is already plural — keep it that way and resist any temptation to collapse to a single cluster-wide log.
- **Within a single-activation grain you do not need consensus to order writes** — only to make them durable. The grain's single activation is the stable leader of its own journal; ordering comes free from the serial executor (`docs/distributed-actor-spec.md` §6). You only need the *log-replication* half: append the already-ordered journal entry to a quorum.
- **Keep placement off the write path.** Single-activation is a control-plane lease (your leader-based or registry-based modes, §9.4.2/§9.4.3), consulted on activation and migration, not on every message. Leader election for a journal group should be a rare reconfiguration, triggered by failure/migration, never by ordinary traffic.
- **A durability barrier on the reply, not a consensus round.** Map the output gate onto `ReplyHandle::send` (§4.5): hold the reply until the journal entry is quorum-acked. The handler stays naive and sequential.

In short: Raft is a bottleneck only when it is one big shared log doing ordering-and-durability for everything. DO avoids both halves of that — it shards to per-object groups, and it deletes the ordering-agreement cost by having a single writer. Adopt both moves and the journal stops being a global chokepoint.

---

## 5. Lifecycle: activation, migration, hibernation, alarms

- **Activation.** The identity always exists conceptually; the runtime materializes the object on first access near the caller.
- **Migration / eviction.** The platform may evict or move an object at any time — for capacity, region failover, or deploy. From the object's view this is "a long pause between cycles"; it rehydrates from durable storage. **In-memory state is not preserved** across eviction or crash, so in-memory state is a cache, never the source of truth.
- **Hibernation.** After idle time the object leaves memory; persisted storage survives. Hibernatable WebSockets park connections so the object can sleep without dropping them or being billed for idle time, re-delivering messages via callbacks (`webSocketMessage`, `webSocketClose`, `webSocketError`).
- **Alarms.** `storage.setAlarm()` durably schedules a future `alarm()` invocation with no client connected — the basis for queues, retries, timeouts, and batch flushes. This maps onto the harness's ticket/waiter and perpetual-loop patterns.

---

## 6. The two example applications

**Artifacts ("git for agents").** One Git repository = one Durable Object. The single-instance property gives each repo a consistent serialization point, which matters when many agents fork and push concurrently. A Git protocol engine in Zig compiled to WASM (~100 KB) runs *inside* the object; Git objects are stored in the object's SQLite, chunked across rows because of the 2 MB max row size; R2 holds snapshots and KV holds auth tokens. Pattern: **"the object is the repo"** — coordination, storage, and protocol logic colocated.

**KV-from-Durable-Objects.** A DO holds a binding to Workers KV (or R2, D1, another DO) and calls out to it. The DO is the strongly-consistent coordinator/cache at one key; KV is the cheap, eventually-consistent global store for cold or bulk data. Pattern: **strong consistency at one object, fronting an eventually-consistent global store.**

---

## 7. Mapping to the actor framework + grain idea

`docs/distributed-actor-spec.md` already supplies the actor half and most of the hard distributed-systems machinery (membership, failure detection, supervision, the replicated journal). A "grain on top of actors" adds three things, all small given what exists:

| DO property | In the actor spec today | Gap to close for a grain |
|---|---|---|
| Single instance per name | `ActorId` = node + path + incarnation; explicit `spawn` | **Virtual activation**: address by stable name, activate on first message, single-activation lease. Lives in the leader-based control plane (§9.4.3 already reserves the log for "singleton placement"). |
| Colocated durable storage | actors are in-memory only | **A per-actor journal seam**: load-on-activate, persist-on-change. |
| Output gate (durable before reply) | replies are synchronous, no durability barrier | Hold the reply in `ReplyHandle::send` (§4.5) until the journal entry is quorum-acked. |
| Input gate (no reentrancy across awaits) | serial executor (§6) | Mostly present; extend gating to cover outstanding storage ops. |
| Quorum-replicated log | replicated Journal / Raft journal groups | Strongest existing piece. Keep it **per grain**, and use only the log-replication half (Section 4.3). |
| Alarms | none | Durable timers that re-activate the grain. |
| Hibernation | actor lives until stopped | Deactivate-on-idle, rehydrate-from-journal. Requires the journal seam first. |

Framing: **a grain is your actor plus a name-based virtual identity, a per-actor journal seam, and a durability barrier on the reply path.** Everything else you already have.

One placement caveat: a DO is a *singleton activation* (one live copy, platform-placed), which trades availability for the single-coordinator guarantee — during migration or partition the object can be briefly unreachable. The gossip-based mode (§9.4.4) chooses the opposite (AP). So "grain" belongs to the **leader-based** or **registry-based** modes, where authoritative placement exists, not the gossip mode.

---

## 8. Open questions / unconfirmed

- **Is the durability group literally Raft?** Cloudflare describes "leader + 5 followers, 3-of-5 quorum." That is Raft-*style* log replication, but the public material does not confirm the full Raft protocol (terms, elections) versus a primary-backup-with-quorum scheme. The leader is pinned to the object's activation, so elections are at most a migration-time event. Worth confirming before copying a mechanism.
- **How is the single-instance lease actually enforced and revoked on migration?** The routing/ownership layer is described only at a high level. The exact lease/fencing mechanism (how the old owner is fenced off before the new one accepts writes) is not public and is the subtle correctness point to get right in our own design.
- **Per-object vs shared follower pools.** "Each object has its own followers" is inferred from the single-owner streaming model; whether followers are dedicated per object or drawn from a shared pool per machine is not stated. Either way the *logical* replication domain is per object.

---

## Sources

- [What are Durable Objects?](https://developers.cloudflare.com/durable-objects/concepts/what-are-durable-objects/)
- [Zero-latency SQLite storage in every Durable Object](https://blog.cloudflare.com/sqlite-in-durable-objects/) (SRS, WAL, 5 followers / 3-of-5 quorum, 16 MB/10 s batching, 30-day PITR)
- [Durable Objects: Easy, Fast, Correct — Choose three](https://blog.cloudflare.com/durable-objects-easy-fast-correct-choose-three/) (input/output gates)
- [Rules of Durable Objects (best practices)](https://developers.cloudflare.com/durable-objects/best-practices/rules-of-durable-objects/)
- [Workers Durable Objects Beta: A New Approach to Stateful Serverless](https://blog.cloudflare.com/introducing-workers-durable-objects/) (single-instance, routing)
- [Durable Objects aren't just durable, they're fast: a 10x speedup for Cloudflare Queues](https://blog.cloudflare.com/how-we-built-cloudflare-queues/) (Coordinator pattern)
- [Artifacts: versioned storage that speaks Git](https://blog.cloudflare.com/artifacts-git-for-agents-beta/)
- [Use KV from Durable Objects](https://developers.cloudflare.com/durable-objects/examples/use-kv-from-durable-objects/)
- [Simon Willison: Zero-latency SQLite storage in every Durable Object](https://simonwillison.net/2024/Oct/13/zero-latency-sqlite-storage-in-every-durable-object/) and [HN discussion](https://news.ycombinator.com/item?id=41664795)
- [SQLite storage API](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/) · [Alarms](https://developers.cloudflare.com/durable-objects/api/alarms/) · [WebSockets / Hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/) · [Data location](https://developers.cloudflare.com/durable-objects/reference/data-location/)
</content>
</invoke>
