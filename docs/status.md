# Implementation Status

## 1. Scope

This page records where the implementation in this tree stands against the specifications: what a spec permits or requires that the code does not yet do, what is deliberately left out and at what cost, and any known defect, where the implementation violates a requirement. It is informative, in the manner of RFC 7942: the specifications state the contract, and none of them cites this page. One section per spec, one line per item; an item leaves when the code meets the spec or the spec changes. A **Defect:** label marks a violated MUST.

## 2. Actor model

- **Trace context and metrics:** envelopes carry no trace context and no metrics are exported (actor §3.6); requests are followed, and depths and latencies derived, only from the event stream.
- **Generated listings:** nothing generates a manifest or an actor's listing of accepted messages (actor §7); actors write them by hand.
- **Default deadline:** the default ask deadline is a fixed constant (actor §3.2); a caller that needs another bound asks within an explicit deadline.

## 3. Actor cluster

- **Certificate-bound node identity:** mutual TLS authenticates the cluster, not the node, so a holder of the cluster certificate can claim any node id the allowlist admits (cluster §11).

## 4. Cluster utilities

- **Leader-anchored singleton:** no singleton commits its activation through the leader-based log, so the convergence caveat of utilities §4.2 (two activations may run while views diverge) holds in every control-plane mode, the leader-based one included.
- **Distributed pub/sub:** no topic fan-out over receptionist-discovered mediators exists.

## 5. Entity

- **Defect:** a host whose decision or deactivation hook panics is stopped by actor supervision without running the deactivation hook and without emitting `Deactivated` (or decrementing the resident count), so its `Activated` goes unmatched while the node lives, violating entity §8 (pairing) and the deactivation hook's once-per-path rule of entity §3.1.

- **Linearizable reads:** not offered. The read path does not reconfirm leadership (entity §7.1), so a caller that needs a linearizable read issues a writing command. The planned route is a check-quorum lease on the shard's leader-election group, with the activation fencing itself when the lease lapses.
- **Follower reads:** only a shard's leader serves reads; no replica serves them under a freshness bound, so read capacity per shard is one node's.
- **Hibernatable connections:** a subscription does not survive hibernation (entity §7.3), so a follower re-subscribes and backfills after every idle eviction of the entity it follows.
- **One read path for followers:** a follower combines a subscription with event reads; no blocking event read exists that answers once the head passes a given sequence number, which would serve a follower with no sink at one request per batch.
- **The full actor context:** a decision's context exposes no child spawning and none of the rest of the actor handler context.
- **Per-entity authorization:** no authorizer is consulted per peer, entity name, and manifest; commands can be gated only by peer and manifest (entity §13).
- **Idle-eviction residency:** idle eviction applies no minimum residency and no jitter, so the SHOULD of entity §8 is unmet: an entity touched just slower than its idle window is evicted and reloaded on every touch, and a type with that access pattern needs a longer idle window.
- **Cross-entity sagas:** no layer composes commands across entities above the single-entity consistency boundary (entity §1.1); multi-entity work is hand-written over idempotent commands.
- **A derive for entity types:** none exists; every entity type writes its registration by hand (entity §1.1).

## 6. Entity storage

- **History below the base:** compaction drops an entity's records at or below its snapshot, facet 0's events included, so every event read carries a base (storage §9.2) and history below it is gone. Compacting only facet records would keep facet 0's events whole and remove the base from the read API, but recovery, transfer, migration, and the store's compaction rewrite would then all have to carry that history.
- **Range-verified blob reads:** a ranged blob get fetches and verifies the whole blob, then slices it (storage §10). A byte range is not verified on its own against the blob's BLAKE3 tree (Bao), so a ranged read costs a whole-blob transfer.
- **No cold blob tier:** an entity's blobs live only on its shard's replicas (storage §10). No external content-addressed object store serves as their durable home with the replicas reduced to read-through caches; the journal's blob operations (storage §3.1) are where one would plug in, with the records and the fence still quorum-replicated.

## 7. Entity facets

- **Third-party facets:** the facet seam has no implementations beyond the six built-in facets, and an entity type cannot supply its own; the tag range storage §12 reserves for facets defined outside the host is unallocated. Opening the seam waits on the tag registry and on cross-version rules for facet payloads.
- **A generic facet harness:** F1 to F4 are exercised per facet, not by one harness over every facet, and F3 has no dedicated test.
- **SQL lazy page hydration:** an activation materializes its whole SQL database before the first command; capturing changes below the database engine and faulting pages in from blobs, so a large database activates without materializing whole, is not done.
- **SQL bounds:** no cap on one transaction's captured pages and no maximum database size are enforced.
- **Workspace scaling:** a file larger than a record bound has no ranged sub-records, files are not faulted in on first access, and file times are not preserved across a guest round trip, so the capture scan's stat pruning does not survive one.
- **Disk lazy block hydration:** rehydration fetches every block the manifests name; blocks are not faulted in on first guest access.
- **Disk overlay dirty tracking:** a capture reads the whole image; a copy-on-write overlay per capture epoch whose extents are the dirty set (privileged and Linux-only) is not implemented.
- **Gating the alarm decision:** an entity type without the alarm facet can implement the alarm decision, which is then never called (entity §4.3).
- **A linear step DSL:** no `async` step, sleep, and retry surface exists over the memo.
- **Defect:** a workspace capture that fails part-way through its scan (a file read error after earlier changed files were found) returns the error but leaves the writes it already staged, which commit with the decision; plan-then-stage requires a failed operation to leave no trace (entity-facets §4.3).

## 8. Workflow

- **Alarm-driven ticks:** no tick is fired by the alarm facet (facets §5.5), so an entity cannot hold work for later without an activation, and a key at rest waits for a caller's message to wake it (workflow §4.4, §7).
- **Step DSL:** there is no linear `async` step, sleep, and retry surface (§5) written over the cycle; a consumer writes the re-entrant form.
- **Effect priority and concurrency bounds:** the desired set launches in full, with no priority and no bound on effects in flight; a consumer with one scarce resource bounds its own effects, and the rule waits for a second such consumer to shape it.
- **Cross-entity effects:** an effect aimed at another entity is an ordinary effect; whether to deduplicate such effects by entity name or to carry them on the entity runtime's sagas (§5) is open.
- **Post-commit emission for consumers:** a consumer stages its own events owed after a commit, separately from the engine's staging of `Settled` events (workflow §10); one staging could serve both.

## 9. Agent

- **Durable alarms for sessions:** nothing reactivates a session without a caller, so resumption is caller-driven (agent §7.3); the entity half exists (facets §5.5).
- **Scheduler singleton:** there are no queued or recurring runs; a scheduler waits on durable alarms.
- **Linearizable reads:** a minority-side leader can serve a stale tail page (agent §10.2) until the entity's leader lease lands (§5).
- **Warm pools and a network dataplane:** every sandbox open pays full provisioning, and no provider offers `Network` (§10).
- **Multi-tenant scheduling:** there are no quotas and no fair share across tenants.
- **Context compaction:** an over-long transcript fails the run (agent §3.1); a checkpoint record that replaces earlier transcript would have to preserve A1.
- **Token streaming:** a model call is one request and one response, so a client sees a step only once it commits.
- **Permission gating:** there is no per-call authorization hook; the tier cap is the only gate, and it is static.
- **Loop-executing tools:** `delegate` is the only tool that runs in the loop rather than the sandbox.
- **Code mode:** the toolset is not an API the model programs against at the `Compute` tier, so every call costs a loop round trip.
- **External trace interop:** root and parent links (agent §10.3) are not mapped onto W3C trace-context ids.
- **Cross-session sagas:** nothing coordinates work above the single-session boundary.
- **Shared post-commit event staging:** the agent runtime and the workflow engine each stage the events owed after a commit (workflow §10).

## 10. Sandbox

- **Defect:** on the microVM grade of the `Native` tier, a call copies the workspace into the guest when it starts and copies the guest's copy back over the workspace when it ends, so a `Workspace` or `Compute` call that writes the workspace meanwhile is erased after its success was journaled, and the next capture records the erasure as durable. This breaks sandbox §3.1 (overlapping calls MUST NOT corrupt one another). The runtime overlaps a step's calls (agent §3.3), so one model response asking for a shell command and a file write triggers it; the container grade shares the directory and is unaffected. The fix either serializes workspace writers against the microVM call or copies back only what the guest changed; `agent-sandbox` documents both and how to test them, and the race reproduces without KVM.
- **Warm pools and snapshots:** every activation provisions its tiers cold (a microVM boots per activation); memory snapshots would also warm-resume a persistent machine (facets §5.4).
- **A network dataplane:** no provider offers `Network`, so S3 is unexercised, and the reference provider refuses at open any profile that names egress or whose cap holds `Network` (sandbox §3.1).
- **Ingress:** a sandboxed server is unreachable from outside (sandbox §1.1); journaled grants could fold in a seam like the machine's (machine §5.1).
- **Permission-hook interplay:** no per-call authorization hook exists (§9); when one does, a journaled `TierAcquired` is its natural grain.
- **Peripheral tiers:** no tier grants a device (a browser) beside the chain (sandbox §4.1); such a tier's own network activity would escape the session's egress, and each would be a versioned change to the tier set.
- **Cooperative compute timeouts:** a compute guest runs to completion under its CPU bound, because yielding or interrupting it needs a clock source the actor system owns (actor §11).
- **Tier-attributed accounting:** provider cost by tier and session is not joined to the journal's acquisitions.

## 11. Machine

- **Warm resume:** a hibernated or migrated machine reboots its guest; resuming its processes would need memory snapshots stored as blobs.
- **Lazy block hydration:** an attach materializes the whole image, so first-attach latency grows with image size.
- **An external blob store:** capture blobs live on every replica of the machine's shard; an object store behind the entity's blob seam would cut idle cost, and needs lazy hydration first.
- **Fleet-wide base-image sharing:** each machine's import stores its own copy of the base image's blocks.
- **Restore and fork:** checkpoints would make both metadata operations, and restore is the only recovery for a compromised image; neither exists, and both need a policy first.
- **Capture before a planned handoff:** a cooperative handoff captures nothing, so a rolling restart costs an attached machine with uncaptured writes M3's window (machine §6.4).
- **Within-connection migration:** a leadership change severs established connections (machine §7).
- **A cross-node front-door relay:** a door bridges only the machines its own node leads, so clients reconnect after a move; a relay would also need flow control across the actor transport.
- **Ending live connections:** no command ends an attached connection; a revocation acts at the next authentication (machine §4.2).
- **Host keys at rest:** the host-key seed is journaled and snapshotted in the clear on every replica, so its confidentiality is the cluster's storage's (machine §10). Answering `machine.DoorPolicy` only to front doors would not narrow that; encrypting the seed at rest would.
- **Richer ingress:** no port forwarding, and no protocol but SSH.
- **Quotas:** no per-owner limit on machine count, disk, CPU, or egress, and no idle reaping.
- **Defect:** machine §5.1 (leadership): a door's leadership refusal rests on its node's view of the shard leader when the client authenticates. When no leader is known (mid-election), or leadership moves before the attach commits, a door whose node does not lead the machine journals an attachment and the leader boots a guest the door cannot reach; the client sees its first channel fail instead of a refusal.

## 12. Edge

- **Defect:** a request whose query string fails to parse (no `kind` on the sessions listing, no `turn` on a follow or a cancel, a non-numeric `from` or `limit`) is answered `400` with a plain-text body before its bearer token is checked. An unauthenticated request can therefore draw a `400` instead of the `401` that edge §4.2 requires, and the body is not the error envelope of edge §4.8. Found by reading the gateway's router (its query extractors run before the handler authenticates); no test covers it.
- **Signed tokens:** only a static token-file verifier exists, so every replica needs the whole token map; a stateless signed-token verifier (HMAC or JWT) behind the token-verifier seam (edge §3.2) would remove that, at the cost of a cryptographic dependency.
- **Per-tenant quotas and session deletion:** a tenant's spend and session count are unbounded, and its sessions outlive its directory index (edge §1.1).
- **Mutual TLS from configuration:** neither the edge nor the node binary exposes a setting for it, so a deployment whose cluster transport needs it (edge §6) rebuilds.
- **WebSocket transport and token introspection:** neither exists (edge §1.1).
