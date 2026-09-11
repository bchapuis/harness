# agent-standalone on bare metal (systemd)

Three nodes as `agent-node.service`, fronted by one or more `agent-gateway.service`. The counterpart to [`k8s/`](../../k8s/), which is the same topology as a StatefulSet; the sizing, timeout, and failure-domain guidance behind both lives in [`docs/standalone-deployment.md`](../../docs/standalone-deployment.md), and the machine it is all derived from in [`docs/hardware-envelope.md`](../../docs/hardware-envelope.md).

```
   HTTPS  ─►  gw-1 : agent-gateway  ─┐  actor transport (cluster secret)
                                       │  node ids 100+, non-voting clients
        ┌──────────────────────────────┴──────────┐
   ┌────┴─────┐      ┌──────────┐      ┌──────────┴┐
   │  node-1  │◄────►│  node-2  │◄────►│  node-3   │   transport 7401
   │ /var/lib │      │ /var/lib │      │ /var/lib  │   no client-facing listener
   │  own NVMe│      │  own NVMe│      │  own NVMe │   share nothing on disk
   └──────────┘      └──────────┘      └───────────┘
```

The nodes share nothing on disk. Each holds its own replica and the journal replicates over the transport, a quorum append per entity (entity spec §7.2). There is no shared filesystem to provision and none to lose.

## Disk layout

One filesystem per node, on **local NVMe**, mounted at `/var/lib/agent`. Not shared, not network-attached.

```
/usr/local/bin/agent-standalone      the node binary
/usr/local/bin/agent-gateway         the gateway binary
/etc/agent/node.env                  0640 root:agent   config + secrets
/etc/agent/tenants                   0640 root:agent   "<principal> <token>" per line
/var/lib/agent/                      the mount point; one filesystem, this node's only state
└── node/                              --data
    ├── entities/<type>/<node-id>/       the durable entity store, one per hosted entity type
    │   ├── manifest                   (shard, entity) -> segment id; held whole in memory
    │   ├── LOCK                       advisory single-writer guard
    │   ├── segments/<id>              one append log per entity: records + snapshots
    │   ├── fences/<shard>             the per-shard durable term fence
    │   ├── seals/<shard>              the per-shard append bound a split or merge
    │   │                              sets: appends at or above it are refused
    │   └── blobs/<segment id>/<hash>  content-addressed blobs (disk blocks, chunks)
    ├── raft/<group>/<node>/           the Raft voter state, one directory per group
    │   ├── term                       current term + vote (JSON; parse failure is the
    │   │                              corruption check protecting election safety)
    │   ├── log                        framed, checksummed entries
    │   └── snapshot                   the compacted prefix
    └── workspaces/<node-id>/          physical-facet materializations, keyed by entity:
                                       the per-session workspace directories, and any
                                       SQL database files or disk images beside them
```

A store is per hosted **entity type** (`entities/<type>/…`), never shared across
types: the fence under it is keyed by shard index, and the same index is a
different leader-election group under each type (entity spec §8.2).

**`entities/` and `raft/` are the source of truth. `workspaces/` is a rebuildable cache** (entity spec §1), keyed by node and entity, and safe to wipe between runs. A node that lost only `workspaces/` rematerializes it from the committed state on next activation. A node that lost `entities/` or `raft/` has lost its replica: do not restart it on the empty directory, because it will be re-replicated to as if it were healthy. Restore the directory from the machine's own backup, or rebuild it as "Replacing a node" describes.

Sizing per node: eight or more cores, 32 GB and up, local NVMe. Memory is consumed by entity *count* rather than concurrency (the manifest is held whole and segments and host handles are cached by the thousand), so undershooting shows up as an OOM under entity growth, not under load. `docs/standalone-deployment.md` carries the full sizing note and the `link ÷ (replication_factor − 1)` ingest ceiling.

**Do not put `/var/lib/agent` on network-attached block storage** (EBS, Persistent Disk, Ceph). It puts a durable flush back near a millisecond and reverses an assumption the storage layer is built on (hardware-envelope §6). It will work, and it will be slower than the design expects.

## Install

On each of the three machines:

```sh
useradd --system --home-dir /var/lib/agent --shell /usr/sbin/nologin agent
install -d -m 0750 -o agent -g agent /var/lib/agent /var/lib/agent/node
install -d -m 0750 -o root    -g agent /etc/agent

install -m 0755 target/release/agent-standalone /usr/local/bin/
install -m 0644 deploy/systemd/agent-node.service /etc/systemd/system/
install -m 0640 -o root -g agent deploy/systemd/node.env.example /etc/agent/node.env
# edit /etc/agent/node.env: AGENT_NODE_ID differs per machine; everything
# else is identical across the cluster.
# (A node has no advertise host: peers dial its own entry in AGENT_PEERS.)

systemctl daemon-reload
systemctl enable --now agent-node
```

Then, on the gateway machine, the same with `agent-gateway.service` and `gateway.env.example`, plus `/etc/agent/tenants`.

Generate the cluster secret **once** and distribute it: `openssl rand -hex 32`. Every node and every gateway must present the same one.

## What must agree cluster-wide

These are worth checking before the first boot, because a mismatch is not always a startup failure:

| value | why |
|---|---|
| `AGENT_SECRET` | the transport handshake refuses a peer that does not present it |
| `AGENT_NODES` and the `--peer` roster | how each node knows it has discovered enough peers to serve |
| `AGENT_MODEL` | covered by the kind digest `SessionCreated` pins; a cluster running two models fails to agree rather than splitting silently |
| `AGENT_SANDBOX` | also digest-covered, so a mixed-mode cluster fails to agree instead of silently splitting confinement |

Node ids are the opposite: unique per machine, and stable across restarts.

## Rolling restart

One node at a time, waiting for the cluster to be healthy between each. The unit sends SIGTERM and waits up to 120 s on purpose: the node hands off leadership of every shard it leads before exiting (entity spec §8.3). Skipping the handoff is *safe* but expensive: each shard it still led waits a full election timeout before its replicas elect, and every entity on those shards rehydrates on the new leader. A node leading many shards therefore turns a rolling restart into that many simultaneous failovers.

```sh
systemctl restart agent-node   # then wait for the node to rejoin before the next
journalctl -u agent-node -f
```

Shortening `TimeoutStopSec` below the worst handoff means systemd SIGKILLs the node mid-handoff: the work is wasted and the failover happens the slow way anyway.

## Replacing a node

The voter roster is fixed at boot: the binary accepts `--id` only in `1..=AGENT_NODES`, the voter set is exactly that range, and nothing in it adds or removes a voter at runtime. So a replacement machine **keeps the dead node's id**:

1. Stop the old machine for good. The cluster runs on the remaining quorum meanwhile: the node never auto-downs a peer (`DowningPolicy::Conservative`), so an absent voter stays `unreachable` until something answers under its id again.
2. Restore `/var/lib/agent/node` on the replacement from the old machine's own backup. That directory holds the id's Raft term and vote, what makes reusing the id safe (actor spec §9.4.3 item 2). Starting the same id on an **empty** directory instead forfeits that state: the node can vote again in a term it already voted in, and it rejoins as a replica with everything to re-replicate; accept that only with the cluster quiescent and the alternative being no third voter at all.
3. If the replacement is not reachable at the old machine's address, point the id's entry in every `AGENT_PEERS` at it and restart the peers one at a time ("Rolling restart"): a node resolves the roster's hosts to addresses at boot.

The same procedure applies to a node whose Raft WAL has poisoned: it stopped voting because it could not persist (actor spec §9.4.3 item 2), and it will not recover on its own. `journalctl -u agent-node | grep '\[raft\]'` shows the reason.

The actor spec's rule for a voter declared `down`, remove it by a committed membership change and admit the replacement under a fresh `NodeId` (§9.1, §9.4.3), has no path in this binary: a fresh id would lie outside the roster, and the roster admits no new voter. That is a limitation of `agent-standalone`, not of the substrate.

## Failure domains

`EntityConfig::failure_domains` defaults to `None`, which treats every node as its own domain: replicas spread across the cluster, but nothing stops all *R* of a shard landing in one rack. On bare metal the mapping is usually obvious and worth setting: rack, chassis, or PDU. See `docs/standalone-deployment.md`, "Failure domains", for how the allocator uses it (`ceil(R / domains)` replicas per domain) and why the mapping must agree on every node.

## TLS

`agent-gateway` serves plain HTTP: it has no TLS flag and binds a bare listener on `AGENT_HTTP_BIND`. That is the only tenant-facing port in the deployment, and the bearer tokens in `/etc/agent/tenants` are credentials that must not cross a plaintext link, so terminate public TLS in front of it: a reverse proxy on the gateway machine, with the gateway bound to loopback or a private address behind it.

The actor transport between nodes and gateways is plaintext as well, guarded by the cluster secret, and expected to run on a private network. The transport itself supports mutual TLS (`Encryption::MutualTls`), but neither binary exposes a flag for it: both name `Encryption::PlaintextTrusted` in code, and a deployment crossing untrusted links has to provision certificates there and rebuild.
