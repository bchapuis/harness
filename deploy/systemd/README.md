# harness-standalone on bare metal (systemd)

Three nodes as `harness-node.service`, fronted by one or more `harness-gateway.service`. The counterpart to [`k8s/`](../../k8s/), which is the same topology as a StatefulSet; the sizing, timeout, and failure-domain guidance behind both lives in [`docs/standalone-deployment.md`](../../docs/standalone-deployment.md), and the machine it is all derived from in [`docs/hardware-envelope.md`](../../docs/hardware-envelope.md).

```
   HTTPS  ─►  gw-1 : harness-gateway  ─┐  actor transport (cluster secret)
                                       │  node ids 100+, non-voting clients
        ┌──────────────────────────────┴──────────┐
   ┌────┴─────┐      ┌──────────┐      ┌──────────┴┐
   │  node-1  │◄────►│  node-2  │◄────►│  node-3   │   transport 7401
   │ /var/lib │      │ /var/lib │      │ /var/lib  │   no client-facing listener
   │  own NVMe│      │  own NVMe│      │  own NVMe │   share nothing on disk
   └──────────┘      └──────────┘      └───────────┘
```

The nodes share nothing on disk. Each holds its own replica and the journal replicates over the transport, a quorum append per grain (granary spec §7.2). There is no shared filesystem to provision and none to lose.

## Disk layout

One filesystem per node, on **local NVMe**, mounted at `/var/lib/harness`. Not shared, not network-attached.

```
/usr/local/bin/harness-standalone      the node binary
/usr/local/bin/harness-gateway         the gateway binary
/etc/harness/node.env                  0640 root:harness — config + secrets
/etc/harness/tenants                   0640 root:harness — "<principal> <token>" per line
/var/lib/harness/                      the mount point; one filesystem, this node's only state
└── node/                              --data
    ├── grains/<node-id>/              the durable grain store
    │   ├── manifest                   (shard, grain) -> segment id; held whole in memory
    │   ├── LOCK                       advisory single-writer guard
    │   ├── segments/<id>              one append log per grain: records + snapshots
    │   ├── fences/<shard>             the per-shard durable term fence
    │   └── blobs/<segment id>/<hash>  content-addressed blobs (disk blocks, chunks)
    ├── raft/<group>/<node>/           the Raft voter state, one directory per group
    │   ├── term                       current term + vote (JSON; parse failure is the
    │   │                              corruption check protecting election safety)
    │   ├── log                        framed, checksummed entries
    │   └── snapshot                   the compacted prefix
    ├── facets/                        physical-facet materializations (SQL dbs, disk images)
    └── workspaces/                    per-session workspace directories
```

**`grains/` and `raft/` are the source of truth. `facets/` and `workspaces/` are rebuildable caches** (granary spec §1) — keyed by node and grain, and safe to wipe between runs. A node that lost only those rematerializes them from the committed state on next activation. A node that lost `grains/` or `raft/` has lost its replica: do not restart it on the empty directory, because it will be re-replicated to as if it were healthy. Give it a **fresh node id** instead (see below).

Sizing per node: eight or more cores, 32 GB and up, local NVMe. Memory is consumed by grain *count* rather than concurrency — the manifest is held whole and segments and host handles are cached by the thousand — so undershooting shows up as an OOM under grain growth, not under load. `docs/standalone-deployment.md` carries the full sizing note and the `link ÷ (replication_factor − 1)` ingest ceiling.

**Do not put `/var/lib/harness` on network-attached block storage** (EBS, Persistent Disk, Ceph). It puts a durable flush back near a millisecond and reverses an assumption the storage layer is built on (hardware-envelope §6). It will work, and it will be slower than the design expects.

## Install

On each of the three machines:

```sh
useradd --system --home-dir /var/lib/harness --shell /usr/sbin/nologin harness
install -d -m 0750 -o harness -g harness /var/lib/harness /var/lib/harness/node
install -d -m 0750 -o root    -g harness /etc/harness

install -m 0755 target/release/harness-standalone /usr/local/bin/
install -m 0644 deploy/systemd/harness-node.service /etc/systemd/system/
install -m 0640 -o root -g harness deploy/systemd/node.env.example /etc/harness/node.env
# edit /etc/harness/node.env: HARNESS_NODE_ID and HARNESS_ADVERTISE_HOST differ per
# machine; everything else is identical across the cluster.

systemctl daemon-reload
systemctl enable --now harness-node
```

Then, on the gateway machine, the same with `harness-gateway.service` and `gateway.env.example`, plus `/etc/harness/tenants`.

Generate the cluster secret **once** and distribute it: `openssl rand -hex 32`. Every node and every gateway must present the same one.

## What must agree cluster-wide

These are worth checking before the first boot, because a mismatch is not always a startup failure:

| value | why |
|---|---|
| `HARNESS_SECRET` | the transport handshake refuses a peer that does not present it |
| `HARNESS_NODES` and the `--peer` roster | how each node knows it has discovered enough peers to serve |
| `HARNESS_MODEL` | covered by the kind digest `SessionCreated` pins — a cluster running two models fails to agree rather than splitting silently |
| `HARNESS_SANDBOX` | also digest-covered, so a mixed-mode cluster fails to agree instead of silently splitting confinement |

Node ids are the opposite: unique per machine, and stable across restarts.

## Rolling restart

One node at a time, waiting for the cluster to be healthy between each. The unit sends SIGTERM and waits up to 120 s on purpose: the node hands off leadership of every shard it leads before exiting (granary spec §8.3). Skipping the handoff is *safe* but expensive — each shard it still led waits a full election timeout before its replicas elect, and every grain on those shards rehydrates on the new leader. A node leading many shards therefore turns a rolling restart into that many simultaneous failovers.

```sh
systemctl restart harness-node   # then wait for the node to rejoin before the next
journalctl -u harness-node -f
```

Shortening `TimeoutStopSec` below the worst handoff means systemd SIGKILLs the node mid-handoff: the work is wasted and the failover happens the slow way anyway.

## Replacing a node

A node declared `down` is terminal and **must not rejoin under the same id** (actor spec §3.6, §9.1). To replace it:

1. Remove it from the voter set by a committed membership change.
2. Wipe `/var/lib/harness/node` on the replacement machine.
3. Give it a **fresh** `HARNESS_NODE_ID` and add it to every node's `HARNESS_PEERS`.

The same applies to a node whose Raft WAL has poisoned — it stopped voting because it could not persist (actor spec §9.4.3 item 2), and it will not recover on its own. `journalctl -u harness-node | grep '\[raft\]'` shows the reason.

## Failure domains

`GranaryConfig::failure_domains` defaults to `None`, which treats every node as its own domain: replicas spread across the cluster, but nothing stops all *R* of a shard landing in one rack. On bare metal the mapping is usually obvious and worth setting — rack, chassis, or PDU. See `docs/standalone-deployment.md`, "Failure domains", for how the allocator uses it (`ceil(R / domains)` replicas per domain) and why the mapping must agree on every node.

## TLS

The transport handshake is guarded by the cluster secret and is expected to run on a private network. Public TLS terminates in front of the gateway — that is the only tenant-facing port in the deployment, and the bearer tokens in `/etc/harness/tenants` are credentials that must not cross a plaintext link. A reverse proxy on the gateway machine is the ordinary arrangement; `harness-gateway` also accepts a `TlsConfig` directly.
