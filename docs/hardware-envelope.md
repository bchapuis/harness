# The Hardware Envelope

**Status:** a design note, not a specification. It states the machine this tree is written for and the rules that follow. Specifications and code cite it as **hw §N**. It constrains *performance* reasoning only — §5 says what it does not license.

Written down once so a review can do arithmetic instead of arguing, and so a number that no longer holds can be found and changed.

## 1. The machine

**A rented dedicated server in a European datacenter** — bare metal, not a shared tenant slot and not a hyperscaler VM.

- **CPU.** AMD EPYC/Ryzen or Ampere Altra ARM: tens to low hundreds of physical cores. Cores are plentiful; the interconnect between them is the scarce part.
- **Memory.** 128–256 GB DDR5, ECC. Comparable to a node's whole durable footprint, not just its working set.
- **Storage.** Directly-attached NVMe, typically software RAID. **Power-loss protection varies by drive model and must be verified** — without it a flush costs fifteen times more (two rows in §2, not one).
- **Network. A 1 Gbps uplink**, 10 Gbps available at extra cost. Replication and public serving share that one 125 MB/s port, and private traffic between nodes rides the same interface. This is the constraint everything below is downstream of.

Two structural consequences: there are **no availability zones** — failure domains are racks and datacenter sites, which is what `GranaryConfig::failure_domains` must be given — and **no managed block storage**, so a lost node is a lost replica.

> Confirm core count, drive model, PLP, and port speed against the machine actually rented; these vary by tier and change over time. The rules depend on the *shape* of the table, not any single row.

## 2. The numbers

After Jeff Dean and Peter Norvig, updated to this envelope. LLM rows included because this tree hosts an agentic system and they sit on the same axis — which turns out to be the most useful thing in the table.

```
Uncontended mutex lock/unlock                 15   ns
Hash 1 KB with XXH3                           50   ns              ~20 GB/s
Main memory reference                         80   ns
Cache line moved between cores               100   ns   300 ns across chiplets
Compress 1 KB with LZ4                       200   ns              ~5 GB/s
Hash 1 KB with BLAKE3                        300   ns              ~3 GB/s
Send 1 KB over a 1 Gbps network            8,000   ns    8 us   UNCHANGED SINCE 2012
fsync small append, NVMe with PLP         30,000   ns   30 us
Read 1 MB sequentially from memory         50,000   ns   50 us     ~20 GB/s
Read 4 KB randomly from NVMe               80,000   ns   80 us   QD1; >1M IOPS at depth
Read 1 MB sequentially from NVMe          150,000   ns  150 us     ~7 GB/s
Round trip within the same datacenter     200,000   ns  200 us
fsync small append, NVMe without PLP      500,000   ns  500 us   consumer drive
Round trip between nearby datacenters   3,000,000   ns    3 ms   ~150 km apart
Send 1 MB over a 1 Gbps network         8,000,000   ns    8 ms   53x reading it from NVMe
Disk seek (rotational)                 10,000,000   ns   10 ms
Frontier LLM, generate 1 token         20,000,000   ns   20 ms
Round trip across Europe               25,000,000   ns   25 ms
Round trip Europe <-> US East          90,000,000   ns   90 ms
Frontier LLM, time to first token                      1,000 ms   short prompt, no cache
Frontier LLM, short response                           3,000 ms   ~100 output tokens
Frontier LLM, reasoning response                      30,000 ms   single call with thinking
```

Four inversions fall out of it. They are the content of this document.

**I1 — The uplink is the only row that did not move.** Since 2012 sequential reads improved ~50×, random reads ~100×, a durable flush ~300×. Sending 1 KB over a 1 Gbps port takes the same 8 µs it always did. The network here is not the fast part of the machine.

**I2 — A byte is ~50× cheaper to persist than to move.** One megabyte: 150 µs to NVMe, 8 ms to a peer. At `replication_factor: 3` every durable byte crosses the uplink twice, so a node's sustainable ingest of *new* data is ~**60 MB/s** against a device that would take 3–7 GB/s. The cluster is network-bound by two orders of magnitude.

**I3 — An fsync is cheaper than a round trip.** It used to be twenty times more expensive. With PLP, 30 µs against a 200 µs round trip: durability is no longer the costly half of a quorum append; *agreement* is. Without PLP the margin closes, which is why the drive line matters.

**I4 — The session's clock is the model's, not the machine's.** A quorum append at ~1 ms is 0.003% of an agent turn. Everything in the storage and consensus layers is, from the session's point of view, free. What *is* visible is measured in seconds: a failover window, a cold rehydrate that ships megabytes, a capture that takes minutes.

Two facts that moved as everyone expects: **memory holds the set, not a slice** (128 GB against grains measured in kilobytes), and **cores are abundant while coordination between them is not** (a shared mutex on a hot path is this decade's disk seek).

## 3. What follows

Defaults, each overridable by a measurement.

**hw §3.1 — Amortize round trips and bytes; never amortize flushes.** "Fewer fsyncs" earns nothing and pays in structure (I3). "Fewer round trips" or "fewer bytes on the wire" buys the terms that dominate (I1, I2).

**hw §3.2 — Prefer holding to tiering.** If it fits, hold it: no spill path, no second format, no promotion policy.

**hw §3.3 — Spend CPU to buy simplicity, and to buy bytes.** Verify every frame on open rather than trusting a checkpoint pointer; re-hash rather than track what was verified; recompute rather than cache-and-invalidate. Each is a rounding error at 5–20 GB/s and deletes a piece of state that could be wrong. And against a 125 MB/s port, LZ4 buys forty bytes of link per byte of CPU: compression on the replication path is earned here as it is not on a fast fabric.

**hw §3.4 — Find the serial resource; it is not the device.** Suspects in order: the uplink, a chain of round trips, a shared lock, a single control loop. The NVMe device is last.

**hw §3.5 — File-per-object is limited by metadata, not throughput.** The cost of one file per grain is inodes, directory size, and descriptors — not the un-shared fsync, which is what the classic objection is about.

**hw §3.6 — A number copied from another system is not a default.** Durable Objects evicts at ten seconds because it prices multi-tenant memory. `RLIMIT_NOFILE` is 65536 because a 2010 distribution said so. SQLite pages are 4 KiB because that was a disk sector. Re-derive the number here, or write down that it was inherited.

**hw §3.7 — Fast hardware buys nothing at the tail.** A median flush of 30 µs and a p99.99 of 200 ms coexist on one device. Every bound, deadline, and backpressure limit exists for the tail and is unaffected by the median. Removing one because the median is fast is the most likely way to misread this document.

**hw §3.8 — Below a millisecond on the session path is invisible; above a hundred is not.** From I4: no saved microseconds justify a weaker bound or a harder-to-read module. Conversely, failover windows, cold starts, capture and restore, and uplink saturation are real user-visible latency and deserve the effort microseconds do not.

**hw §3.9 — Every replicated byte is billed R−1 times to the uplink.** Dedup, delta encoding, and compression pay R−1 times over; so does write amplification, in the wrong direction. A snapshot that ships whole state, a checkpoint that re-sends unchanged blocks, a rebalance that copies a shard: each costs 8 ms per megabyte per peer.

## 4. Where this already decided something

The decisions this envelope settled — group commit withdrawn, `sync_all` over `sync_data`, whole-log verification on open, XXH3 and BLAKE3, chunked content-addressed blobs, the manifest held whole, local replay reads — each carry their reasoning in the code that implements them. What it has *not* settled is in [TODO.md](../TODO.md); the reviews that produced both are in the history.

## 5. What this does not license

**No safety property is a performance trade.** Quorum intersection, the term fence, torn-tail truncation, per-frame checksums, at-most-once delivery, the output gate: none are here because storage was slow, and none get cheaper to remove because it is fast.

**A bound is not an optimization.** Bounded mailboxes, in-flight chunk limits, segment and cache capacities, maximum record and image sizes: each exists so an unlucky input cannot consume the machine. 128 GB changes what the number should be, never whether there is one.

**The simulator does not run on this hardware.** Deterministic simulation (actor §18) runs production code on one logical thread over virtual time, so anything justified by real concurrency or real device behaviour stays behind a seam it can replace — which is why store I/O is a `BlockingIo` seam defaulting to inline.

**hw §3.8 is not an excuse.** "The model takes a second anyway" justifies the simpler design over the faster one, never one that is both slower *and* more complex — and it says nothing about throughput: a node serving many sessions is bounded by aggregate work, not one session's patience.

## 6. When this stops being true

- **A 10 Gbps uplink** — the most likely change, the cheapest, and the highest-leverage: I2 goes from 50× to 5×, and replicated ingest from ~60 MB/s to ~600 MB/s. Price the port before optimizing the replication path.
- **A drive line without PLP** — the flush row moves to ~500 µs, I3's margin closes, group commit becomes arguable again.
- **Network-attached block storage** — flush returns to ~1 ms and I3 reverses outright.
- **A memory-limited container** — every "just hold it" decision becomes an OOM.
- **Replicas spanning sites or regions** — quorum latency then dominates everything, including timeout defaults, and `failure_domains` becomes a latency decision as much as an availability one.

A deployment outside the envelope is not unsupported, only *differently tuned*. Where that set of numbers is large, say so in the deployment guide rather than moving a default the majority case depends on.
