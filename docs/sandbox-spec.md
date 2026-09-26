# Sandbox Execution Tiers: Specification

**Status:** Draft v1
**Scope:** The execution-tier model behind the agent runtime's sandbox seam ([`agent-spec.md`](agent-spec.md) §5.3, §5.6): what each tier grants and withholds, what a provider MUST guarantee to offer it, and the invariants binding every provider. The agent runtime specification owns the tier *contract* (the declaration, the record, the cap, the agreement); this document owns the tier *semantics*.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the agent runtime specification are cited as **agent §N**, of the core specification as **core §N**, of the utilities specification as **util §N**, of the entity specification ([`entity-spec.md`](entity-spec.md)) as **entity §N**, of the machine specification ([`machine-spec.md`](machine-spec.md)) as **machine §N**, and of this document as plain **§N**. Invariants defined here are numbered **S1, S2, …**, apart from the core catalogue (core §18.5 #1–#22), the utilities catalogue (util §6 U1–U2), and the agent runtime catalogue (agent §11 A1–A7).

> **Design stance.** A sandbox that offers everything offers too much: most tool calls only read and write files, and granting them a shell grants the kernel's attack surface for free. The tier model makes capability proportional to need. Each tier adds one dangerous thing (guest code, network, native processes), taken only through a journaled acquisition, so the tiers that expose kernel attack surface run rarely and the journal records when. Confinement is mandated per tier as a *property*, not a product: what removes a class of escape is required; which technology removes it is the provider's choice (agent §1.1).

---

## 1. Scope and layering

The tiers are the four the agent runtime declares (agent §5.2), and the obligations bind any `SandboxProvider` (agent §5.3) that offers them. This document adds **no seam method and no record**: everything journaled (`TierAcquired`, `WorkspaceReset`, tool outcomes) is agent vocabulary (agent §5.5, §5.6), and the seam shape (one provider, one `Sandbox` per activation, agent A7) is unchanged.

### 1.1 Non-goals

- **An isolation-technology mandate.** Obligations are properties (a path is unrepresentable; egress is default-deny), not products. The first provider's product choices are a non-normative reference realization (§3.5) that binds no other provider.
- **A permission system.** The tier cap is static, fixed at deployment (agent §5.3 item 4); the dynamic per-call authorization hook is future work (agent §13).
- **Ingress.** No tier accepts inbound connections in v1; a sandboxed server is reachable only from inside its own environment (future work, §7). The persistent machine ([`machine-spec.md`](machine-spec.md)), a sibling consumer of the same substrate, accepts inbound connections through its own ingress seam outside this tier model (machine §5), which grants the agent sandbox no listener.

---

## 2. The tier model

### 2.1 Tiers are capability sets

A tier names what a call may touch. Each grants one additional capability over the workspace and withholds the rest:

| Tier | Grants | Withholds |
|---|---|---|
| `Workspace` | the session's scoped filesystem, through host-implemented typed tools | guest code; network; ambient clock and entropy |
| `Compute` | arbitrary guest code over the workspace | network; ambient clock, entropy, OS identity |
| `Network` | compute, plus egress to the profile's allowlist (agent §5.3 item 4) | egress beyond the allowlist; all ingress |
| `Native` | OS processes and native binaries inside the confined environment | everything outside the environment |

The names are not numbers: the four form an inclusion chain, `Workspace ⊂ Compute ⊂ Network ⊂ Native`, but a peripheral tier would sit beside it (§7).

### 2.2 Order and the cap

The kind's **tier cap** (agent §5.3 item 4) is a *set* of tiers, not a maximum, checked by **membership**: `Kinds::register` asserts that every declared tool's tier is an element of the cap, never that some greater member covers it. A cap of `[Native]` therefore rejects a kind whose tools declare `Workspace`: a cap lists every tier its tools declare, and a miss panics as a deployment-configuration error, not a dispatch-time outcome (§2.3, **S4**). A set, unlike a maximum, admits a peripheral tier beside the chain, and the shape is fixed once because the cap is digest-covered (agent §7.1). Where the chain orders two tiers, holding the greater MUST imply the lesser's grants (a `Native` environment can read its workspace) but not its *withholdings* (acquiring `Network` does not un-grant compute).

### 2.3 Acquisition, provider-side

The agent runtime journals a `TierAcquired` record before the first call at an unheld tier and passes every call's declared tier to `Sandbox::call` (agent §5.6, §6.4). The provider's half:

1. **Granted only as journaled.** A provider MUST NOT execute a call at a tier the agent runtime did not pass; since the runtime passes only declared, cap-checked tiers, a conforming pair grants nothing the journal does not show (S4).
2. **Provisioned lazily.** The provider builds a tier's environment on the first call that carries it (agent §5.3 item 1), so a session that never acquires more holds only its workspace. Providers MAY pool or pre-warm environments across sessions (§7), provided no working state leaks between sessions (agent A7).
3. **Additive, activation-scoped.** Held tiers only grow within an activation and all die with it: `release` MUST tear down every provisioned tier's environment and remains idempotent (S5). A fresh activation starts from `Workspace` and re-acquires under new records (agent §5.5).

---

## 3. Provider obligations per tier

### 3.1 `Workspace`: confinement by capability handle

The workspace MUST be reachable only through pre-opened directory handles (openat-style), so a path outside the session's workspace is *unrepresentable*, not merely rejected. String sanitization and canonicalize-then-check are **non-conforming**: they filter a representable escape, and the symlink/TOCTOU class survives filtering. Workspace tools take no ambient clock or entropy: they are pure functions of the call and the filesystem, so they run unmodified under the deterministic simulator (core §18.1).

### 3.2 `Compute`: hermetic guests

A compute environment executes guest code with no ambient wall clock, entropy, network, or OS identity: every capability the guest holds enters through a host function the provider chose to expose, and the only filesystem it sees is §3.1's workspace. Its outputs are therefore a function of the call, the workspace, and an injected seed. A conforming compute engine MUST be runnable inside the deterministic simulator (core §18.1 applied below the seam), which makes "deterministic" checkable. Resource limits (memory, fuel or epoch-bounded CPU) are REQUIRED; their values are profile configuration (agent §5.3 item 4).

The limits MUST also cover what a guest can make the *host* pay: the bytes handed to a compiler (compilation precedes any execution meter), the table and instance allocations instantiation performs host-side, and any buffer a host function or the call's output materializes outside the guest. A guest-controlled integer (a length in a host-function call, a packed output size) MUST be bounded, or resolved as a view into the guest's own bounded memory, before it sizes anything on the host: an unmetered host allocation is a capability the tier never granted.

### 3.3 `Network`: granted egress only

Egress is **default-deny**: the only reachable hosts are those on the profile's allowlist that the session's `TierAcquired` record granted by reference (agent §5.3 item 4), and the provider MUST NOT widen it per call or per session. Egress MUST be attributable to its session (per-session source identity, proxy, or namespace, at the provider's choice), so provider logs plus the journal answer "which session reached this host". Ingress stays withheld (§1.1).

### 3.4 `Native`: confinement and the threat model

A native environment runs OS processes with full expressiveness, so its obligation is stated as blast radius: a native tool's effects MUST be confined to the environment. Filesystem outside the workspace mounts, processes outside the environment's lifetime, and network beyond what the held tiers grant are unreachable; this is the by-construction leg of agent A7. The code here is composed by the model from inputs prompt injection makes attacker-influenced: treat it as hostile-code execution.

- Production deployments SHOULD confine native environments by OS mechanism (user namespaces, seccomp, Landlock) or by hardware virtualization (a microVM). Shared-kernel confinement is SHOULD-grade because kernel privilege-escalation bugs price its guarantee (the guest still speaks to the host kernel); a microVM converts that residual into a second, far smaller escape problem, at provisioning cost. Multi-tenant deployments SHOULD NOT rely on shared-kernel confinement alone.
- A development or single-tenant provider MAY run native environments unconfined, and MUST then document itself as **trusted-input only**. The standalone deployment ships no such provider: every shell-capable mode (`docker`, `firecracker`) is confined, and the runtime-free fallback (`durable`) offers typed file tools with no shell.
- Either way, the journal bounds exposure: `Native` runs only where a declared tool requires it, under a cap the kind agreed to, after a journaled acquisition (agent §5.6). Frequency reduction is part of the defense, not a substitute for it.

### 3.5 Reference realization (non-normative)

The first provider's choices bind no other provider; the catalogue (§6) verifies the properties of §3.1–§3.4, never these names.

- `Workspace`: cap-std's `Dir` as the capability handle of §3.1.
- `Compute`: wasmtime. Its defaults match §3.2's hermeticity (no ambient WASI unless the host wires it), and its fuel metering implements the CPU limit: each call runs synchronously to completion under a fuel budget from `ComputeLimits`, and a guest that exhausts it traps deterministically, so the bound reproduces under the simulator. Epoch interruption stays off: its ticking needs an ambient timer thread, which §3.2's simulator requirement forbids (§7). Either mechanism satisfies §3.2; fuel keeps the engine deterministic.
- `Compute`, scripting surface: a QuickJS interpreter compiled to a wasm module and registered with the provider, so the model writes JavaScript rather than wasm. The engine's clock and entropy are seeded host functions, never the OS, so a script's output stays a function of the call, the workspace, and the seed (S2). A wasi-libc-linked interpreter imports a small WASI surface that the host satisfies with deterministic stubs, which conforms because §3.2 bans *ambient* capabilities, not host-injected ones. Any per-language interpreter joins the same way.
- `Native`: Firecracker. A microVM confines single-tenant and multi-tenant deployments alike (§3.4's stronger grade), and its snapshot-restore is the warm-pool answer §7 anticipates. A deployment that configures no microVM answers `Native` with the container of the next bullet; the provider has no unconfined realization (§3.4). Firecracker exposes block devices and vsock, never a shared filesystem, so the workspace travels as a capped tar stream over vsock around each call (push → exec → pull), packed and unpacked through §3.1's capability handle, with entries that would represent a path outside the workspace (absolute paths, `..`, absolute symlink targets) dropped at the boundary. The guest half is a static agent booted as the VM's init (`guest/fc-agent`). No network device exists, so holding `Native` here delivers an empty egress allowlist, and the next bullet's open-time refusal applies unchanged. The persistent machine reuses the microVM, the vsock transport, and the tar codec (`machine-host`'s `ws_sync`); machine §2.1 owns the differences.
- `Native`, development fallback: an OCI container driven through the `docker` CLI (feature `native` of `agent-sandbox`, over `machine_host::container`, which the persistent machine also holds guests with, machine §2.1), with the workspace bind-mounted, `--network none`, one container per activation. This is shared-kernel confinement, §3.4's SHOULD grade, for single-tenant development (on macOS, Docker Desktop's Linux VM is an incidental layer, not the microVM grade). Because holding `Native` implies `Network`'s grants (§2.2) and neither native realization has a dataplane, `TieredSandboxes::open` refuses with a `SandboxError` any profile whose egress allowlist (`SandboxProfile::egress`) is non-empty or whose explicit cap (`SandboxProfile::tier_cap`) contains `Network`. The refusal is unconditional under every feature set, so `open`'s contract does not vary with which tiers a build compiles in; the agent runtime fails each call that needed the sandbox as a `ToolError` outcome and retries the open on the next (agent §5.4).

---

## 4. Provider conduct under loss

An environment can vanish at any moment (a crash, an eviction, an idle release), and one rule governs that moment: **the loss is reported, never absorbed**. A provider that quietly rebuilds, silently re-grants, or lets a loss pass as an ordinary failure undoes every record above it. Agent §5.5 specifies the records; this section specifies the provider conduct beneath them, at two grains:

- **Whole-environment loss** (crash, eviction, idle release): the next activation opens a fresh sandbox, the agent runtime journals `WorkspaceReset`, and the held set restarts at `Workspace`. The provider MUST NOT carry tier grants across that boundary: a pooled or pre-warmed environment handed to a fresh activation starts at `Workspace`, whatever its previous tenant held.
- **Single-tier loss** (a compute engine dies; the egress proxy is gone) while the sandbox survives: the calls that needed the tier fail as `ToolError` outcomes (agent §5.4), never as a run failure or a silent re-grant. The provider MAY re-provision the tier lazily under the acquisition this activation already journaled; it MUST NOT hold a session at a tier its activation never journaled.

Reporting a loss requires detecting it: a provider that can observe the environment itself is gone (the workspace's backing directory deleted, the engine's state vanished) MUST report the call as environment loss, the agent runtime's `EnvironmentLost` outcome, never as an ordinary tool failure, even when the call succeeded. An open handle keeps a deleted directory's descriptor alive, so a call can return `Ok` against an environment with no durable home (an empty listing, a sync of nothing), and only an ambient check of the path, not a read through the handle, witnesses the deletion. Only environment loss engages agent §5.5's reset protocol; an ordinary failure leaves the model retrying against vanished state while the journal asserts a workspace the world lost.

### 4.1 The fault model at this seam

This section defines what "something is lost" *is*: the faults a test may inject, and which invariant must survive each. A sweep whose faults are not the seam's asserts nothing while looking like coverage.

**The faults are the seam's answers, not the environment's internals.** The simulator's fault surface is the four nondeterminism traits (core §4.6, §7) and the transport under them; none of that reaches inside a sandbox call, which crosses a *process* boundary (a wasm instance, a container, a microVM) the simulator does not model. The injectable vocabulary is therefore exactly what a `Sandbox` implementation can answer, and it is closed:

| Fault | What the seam answers | What must survive |
|---|---|---|
| **Environment loss** | a call returns the agent runtime's `EnvironmentLost` outcome (§4, agent §5.5) | the next activation opens fresh, `WorkspaceReset` is journaled, and the held set restarts at `Workspace` (S4) |
| **Single-tier loss** | a call at a provisioned tier fails as an ordinary `ToolError` while the sandbox lives (§4) | the run continues; the tier may be re-provisioned under the acquisition already journaled, never under one that was not (S4) |
| **Refused acquisition** | not a runtime fault: a tool whose tier lies outside the kind's cap panics at `Kinds::register` before any deployment runs (§2.2), and the loop performs no per-call cap check | no session carries such a call, so no `TierAcquired` and no effect at it (S4); held by construction, not injection |
| **Slow call** | a call that outlives the caller's deadline | the agent runtime's own timeout path, not a provider re-entry; `release` still tears down whatever was provisioned (S5) |

**Two invariants are assertable this way and three are not.** S4 and S5 concern the *bookkeeping around* calls (the journaled order of acquisitions, the monotone held set, the activation boundary, release's completeness and idempotence), so they hold or fail against any conforming `Sandbox`, including a test double that only answers. S1, S2, and S3 concern what a real environment *cannot do* (represent an outside path, escape hermeticity, pass default-deny egress); a double cannot violate them, so a sweep over one would report them green while exercising nothing. They are verified by construction plus the adversarial scenario tests §6 names.

**The consequence for a workload.** A conforming sweep drives an agent whose tools declare several tiers, injects the three seam-answered faults under seed control, and asserts S4 and S5 over the journal alongside the agent runtime's catalogue (agent §11). It does **not** cover confinement: a green sweep is no evidence about escape.

---

## 5. Relation to the agent runtime specification

The agent runtime owns the seam's contract; everything else about tiers is this document's (§2–§4, §6).

| Concern | Owner |
|---|---|
| `Tier` enum, `ToolDecl.tier`, registry allowlist | agent §5.2 |
| `Sandbox::call(tier, …)`, lazy open, per-activation binding (A7) | agent §5.3 |
| Tier cap and the profile's egress allowlist | agent §5.3 item 4 |
| Acquisition discipline, `TierAcquired`, write-ahead position | agent §5.6, §6.4 |
| `WorkspaceReset`, orphaned calls, the divergence window | agent §5.5 |
| Digest coverage (cluster-wide agreement on tiers and cap) | agent §7.1 |

Nothing here changes the seam: a provider that executes every call in one environment conforms for a kind whose tools all declare one tier and whose cap is that singleton.

---

## 6. Conformance

The catalogue mirrors the sibling catalogues (core §18.5, util §6, agent §11) in form.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| S1 | **Workspace confinement.** At every tier, no path outside the session's workspace is representable to a tool or a guest: confinement is by capability handle, never by sanitization of a representable escape. | §3.1, §3.2 | by construction of the provider (handle-scoped filesystem); adversarial traversal scenario tests (`agent-sandbox/tests/workspace.rs`), plus per-tier confinement smokes (`tests/quickjs.rs`, `tests/native.rs`, `tests/firecracker.rs`) |
| S2 | **Compute hermeticity.** A `Compute` environment takes no ambient clock, entropy, network, or OS identity; its outputs are a function of the call, the workspace, and the injected seed. | §3.2 | by construction (the linker exposes only the chosen host functions); differential run, same call + workspace + seed (`agent-sandbox/tests/compute.rs`, `tests/quickjs.rs`); a run under the deterministic simulator *(future)* |
| S3 | **Granted egress only.** A `Network` environment's egress reaches only hosts on the allowlist its journaled acquisition granted; default-deny; per-session attributable. | §3.3, §2.3 | egress audit scenario tests *(future)*; by construction where the provider owns the dataplane |
| S4 | **Journaled, monotone, capped acquisition.** A `TierAcquired` record precedes the first effect at its tier within the activation; the held set only grows within an activation, never crosses an activation boundary, and never leaves the kind's cap. | §2.3, §4; agent §5.6, §6.4 | write-ahead-order journal audit at quiescence (agent §6.3, E5) and scripted-sandbox scenario tests (`agent/tests/conformance_sandbox.rs`); registration-time cap check, panicking like a duplicate name |
| S5 | **Per-tier release.** `release` tears down every provisioned tier's environment and is idempotent; deactivation releases all held tiers (the per-tier face of agent A7). | §2.3; agent §5.3 | provider open/release accounting (`agent-sandbox/tests/workspace.rs`, `tests/compute.rs`), and per-tier teardown (`tests/native.rs`: container removed, idempotent; `tests/firecracker.rs`: VM killed and control directory removed, idempotent); the existing A7 checker covers the binding window |

The first provider is `agent-sandbox`, realizing §3.5's choices for the tiers it offers (and refusing `Network` at `open`, §3.5). The catalogue is machine-readable as `s_catalogue()` beside that crate's conformance suite, guarded by the siblings' drift-test pattern; entries marked *(future)* name the intended method in core §17's verification vocabulary, not an existing test.

---

## 7. Future work

- **Warm pools and snapshots, per tier.** Pooling compute isolates and snapshot-restoring native microVMs (the provisioning cost §3.4 notes); the agent runtime's hooks are future work too (agent §13). The persistent machine persists a microVM's *disk* as an entity facet (entity §7.15); memory snapshots are the shared upgrade that would warm-resume a machine's processes and pre-boot a pooled `Native` environment here.
- **Ingress.** Exposing a sandboxed server (preview deployments, callback receivers) behind explicit, journaled grants (§1.1), by folding a seam like the persistent machine's (machine §5) into the tier model so a tool may expose a port.
- **Permission-hook interplay.** When the dynamic per-call authorization hook lands (agent §13), acquisition is its natural coarse grain: approve once per tier against the journaled `TierAcquired`, rather than per call.
- **Peripheral tiers.** A tier granting a remote peripheral driven through host functions; a browser (navigate, read, screenshot, fill) is the canonical candidate. A peripheral sits beside the inclusion chain: it grants a device, not a superset of compute, and its own network activity (the pages a browser fetches, the scripts they run) happens at the peripheral, outside the session's egress. Offering one therefore implies no `Network`-tier containment of what the peripheral touches; a deployment needing that would police it at the peripheral. The cap's set shape (§2.2) leaves room; each peripheral would join as a new named tier, a versioned change to the agent runtime's `Tier` enum (agent §5.2).
- **Async fuel-yield and epoch interruption.** The compute tier runs each guest synchronously to completion under its fuel budget (§3.5); yielding to the executor on fuel exhaustion, or interrupting on epochs, would let a guest cooperate with the agent runtime's per-tool timeout instead of trapping, but epoch ticking needs an ambient timer thread the determinism contract forbids (core §18.1), so both wait on a simulator-owned clock source.
- **Tier-attributed accounting.** Provider cost by tier and session, joined to the journal's acquisitions; part of the multi-tenant economics the agent runtime defers (agent §1.1).
