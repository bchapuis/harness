# Sandbox Execution Tiers: Specification

**Status:** Draft v1
**Scope:** The execution-tier model behind the agent runtime's sandbox seam ([`agent-spec.md`](agent-spec.md) §5.3, §5.6): what each tier grants and withholds, what a provider MUST guarantee to offer it, and the invariants binding every provider. The agent runtime specification owns the tier *contract* (the declaration, the record, the cap, the agreement); this document owns the tier *semantics*.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the agent runtime specification are cited as **agent §N**, sections of the core specification as **core §N**, sections of the utilities specification as **util §N**, sections of the entity specification ([`entity-spec.md`](entity-spec.md)) as **entity §N**, sections of the machine specification ([`machine-spec.md`](machine-spec.md)) as **machine §N**, and sections of this document as plain **§N**. Invariants defined here are numbered **S1, S2, …**, kept apart from the core catalogue (core §18.5 #1–#22), the utilities catalogue (util §6 U1–U2), and the agent runtime catalogue (agent §11 A1–A8).

> **Design stance.** A sandbox that offers everything offers too much: most tool calls read and write files, and granting them a shell grants the kernel's attack surface for free. The tier model makes capability proportional to need. Each tier names one additional dangerous thing (guest code, network, native processes), and a session acquires it only by a journaled acquisition, so the tiers that expose kernel attack surface run rarely and the journal records when. Confinement is mandated as a *property* per tier, never as a product: what removes a class of escape is required; which technology removes it is the provider's choice (agent §1.1).

---

## 1. Scope and layering

This document specifies the semantics of the four tiers the agent runtime declares (agent §5.2), the obligations a `SandboxProvider` (agent §5.3) MUST meet to offer each, and the conformance catalogue (§6) those obligations roll up to. It adds **no seam method and no record**: everything journaled (`TierAcquired`, `WorkspaceReset`, tool outcomes) is agent vocabulary (agent §5.5, §5.6), and the seam shape (one provider, one `Sandbox` per activation, agent A8) is unchanged.

### 1.1 Non-goals

- **An isolation-technology mandate.** Obligations here are properties (a path is unrepresentable; egress is default-deny), never products. The first provider's product choices are recorded as a non-normative reference realization (§3.5); the obligations bind every provider, the choices bind none.
- **A permission system.** The tier cap is static, fixed at deployment (agent §5.3 item 4); the dynamic per-call authorization hook remains future work (agent §13).
- **Ingress.** No tier accepts inbound connections in v1; a sandboxed server is reachable only from inside its own environment. Exposing one is future work (§7). A *sibling* consumer of the same substrate, the persistent machine ([`machine-spec.md`](machine-spec.md)), accepts an inbound connection, but through an ingress seam of its own defined outside this tier model, not by granting the agent sandbox a listener; the withholding here is unchanged.

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

The names are deliberately not numbers. The four form an inclusion chain, `Workspace ⊂ Compute ⊂ Network ⊂ Native`, but the naming leaves room for tiers that would not: a peripheral tier would sit beside the chain, not on it (§7).

### 2.2 Order and the cap

The kind's **tier cap** (agent §5.3 item 4) is a *set* of tiers, not a maximum. Over today's chain the two shapes coincide (a set grants what its greatest member grants); the set shape is chosen because a future peripheral tier (§7) would sit beside the chain, where no "highest tier" exists, and the cap is digest-covered (agent §7.1), so its shape is chosen once. Where the chain orders two tiers, holding the greater MUST imply the lesser's grants (a `Native` environment can read its workspace); it does not re-impose the lesser's *withholdings* (acquiring `Network` does not un-grant compute).

### 2.3 Acquisition, provider-side

The agent runtime journals a `TierAcquired` record before the first call at an unheld tier and passes every call's declared tier to `Sandbox::call` (agent §5.6, §6.4). The provider's half of that discipline:

1. **Granted only as journaled.** A provider MUST NOT execute a call at a tier the agent runtime did not pass, and the agent runtime passes only declared, cap-checked tiers; a conforming pair therefore grants nothing the journal does not show (S4).
2. **Provisioned lazily.** The provider builds a tier's environment on the first call that carries it (agent §5.3 item 1); a session that sits idle or never acquires more holds only its workspace. Providers MAY pool or pre-warm environments across sessions (§7), provided no working state leaks between sessions (agent A8).
3. **Additive, activation-scoped.** Held tiers only grow within an activation and all die with it: `release` MUST tear down every provisioned tier's environment and remains idempotent (S5). A fresh activation starts from `Workspace` and re-acquires under new records (agent §5.5).

---

## 3. Provider obligations per tier

### 3.1 `Workspace`: confinement by capability handle

The workspace MUST be reachable only through pre-opened directory handles (openat-style): a path outside the session's workspace is then *unrepresentable*, not merely rejected. String sanitization and canonicalize-then-check are **non-conforming** implementations of this obligation: they filter a representable escape rather than remove the representation, and the symlink/TOCTOU class survives filtering. Workspace tools take no ambient clock or entropy; they are pure functions of the call and the filesystem, which lets them run unmodified under the deterministic simulator (core §18.1).

### 3.2 `Compute`: hermetic guests

A compute environment executes guest code with no ambient wall clock, no ambient entropy, no network, and no OS identity: every capability the guest holds enters through a host function the provider chose to expose, and the filesystem it sees is the workspace of §3.1 and nothing else. Its outputs are therefore a function of the call, the workspace, and an injected seed. A conforming compute engine MUST be runnable inside the deterministic simulator: this is core §18.1's discipline applied below the seam, and the property that makes "deterministic" checkable rather than aspirational. Resource limits (memory, fuel or epoch-bounded CPU) are REQUIRED; their values are profile configuration (agent §5.3 item 4).

The limits MUST cover what a guest can make the *host* pay, not only the guest's own memory and CPU: the bytes handed to a compiler (compilation runs before any execution meter exists), the table and instance allocations instantiation performs host-side, and the size of any buffer a host function or the call's output materializes outside the guest. A guest-controlled integer (a length in a host-function call, a packed output size) MUST be bounded, or resolved as a view into the guest's own bounded memory, before it sizes anything on the host. A guest that can name an unmetered host allocation has a capability the tier never granted.

### 3.3 `Network`: granted egress only

Egress is **default-deny**. The only reachable hosts are those on the profile's allowlist, the one the session's `TierAcquired` record granted by reference (agent §5.3 item 4); the provider MUST NOT widen it per call or per session. Egress MUST be attributable to its session (per-session source identity, proxy, or namespace, at the provider's choice), so an operator can answer "which session reached this host" from provider logs plus the journal. Ingress remains withheld (§1.1).

### 3.4 `Native`: confinement and the threat model

A native environment runs OS processes with the full expressiveness that implies, so its obligation is stated as blast radius: an effect of a native tool MUST be confined to the environment. Filesystem outside the workspace mounts, processes outside the environment's lifetime, and network beyond what the held tiers grant are all unreachable. That is the by-construction leg of agent A8. The code running here is model-composed under inputs the operator did not author, which prompt injection makes attacker-influenced: treat it as hostile-code execution.

- Production deployments SHOULD confine native environments by OS mechanism (user namespaces, seccomp, Landlock) or by hardware virtualization (a microVM). Shared-kernel confinement is SHOULD-grade because its guarantee is priced by kernel privilege-escalation bugs (the guest still speaks to the host kernel); a microVM converts that residual into a second, far smaller escape problem, at provisioning cost. Multi-tenant deployments SHOULD NOT rely on shared-kernel confinement alone.
- A development or single-tenant provider MAY run native environments unconfined, and MUST then document itself as **trusted-input only**. The standalone deployment ships no such provider: every shell-capable mode (`docker`, `firecracker`) is confined, and the runtime-free fallback (`durable`) offers typed file tools with no shell.
- Either way, the journal already bounds exposure: `Native` runs only where a declared tool requires it, under a cap the kind agreed to, after a journaled acquisition (agent §5.6). Frequency reduction is part of the defense, not a substitute for it.

### 3.5 Reference realization (non-normative)

The obligations of §3.1–§3.4 are properties. The first provider's choices, which bind no one else:

- `Workspace`: cap-std's `Dir` as the capability handle of §3.1.
- `Compute`: wasmtime. Its defaults match §3.2's hermeticity (no ambient WASI unless the host wires it), and its fuel metering alone implements the CPU limit §3.2 requires: each call runs synchronously to completion under a fuel budget from `ComputeLimits`, and a guest that exhausts it traps deterministically, so the bound reproduces under the simulator. Epoch interruption stays off: its ticking needs an ambient timer thread, which the simulator requirement of §3.2 forbids (§7). "fuel or epoch-bounded CPU" is §3.2's vocabulary on purpose: either satisfies the obligation, and this provider takes the one that keeps the engine deterministic.
- `Compute`, scripting surface: a QuickJS interpreter compiled to a wasm module, registered with the provider so the model writes JavaScript rather than hand-authored wasm. Determinism is preserved by replacement, not exception: the engine's clock and entropy are seeded host functions, never the OS, so a script's output stays a function of the call, the workspace, and the seed (S2). A wasi-libc-linked interpreter imports a small WASI surface; the host satisfies it with deterministic stubs, which is conforming because §3.2 bans *ambient* capabilities, not host-injected ones. Any per-language interpreter joins the same way; the tier stays interpreter-agnostic.
- `Native`: Firecracker. A microVM confines single-tenant and multi-tenant deployments alike (§3.4's stronger grade), and its snapshot-restore is the warm-pool answer §7 anticipates. A deployment that configures no microVM answers `Native` with the confined container of the next bullet instead; the provider has no unconfined realization (§3.4). Firecracker exposes block devices and vsock, never a shared filesystem, so the workspace travels as a capped tar stream over vsock, bracketing each call (push → exec → pull), packed and unpacked entirely through §3.1's capability handle, with entries that would represent a path outside the workspace (absolute paths, `..`, absolute symlink targets) dropped at the boundary. The guest half is a static agent booted as the VM's init (`guest/fc-agent`); like the docker fallback, holding `Native` here delivers exactly an empty egress allowlist (no network device exists), and the open-time refusal the docker bullet states applies unchanged. The sibling persistent machine ([`machine-spec.md`](machine-spec.md)) reuses the microVM, the vsock transport, and the tar codec (shared as `machine-host`'s `ws_sync`); machine §2.1 owns the differences, chiefly a durable disk facet for the rootfs (entity §7.15), a `/workspace` volume synced per capture quiescent point rather than per call (machine §4), a network seam (machine §5), and a guest half that boots the user's own image. The agent sandbox keeps the workspace-only tar and the host-owned guest, and withholds ingress (§1.1).
- `Native`, development fallback: an OCI container driven through the `docker` CLI (feature `native` of `agent-sandbox`, over `machine_host::container`, the same mechanism the persistent machine's binding holds guests with, machine §2.1), with the workspace bind-mounted, `--network none`, one container per activation. Shared-kernel confinement, §3.4's SHOULD grade (on macOS the container additionally sits behind Docker Desktop's Linux VM, an incidental layer, not the microVM grade); single-tenant development use. Because holding `Native` implies `Network`'s grants (§2.2) and neither native realization has a dataplane (`--network none` here; no network device in the microVM), `TieredSandboxes::open` refuses with a `SandboxError` any profile whose egress allowlist (`SandboxProfile::egress`) is non-empty or whose explicit cap (`SandboxProfile::tier_cap`) contains `Network`. The refusal is unconditional, under every feature set alike, so `open`'s contract does not vary with which tiers a build compiles in; the agent runtime fails each call that needed the sandbox as a `ToolError` outcome and retries the open on the next (agent §5.4).

A provider choosing other products conforms all the same: the catalogue (§6) verifies the properties, never these names.

---

## 4. Provider conduct under loss

An environment can vanish at any moment: a crash, an eviction, an idle release. One rule governs that moment: **the loss is reported, never absorbed**. The journal and the model must learn that state is gone; a provider that quietly rebuilds, silently re-grants, or lets a loss masquerade as an ordinary failure undoes every record above it. Agent §5.5 specifies the records; this section specifies the provider conduct beneath them, at two grains:

- **Whole-environment loss** (crash, eviction, idle release): the next activation opens a fresh sandbox, the agent runtime journals `WorkspaceReset`, and the held set restarts at `Workspace`. The provider MUST NOT carry tier grants across that boundary: a pooled or pre-warmed environment handed to a fresh activation starts at `Workspace`, whatever its previous tenant held.
- **Single-tier loss** (a compute engine dies; the egress proxy is gone) while the sandbox survives: the calls that needed the tier fail as `ToolError` outcomes (agent §5.4), never as a run failure, never as a silent re-grant. The provider MAY re-provision the tier lazily under the acquisition this activation already journaled; it MUST NOT hold a session at a tier its activation never journaled.

Reporting a loss requires detecting it: a provider that can observe that the environment itself is gone (the workspace's backing directory no longer exists; the engine's state vanished beneath it) MUST report the failing call as environment loss, the agent runtime's `EnvironmentLost` outcome, never as an ordinary tool failure. Only environment loss engages §5.5's reset protocol; an ordinary failure leaves the model retrying against state that no longer exists, while the journal asserts a workspace the world lost.

### 4.1 The fault model at this seam

§4 says what a provider does when something is lost. This says what "something is lost" *is*: the vocabulary a test may inject, and which invariant is expected to survive each. A sweep whose faults are not the seam's faults asserts nothing while looking like coverage.

**The faults are the seam's answers, not the environment's internals.** The simulator's fault surface is the four nondeterminism traits (core §4.6, §7) and the transport under them; none of that reaches inside a sandbox call, which crosses a *process* boundary (a wasm instance, a container, a microVM) that the simulator does not model. What is injectable is therefore exactly what a `Sandbox` implementation can answer, and the vocabulary is closed:

| Fault | What the seam answers | What must survive |
|---|---|---|
| **Environment loss** | a call returns the agent runtime's `EnvironmentLost` outcome (§4, agent §5.5) | the next activation opens fresh, `WorkspaceReset` is journaled, and the held set restarts at `Workspace` (S4) |
| **Single-tier loss** | a call at a provisioned tier fails as an ordinary `ToolError` while the sandbox lives (§4) | the run continues; the tier may be re-provisioned under the acquisition already journaled, and never under one that was not (S4) |
| **Refused acquisition** | not a runtime fault the seam answers: a tool whose declared tier lies outside the kind's cap panics at `Kinds::register`, before any deployment runs, and the loop performs no per-call cap check | no session ever carries such a call, so no `TierAcquired` for it and no effect at it (S4); a sweep holds this by construction, not by injection |
| **Slow call** | a call that outlives the caller's deadline | the agent runtime's own timeout path, not a provider re-entry; `release` still tears down whatever was provisioned (S5) |

**Two invariants are assertable this way and three are not**, which is the part that decides whether a sweep is worth writing. S4 and S5 are claims about the *bookkeeping around* calls (the journaled order of acquisitions, the monotone held set, the activation boundary, the completeness and idempotence of release), so they hold or fail against any conforming `Sandbox`, including a test double that only answers. S1, S2 and S3 are claims about what a real environment *cannot do*: an unrepresentable path, a hermetic guest, a default-deny egress. A double cannot violate them and cannot be made to, so a sweep over one would report them green while exercising nothing. They stay where they are: by construction, plus the adversarial scenario tests §6 names.

**The consequence for a workload.** A conforming sweep drives an agent whose tools declare several tiers, injects the three seam-answered faults above under seed control, and asserts S4 and S5 over the journal alongside the agent runtime's own catalogue (agent §11). It does **not** claim to cover confinement. That distinction is why this section exists: the gap a sweep here closes is the acquisition bookkeeping under loss, and stating it stops a later reader from reading a green sweep as evidence about escape.

---

## 5. Relation to the agent runtime specification

| Concern | Owner |
|---|---|
| `Tier` enum, `ToolDecl.tier`, registry allowlist | agent §5.2 |
| `Sandbox::call(tier, …)`, lazy open, per-activation binding (A8) | agent §5.3 |
| Tier cap and the profile's egress allowlist | agent §5.3 item 4 |
| Acquisition discipline, `TierAcquired`, write-ahead position | agent §5.6, §6.4 |
| `WorkspaceReset`, orphaned calls, the divergence window | agent §5.5 |
| Digest coverage (cluster-wide agreement on tiers and cap) | agent §7.1 |
| Tier grants and withholdings, the cap's set shape | §2 |
| Per-tier provider obligations and the threat model | §3 |
| Reference realization (non-normative) | §3.5 |
| Provider conduct under loss | §4 |
| The fault model at the seam, and what a sweep there can assert | §4.1 |
| S-catalogue | §6 |

Nothing here changes the seam: a pre-tier provider that executes every call in one environment is a degenerate conforming provider for a kind whose tools all declare one tier and whose cap is that singleton.

---

## 6. Conformance

The catalogue mirrors the sibling catalogues (core §18.5, util §6, agent §11) in form.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| S1 | **Workspace confinement.** At every tier, no path outside the session's workspace is representable to a tool or a guest: confinement is by capability handle, never by sanitization of a representable escape. | §3.1, §3.2 | by construction of the provider (handle-scoped filesystem); adversarial traversal scenario tests (`agent-sandbox/tests/workspace.rs`), plus per-tier confinement smokes (`tests/native.rs`, `tests/firecracker.rs`) |
| S2 | **Compute hermeticity.** A `Compute` environment takes no ambient clock, entropy, network, or OS identity; its outputs are a function of the call, the workspace, and the injected seed. | §3.2 | by construction (the linker exposes only the chosen host functions); differential run, same call + workspace + seed (`agent-sandbox/tests/compute.rs`, `tests/quickjs.rs`); a run under the deterministic simulator *(future)* |
| S3 | **Granted egress only.** A `Network` environment's egress reaches only hosts on the allowlist its journaled acquisition granted; default-deny; per-session attributable. | §3.3, §2.3 | egress audit scenario tests *(future)*; by construction where the provider owns the dataplane |
| S4 | **Journaled, monotone, capped acquisition.** A `TierAcquired` record precedes the first effect at its tier within the activation; the held set only grows within an activation, never crosses an activation boundary, and never leaves the kind's cap. | §2.3, §4; agent §5.6, §6.4 | write-ahead-order journal audit at quiescence (agent §6.3, E5) and scripted-sandbox scenario tests (`agent/tests/conformance_sandbox.rs`); registration-time cap check, panicking like a duplicate name |
| S5 | **Per-tier release.** `release` tears down every provisioned tier's environment and is idempotent; deactivation releases all held tiers (the per-tier face of agent A8). | §2.3; agent §5.3 | provider open/release accounting (`agent-sandbox/tests/workspace.rs`, `tests/compute.rs`), and per-tier teardown (`tests/native.rs`: container removed, idempotent; `tests/firecracker.rs`: VM killed and control directory removed, idempotent); the existing A8 checker covers the binding window |

The first provider is `agent-sandbox`, realizing §3.5's choices for the tiers it offers: `Workspace` behind a cap-std handle (the default feature), `Compute` behind hermetic wasmtime guests (feature `compute`), and `Native` behind an OCI container driven through the docker CLI (feature `native`, §3.5's development fallback) or behind a Firecracker microVM (feature `firecracker`, §3.5's reference choice: one VM per activation, the workspace synced over vsock, Linux + `/dev/kvm` at runtime; the VM lifecycle, vsock framing, and workspace tar codec live in the shared `machine-host` crate, which the persistent machine's binding reuses, machine §2.1); `Network` remains unoffered there, at two points: `open` refuses with a `SandboxError`, under every feature set, a profile whose egress allowlist is non-empty or whose explicit cap contains `Network` (§3.5), after which the agent runtime fails each call that needed the sandbox as a `ToolError` outcome and retries the open on the next (agent §5.4); a `Network` call that reaches `Sandbox::call` anyway (a cap left implicit, so `open` saw no `Network` in it) fails per call as a `ToolError` outcome (agent §5.4). The catalogue is machine-readable as `s_catalogue()` beside that crate's conformance suite, guarded by the same drift-test pattern as its siblings; the entries still marked *(future)* name the intended method in the established vocabulary (core §17's verification kinds), not an existing test.

---

## 7. Future work

- **Warm pools and snapshots, per tier.** Pooling compute isolates and snapshot-restoring native microVMs (the provisioning-latency answer §3.4's grading alludes to); the agent runtime-side hooks are already future work there (agent §13). The sibling persistent machine ([`machine-spec.md`](machine-spec.md)) already persists a microVM's *disk* as an entity facet (entity §7.15); the memory-snapshot half is the shared upgrade that would both warm-resume a machine's processes and pre-boot a pooled `Native` environment here.
- **Ingress.** Exposing a sandboxed server (preview deployments, callback receivers) behind explicit, journaled grants; today all ingress is withheld (§1.1). The persistent machine (machine §5) realizes an authenticated, attributable, isolated ingress seam for its own consumer; folding a comparable seam back into the agent sandbox's tier model, so a tool may expose a port, is the work named here.
- **Permission-hook interplay.** When the dynamic per-call authorization hook lands (agent §13), acquisition is its natural coarse entity: approve-once-per-tier against the journaled `TierAcquired`, rather than per call.
- **Peripheral tiers.** A tier granting a remote peripheral driven through host functions; a browser (navigate, read, screenshot, fill) is the canonical candidate. A peripheral sits beside the inclusion chain, not on it: it grants a device, not a superset of compute, and its own network activity (the pages a browser fetches, the scripts those pages run) happens at the peripheral, outside the session's egress. Offering one would therefore not imply `Network`-tier containment of what the peripheral touches, and a deployment needing that containment would police it at the peripheral. The cap's set shape (§2.2) already leaves room; each peripheral would join as a new named tier, a versioned change to the agent runtime's `Tier` enum (agent §5.2).
- **Async fuel-yield and epoch interruption.** The compute tier runs each guest synchronously to completion under its fuel budget (§3.5); yielding to the executor on fuel exhaustion, or interrupting on epochs, would let a guest cooperate with the agent runtime's per-tool timeout instead of trapping, but epoch ticking needs an ambient timer thread the determinism contract forbids under simulation (core §18.1), so both wait on a simulator-owned clock source.
- **Tier-attributed accounting.** Provider cost by tier and session, joined to the journal's acquisitions; part of the multi-tenant economics the agent runtime defers (agent §1.1).
