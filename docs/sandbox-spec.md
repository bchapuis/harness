# Sandbox Execution Tiers: Specification

**Status:** Draft v1
**Scope:** The execution-tier model behind the agentic harness's sandbox seam ([`agentic-harness-spec.md`](agentic-harness-spec.md) §5.3, §5.6): what each tier grants and withholds, what a provider MUST guarantee to offer it, and the invariants binding every provider. The harness specification owns the tier *contract* (the declaration, the record, the cap, the agreement); this document owns the tier *semantics*.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the agentic harness specification are cited as **harness §N**, sections of the core specification as **core §N**, sections of the utilities specification as **util §N**, sections of the granary specification ([`granary-spec.md`](granary-spec.md)) as **grain §N**, sections of the machine specification ([`machine-spec.md`](machine-spec.md)) as **machine §N**, and sections of this document as plain **§N**. Invariants defined here are numbered **S1, S2, …**, kept apart from the core catalogue (core §18.5 #1–#22), the utilities catalogue (util §6 U1–U2), and the harness catalogue (harness §11 H1–H8).

> **Design stance.** A sandbox that offers everything offers too much: most tool calls read and write files, and granting them a shell grants the kernel's attack surface for free, though most tasks never need more than the lowest tier. The tier model makes capability proportional to need. Each tier names one additional dangerous thing (guest code, network, native processes), and a session acquires it only by a journaled acquisition. The strongest control in the scheme is frequency reduction: the tiers that expose kernel attack surface run rarely, and the journal records when. Confinement is mandated as a *property* per tier, never as a product: what removes a class of escape is required; which technology removes it is the provider's choice (harness §1.1).

---

## 1. Scope and layering

This document specifies the semantics of the four tiers the harness declares (harness §5.2), the obligations a `SandboxProvider` (harness §5.3) MUST meet to offer each, and the conformance catalogue (§6) those obligations roll up to. It adds **no seam method and no record**: everything journaled (`TierAcquired`, `WorkspaceReset`, tool outcomes) is harness vocabulary (harness §5.5, §5.6), and the seam shape (one provider, one `Sandbox` per activation, harness H8) is unchanged. To the harness, a provider conforming to this document is just an implementation of the same two traits.

### 1.1 Non-goals

- **An isolation-technology mandate.** Obligations here are properties (a path is unrepresentable; egress is default-deny), never products. The first provider's product choices are recorded as a non-normative reference realization (§3.5); the obligations bind every provider, the choices bind none. The harness non-goal (harness §1.1) holds: *how* remains the provider's choice.
- **A permission system.** The tier cap is static, fixed at deployment (harness §5.3 item 4); the dynamic per-call authorization hook remains future work (harness §13).
- **Ingress.** No tier accepts inbound connections in v1; a sandboxed server is reachable only from inside its own environment. Exposing one is future work (§7). A *sibling* consumer of the same substrate, the persistent machine ([`machine-spec.md`](machine-spec.md)), accepts an inbound connection, but through an ingress seam of its own defined outside this tier model, not by granting the agent sandbox a listener; the withholding here is unchanged.

---

## 2. The tier model

### 2.1 Tiers are capability sets

A tier names what a call may touch. Each grants one additional capability over the workspace and withholds the rest:

| Tier | Grants | Withholds |
|---|---|---|
| `Workspace` | the session's scoped filesystem, through host-implemented typed tools | guest code; network; ambient clock and entropy |
| `Compute` | arbitrary guest code over the workspace | network; ambient clock, entropy, OS identity |
| `Network` | compute, plus egress to the profile's allowlist (harness §5.3 item 4) | egress beyond the allowlist; all ingress |
| `Native` | OS processes and native binaries inside the confined environment | everything outside the environment |

The names are deliberately not numbers. The four form an inclusion chain, `Workspace ⊂ Compute ⊂ Network ⊂ Native`, but the naming leaves room for tiers that would not: a peripheral tier would sit beside the chain, not on it (§7).

### 2.2 Order and the cap

The kind's **tier cap** (harness §5.3 item 4) is a *set* of tiers, not a maximum. Over today's chain the two shapes coincide (a set grants what its greatest member grants), but the set shape is deliberate: a future peripheral tier (§7) would sit beside the chain, where no "highest tier" exists, and the cap is digest-covered (harness §7.1), so its shape is chosen once. Where the chain orders two tiers, holding the greater MUST imply the lesser's grants (a `Native` environment can read its workspace); it does not re-impose the lesser's *withholdings* (acquiring `Network` does not un-grant compute).

### 2.3 Acquisition, provider-side

The harness journals a `TierAcquired` record before the first call at an unheld tier and passes every call's declared tier to `Sandbox::call` (harness §5.6, §6.4). The provider's half of that discipline:

1. **Granted only as journaled.** A provider MUST NOT execute a call at a tier the harness did not pass, and the harness passes only declared, cap-checked tiers; a conforming pair therefore grants nothing the journal does not show (S4).
2. **Provisioned lazily.** The provider builds a tier's environment on the first call that carries it (harness §5.3 item 1); a session that sits idle or never acquires more holds only its workspace. Providers MAY pool or pre-warm environments across sessions (§7), provided no working state leaks between sessions (harness H8).
3. **Additive, activation-scoped.** Held tiers only grow within an activation and all die with it: `release` MUST tear down every provisioned tier's environment and remains idempotent (S5). A fresh activation starts from `Workspace` and re-acquires under new records (harness §5.5).

---

## 3. Provider obligations per tier

### 3.1 `Workspace`: confinement by capability handle

The workspace MUST be reachable only through pre-opened directory handles (openat-style): a path outside the session's workspace is then *unrepresentable*, not merely rejected. String sanitization and canonicalize-then-check are **non-conforming** implementations of this obligation: they filter a representable escape rather than remove the representation, and the symlink/TOCTOU class survives filtering. Workspace tools take no ambient clock or entropy; they are pure functions of the call and the filesystem, which lets them run unmodified under the deterministic simulator (core §18.1).

### 3.2 `Compute`: hermetic guests

A compute environment executes guest code with no ambient wall clock, no ambient entropy, no network, and no OS identity: every capability the guest holds enters through a host function the provider chose to expose, and the filesystem it sees is the workspace of §3.1 and nothing else. Its outputs are therefore a function of the call, the workspace, and an injected seed. A conforming compute engine MUST be runnable inside the deterministic simulator: this is core §18.1's discipline applied below the seam, and the property that makes "deterministic" checkable rather than aspirational. Resource limits (memory, fuel or epoch-bounded CPU) are REQUIRED; their values are profile configuration (harness §5.3 item 4).

The limits MUST cover what a guest can make the *host* pay, not only the guest's own memory and CPU: the bytes handed to a compiler (compilation runs before any execution meter exists), the table and instance allocations instantiation performs host-side, and the size of any buffer a host function or the call's output materializes outside the guest. A guest-controlled integer (a length in a host-function call, a packed output size) MUST be bounded, or resolved as a view into the guest's own bounded memory, before it sizes anything on the host. A guest that can name an unmetered host allocation has a capability the tier never granted.

### 3.3 `Network`: granted egress only

Egress is **default-deny**. The only reachable hosts are those on the profile's allowlist, the one the session's `TierAcquired` record granted by reference (harness §5.3 item 4); the provider MUST NOT widen it per call or per session. Egress MUST be attributable to its session (per-session source identity, proxy, or namespace, at the provider's choice), so an operator can answer "which session reached this host" from provider logs plus the journal. Ingress remains withheld (§1.1).

### 3.4 `Native`: confinement and the threat model

A native environment runs OS processes with the full expressiveness that implies, so its obligation is stated as blast radius: an effect of a native tool MUST be confined to the environment. Filesystem outside the workspace mounts, processes outside the environment's lifetime, and network beyond what the held tiers grant are all unreachable. That is the by-construction leg of harness H8, and it is where the threat model earns its keep: the code running here is model-composed under inputs the operator did not author, which prompt injection makes attacker-influenced. Treat it as hostile-code execution.

- Production deployments SHOULD confine native environments by OS mechanism (user namespaces, seccomp, Landlock) or by hardware virtualization (a microVM). The grading is deliberate: shared-kernel confinement is SHOULD-grade because its guarantee is priced by kernel privilege-escalation bugs (the guest still speaks to the host kernel); a microVM converts that residual into a second, far smaller escape problem, at provisioning cost. Multi-tenant deployments SHOULD NOT rely on shared-kernel confinement alone.
- A development or single-tenant provider MAY run native environments unconfined, and MUST then document itself as **trusted-input only**. The standalone deployment ships no such provider: every shell-capable mode (`docker`, `firecracker`) is confined, and the runtime-free fallback (`durable`) offers typed file tools with no shell.
- Either way, the journal already bounds exposure: `Native` runs only where a declared tool requires it, under a cap the kind agreed to, after a journaled acquisition (harness §5.6). Frequency reduction is part of the defense, not a substitute for it.

### 3.5 Reference realization (non-normative)

The obligations of §3.1–§3.4 are properties, and §1.1 keeps them that way. The first provider, however, makes opinionated choices, recorded here so the code round inherits decisions rather than defaults:

- `Workspace`: cap-std's `Dir` as the capability handle of §3.1.
- `Compute`: wasmtime. Its defaults match §3.2's hermeticity (no ambient WASI unless the host wires it), and its fuel metering and epoch interruption implement the resource limits §3.2 requires; "fuel or epoch-bounded CPU" is its vocabulary on purpose.
- `Compute`, scripting surface: a QuickJS interpreter compiled to a wasm module, registered with the provider so the model writes JavaScript rather than hand-authored wasm. Determinism is preserved by replacement, not exception: the engine's clock and entropy are seeded host functions, never the OS, so a script's output stays a function of the call, the workspace, and the seed (S2). A wasi-libc-linked interpreter imports a small WASI surface; the host satisfies it with deterministic stubs, which is conforming because §3.2 bans *ambient* capabilities, not host-injected ones. Any per-language interpreter joins the same way; the tier stays interpreter-agnostic.
- `Native`: Firecracker. A microVM confines single-tenant and multi-tenant deployments alike (§3.4's stronger grade), and its snapshot-restore is the warm-pool answer §7 anticipates. Development deployments stay unconfined and trusted-input only, per §3.4. Firecracker exposes block devices and vsock, never a shared filesystem, so the workspace travels as a capped tar stream over vsock, bracketing each call (push → exec → pull), packed and unpacked entirely through §3.1's capability handle, with entries that would represent a path outside the workspace (absolute paths, `..`, absolute symlink targets) dropped at the boundary. The guest half is a static agent booted as the VM's init (`guest/fc-agent`); like the docker fallback, holding `Native` here delivers exactly an empty egress allowlist (no network device exists), and a profile naming egress is refused at open. The sibling persistent machine ([`machine-spec.md`](machine-spec.md)) reuses the microVM, the vsock transport, and the tar codec itself (shared as `microvm`'s `ws_sync`): its *rootfs* replaces the per-call tar with a durable *disk* facet (grain §7.15), so the whole rootfs persists block-for-block, while its `/workspace` volume travels by the same codec against a durable workspace facet (grain §7.11) — per capture quiescent point rather than per call (machine §4) — and it adds a network seam (machine §5), so a user reaches the guest over SSH and the guest reaches out under a journaled egress policy. The machine also inverts the guest half: where this tier boots the host-owned `fc-agent` as init over a host-owned image, a machine boots the *user's own rootfs* (which ships its guest agent), so its vsock peer is untrusted user code rather than host code. The agent sandbox keeps the workspace-only tar and the host-owned guest, and withholds ingress (§1.1).
- `Native`, development fallback: an OCI container driven through the `docker` CLI (feature `native` of `harness-sandbox`) — the workspace bind-mounted, `--network none`, one container per activation. Shared-kernel confinement, §3.4's SHOULD grade (on macOS the container additionally sits behind Docker Desktop's Linux VM — an incidental layer, not the microVM grade); single-tenant development use. Because holding `Native` implies `Network`'s grants (§2.2), the provider refuses at open a profile whose egress allowlist is non-empty: `--network none` delivers exactly an empty allowlist, and a silent withholding would be worse than a loud refusal.

A provider choosing other products conforms all the same: the catalogue (§6) verifies the properties, never these names.

---

## 4. Provider conduct under loss

An environment can vanish at any moment: a crash, an eviction, an idle release. One rule governs that moment: **the loss is reported, never absorbed**. The journal and the model must learn that state is gone; a provider that quietly rebuilds, silently re-grants, or lets a loss masquerade as an ordinary failure undoes every record above it. Harness §5.5 specifies the records; this section specifies the provider conduct beneath them, at two grains:

- **Whole-environment loss** (crash, eviction, idle release): the next activation opens a fresh sandbox, the harness journals `WorkspaceReset`, and the held set restarts at `Workspace`. The provider MUST NOT carry tier grants across that boundary: a pooled or pre-warmed environment handed to a fresh activation starts at `Workspace`, whatever its previous tenant held.
- **Single-tier loss** (a compute engine dies; the egress proxy is gone) while the sandbox survives: the calls that needed the tier fail as `ToolError` outcomes (harness §5.4), never as a run failure, never as a silent re-grant. The provider MAY re-provision the tier lazily under the acquisition this activation already journaled; it MUST NOT hold a session at a tier its activation never journaled.

Reporting a loss requires detecting it: a provider that can observe that the environment itself is gone (the workspace's backing directory no longer exists; the engine's state vanished beneath it) MUST report the failing call as environment loss, the harness's `EnvironmentLost` outcome, never as an ordinary tool failure. Only environment loss engages §5.5's reset protocol; an ordinary failure leaves the model retrying against state that no longer exists, while the journal asserts a workspace the world lost.

The asymmetry is deliberate: losing everything is a journaled reset the model is told about; losing one tier is a tool failure the model reacts to. Both keep the journal ahead of the world.

---

## 5. Relation to the harness specification

| Concern | Owner |
|---|---|
| `Tier` enum, `ToolDecl.tier`, registry allowlist | harness §5.2 |
| `Sandbox::call(tier, …)`, lazy open, per-activation binding (H8) | harness §5.3 |
| Tier cap and the profile's egress allowlist | harness §5.3 item 4 |
| Acquisition discipline, `TierAcquired`, write-ahead position | harness §5.6, §6.4 |
| `WorkspaceReset`, dangling calls, the divergence window | harness §5.5 |
| Digest coverage (cluster-wide agreement on tiers and cap) | harness §7.1 |
| Tier grants and withholdings, the cap's set shape | §2 |
| Per-tier provider obligations and the threat model | §3 |
| Reference realization (non-normative) | §3.5 |
| Provider conduct under loss | §4 |
| S-catalogue | §6 |

Nothing here changes the seam: a pre-tier provider that executes every call in one environment is a degenerate conforming provider for a kind whose tools all declare one tier and whose cap is that singleton.

---

## 6. Conformance

The catalogue mirrors the sibling catalogues (core §18.5, util §6, harness §11) in form.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| S1 | **Workspace confinement.** At every tier, no path outside the session's workspace is representable to a tool or a guest: confinement is by capability handle, never by sanitization of a representable escape. | §3.1, §3.2 | by construction of the provider (handle-scoped filesystem); adversarial traversal scenario tests (`harness-sandbox/tests/workspace.rs`) |
| S2 | **Compute hermeticity.** A `Compute` environment takes no ambient clock, entropy, network, or OS identity; its outputs are a function of the call, the workspace, and the injected seed. | §3.2 | by construction (the linker exposes only the chosen host functions); differential run, same call + workspace + seed (`harness-sandbox/tests/compute.rs`); a run under the deterministic simulator *(future)* |
| S3 | **Granted egress only.** A `Network` environment's egress reaches only hosts on the allowlist its journaled acquisition granted; default-deny; per-session attributable. | §3.3, §2.3 | egress audit scenario tests *(future)*; by construction where the provider owns the dataplane |
| S4 | **Journaled, monotone, capped acquisition.** A `TierAcquired` record precedes the first effect at its tier within the activation; the held set only grows within an activation, never crosses an activation boundary, and never leaves the kind's cap. | §2.3, §4; harness §5.6, §6.4 | journal audit at quiescence (H2-style) and scripted-sandbox scenario tests (`harness/tests/conformance_sandbox.rs`); registration-time cap check, panicking like a duplicate name |
| S5 | **Per-tier release.** `release` tears down every provisioned tier's environment and is idempotent; deactivation releases all held tiers (the per-tier face of harness H8). | §2.3; harness §5.3 | provider open/release accounting (`harness-sandbox/tests/workspace.rs`, `tests/compute.rs`); the existing H8 checker covers the binding window |

The first provider is `harness-sandbox`, realizing §3.5's choices for the tiers it offers: `Workspace` behind a cap-std handle (the default feature), `Compute` behind hermetic wasmtime guests (feature `compute`), and `Native` behind an OCI container driven through the docker CLI (feature `native`, §3.5's development fallback) or behind a Firecracker microVM (feature `firecracker`, §3.5's reference choice — one VM per activation, the workspace synced over vsock, Linux + `/dev/kvm` at runtime; the VM lifecycle, vsock framing, and workspace tar codec live in the shared `microvm` crate, which the persistent machine's binding reuses, machine §2.1); `Network` remains unoffered there, failing as a `ToolError` outcome (harness §5.4). The catalogue is machine-readable as `s_catalogue()` beside that crate's conformance suite, guarded by the same drift-test pattern as its siblings; the entries still marked *(future)* name the intended method in the established vocabulary (core §17's verification kinds), not an existing test.

---

## 7. Future work

- **Warm pools and snapshots, per tier.** Pooling compute isolates and snapshot-restoring native microVMs (the provisioning-latency answer §3.4's grading alludes to); the harness-side hooks are already future work there (harness §13). The sibling persistent machine ([`machine-spec.md`](machine-spec.md)) already persists a microVM's *disk* as a granary facet (grain §7.15); the memory-snapshot half is the shared upgrade that would both warm-resume a machine's processes and pre-boot a pooled `Native` environment here.
- **Ingress.** Exposing a sandboxed server (preview deployments, callback receivers) behind explicit, journaled grants; today all ingress is withheld (§1.1). The persistent machine (machine §5) realizes an authenticated, attributable, isolated ingress seam for its own consumer; folding a comparable seam back into the agent sandbox's tier model, so a tool may expose a port, is the work named here.
- **Permission-hook interplay.** When the dynamic per-call authorization hook lands (harness §13), acquisition is its natural coarse grain: approve-once-per-tier against the journaled `TierAcquired`, rather than per call.
- **Peripheral tiers.** A tier granting a remote peripheral driven through host functions; a browser (navigate, read, screenshot, fill) is the canonical candidate. A peripheral sits beside the inclusion chain, not on it: it grants a device, not a superset of compute, and its own network activity (the pages a browser fetches, the scripts those pages run) happens at the peripheral, outside the session's egress. A peripheral tier would have to state that limit outright: offering one would not imply `Network`-tier containment of what the peripheral touches, and a deployment needing that containment would police it at the peripheral. The cap's set shape (§2.2) already leaves room; each peripheral would join as a new named tier, a versioned change to the harness's `Tier` enum (harness §5.2).
- **Tier-attributed accounting.** Provider cost by tier and session, joined to the journal's acquisitions; part of the multi-tenant economics the harness defers (harness §1.1).
