# Sandbox Execution Tiers: Specification

**Status:** Draft v1
**Scope:** The execution-tier model behind the agentic harness's sandbox seam ([`agentic-harness-spec.md`](agentic-harness-spec.md) §5.3, §5.6): what each tier grants and withholds, what a provider MUST guarantee to offer it, and the invariants binding every provider. The harness specification owns the tier *contract* — the declaration, the record, the cap, the agreement; this document owns the tier *semantics*.

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119.

Sections of the agentic harness specification are cited as **harness §N**, sections of the core specification as **core §N**, sections of the utilities specification as **util §N**, and sections of this document as plain **§N**. Invariants defined here are numbered **S1, S2, …**, kept apart from the core catalogue (core §18.5 #1–#22), the utilities catalogue (util §6 U1–U2), and the harness catalogue (harness §11 H1–H8).

> **Design stance.** A sandbox that offers everything offers too much: most tool calls read and write files, and granting them a shell grants the kernel's attack surface for free (the lesson of Cloudflare's *Project Think* execution ladder — "most tasks never leave Tier 0"). The tier model makes capability proportional to need. Each tier names one additional dangerous thing — guest code, network, a browser, native processes — and a session acquires it only by a journaled escalation. The strongest control in the scheme is frequency reduction: the tiers where kernel attack surface exists at all run rarely, and the journal records when. Confinement itself is mandated as a *property* per tier, never as a product: what removes a class of escape is required; which technology removes it is the provider's choice (harness §1.1).

---

## 1. Scope and layering

This document specifies the semantics of the five tiers the harness declares (harness §5.2), the obligations a `SandboxProvider` (harness §5.3) MUST meet to offer each, and the conformance catalogue (§6) those obligations roll up to. It adds **no seam method and no record**: everything journaled — `TierEscalated`, `WorkspaceReset`, tool outcomes — is harness vocabulary (harness §5.5, §5.6), and the seam shape (one provider, one `Sandbox` per activation, harness H8) is unchanged. A provider conforming to this document is, to the harness, just an implementation of the same two traits.

### 1.1 Non-goals

- **An isolation-technology mandate.** Obligations here are properties (a path is unrepresentable; egress is default-deny), never products (cap-std, wasmtime, bubblewrap, Firecracker — named as worked examples only). The harness non-goal (harness §1.1) holds: *how* remains the provider's choice.
- **A permission system.** The tier cap is static, fixed at deployment (harness §5.3 item 4); the dynamic per-call authorization hook remains future work (harness §13).
- **Ingress.** No tier accepts inbound connections in v1; a sandboxed server is reachable only from inside its own environment. Exposing one is future work (§7).

---

## 2. The tier model

### 2.1 Tiers are capability sets

A tier names what a call may touch. Each grants one additional capability over the workspace and withholds the rest:

| Tier | Grants | Withholds |
|---|---|---|
| `Workspace` | the session's scoped filesystem, through host-implemented typed tools | guest code; network; ambient clock and entropy |
| `Compute` | arbitrary guest code over the workspace | network; ambient clock, entropy, OS identity |
| `Network` | compute, plus egress to the profile's allowlist (harness §5.3 item 4) | egress beyond the allowlist; all ingress |
| `Browser` | a remote browser peripheral, driven through host functions | local browser processes; the session's own network egress (§3.4 states the peripheral's honestly) |
| `Native` | OS processes and native binaries inside the confined environment | everything outside the environment |

The names are deliberately not numbers. Four of the five form an inclusion chain — `Workspace ⊂ Compute ⊂ Network ⊂ Native` — but `Browser` sits beside it, above `Workspace` and comparable to nothing else: it grants a peripheral, not a superset of compute.

### 2.2 Order and the cap

Because `Browser` breaks the chain, the kind's **tier cap** (harness §5.3 item 4) is a *set* of tiers, not a maximum: `{Workspace, Browser}` is a meaningful cap no "highest tier" could express. Where the chain does order two tiers, holding the greater MUST imply the lesser's grants (a `Native` environment can read its workspace); it does not imply the lesser's *withholdings* are re-imposed (a `Network` escalation does not un-grant compute).

### 2.3 Escalation, provider-side

The harness journals a `TierEscalated` record before the first call at an unheld tier and passes every call's declared tier to `Sandbox::call` (harness §5.6, §6.4). The provider's half of that discipline:

1. **Granted only as journaled.** A provider MUST NOT execute a call at a tier the harness did not pass, and the harness passes only declared, cap-checked tiers — so a conforming pair grants nothing the journal does not show (S4).
2. **Provisioned lazily.** A tier's environment is built on the first call carrying it (harness §5.3 item 1); an idle or never-escalating session holds only its workspace. Providers MAY pool or pre-warm environments across sessions (§7), provided no working state leaks between sessions (harness H8).
3. **Additive, activation-scoped.** Held tiers only grow within an activation and all die with it: `release` MUST tear down every provisioned tier's environment and remains idempotent (S5). A fresh activation re-escalates from `Workspace` under new records (harness §5.5).

---

## 3. Provider obligations per tier

### 3.1 `Workspace`: confinement by capability handle

The workspace MUST be reachable only through pre-opened directory handles (openat-style; cap-std's `Dir` is the worked example): a path outside the session's workspace is then *unrepresentable*, not merely rejected. String sanitization and canonicalize-then-check are **non-conforming** implementations of this obligation — they filter a representable escape rather than remove the representation, and the symlink/TOCTOU class survives filtering. Workspace tools take no ambient clock or entropy; they are pure functions of the call and the filesystem, which is what lets them run unmodified under the deterministic simulator (core §18.1).

### 3.2 `Compute`: hermetic guests

A compute environment executes guest code with no ambient wall clock, no ambient entropy, no network, and no OS identity: every capability the guest holds enters through a host function the provider chose to expose, and the filesystem it sees is the workspace of §3.1 and nothing else. Its outputs are therefore a function of the call, the workspace, and an injected seed. A conforming compute engine MUST be runnable inside the deterministic simulator — this is core §18.1's discipline applied below the seam, and it is the property that makes "deterministic" checkable rather than aspirational. Resource limits (memory, fuel or epoch-bounded CPU) are REQUIRED; their values are profile configuration (harness §5.3 item 4).

### 3.3 `Network`: granted egress only

Egress is **default-deny**. The only reachable hosts are those of the profile's allowlist — the one the session's `TierEscalated` record granted by reference (harness §5.3 item 4); the provider MUST NOT widen it per call or per session. Egress MUST be attributable to its session (per-session source identity, proxy, or namespace — provider's choice), so that an operator can answer "which session reached this host" from provider logs plus the journal. Ingress remains withheld (§1.1).

### 3.4 `Browser`: an honest peripheral

The browser is a remote peripheral driven through host functions (navigate, read, screenshot, fill); the session holds no browser process and no raw socket to one. The peripheral's own network activity — the pages it fetches, the scripts those pages run — happens at the peripheral, outside the session's egress story, and a provider MUST present it that way: offering `Browser` does not imply `Network`-tier containment of what the browser touches, and a deployment that needs that containment must police it at the peripheral. This honesty clause is the reason `Browser` sits beside the chain, not on it (§2.1).

### 3.5 `Native`: confinement and the threat model

A native environment runs OS processes with the full expressiveness that implies, so its obligation is stated as blast radius: an effect of a native tool MUST be confined to the environment — filesystem outside the workspace mounts, processes outside the environment's lifetime, network beyond what the held tiers grant, all unreachable. That is the by-construction leg of harness H8, and it is where the threat model earns its keep: the code running here is model-composed under inputs the operator did not author, which prompt injection makes attacker-influenced. Treat it as hostile-code execution.

- Production deployments SHOULD confine native environments by OS mechanism — user namespaces, seccomp, Landlock; bubblewrap is the assembled worked example — or by hardware virtualization (a microVM; Firecracker is the worked example). The grading is deliberate: shared-kernel confinement is SHOULD-grade because its guarantee is priced by kernel privilege-escalation bugs (the guest still speaks to the host kernel); a microVM converts that residual into a second, far smaller escape problem, at provisioning cost. Multi-tenant deployments SHOULD NOT rely on shared-kernel confinement alone.
- A development or single-tenant provider MAY run native environments unconfined, and MUST then document itself as **trusted-input only** — the standalone deployment's `LocalSandboxes` is exactly this, and says so (its module header and the deployment guide's limitations both carry the warning).
- Either way, the journal already bounds exposure: `Native` runs only where a declared tool requires it, under a cap the kind agreed to, after a journaled escalation (harness §5.6) — frequency reduction is part of the defense, not a substitute for it.

---

## 4. Loss and honesty per tier

Harness §5.5 specifies the records; this section specifies the provider conduct beneath them, at two grains:

- **Whole-environment loss** (crash, eviction, idle release): the next activation opens a fresh sandbox, the harness journals `WorkspaceReset`, and the held set restarts at `Workspace`. The provider MUST NOT carry tier grants across that boundary — a pooled or pre-warmed environment handed to a fresh activation starts at `Workspace` regardless of what its previous tenant held.
- **Single-tier loss** (a compute engine dies; the egress proxy is gone) while the sandbox survives: the calls that needed the tier fail as `ToolError` outcomes (harness §5.4) — never a run failure, never a silent re-grant. The provider MAY re-provision the tier lazily under the escalation this activation already journaled; it MUST NOT hold a session at a tier its activation never journaled.

The asymmetry is deliberate: losing everything is a journaled reset the model is told about; losing one tier is a tool failure the model reacts to. Both keep the journal ahead of the world.

---

## 5. Relation to the harness specification

| Concern | Owner |
|---|---|
| `Tier` enum, `ToolDecl.tier`, registry allowlist | harness §5.2 |
| `Sandbox::call(tier, …)`, lazy open, per-activation binding (H8) | harness §5.3 |
| Tier cap and the profile's egress allowlist | harness §5.3 item 4 |
| Escalation discipline, `TierEscalated`, write-ahead position | harness §5.6, §6.4 |
| `WorkspaceReset`, dangling calls, divergence honesty | harness §5.5 |
| Digest coverage (cluster-wide agreement on tiers and cap) | harness §7.1 |
| Tier grants and withholdings, the cap's set shape | §2 |
| Per-tier provider obligations and the threat model | §3 |
| Provider conduct under loss | §4 |
| S-catalogue | §6 |

Nothing here changes the seam: a pre-tier provider that executes every call in one environment is a degenerate conforming provider for a kind whose tools all declare one tier and whose cap is that singleton.

---

## 6. Conformance

The catalogue mirrors the sibling catalogues (core §18.5, util §6, harness §11) in form.

| # | Invariant | Defined in | Verified by |
|---|---|---|---|
| S1 | **Workspace confinement.** At every tier, no path outside the session's workspace is representable to a tool or a guest: confinement is by capability handle, never by sanitization of a representable escape. | §3.1, §3.2 | by construction of the provider (handle-scoped filesystem); adversarial traversal scenario tests *(future)* |
| S2 | **Compute hermeticity.** A `Compute` environment takes no ambient clock, entropy, network, or OS identity; its outputs are a function of the call, the workspace, and the injected seed. | §3.2 | by construction (the capability is not exposed); differential run under the deterministic simulator *(future)* |
| S3 | **Granted egress only.** A `Network` environment's egress reaches only hosts on the allowlist its journaled escalation granted; default-deny; per-session attributable. | §3.3, §2.3 | egress audit scenario tests *(future)*; by construction where the provider owns the dataplane |
| S4 | **Journaled, monotone, capped escalation.** A `TierEscalated` record precedes the first effect at its tier within the activation; the held set only grows within an activation, never crosses an activation boundary, and never leaves the kind's cap. | §2.3, §4; harness §5.6, §6.4 | journal audit at quiescence (H2-style); scripted-sandbox scenario tests *(future)* |
| S5 | **Per-tier release.** `release` tears down every provisioned tier's environment and is idempotent; deactivation releases all held tiers (the per-tier face of harness H8). | §2.3; harness §5.3 | provider open/release accounting in scenario tests *(future)*; the existing H8 checker covers the binding window |

Stated honestly: no production tier provider exists yet. The harness ships the scripted sandbox (simulation) and the standalone deployment an unconfined dev provider, so the entries marked *(future)* name the intended method in the established vocabulary (core §17's verification kinds), not an existing test. The catalogue becomes machine-readable — an `s_catalogue()` guarded by the same drift-test pattern as its siblings — with the first provider implementation. Spec-first is the house method: the harness specification itself landed before the harness.

---

## 7. Future work

- **Warm pools and snapshots, per tier.** Pooling compute isolates and snapshot-restoring native microVMs (the provisioning-latency answer §3.5's grading alludes to); the harness-side hooks are already future work there (harness §13).
- **Ingress.** Exposing a sandboxed server (preview deployments, callback receivers) behind explicit, journaled grants; today all ingress is withheld (§1.1).
- **Permission-hook interplay.** When the dynamic per-call authorization hook lands (harness §13), escalation is its natural coarse grain: approve-once-per-tier against the journaled `TierEscalated`, rather than per call.
- **A browser provider.** §3.4 specifies the honesty contract; no provider design exists.
- **Tier-attributed accounting.** Provider cost by tier and session, joined to the journal's escalations; part of the multi-tenant economics the harness defers (harness §1.1).
