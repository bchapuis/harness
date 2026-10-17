# Sandbox Execution Tiers: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119. Sections of this document are cited as plain **§N**; other documents carry a prefix: **agent §N**, **entity §N**, **actor §N**, **machine §N**. Invariants defined here are **S1–S5** (§6).

## 1. Scope

A sandbox provider supplies the isolated environments the agent runtime's tools run in, behind the runtime's sandbox seam (agent §2.3). This document is the provider's side of that seam: what each tier grants and withholds, what a provider MUST guarantee to offer it, and how it reports loss. The runtime's side (when it opens, what it journals, what it passes) is the agent specification's (agent §2.3, §4.5). This document adds no seam method and no record: `TierAcquired`, `WorkspaceReset`, and tool outcomes are the runtime's vocabulary. The first provider is `agent-sandbox`'s `TieredSandboxes`.

**Rationale:** a sandbox that offers everything offers too much. Most calls only read and write files, and granting them a shell grants the kernel's attack surface for free. Each tier adds one dangerous capability (guest code, network, native processes), taken only through a journaled acquisition, so the tiers that expose the kernel run rarely and the journal records when. Confinement is mandated per tier as a property, not a product.

### 1.1 Non-goals

- **An isolation-technology mandate:** obligations are properties (a path is unrepresentable; egress is default-deny), not products.
- **A permission system:** the tier cap is static deployment configuration (agent §2.4).
- **Ingress:** no tier accepts inbound connections; a sandboxed server is reachable only from inside its own environment. The persistent machine accepts inbound connections through its own seam (machine §2), outside this model.

## 2. Seams

### 2.1 The provider (`SandboxProvider`, `Sandbox`)

A plug point: a provider implements it, and the agent runtime consumes it, one environment per session activation (signatures in agent §2.3). `open` receives the session id, the kind's `SandboxProfile` (a provider-interpreted image, the tier cap, the `Network` egress allowlist, and `ComputeLimits`), and the session's workspace directory.

- **The workspace is the caller's.** It is the session's workspace facet (facets §4.1), materialized by the runtime before the open. A provider MAY create it when absent, and MUST NOT delete or truncate it, on `release` or ever: its durability is the caller's, captured after each call.
- **A grant is honored or refused.** A provider MUST refuse at `open`, with a `SandboxError`, a profile whose grants it cannot honor at every tier it offers (for example a non-empty egress allowlist where its `Native` realization has no dataplane, since holding `Native` implies `Network`'s grants, §3.2). It MUST NOT open and silently withhold a grant.
- **Calls run at the tier passed.** `call` MUST execute at the tier the runtime passes and no other (§3.3).
- **Calls may overlap.** The runtime MAY issue several calls on one `Sandbox` concurrently. A provider MUST execute them without corrupting one another, and the runtime's capture after each records the directory as it then stands, concurrent writes included.
- **Loss is reported as loss.** A call MUST resolve as `ToolError::EnvironmentLost` when the environment itself is gone (§5.2), and any other failure as another `ToolError`.
- **Release is total.** `release` MUST tear down every provisioned tier's environment, and MUST be idempotent (S5).

### 2.2 The toolset (`ToolDecl`)

A sandboxed tool's name, tier, and input are the provider's: the provider defines which tools it executes at each tier and what each takes, and publishes them as `ToolDecl`s (`TieredSandboxes::tools`, assembled from `workspace_tools()`, `run_js_tool()`, and a `shell` for the configured native realization). A kind declares the provider's tools as published (agent §2.4).

- A provider MUST answer a call naming a tool it does not implement at the call's tier with a `ToolError`, executing nothing.
- A provider SHOULD publish only the tools it can execute as configured, so a kind's digest never asserts an environment the provider lacks.

## 3. The tier model

### 3.1 Tiers are capability sets

A tier names what a call may touch. Each grants one capability over the workspace and withholds the rest:

| Tier | Grants | Withholds |
|---|---|---|
| `Workspace` | the session's scoped filesystem, through host-implemented typed tools | guest code; network; ambient clock and entropy |
| `Compute` | arbitrary guest code over the workspace | network; ambient clock, entropy, OS identity |
| `Network` | compute, plus egress to the profile's allowlist | egress beyond the allowlist; all ingress |
| `Native` | OS processes and native binaries inside the confined environment | everything outside the environment |

The four form an inclusion chain, `Workspace ⊂ Compute ⊂ Network ⊂ Native`, but the names are not numbers: a peripheral tier would sit beside the chain (§7).

### 3.2 Order and the cap

A kind's tier cap is a set of tiers, checked by membership: every declared tool's tier MUST be an element of the cap, never merely covered by a greater member, so a cap of `[Native]` rejects a tool declaring `Workspace` (agent §2.4), and a cap MAY hold `Native` without `Network`. A set, unlike a maximum, also admits a peripheral tier beside the chain. Where the chain orders two tiers, holding the greater MUST imply the lesser's grants (a `Native` environment can read its workspace) but not its withholdings (acquiring `Network` does not un-grant compute).

### 3.3 Acquisition, provider side

The runtime journals `TierAcquired` before the first call at an unheld tier and passes every call's declared tier (agent §4.5). The provider's half:

1. **Granted only as journaled:** a provider MUST NOT execute a call at a tier the runtime did not pass. The runtime passes only declared, cap-checked tiers, so a conforming pair grants nothing the journal does not show (S4).
2. **Provisioned lazily:** a provider builds a tier's environment on the first call that carries it, so a session that never acquires more holds only its workspace. A provider MAY pool or pre-warm environments across sessions, provided no working state leaks between sessions (agent A7).
3. **Additive, activation-scoped:** held tiers only grow within an activation and all die with it; `release` tears down every provisioned tier (S5), and a fresh activation starts from `Workspace`.

## 4. Provider obligations per tier

### 4.1 `Workspace`: confinement by capability handle

The workspace MUST be reachable only through pre-opened directory handles (openat-style), so a path outside the session's workspace is unrepresentable, not merely rejected. String sanitization and canonicalize-then-check are non-conforming: they filter a representable escape, and the symlink and TOCTOU class survives filtering. Workspace tools take no ambient clock or entropy: they are pure functions of the call and the filesystem, so they run unmodified under the deterministic simulator (actor §2.12.2).

### 4.2 `Compute`: hermetic guests

A compute environment executes guest code with no ambient wall clock, entropy, network, or OS identity: every capability the guest holds enters through a host function the provider chose to expose, and the only filesystem it sees is §4.1's workspace. Its outputs are therefore a function of the call, the workspace, and an injected seed. A conforming compute engine MUST be runnable inside the deterministic simulator (actor §2.12.2), which makes "deterministic" checkable. Resource limits (memory and CPU) are REQUIRED; their values are profile configuration (`ComputeLimits`).

The limits MUST also cover what a guest can make the host pay: the bytes handed to a compiler (compilation precedes any execution meter), the table and instance allocations instantiation performs host-side, and any buffer a host function or the call's output materializes outside the guest. A guest-controlled integer (a length in a host-function call, a packed output size) MUST be bounded, or resolved as a view into the guest's own bounded memory, before it sizes anything on the host: an unmetered host allocation is a capability the tier never granted.

### 4.3 `Network`: granted egress only

Egress is default-deny: the only reachable hosts are those on the profile's allowlist that the session's `TierAcquired` record granted by reference, and a provider MUST NOT widen it per call or per session. Egress MUST be attributable to its session (a per-session source identity, proxy, or namespace, at the provider's choice), so provider logs plus the journal answer "which session reached this host". Ingress stays withheld (§1.1).

### 4.4 `Native`: confinement and the threat model

A native environment runs OS processes with full expressiveness, so its obligation is stated as blast radius: a native tool's effects MUST be confined to the environment. Filesystem outside the workspace mounts, processes outside the environment's lifetime, and network beyond what the held tiers grant are unreachable: the by-construction leg of agent A7. The code here is composed by a model from inputs prompt injection makes attacker-influenced, so it is hostile-code execution.

- Production deployments SHOULD confine native environments by OS mechanism (user namespaces, seccomp, Landlock) or by hardware virtualization (a microVM). Shared-kernel confinement is SHOULD-grade because kernel privilege-escalation bugs price its guarantee; a microVM turns that residual into a second, far smaller escape problem, at provisioning cost. Multi-tenant deployments SHOULD NOT rely on shared-kernel confinement alone.
- A development or single-tenant provider MAY run native environments unconfined, and MUST then document itself as trusted-input only.
- Either way, the journal bounds exposure: `Native` runs only where a declared tool requires it, under a cap the kind agreed to, after a journaled acquisition. Frequency reduction is part of the defense, not a substitute for it.

## 5. Loss

### 5.1 Conduct under loss

An environment can vanish at any moment (a crash, an eviction, an idle release), and the loss MUST be reported, never absorbed: a provider that quietly rebuilds, silently re-grants, or lets a loss pass as an ordinary failure undoes every record above it. The rule holds at two grains:

- **Whole-environment loss:** the next activation opens a fresh sandbox, the runtime journals `WorkspaceReset`, and the held set restarts at `Workspace`. A provider MUST NOT carry tier grants across that boundary: a pooled or pre-warmed environment handed to a fresh activation starts at `Workspace`, whatever its previous tenant held.
- **Single-tier loss** (a compute engine dies, a container is gone) while the sandbox survives: the calls that needed the tier fail as ordinary `ToolError` outcomes, never as a run failure or a silent re-grant. The provider MAY re-provision the tier lazily under the acquisition this activation already journaled, and MUST NOT hold a session at a tier its activation never journaled.

### 5.2 Detecting loss

A provider that can observe that the environment itself is gone (the workspace's backing directory deleted, the engine's state vanished) MUST report the call as `ToolError::EnvironmentLost`, even when the call itself succeeded. Only that outcome engages the runtime's reset (agent §4.4); an ordinary failure would leave the model retrying against vanished state while the journal asserts a workspace the world lost. An open handle keeps a deleted directory's descriptor alive, so only an ambient check of the path, not a read through the handle, witnesses the deletion.

### 5.3 The fault vocabulary

What a `Sandbox` can answer at this seam is closed, and so is what must survive each answer:

| Fault | What the seam answers | What must survive |
|---|---|---|
| Environment loss | a call returns `EnvironmentLost` (§5.2) | the runtime journals `WorkspaceReset`, and the held set restarts at `Workspace` (S4) |
| Single-tier loss | a call at a provisioned tier fails as an ordinary `ToolError` (§5.1) | the run continues; the tier is re-provisioned only under the acquisition already journaled (S4) |
| Slow call | a call outlives the runtime's deadline | the runtime's timeout path, not a provider re-entry; `release` still tears down what was provisioned (S5) |

A refused acquisition is not a runtime fault: a tool whose tier the cap excludes is rejected at registration (agent §2.4), so no session carries a call at it.

## 6. Invariants

The Rust copy of this table, with the suites that verify each row, is `agent-sandbox`'s conformance catalogue.

| # | Invariant | Defined in |
|---|---|---|
| S1 | **Workspace confinement.** At every tier, no path outside the session's workspace is representable to a tool or a guest: confinement is by capability handle, never by sanitization of a representable escape. | §4.1, §4.2 |
| S2 | **Compute hermeticity.** A `Compute` environment takes no ambient clock, entropy, network, or OS identity; its outputs are a function of the call, the workspace, and the injected seed. | §4.2 |
| S3 | **Granted egress only.** A `Network` environment's egress reaches only hosts on the allowlist its journaled acquisition granted; default-deny; attributable per session. | §4.3, §3.3 |
| S4 | **Journaled, monotone, capped acquisition.** A `TierAcquired` record precedes the first effect at its tier within the activation; the held set only grows within an activation, never crosses an activation boundary, and never leaves the kind's cap. | §3.3, §5; agent §4.5, §3.3 |
| S5 | **Per-tier release.** `release` tears down every provisioned tier's environment and is idempotent; deactivation releases all held tiers (the per-tier face of agent A7). | §3.3; agent §2.3 |

## 7. Deferred

- **Warm pools and snapshots:** every activation provisions its tiers cold (a microVM boots per activation); memory snapshots would also warm-resume a persistent machine (facets §4.4).
- **A network dataplane:** no provider offers `Network`, so S3 is unexercised and the reference provider refuses a profile that names egress.
- **Ingress:** a sandboxed server is unreachable from outside; journaled grants could fold in a seam like the machine's (machine §2).
- **Permission-hook interplay:** when a per-call authorization hook lands (agent §11), a journaled `TierAcquired` is its natural grain.
- **Peripheral tiers:** a tier granting a device (a browser) beside the chain; its own network activity would escape the session's egress, and each is a versioned change to `Tier`.
- **Cooperative compute timeouts:** a guest runs to completion under its CPU bound, since yielding or interrupting needs a simulator-owned clock source (actor §2.12.2).
- **Tier-attributed accounting:** provider cost by tier and session is not joined to the journal's acquisitions.
