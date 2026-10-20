# Sandbox Execution Tiers: Specification

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, and **MAY** carry the meanings defined in RFC 2119. Invariants defined here are **S1–S5** (§7).

## 1. Scope

A sandbox provider supplies the isolated environments in which the agent runtime's tools run, behind the runtime's sandbox seam (agent §3.3); the runtime is its only caller. This document is the provider's side: the execution tiers, what a provider guarantees at each, and how it reports loss. The seam's signature, the runtime's side (when it opens, what it journals, which tier each call carries), and the records `TierAcquired` and `WorkspaceReset` are the agent's (agent §3.3, §5.3–§5.5); this document adds no seam operation and no record.

### 1.1 Non-goals

- **No mandated isolation technology:** every obligation is a property, met by any means the provider chooses.
- **No per-call permission system:** the only gate on a tier is the kind's tier cap (agent §6.1).
- **No ingress:** no tier accepts inbound connections, and a server a tool starts is reachable only from inside its environment. The persistent machine's ingress (machine §5.1) is outside this model.

### 1.2 Conformance classes

- A **provider**: §3–§6, for every tier it offers.
- A **deployment** that chooses a provider: §5.4.

The runtime is bound by agent §3.3, §5.5, not by this document.

## 2. Model

- **Sandbox**: the isolated environment one open yields, through which every sandboxed call of one session activation runs.
- **Workspace**: the session's working directory, its workspace facet (facets §5.1), which the runtime owns, materializes before the open, and captures after each call (agent §5.3).
- **Tier**: the named capability set a sandboxed call requires and carries (§4.1).
- **Tier environment**: what a provider provisions to execute calls at one tier.
- **Guest**: code that a compute, network, or native environment executes for a call.
- **Ambient capability**: one a tool or guest reaches without the provider handing it in: the wall clock, entropy, the network, operating-system identity, a file named by path.
- **Sandbox profile**: the kind's sandbox configuration, passed at open (agent §3.3): an image the provider interprets, the **tier cap** (§4.2), the **egress allowlist** of hosts the network tier can reach, and compute resource bounds.
- **Held set**: the tiers the current activation has acquired, each through a journaled `TierAcquired` (agent §5.5), which grants the egress allowlist by reference.
- **Environment loss**: the sandbox itself is gone (its workspace directory or its state has vanished). **Single-tier loss**: one tier environment is gone while the sandbox survives.

## 3. Seams

### 3.1 The provider (`SandboxProvider`, `Sandbox`)

A plug point the agent runtime owns, with its signature and the runtime's side in agent §3.3; the runtime opens one sandbox per session activation. `open` takes the session id, the sandbox profile, and the workspace directory, and yields a sandbox or a `SandboxError`; `call` takes a tier, a tool name, and an input, and yields a value or a `ToolError`; `release` tears the sandbox down. A provider:

- **Leaves the workspace to its owner:** a provider MAY create the workspace when absent, and MUST NOT delete or truncate it, even on `release`.
- **Honors a grant or refuses it:** a provider MUST refuse at `open`, with a `SandboxError`, a profile whose grants it cannot honor at every tier it offers, and MUST NOT open and silently withhold a grant. So a provider whose native environments have no network path MUST refuse a non-empty egress allowlist, since `Native` carries `Network`'s grants (§4.2).
- **Runs a call at the tier passed:** `call` MUST execute at exactly the tier the runtime passes (§4.3).
- **Keeps overlapping calls apart:** a provider MUST execute the calls the runtime overlaps on one sandbox (agent §3.3) without corrupting one another; each capture (agent §5.3) records the workspace as it stands, concurrent writes included.
- **Reports loss as loss:** a call MUST resolve as `ToolError::EnvironmentLost` when the environment itself is gone (§6.2), and any other failure as another `ToolError`.
- **Releases totally:** `release` MUST tear down every provisioned tier environment, and MUST be idempotent (S5).

### 3.2 The toolset

A provider defines the tools it executes, each with a name, a tier, and an input, and publishes them as tool declarations for kinds to declare (agent §6.1).

- A provider MUST answer a call naming a tool it does not implement at the call's tier with a `ToolError`, executing nothing.
- A provider SHOULD publish only the tools it can execute as configured.

## 4. The tier model

### 4.1 Tiers are capability sets

A tier names what a call can touch, granting one capability over the workspace and withholding the rest:

| Tier | Grants | Withholds |
|---|---|---|
| `Workspace` | the session's confined filesystem, through typed tools the provider implements | guest code; network; ambient clock and entropy |
| `Compute` | arbitrary guest code over the workspace | network; ambient clock, entropy, operating-system identity |
| `Network` | compute, plus egress to the profile's allowlist | egress beyond the allowlist; all ingress |
| `Native` | operating-system processes and native binaries inside the confined environment | everything outside the environment |

The four form an inclusion chain, `Workspace ⊂ Compute ⊂ Network ⊂ Native`. Tiers are names, not ranks: only the chain orders them.

At every tier, an effect outlives its sandbox only if it lands in the workspace and the runtime captures it (agent §5.3). Everything else a tier environment holds (memory, processes, files outside the workspace) lasts at most until the sandbox is released (§4.3) or lost (§6).

### 4.2 Order and the cap

A tier cap is a set of tiers, and a tool's tier is within it only as a member: a greater member does not cover it. So a cap of {`Native`} excludes a tool declaring `Workspace`, and a cap MAY hold `Native` without `Network`. Kind registration enforces the cap (agent §6.1).

Where the chain orders two tiers, a provider MUST give an environment at the greater tier the lesser tier's grants, but not its withholdings (a network environment reaches its allowlisted hosts, which compute withholds).

### 4.3 Acquisition

The runtime journals `TierAcquired` before the first call at a tier the activation does not hold, and passes every call's declared tier (agent §5.5). The provider's half:

- **Granted only as journaled:** a provider runs each call at the tier passed (§3.1), and the runtime passes only declared, cap-checked tiers, so a conforming pair grants nothing the journal does not show (S4).
- **Provisioned on first use:** a provider provisions a tier environment on the first call at the tier, so a session that never acquires more holds only its workspace. A provider MAY pool or pre-warm environments across sessions, provided no working state passes between them (agent A7).
- **Additive and activation-scoped:** the held set only grows within an activation and dies with it at release (§3.1), so a fresh activation starts from `Workspace`.

## 5. Provider obligations per tier

### 5.1 `Workspace`: confinement by capability

A provider MUST make every path outside the workspace unrepresentable to a workspace tool: a tool reaches files only through a handle on the workspace directory, opened in advance, that resolves each path inside it, so no relative component, absolute path, or symbolic link names an outside file. Filtering a representable path (string sanitization, or canonicalizing then checking) does not conform. Workspace tools take no ambient clock or entropy: each is a pure function of the call and the workspace's contents (actor §11).

### 5.2 `Compute`: hermetic guests

A compute environment runs guest code with no ambient capability: every capability the guest holds enters through a host function the provider exposes, and the only filesystem it sees is the workspace, confined as in §5.1. Its outputs are thus a function of the call, the workspace, and a seed the provider injects. It MUST satisfy the deterministic profile (actor §11).

Bounds on each call's memory and CPU, at the profile's values, are REQUIRED, and MUST also cover what a guest can make the host spend: bytes handed to a compiler, allocations instantiation performs on the host, and any buffer a host function or the call's output materializes outside the guest. A guest-controlled integer (a length passed to a host function, a packed output size) MUST be bounded, or resolved as a view into the guest's bounded memory, before it sizes anything on the host.

### 5.3 `Network`: granted egress only

Egress is default-deny: a network environment reaches only the hosts on the profile's egress allowlist that the session's `TierAcquired` record granted, and a provider MUST NOT widen that set per call or per session. Every egress flow MUST be attributable to its session: the provider's records, joined with the journal, name the session that reached a host.

### 5.4 `Native`: confinement

A native tool's effects MUST be confined to its environment: host files outside the workspace, processes beyond the environment's lifetime, and network beyond the held tiers' grants are unreachable from it. This is the by-construction half of agent A7.

- A production deployment SHOULD confine native environments by **shared-kernel confinement** (processes on the host's kernel, restricted by it, so an escape needs a kernel privilege escalation) or **hardware isolation** (processes in a virtual machine, reaching the host only through the virtual machine monitor). A multi-tenant deployment SHOULD NOT rely on shared-kernel confinement alone.
- A provider for development or for a single tenant MAY run native environments unconfined, and MUST then document itself as fit for trusted input only.

## 6. Loss

### 6.1 Conduct under loss

An environment can vanish at any moment. A provider MUST report a loss, never absorb it: it MUST NOT answer a call that needed a lost environment from a silently rebuilt one, nor re-grant a tier the activation did not journal. The rule holds at two grains:

- **Environment loss:** the runtime journals `WorkspaceReset`, and the fresh sandbox that follows starts with the held set at `Workspace` (agent §5.4, §5.5). A provider MUST NOT carry tier grants across that boundary: a pooled or pre-warmed environment handed to a fresh sandbox starts at `Workspace`, whatever its previous tenant held.
- **Single-tier loss:** the calls that needed the tier fail as ordinary `ToolError` outcomes, never as a run failure or a silent re-grant. The provider MAY provision the tier again on first use, under the acquisition this activation already journaled, and MUST NOT hold a session at a tier its activation never journaled.

### 6.2 Detecting loss

A provider that can observe environment loss MUST report the call as `ToolError::EnvironmentLost`, even when the call itself succeeded. Removal of the workspace directory is environment loss even while the provider holds it open. Only that outcome engages the runtime's reset (agent §5.4).

### 6.3 The fault vocabulary

The answers a sandbox can give at this seam, and what holds after each, are closed:

| Fault | What the seam answers | What then holds |
|---|---|---|
| Environment loss | a call returns `ToolError::EnvironmentLost` (§6.2) | the runtime journals `WorkspaceReset`, and the held set restarts at `Workspace` (S4) |
| Single-tier loss | a call at a provisioned tier fails as an ordinary `ToolError` (§6.1) | the run continues; the tier is provisioned again only under the acquisition already journaled (S4) |
| Slow call | a call outlives the runtime's deadline | the runtime's timeout path, not a provider re-entry; a release still tears down what was provisioned (S5) |

A refused acquisition is no runtime fault: kind registration enforces the cap (§4.2), so no session carries a call at an excluded tier.

## 7. Invariants

| # | Invariant | Defined in |
|---|---|---|
| S1 | **Workspace confinement.** At every tier, no path outside the session's workspace is representable to a tool or a guest: confinement is by capability handle, never by sanitization of a representable escape. | §5.1, §5.2 |
| S2 | **Compute hermeticity.** A `Compute` environment takes no ambient clock, entropy, network, or operating-system identity; its outputs are a function of the call, the workspace, and the injected seed. | §5.2 |
| S3 | **Granted egress only.** A `Network` environment's egress reaches only hosts on the allowlist its journaled acquisition granted; default-deny; attributable per session. | §5.3, §4.3 |
| S4 | **Journaled, monotone, capped acquisition.** A `TierAcquired` record precedes the first effect at its tier within the activation; the held set only grows within an activation, never crosses an activation boundary, and never leaves the kind's cap. | §4.3, §6; agent §5.5, §4.3 |
| S5 | **Per-tier release.** A release tears down every provisioned tier's environment and is idempotent; deactivation releases all held tiers (the per-tier face of agent A7). | §4.3; agent §3.3 |

## 8. Security considerations

- **Threat model:** a model writes guest code from inputs prompt injection can steer, so running it is hostile-code execution, and the paths a workspace tool receives are equally untrusted.
- **Attack surface:** escape from the workspace by path (S1); host resources a guest makes the host spend (§5.2); ungranted or unattributable egress (S3); the host kernel beneath a native environment (§5.4); working state passing between sessions through pooled environments (§4.3).
- **Loss:** a loss absorbed rather than reported leaves the journal asserting a workspace or a grant that is gone (§6).
- **Exposure is journaled:** a tier above `Workspace` runs only for a declared tool, within the kind's cap, after a journaled acquisition (§4.3); running the kernel-exposing tiers rarely narrows exposure but does not replace confinement (§5.4).
- Who can cause sandboxed calls, by addressing a session, is the edge's concern (edge §4.2, §4.3).
