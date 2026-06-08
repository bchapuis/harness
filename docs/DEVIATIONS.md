# Intentional Deviations

This file tracks divergences between the implementation and a literal reading of
[`distributed-actor-spec.md`](./distributed-actor-spec.md). **Active deviations**
are ones the project knowingly carries (the "DOCUMENT-BOTH" outcome); **Resolved**
records divergences that have since been reconciled — by amending the spec, fixing
the implementation, or both — so the rationale isn't lost to git history.

Most spec/impl reconciliations leave nothing here: where the spec was amended to
bless a sound implementation choice (concrete `ActorId`, `register` on `Actor`,
infallible `resolve`, `-> impl Future` hooks, …), the agreement now lives in the
spec text itself, often marked with a `> **… (non-normative).**` note. Only the
*judgment calls worth remembering* — the two below — are logged in **Resolved**.

Reconciliation rubric (for context):

- **R1** safety MUST violated → fix the implementation.
- **R2** Rust-forced & safety-neutral → amend the spec.
- **R3** sound extra surface at SHOULD/MAY → amend the spec.
- **R4** two valid readings → **document both (this file)**.
- **R5** spec right, impl weaker on a SHOULD → fix the implementation (may defer).

A deviation row is **debt**: it should carry a resolution direction and shrink over
time, not ossify. A row only stays permanently if both forms are genuinely
equivalent in every property that matters.

## Active deviations

_None._ Both formerly-documented R4 items were reconciled by judging which side
was **sounder** and amending the spec to match the implementation (see below).

## Resolved

| Date | Area | Resolution | Why this was the soundest direction |
|---|---|---|---|
| 2026-06-08 | §7 transport: trait shape + "transport MUST report association loss" (rule 4) | **Amended spec** to the impl's `send`/`shutdown` + construction-channel shape, and downgraded rule 4 to MAY (optional optimization). | SWIM is *designed* to decide liveness by probing independent of connection state (Das et al.); coupling failure detection to transport/socket events is the very thing it avoids, so SWIM-authoritative detection (§10) is the sounder design. The `send`/`shutdown` shape is also more idiomatic than returning single-consumer streams from `&self`. |
| 2026-06-08 | §9.2(3) convergence = "all `up` members agree on the set" (seen-by) | **Amended spec** to relax "converged" to the leader's locally-stable, fully-reachable view (the impl's model). | §1.2 declares the system has **no consensus** and is **eventually consistent**; a seen-by/vector-clock agreement is a consensus property that contradicts the stated model. Safety comes from the lattice (monotonic `up`, terminal/leader-gated `down`), not global agreement. |
| 2026-06-08 | §10 `T_suspect` does not scale with cluster size (SHOULD) | **Fixed impl**: `tick` now scales the suspicion window logarithmically (`max(1, floor(log2(n)))`), base for ≤3 members. | The spec is right — scaling the suspicion window with size is standard SWIM/Lifeguard practice to bound the false-positive rate. Small/clean fix; small clusters keep the base timeout so test timing is unaffected. |
| 2026-06-08 | §11.2 backoff has no jitter (RECOMMENDED) | **Fixed impl**: equal-jitter applied to the restart backoff, randomness drawn from the system's seeded `Entropy` via a new `ActorSystem::next_random`. | Jitter genuinely desynchronizes restart storms, and the MUST (backoff supported) was already met. Sourced from the one seeded `Entropy` (no second PRNG), so §18.2 reproducibility holds; drawn only on a real jittered sleep so it never perturbs other runs. |
| 2026-06-08 | §18.1 no static enforcement of "no ambient nondeterminism" (SHOULD) | **Fixed impl**: added a `clippy.toml` `disallowed-methods` lint forbidding wall-clock / OS-thread / unseeded-RNG APIs in the runtime-agnostic crates; `actor-runtime` is the one allowed boundary. | The spec is right and the guard-rail is cheap; it turns a convention into a compile-time barrier. |
| 2026-06-08 | §18.5 intro claimed all 21 invariants are "checked continuously" | **Amended spec** to describe layered verification — safety properties as continuous checkers, the rest as conformance / compile-fail / differential, recorded in the catalogue. | Most invariants are not true continuous-safety properties over the event stream (e.g. re-watch legitimately yields a second `Terminated`, so "exactly-once per pair" is not invariant); forcing them adds false-positive risk for no gain. The layered framing matches §18.6 and the catalogue SSOT. |
| 2026-06-08 | §18.3 corruption / stale-gossip / handler-fault injectors | **Amended spec** to note these are realized by mechanisms that fit the layer (byte-corruption at the real-bytes TCP layer; stale/replayed gossip from the transport fault set; handler/`started` faults via workload actors). | The capabilities are exercised; the in-memory sim carries structured frames (nothing to bit-flip), so demanding a bespoke injector for each would test plumbing, not behavior. |
| 2026-06-08 | §18.6 mailbox/executor model-checking (SHOULD) | **Amended spec** to mark loom/kani as an optional cross-check. | The simulator already drives the executor on a single-thread scheduler with seed-randomized ready-task selection, so it explores interleavings deterministically and reproducibly; a separate model-checker is complementary, not a prerequisite. |
| 2026-06-08 | §16 metrics & cross-node tracing (RECOMMENDED) | **No change** — already conformant. | RECOMMENDED, not MUST; the §16 note already records them as open, and the structured event stream (which can drive them) is complete. |
| 2026-06-08 | Post-cleanup signature re-verification (§3.1.1, §3.4, §4.1, §7.1, App. A) | **Amended spec** to fix small inaccuracies the clean-up's hand-edited signatures introduced: `resolve` takes `ActorId` by value (not `&ActorId`); `deliver` is an internal host op, not an `ActorSystem` trait method; the wire `Envelope.manifest` is an owned `String`; `Ctx::spawn_with` and `watch`'s `Handler<Terminated>` bound added; `Actor::register`/`supervision` consolidated into the §3.1.1 trait; App. A `lookup` is sync (no `.await`), `register` takes `&ActorRef`. | A full re-audit (3 parallel sweeps) found the implementation sound in every case; only the freshly hand-written spec text was off, so the spec moved to match the code exactly. |
