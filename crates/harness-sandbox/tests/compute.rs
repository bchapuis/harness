//! Compute-tier conformance (feature `compute`): S2 (hermeticity and
//! determinism — same call + workspace + seed → byte-identical results) of
//! sandbox spec §6, plus the guest ABI and the lazy build/release
//! accounting.

#![cfg(feature = "compute")]

mod support;

use std::path::PathBuf;
use std::sync::Arc;

use futures::executor::block_on;
use harness::ComputeLimits;
use harness::Sandbox;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use harness_sandbox::TieredSandboxes;
use serde_json::Value;
use serde_json::json;

/// A guest exercising the whole host surface: it stores the injected seed
/// little-endian at offset 0, writes those 8 bytes to `seed.bin` via
/// `ws_write`, and returns the static JSON at offset 32.
const SEED_WRITER: &str = r#"
(module
  (import "harness" "seed" (func $seed (result i64)))
  (import "harness" "ws_write" (func $ws_write (param i32 i32 i32 i32) (result i64)))
  (memory (export "memory") 1)
  (data (i32.const 16) "seed.bin")
  (data (i32.const 32) "{\"ok\":true}")
  (global $bump (mut i32) (i32.const 1024))
  (func (export "alloc") (param i32) (result i32)
    (local i32)
    global.get $bump
    local.set 1
    global.get $bump
    local.get 0
    i32.add
    global.set $bump
    local.get 1)
  (func (export "run") (param i32 i32) (result i64)
    i32.const 0
    call $seed
    i64.store
    i32.const 16
    i32.const 8
    i32.const 0
    i32.const 8
    call $ws_write
    drop
    ;; (32 << 32) | 11: the static output above
    i64.const 137438953483))
"#;

/// A guest echoing its input back as its output.
const ECHO: &str = r#"
(module
  (memory (export "memory") 1)
  (global $bump (mut i32) (i32.const 1024))
  (func (export "alloc") (param i32) (result i32)
    (local i32)
    global.get $bump
    local.set 1
    global.get $bump
    local.get 0
    i32.add
    global.set $bump
    local.get 1)
  (func (export "run") (param i32 i32) (result i64)
    local.get 0
    i64.extend_i32_u
    i64.const 32
    i64.shl
    local.get 1
    i64.extend_i32_u
    i64.or))
"#;

/// A guest copying `in.txt` to `out.txt` through `ws_read`/`ws_write`.
const COPIER: &str = r#"
(module
  (import "harness" "ws_read" (func $ws_read (param i32 i32 i32 i32) (result i64)))
  (import "harness" "ws_write" (func $ws_write (param i32 i32 i32 i32) (result i64)))
  (memory (export "memory") 1)
  (data (i32.const 8) "{}")
  (data (i32.const 16) "in.txt")
  (data (i32.const 24) "out.txt")
  (global $bump (mut i32) (i32.const 1024))
  (func (export "alloc") (param i32) (result i32)
    (local i32)
    global.get $bump
    local.set 1
    global.get $bump
    local.get 0
    i32.add
    global.set $bump
    local.get 1)
  (func (export "run") (param i32 i32) (result i64)
    (local $n i64)
    i32.const 16
    i32.const 6
    i32.const 2048
    i32.const 256
    call $ws_read
    local.set $n
    i32.const 24
    i32.const 7
    i32.const 2048
    local.get $n
    i32.wrap_i64
    call $ws_write
    drop
    ;; (8 << 32) | 2: "{}"
    i64.const 34359738370))
"#;

/// A guest that spins forever: only fuel stops it.
const FUEL_BOMB: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) i32.const 0)
  (func (export "run") (param i32 i32) (result i64)
    (loop $spin br $spin)
    i64.const 0))
"#;

/// A guest asking for a capability outside the defined surface (`harness`
/// plus the deterministic WASI stubs): it must not even instantiate.
const AMBIENT_WANTER: &str = r#"
(module
  (import "env" "fetch" (func $f (param i32 i32) (result i32)))
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) i32.const 0)
  (func (export "run") (param i32 i32) (result i64) i64.const 0))
"#;

/// One opened sandbox plus the ambient path of its workspace directory, so
/// tests can plant binary fixtures (the typed `write_file` tool is UTF-8;
/// planting bytes is the test's job, with the test's ambient authority).
struct Bench {
    sandbox: Arc<dyn Sandbox>,
    dir: PathBuf,
    _root: tempfile::TempDir,
}

fn bench(seed: u64, profile: &SandboxProfile) -> (TieredSandboxes, Bench) {
    bench_custom(seed, profile, |provider| provider)
}

/// A bench whose provider the test customizes first (registered modules).
fn bench_custom(
    seed: u64,
    profile: &SandboxProfile,
    customize: impl FnOnce(TieredSandboxes) -> TieredSandboxes,
) -> (TieredSandboxes, Bench) {
    let root = tempfile::tempdir().expect("tempdir");
    let provider = customize(
        TieredSandboxes::new(root.path())
            .expect("provider")
            .with_seed(seed),
    );
    let sandbox = block_on(provider.open(&SessionId::new("s"), profile)).expect("workspace open");
    let dir = std::fs::read_dir(root.path())
        .expect("root")
        .next()
        .expect("session dir")
        .expect("entry")
        .path();
    (
        provider,
        Bench {
            sandbox,
            dir,
            _root: root,
        },
    )
}

fn plant(bench: &Bench, name: &str, wat: &str) {
    std::fs::write(bench.dir.join(name), wat::parse_str(wat).expect("wat")).expect("plant");
}

fn run(bench: &Bench, input: Value) -> Result<Value, ToolError> {
    block_on(bench.sandbox.call(Tier::Compute, "run_module", input))
}

// ---------------------------------------------------------------------------
// S2: determinism — a function of the call, the workspace, and the seed
// ---------------------------------------------------------------------------

#[test]
fn same_call_workspace_and_seed_reproduce_byte_identically() {
    let profile = SandboxProfile::default();
    let observe = |seed: u64| {
        let (_provider, bench) = bench(seed, &profile);
        plant(&bench, "job.wasm", SEED_WRITER);
        let out = run(&bench, json!({"module": "job.wasm"})).expect("run");
        let written = std::fs::read(bench.dir.join("seed.bin")).expect("seed.bin");
        (out, written)
    };

    let (out_a, seed_a) = observe(42);
    let (out_b, seed_b) = observe(42);
    assert_eq!(out_a, json!({"ok": true}));
    assert_eq!(out_a, out_b, "same seed, same output (S2)");
    assert_eq!(seed_a, seed_b, "same seed, same workspace effect (S2)");

    let (_, seed_c) = observe(43);
    assert_ne!(seed_a, seed_c, "the seed is genuinely injected");
}

#[test]
fn the_guest_receives_its_input_and_returns_its_output() {
    let (_provider, bench) = bench(7, &SandboxProfile::default());
    plant(&bench, "echo.wasm", ECHO);
    let payload = json!({"answer": 42, "list": [1, 2, 3]});
    let out = run(&bench, json!({"module": "echo.wasm", "input": payload})).expect("run");
    assert_eq!(out, payload);
}

#[test]
fn guests_reach_the_workspace_through_host_functions_only() {
    let (_provider, bench) = bench(7, &SandboxProfile::default());
    plant(&bench, "copy.wasm", COPIER);
    std::fs::write(bench.dir.join("in.txt"), "via the handle").expect("in.txt");
    run(&bench, json!({"module": "copy.wasm"})).expect("run");
    assert_eq!(
        std::fs::read_to_string(bench.dir.join("out.txt")).expect("out.txt"),
        "via the handle"
    );
}

// ---------------------------------------------------------------------------
// S2: hermeticity and the REQUIRED resource bound
// ---------------------------------------------------------------------------

#[test]
fn a_runaway_guest_is_stopped_by_fuel_deterministically() {
    let profile = SandboxProfile {
        compute: ComputeLimits {
            fuel: 100_000,
            ..ComputeLimits::default()
        },
        ..SandboxProfile::default()
    };
    let observe = || {
        let (_provider, bench) = bench(7, &profile);
        plant(&bench, "bomb.wasm", FUEL_BOMB);
        run(&bench, json!({"module": "bomb.wasm"}))
    };
    let a = observe();
    let b = observe();
    assert!(
        matches!(&a, Err(ToolError::Sandbox(e)) if e.contains("fuel")),
        "fuel is the bound (§3.2), got {a:?}"
    );
    assert_eq!(a, b, "the trap reproduces — fuel metering is deterministic");
}

#[test]
fn a_module_importing_beyond_the_defined_surface_fails_to_instantiate() {
    let (_provider, bench) = bench(7, &SandboxProfile::default());
    plant(&bench, "ambient.wasm", AMBIENT_WANTER);
    let out = run(&bench, json!({"module": "ambient.wasm"}));
    assert!(
        matches!(&out, Err(ToolError::Sandbox(e)) if e.contains("instantiate")),
        "the capability is absent, not filtered (§3.2), got {out:?}"
    );
}

// ---------------------------------------------------------------------------
// Lazy build and per-tier release (sandbox spec §2.3 item 2, S5)
// ---------------------------------------------------------------------------

#[test]
fn the_compute_engine_is_built_on_first_use_and_dropped_on_release() {
    let (provider, bench) = bench(7, &SandboxProfile::default());
    plant(&bench, "echo.wasm", ECHO);
    assert_eq!(
        provider.stats.compute_built(),
        0,
        "opening grants Workspace and nothing else (§5.6 item 1)"
    );
    run(&bench, json!({"module": "echo.wasm", "input": 1})).expect("run");
    run(&bench, json!({"module": "echo.wasm", "input": 2})).expect("run");
    assert_eq!(
        provider.stats.compute_built(),
        1,
        "one engine per sandbox, built lazily"
    );
    block_on(bench.sandbox.release());
    assert_eq!(provider.stats.released(), 1);
}

// ---------------------------------------------------------------------------
// Guest-induced host costs are bounded (sandbox spec §3.2, second paragraph)
// ---------------------------------------------------------------------------

/// A guest claiming a 4 GiB output: the packed length must be capped before
/// the host materializes anything.
const OUTPUT_BOMB: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) i32.const 0)
  (func (export "run") (param i32 i32) (result i64)
    ;; (0 << 32) | 0xFFFF_FFFF
    i64.const 4294967295))
"#;

/// A guest whose table declaration alone would cost the host gigabytes at
/// instantiation, before the first instruction runs.
const TABLE_BOMB: &str = r#"
(module
  (table 100000000 funcref)
  (memory (export "memory") 1)
  (func (export "alloc") (param i32) (result i32) i32.const 0)
  (func (export "run") (param i32 i32) (result i64) i64.const 0))
"#;

#[test]
fn an_oversized_output_claim_is_refused_before_allocation() {
    let (_provider, bench) = bench(7, &SandboxProfile::default());
    plant(&bench, "bomb.wasm", OUTPUT_BOMB);
    let out = run(&bench, json!({"module": "bomb.wasm"}));
    assert!(
        matches!(&out, Err(ToolError::Sandbox(e)) if e.contains("over the")),
        "a guest-controlled output length must be capped (§3.2), got {out:?}"
    );
}

#[test]
fn a_table_bomb_fails_at_instantiation_within_limits() {
    let (_provider, bench) = bench(7, &SandboxProfile::default());
    plant(&bench, "bomb.wasm", TABLE_BOMB);
    let out = run(&bench, json!({"module": "bomb.wasm"}));
    assert!(
        matches!(&out, Err(ToolError::Sandbox(e)) if e.contains("instantiate")),
        "instantiation-time host allocations are limited (§3.2), got {out:?}"
    );
}

#[test]
fn an_oversized_module_is_refused_before_compilation() {
    let (_provider, bench) = bench(7, &SandboxProfile::default());
    // 16 MiB + 1 of garbage: refused by size before the compiler sees it —
    // compilation runs before fuel exists to meter it (§3.2).
    std::fs::write(bench.dir.join("big.wasm"), vec![0u8; 16 * 1024 * 1024 + 1]).expect("plant");
    let out = run(&bench, json!({"module": "big.wasm"}));
    assert!(
        matches!(&out, Err(ToolError::Sandbox(e)) if e.contains("cap")),
        "module bytes are bounded before compilation (§3.2), got {out:?}"
    );
}

// ---------------------------------------------------------------------------
// Registered modules, the module cache, and run_js routing
// ---------------------------------------------------------------------------

#[test]
fn registered_modules_win_over_workspace_paths() {
    let echo = wat::parse_str(ECHO).expect("wat");
    let (_provider, bench) = bench_custom(7, &SandboxProfile::default(), |p| {
        p.with_module("mod.wasm", echo)
    });
    // A guest plants a different module under the same name: the registered
    // one still runs — a guest write can never shadow deployment code.
    plant(&bench, "mod.wasm", SEED_WRITER);
    let out = run(&bench, json!({"module": "mod.wasm", "input": {"x": 1}})).expect("run");
    assert_eq!(
        out,
        json!({"x": 1}),
        "the registered echo ran, not the planted module"
    );
}

#[test]
fn compiled_modules_are_cached_per_sandbox() {
    let (provider, bench) = bench(7, &SandboxProfile::default());
    plant(&bench, "echo.wasm", ECHO);
    run(&bench, json!({"module": "echo.wasm", "input": 1})).expect("run");
    run(&bench, json!({"module": "echo.wasm", "input": 2})).expect("run");
    assert_eq!(
        provider.stats.modules_compiled(),
        1,
        "the second call hits the cache"
    );
    plant(&bench, "other.wasm", SEED_WRITER);
    run(&bench, json!({"module": "other.wasm"})).expect("run");
    assert_eq!(provider.stats.modules_compiled(), 2);
}

#[test]
fn run_js_routes_through_the_registered_runner() {
    // The fake runner echoes its input: run_js's contract wraps the script
    // and input as {script, input} for the runner, which the echo exposes.
    let echo = wat::parse_str(ECHO).expect("wat");
    let (_provider, bench) = bench_custom(7, &SandboxProfile::default(), |p| {
        p.with_module("qjs.wasm", echo)
    });
    let out = block_on(bench.sandbox.call(
        Tier::Compute,
        "run_js",
        json!({"code": "1 + 1", "input": {"n": 41}}),
    ))
    .expect("run_js");
    assert_eq!(out, json!({"script": "1 + 1", "input": {"n": 41}}));
}

#[test]
fn run_js_reads_a_workspace_file_when_asked() {
    let echo = wat::parse_str(ECHO).expect("wat");
    let (_provider, bench) = bench_custom(7, &SandboxProfile::default(), |p| {
        p.with_module("qjs.wasm", echo)
    });
    std::fs::write(bench.dir.join("main.js"), "input.n * 2").expect("plant script");
    let out = block_on(bench.sandbox.call(
        Tier::Compute,
        "run_js",
        json!({"file": "main.js", "input": {"n": 21}}),
    ))
    .expect("run_js");
    assert_eq!(out, json!({"script": "input.n * 2", "input": {"n": 21}}));
}

#[test]
fn run_js_without_a_registered_runner_is_a_sandbox_failure() {
    let (_provider, bench) = bench(7, &SandboxProfile::default());
    let out = block_on(
        bench
            .sandbox
            .call(Tier::Compute, "run_js", json!({"code": "1"})),
    );
    assert!(
        matches!(&out, Err(ToolError::Sandbox(e)) if e.contains("with_quickjs")),
        "the failure names the fix, got {out:?}"
    );
}

#[test]
fn run_js_requires_exactly_one_source() {
    let echo = wat::parse_str(ECHO).expect("wat");
    let (_provider, bench) = bench_custom(7, &SandboxProfile::default(), |p| {
        p.with_module("qjs.wasm", echo)
    });
    let both = block_on(bench.sandbox.call(
        Tier::Compute,
        "run_js",
        json!({"code": "1", "file": "a.js"}),
    ));
    assert!(matches!(both, Err(ToolError::InvalidArguments(_))));
    let neither = block_on(bench.sandbox.call(Tier::Compute, "run_js", json!({})));
    assert!(matches!(neither, Err(ToolError::InvalidArguments(_))));
}
