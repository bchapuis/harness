//! QuickJS runner conformance (feature `quickjs`, the committed artifact):
//! the hermeticity allowlist (S1/S2 by construction), the S2 determinism
//! differential, and the JS surface (workspace, console, exceptions, fuel).
//!
//! This suite builds only with the `quickjs` feature, which embeds
//! `modules/qjs.wasm` — build it with `guest/qjs-runner/build.sh` first.

#![cfg(feature = "quickjs")]

use std::collections::BTreeSet;
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
use harness_sandbox::quickjs_module;
use serde_json::Value;
use serde_json::json;

struct Bench {
    sandbox: Arc<dyn Sandbox>,
    dir: PathBuf,
    _root: tempfile::TempDir,
}

fn bench(seed: u64, profile: &SandboxProfile) -> Bench {
    let root = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new().with_seed(seed).with_quickjs();
    let dir = root.path().join("s");
    let sandbox = block_on(provider.open(&SessionId::new("s"), profile, &dir)).expect("open");
    Bench {
        sandbox,
        dir,
        _root: root,
    }
}

fn run_js(bench: &Bench, input: Value) -> Result<Value, ToolError> {
    block_on(bench.sandbox.call(Tier::Compute, "run_js", input))
}

// ---------------------------------------------------------------------------
// Hermeticity: the artifact imports only the defined surface
// ---------------------------------------------------------------------------

#[test]
fn the_runner_imports_only_the_defined_surface() {
    // The host surface the runner may speak to (compute.rs): the `harness`
    // capabilities and the deterministically stubbed WASI subset. A
    // wasi-libc upgrade that wants more fails here, never silently.
    let allowed: BTreeSet<(&str, &str)> = [
        ("harness", "seed"),
        ("harness", "ws_size"),
        ("harness", "ws_read"),
        ("harness", "ws_write"),
        ("wasi_snapshot_preview1", "random_get"),
        ("wasi_snapshot_preview1", "clock_time_get"),
        ("wasi_snapshot_preview1", "fd_write"),
        ("wasi_snapshot_preview1", "environ_sizes_get"),
        ("wasi_snapshot_preview1", "environ_get"),
        ("wasi_snapshot_preview1", "proc_exit"),
        ("wasi_snapshot_preview1", "fd_close"),
        ("wasi_snapshot_preview1", "fd_fdstat_get"),
        ("wasi_snapshot_preview1", "fd_seek"),
    ]
    .into_iter()
    .collect();

    let engine = wasmtime::Engine::default();
    let module = wasmtime::Module::new(&engine, &quickjs_module()[..]).expect("valid module");
    for import in module.imports() {
        let key = (import.module(), import.name());
        assert!(
            allowed.contains(&key),
            "runner imports {key:?}, outside the host surface; either it is an \
             ambient capability (reject the build) or a new wasi-libc stub to add \
             deliberately to wasi_stubs in compute.rs"
        );
    }
}

// ---------------------------------------------------------------------------
// S2: determinism across the JS surface
// ---------------------------------------------------------------------------

#[test]
fn random_and_workspace_reproduce_byte_identically_per_seed() {
    let script = "const r = Math.random(); workspace.write('r.txt', String(r)); \
                  ({ r, t: Date.now() })";
    let observe = |seed: u64| {
        let bench = bench(seed, &SandboxProfile::default());
        let out = run_js(&bench, json!({ "code": script })).expect("run_js");
        let file = std::fs::read_to_string(bench.dir.join("r.txt")).expect("r.txt");
        (out, file)
    };
    let (a_out, a_file) = observe(42);
    let (b_out, b_file) = observe(42);
    assert_eq!(a_out, b_out, "same seed, same result (S2)");
    assert_eq!(a_file, b_file, "same seed, same workspace effect (S2)");

    let (c_out, _) = observe(43);
    assert_ne!(a_out, c_out, "the seed genuinely drives Math.random");

    // Date is frozen: time does not advance.
    let t = a_out["result"]["t"].as_f64().expect("t");
    assert_eq!(t, 1_700_000_000_000.0);
}

// ---------------------------------------------------------------------------
// The JS surface
// ---------------------------------------------------------------------------

#[test]
fn input_is_exposed_and_the_result_returns() {
    let bench = bench(7, &SandboxProfile::default());
    let out = run_js(
        &bench,
        json!({ "code": "input.a + input.b", "input": { "a": 2, "b": 40 } }),
    )
    .expect("run_js");
    assert_eq!(out["result"], json!(42));
}

#[test]
fn console_output_is_captured() {
    let bench = bench(7, &SandboxProfile::default());
    let out = run_js(&bench, json!({ "code": "console.log('hello', 42); 1" })).expect("run_js");
    assert_eq!(out["result"], json!(1));
    assert_eq!(out["console"], json!("hello 42\n"));
}

#[test]
fn a_thrown_exception_is_a_script_outcome_not_a_tool_error() {
    let bench = bench(7, &SandboxProfile::default());
    let out = run_js(&bench, json!({ "code": "throw new Error('boom')" })).expect("run_js");
    assert!(
        out.get("error")
            .and_then(Value::as_str)
            .is_some_and(|e| e.contains("boom")),
        "a JS exception returns as {{error}}, got {out:?}"
    );
}

#[test]
fn a_runaway_script_is_stopped_by_fuel() {
    // A small fuel budget: interpreter init plus an infinite loop must trap.
    let profile = SandboxProfile {
        compute: ComputeLimits {
            fuel: 50_000_000,
            ..ComputeLimits::default()
        },
        ..SandboxProfile::default()
    };
    let bench = bench(7, &profile);
    let out = run_js(&bench, json!({ "code": "while (true) {}" }));
    assert!(
        matches!(&out, Err(ToolError::Sandbox(e)) if e.contains("fuel")),
        "an infinite JS loop is bounded by fuel (§3.2), got {out:?}"
    );
}

#[test]
fn workspace_access_is_confined_to_the_handle() {
    let bench = bench(7, &SandboxProfile::default());
    // A sentinel one level up: a path escape must not reach it.
    run_js(
        &bench,
        json!({ "code": "workspace.write('in.txt', 'hello'); null" }),
    )
    .expect("run_js");
    let out = run_js(
        &bench,
        json!({ "code": "[workspace.read('in.txt'), workspace.read('../escape')]" }),
    )
    .expect("run_js");
    assert_eq!(
        out["result"],
        json!(["hello", null]),
        "in-workspace read works; an escaping read is null (S1)"
    );
}

#[test]
fn the_default_fuel_budget_covers_a_small_script() {
    // Interpreter init plus a trivial script must fit the default budget, or
    // run_js is unusable out of the box. If this fails, raise the default in
    // ComputeLimits and note it.
    let bench = bench(7, &SandboxProfile::default());
    let out = run_js(&bench, json!({ "code": "1 + 1" })).expect("default budget too small");
    assert_eq!(out["result"], json!(2));
}
