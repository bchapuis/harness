//! The `Compute` tier (sandbox spec §3.2): hermetic wasmtime guests over the
//! workspace.
//!
//! **Hermeticity by construction.** The linker defines the `harness` host
//! module and a fixed set of deterministic WASI stubs (seeded entropy, a
//! frozen clock, discarded stdio — what wasi-libc needs to exist at all),
//! and nothing else: instantiating a module that imports beyond that set
//! fails as a `ToolError` outcome. Every capability a guest holds is one
//! this module chose to expose; none is ambient. The filesystem a guest can
//! touch is the workspace capability handle of §3.1 and nothing else, and
//! its outputs are a function of the call, the workspace, and the injected
//! seed (S2).
//!
//! **Determinism config.** Fuel metering is the REQUIRED CPU bound (§3.2):
//! it is deterministic, so a fuel-exhausted guest traps at the same
//! instruction on every run. Epoch interruption is deliberately absent — its
//! ticking needs an ambient timer thread, which the simulation determinism
//! contract forbids (core spec §18.1). NaN canonicalization and
//! deterministic relaxed-SIMD close the value-level holes; parallel
//! compilation is off so no thread pool exists. One `Store` per call: no
//! guest state survives between calls except through the workspace.
//!
//! **Modules.** A call names either a deployment-registered module (the
//! QuickJS runner; registered names win, so a guest write never shadows
//! them) or a workspace path. Compiled modules are cached per sandbox by
//! content digest: code, not working state, so caching leaks nothing.
//!
//! **Guest ABI v1** (frozen once a tool description teaches it):
//!
//! - exports: `memory`, `alloc(len: i32) -> i32`, and
//!   `run(ptr: i32, len: i32) -> i64` — the input JSON is written at
//!   `alloc(len)`, and the return packs the output's location as
//!   `(ptr << 32) | len`, UTF-8 JSON.
//! - imports, module `harness`:
//!   - `seed() -> i64` — the session's injected seed (S2);
//!   - `ws_size(path_ptr, path_len) -> i64` — a file's byte length, or -1;
//!   - `ws_read(path_ptr, path_len, dst_ptr, dst_cap) -> i64` — copies up to
//!     `dst_cap` bytes, returns the file's full length, or -1;
//!   - `ws_write(path_ptr, path_len, src_ptr, src_len) -> i64` — writes the
//!     file (creating parents), returns `src_len`, or -1.
//! - imports, module `wasi_snapshot_preview1` (for wasi-libc-linked guests
//!   like the QuickJS runner): `random_get` (seeded xorshift),
//!   `clock_time_get` (frozen epoch plus a per-call monotonic step),
//!   `fd_write` (accepted and discarded), empty environ, and benign errno
//!   stubs — each deterministic, never the OS.
//!
//! Filesystem failures return -1 to the guest rather than trapping: a guest
//! probing for an absent file is normal control flow. Out-of-bounds memory
//! and non-UTF-8 paths trap: those are ABI violations, and the trap surfaces
//! as the call's `ToolError` outcome (harness spec §5.4).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::Mutex;

use cap_std::fs::Dir;
use harness::ComputeLimits;
use harness::OnDangling;
use harness::Tier;
use harness::ToolDecl;
use harness::ToolError;
use serde_json::Value;
use serde_json::json;
use wasmtime::Caller;
use wasmtime::Config;
use wasmtime::Engine;
use wasmtime::Linker;
use wasmtime::Memory;
use wasmtime::Module;
use wasmtime::Store;
use wasmtime::StoreLimits;
use wasmtime::StoreLimitsBuilder;

use crate::provider::TierStats;

/// The registered name `run_js` routes to: the QuickJS runner module
/// (feature `quickjs`, or any module a deployment registers under this name
/// that honors the runner contract).
pub(crate) const QJS_MODULE: &str = "qjs.wasm";

/// The compute tool declaration, ready for [`harness::Kind::tool`]: guest
/// code is arbitrary, so a dangling call is never blindly re-executed — the
/// model decides (`OnDangling::Interrupt`, harness spec §5.5).
pub fn run_module_tool() -> ToolDecl {
    ToolDecl {
        name: "run_module".to_string(),
        description: "Execute a WebAssembly module from the session workspace, hermetically: \
                      no clock, no network, no filesystem beyond the workspace. The module \
                      must export `memory`, `alloc(len: i32) -> i32`, and `run(ptr: i32, \
                      len: i32) -> i64`: the input JSON is written at `alloc(len)`, and `run` \
                      returns `(ptr << 32) | len` locating the UTF-8 JSON output. Host \
                      imports, module `harness`: `seed() -> i64`, `ws_size(path_ptr, \
                      path_len) -> i64`, `ws_read(path_ptr, path_len, dst_ptr, dst_cap) -> \
                      i64`, `ws_write(path_ptr, path_len, src_ptr, src_len) -> i64`."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "module": {
                    "type": "string",
                    "description": "Workspace-relative path to the .wasm module."
                },
                "input": {
                    "description": "JSON input handed to the guest's `run`."
                }
            },
            "required": ["module"]
        }),
        tier: Tier::Compute,
        on_dangling: OnDangling::Interrupt,
        timeout: None,
    }
}

/// The JavaScript tool declaration (the QuickJS runner's model-facing face):
/// scripts are arbitrary code, so a dangling call interrupts rather than
/// blindly re-executing (harness spec §5.5).
pub fn run_js_tool() -> ToolDecl {
    ToolDecl {
        name: "run_js".to_string(),
        description: "Run JavaScript (QuickJS) hermetically over the session workspace: no \
                      network, no Node APIs, no timers; files only through `workspace`. \
                      Provide the script inline as `code`, or name a workspace `file` \
                      (exactly one of the two). The optional `input` value is available to \
                      the script as the global `input`. The script's completion value \
                      returns as `result` with captured `console` output; a thrown \
                      exception returns `error` instead. Determinism: `Math.random()` is \
                      seeded per session and `Date.now()` is frozen. Workspace access: \
                      `workspace.read(path)` returns a string or null; \
                      `workspace.write(path, content)` returns a boolean."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "JavaScript source to run."
                },
                "file": {
                    "type": "string",
                    "description": "Workspace-relative path to a .js file (alternative to `code`)."
                },
                "input": {
                    "description": "Value exposed to the script as the global `input`."
                }
            }
        }),
        tier: Tier::Compute,
        on_dangling: OnDangling::Interrupt,
        timeout: None,
    }
}

/// Caps on guest-induced **host-side** costs (sandbox spec §3.2: the
/// REQUIRED limits cover what a guest can make the host pay, not only guest
/// memory and CPU). Fuel meters execution and `StoreLimits` meters guest
/// memory; these bound everything else a hostile module could size: the
/// bytes handed to the compiler, the path and output lengths host functions
/// materialize, and the table slots instantiation allocates host-side.
/// Host functions never allocate guest-sized buffers at all — they stream
/// through views of guest memory — so these caps are the residual surface.
const MAX_MODULE_BYTES: usize = 16 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 256 * 1024;
const MAX_PATH_BYTES: i32 = 4096;
const MAX_TABLE_ELEMENTS: usize = 100_000;
/// Cap on a `run_js` script read from a workspace file.
const MAX_SCRIPT_BYTES: usize = 256 * 1024;
/// Compiled-module cache entries per sandbox; eviction clears the whole
/// cache — the one policy with no clock and no order sensitivity.
const CACHE_ENTRIES: usize = 8;
/// The frozen base the WASI clock stub reports (S2: time is injected, never
/// ambient). Advances by a fixed step per read so naive elapsed-time code
/// sees monotonic values, deterministically.
const WASI_EPOCH_NANOS: u64 = 1_700_000_000_000_000_000;
const WASI_CLOCK_STEP_NANOS: u64 = 1_000_000;

/// What the host functions see: the workspace handle, the injected seed, the
/// store's resource limiter, and the deterministic WASI stub state.
struct HostState {
    dir: Arc<Dir>,
    seed: u64,
    limits: StoreLimits,
    /// xorshift64* state for the `random_get` stub; seeded per call from the
    /// session seed, so each call sees the same deterministic stream.
    wasi_rng: u64,
    /// Reads of the `clock_time_get` stub so far, this call.
    wasi_clock_reads: u64,
}

/// One session's compute engine, built on the first `Compute` call and
/// dropped on release (S5).
pub(crate) struct ComputeTier {
    engine: Engine,
    /// The host surface, defined once per tier: instantiation only ever
    /// consults it, so every call shares one immutable linker instead of
    /// re-registering a dozen host functions per call.
    linker: Linker<HostState>,
    limits: ComputeLimits,
    /// Deployment-registered modules (provider.rs): resolved before any
    /// workspace path.
    modules: Arc<BTreeMap<String, Arc<[u8]>>>,
    /// Compiled modules by content digest. Code, not working state.
    cache: Mutex<BTreeMap<u64, Module>>,
    stats: TierStats,
}

impl ComputeTier {
    pub(crate) fn new(
        limits: ComputeLimits,
        modules: Arc<BTreeMap<String, Arc<[u8]>>>,
        stats: TierStats,
    ) -> Result<ComputeTier, ToolError> {
        let mut config = Config::new();
        // The deterministic CPU bound (§3.2). No epoch interruption: fuel is
        // the bound, and it reproduces under the simulator.
        config.consume_fuel(true);
        config.cranelift_nan_canonicalization(true);
        config.relaxed_simd_deterministic(true);
        // Threads and parallel compilation need no disabling: this crate
        // compiles wasmtime without the `threads` and `parallel-compilation`
        // features, so neither capability exists to switch off — absence by
        // construction, like the rest of the hermeticity story.
        let engine =
            Engine::new(&config).map_err(|e| ToolError::Sandbox(format!("compute engine: {e}")))?;
        let linker = host_linker(&engine)?;
        Ok(ComputeTier {
            engine,
            linker,
            limits,
            modules,
            cache: Mutex::new(BTreeMap::new()),
            stats,
        })
    }

    /// Execute one compute call to completion. Synchronous by design: fuel,
    /// not the harness timeout, bounds a runaway guest (crate docs).
    pub(crate) fn run(
        &self,
        dir: &Arc<Dir>,
        seed: u64,
        name: &str,
        input: &Value,
    ) -> Result<Value, ToolError> {
        match name {
            "run_module" => {
                let path = input.get("module").and_then(Value::as_str).ok_or_else(|| {
                    ToolError::InvalidArguments("`module` must be a string".to_string())
                })?;
                let bytes = self.resolve(dir, path)?;
                let guest_input = input.get("input").cloned().unwrap_or(Value::Null);
                self.execute(dir, seed, &bytes, &guest_input)
            }
            "run_js" => {
                let source = js_source(dir, input)?;
                let Some(runner) = self.modules.get(QJS_MODULE) else {
                    return Err(ToolError::Sandbox(format!(
                        "run_js needs the `{QJS_MODULE}` runner module: register it with \
                         TieredSandboxes::with_quickjs() or with_module()"
                    )));
                };
                let runner = Arc::clone(runner);
                let guest_input = json!({
                    "script": source,
                    "input": input.get("input").cloned().unwrap_or(Value::Null),
                });
                self.execute(dir, seed, &runner, &guest_input)
            }
            other => Err(crate::provider::unknown_tool(other)),
        }
    }

    /// A module's bytes: registered names first (unshadowable), then the
    /// workspace path through the capability handle (S1 holds at the compute
    /// tier: the guest's code, like its data, is workspace-confined).
    fn resolve(&self, dir: &Arc<Dir>, path: &str) -> Result<Arc<[u8]>, ToolError> {
        if let Some(bytes) = self.modules.get(path) {
            return Ok(Arc::clone(bytes));
        }
        dir.read(path)
            .map(Arc::from)
            .map_err(|e| ToolError::Sandbox(format!("run_module: {path}: {e}")))
    }

    /// Compile through the per-sandbox cache. Compilation runs before fuel
    /// exists to meter it, so its input is bounded instead (sandbox spec
    /// §3.2): an unbounded module is an unbounded compile.
    fn compile_cached(&self, bytes: &[u8]) -> Result<Module, ToolError> {
        if bytes.len() > MAX_MODULE_BYTES {
            return Err(ToolError::Sandbox(format!(
                "compute: module is {} bytes, over the {MAX_MODULE_BYTES}-byte cap",
                bytes.len()
            )));
        }
        let key = fnv64(bytes);
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(module) = cache.get(&key) {
            return Ok(module.clone());
        }
        let module = Module::new(&self.engine, bytes).map_err(sandbox_err("module"))?;
        self.stats.count_module_compiled();
        if cache.len() >= CACHE_ENTRIES {
            cache.clear();
        }
        cache.insert(key, module.clone());
        Ok(module)
    }

    /// One guest execution: fresh store, fresh instance, the ABI round trip.
    fn execute(
        &self,
        dir: &Arc<Dir>,
        seed: u64,
        module_bytes: &[u8],
        guest_input: &Value,
    ) -> Result<Value, ToolError> {
        let module = self.compile_cached(module_bytes)?;

        let mut store = Store::new(
            &self.engine,
            HostState {
                dir: Arc::clone(dir),
                seed,
                // Beyond linear memory, instantiation itself allocates
                // host-side (a `(table N funcref)` declaration is N pointers
                // before the first instruction runs): every axis is capped.
                limits: StoreLimitsBuilder::new()
                    .memory_size(self.limits.memory_bytes as usize)
                    .table_elements(MAX_TABLE_ELEMENTS)
                    .instances(1)
                    .memories(1)
                    .tables(4)
                    .build(),
                wasi_rng: seed | 1,
                wasi_clock_reads: 0,
            },
        );
        store.limiter(|state| &mut state.limits);
        store
            .set_fuel(self.limits.fuel)
            .map_err(|e| ToolError::Sandbox(format!("compute: fuel: {e}")))?;

        // An import outside the defined surface fails here: hermeticity is
        // the absence of the capability, not a filter (§3.2).
        let instance = self
            .linker
            .instantiate(&mut store, &module)
            .map_err(sandbox_err("instantiate"))?;
        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| ToolError::Sandbox("compute: no exported `memory`".to_string()))?;
        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "alloc")
            .map_err(sandbox_err("alloc"))?;
        let run = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, "run")
            .map_err(sandbox_err("run"))?;

        let input_json = serde_json::to_vec(guest_input).expect("serializable");
        let len = i32::try_from(input_json.len())
            .map_err(|_| ToolError::InvalidArguments("input too large".to_string()))?;
        let ptr = alloc.call(&mut store, len).map_err(sandbox_err("alloc"))?;
        memory
            .write(&mut store, ptr as u32 as usize, &input_json)
            .map_err(|e| ToolError::Sandbox(format!("compute: input write: {e}")))?;

        let packed = run
            .call(&mut store, (ptr, len))
            .map_err(|e| ToolError::Sandbox(format!("compute: {e:#}")))?;
        let out_ptr = (packed >> 32) as u32 as i32;
        let out_len = packed as u32;
        // The output is read as a view into guest memory, never copied into
        // a guest-sized host buffer, and capped before parsing: the packed
        // length is guest-controlled, and it feeds the journal and the model
        // context.
        if out_len as usize > MAX_OUTPUT_BYTES {
            return Err(ToolError::Sandbox(format!(
                "compute: output is {out_len} bytes, over the {MAX_OUTPUT_BYTES}-byte cap"
            )));
        }
        // The cap above keeps `out_len` well within `i32`, so the guest-slice
        // helper's `i32` length never truncates here.
        let out = guest_slice(
            memory.data(&store),
            out_ptr,
            out_len as i32,
            "compute: output",
        )
        .map_err(|e| ToolError::Sandbox(e.to_string()))?;
        serde_json::from_slice(out)
            .map_err(|e| ToolError::Sandbox(format!("compute: output is not JSON: {e}")))
    }
}

/// The `run_js` script source: inline `code`, or a capped workspace `file`
/// read through the handle — exactly one of the two.
fn js_source(dir: &Arc<Dir>, input: &Value) -> Result<String, ToolError> {
    let code = input.get("code").and_then(Value::as_str);
    let file = input.get("file").and_then(Value::as_str);
    match (code, file) {
        (Some(code), None) => Ok(code.to_string()),
        (None, Some(file)) => {
            let bytes = dir
                .read(file)
                .map_err(|e| ToolError::Sandbox(format!("run_js: {file}: {e}")))?;
            if bytes.len() > MAX_SCRIPT_BYTES {
                return Err(ToolError::Sandbox(format!(
                    "run_js: {file}: script is {} bytes, over the {MAX_SCRIPT_BYTES}-byte cap",
                    bytes.len()
                )));
            }
            String::from_utf8(bytes)
                .map_err(|_| ToolError::InvalidArguments("script is not UTF-8".to_string()))
        }
        _ => Err(ToolError::InvalidArguments(
            "exactly one of `code` or `file` is required".to_string(),
        )),
    }
}

/// The host surface a guest may import (§3.2): the `harness` capabilities
/// over the workspace handle and the seed, plus deterministic WASI stubs for
/// wasi-libc-linked guests. Every function below is a deliberate grant;
/// nothing reaches the OS.
fn host_linker(engine: &Engine) -> Result<Linker<HostState>, ToolError> {
    let mut linker: Linker<HostState> = Linker::new(engine);
    let wire = |e: wasmtime::Error| ToolError::Sandbox(format!("compute host: {e}"));
    linker
        .func_wrap("harness", "seed", |caller: Caller<'_, HostState>| {
            caller.data().seed as i64
        })
        .map_err(wire)?;
    linker
        .func_wrap(
            "harness",
            "ws_size",
            |mut caller: Caller<'_, HostState>,
             path_ptr: i32,
             path_len: i32|
             -> wasmtime::Result<i64> {
                let path = guest_path(&mut caller, path_ptr, path_len)?;
                // Regular files only: a directory's "size" is filesystem
                // trivia, not workspace state, and S2 promises outputs leak
                // no OS state.
                Ok(caller
                    .data()
                    .dir
                    .metadata(&path)
                    .ok()
                    .filter(|m| m.is_file())
                    .map(|m| m.len() as i64)
                    .unwrap_or(-1))
            },
        )
        .map_err(wire)?;
    linker
        .func_wrap(
            "harness",
            "ws_read",
            |mut caller: Caller<'_, HostState>,
             path_ptr: i32,
             path_len: i32,
             dst_ptr: i32,
             dst_cap: i32|
             -> wasmtime::Result<i64> {
                let path = guest_path(&mut caller, path_ptr, path_len)?;
                let memory = guest_memory(&mut caller)?;
                // Stream the file straight into the guest's own memory: the
                // host never holds a file- or cap-sized buffer, so neither
                // a large file nor a large `dst_cap` costs the host more
                // than the guest's bounded memory already does.
                let (data, state) = memory.data_and_store_mut(&mut caller);
                let Ok(file) = state.dir.open(&path) else {
                    return Ok(-1);
                };
                let Ok(meta) = file.metadata() else {
                    return Ok(-1);
                };
                if !meta.is_file() {
                    return Ok(-1);
                }
                let size = meta.len();
                let copy = size.min(dst_cap.max(0) as u64) as i32;
                let dst = guest_slice_mut(data, dst_ptr, copy, "ws_read")?;
                let mut file = file;
                let mut filled = 0;
                while filled < dst.len() {
                    match std::io::Read::read(&mut file, &mut dst[filled..]) {
                        Ok(0) => break,
                        Ok(n) => filled += n,
                        Err(_) => return Ok(-1),
                    }
                }
                Ok(size as i64)
            },
        )
        .map_err(wire)?;
    linker
        .func_wrap(
            "harness",
            "ws_write",
            |mut caller: Caller<'_, HostState>,
             path_ptr: i32,
             path_len: i32,
             src_ptr: i32,
             src_len: i32|
             -> wasmtime::Result<i64> {
                let path = guest_path(&mut caller, path_ptr, path_len)?;
                let memory = guest_memory(&mut caller)?;
                // The source is a view into guest memory, never a copy: the
                // guest cannot size a host allocation, and a range outside
                // its own memory is an ABI violation that traps.
                let (data, state) = memory.data_and_store_mut(&mut caller);
                let bytes = guest_slice(data, src_ptr, src_len, "ws_write")?;
                if let Some(parent) = std::path::Path::new(&path).parent()
                    && !parent.as_os_str().is_empty()
                    && state.dir.create_dir_all(parent).is_err()
                {
                    return Ok(-1);
                }
                Ok(match state.dir.write(&path, bytes) {
                    Ok(()) => src_len.max(0) as i64,
                    Err(_) => -1,
                })
            },
        )
        .map_err(wire)?;
    wasi_stubs(&mut linker).map_err(wire)?;
    Ok(linker)
}

/// Deterministic `wasi_snapshot_preview1` stubs: just enough for a
/// wasi-libc-linked guest (the QuickJS runner) to initialize and run, every
/// one a pure function of the call and the seed (S2). The artifact's
/// imports-allowlist test pins this surface: a wasi-libc upgrade that wants
/// more fails loudly there, never silently here.
fn wasi_stubs(linker: &mut Linker<HostState>) -> wasmtime::Result<()> {
    const WASI: &str = "wasi_snapshot_preview1";
    // errno values from the WASI spec.
    const SUCCESS: i32 = 0;
    const BADF: i32 = 8;

    linker.func_wrap(
        WASI,
        "random_get",
        |mut caller: Caller<'_, HostState>, buf: i32, len: i32| -> wasmtime::Result<i32> {
            let memory = guest_memory(&mut caller)?;
            let (data, state) = memory.data_and_store_mut(&mut caller);
            let dst = guest_slice_mut(data, buf, len, "random_get")?;
            for chunk in dst.chunks_mut(8) {
                // xorshift64*: deterministic, seeded per call.
                state.wasi_rng ^= state.wasi_rng << 13;
                state.wasi_rng ^= state.wasi_rng >> 7;
                state.wasi_rng ^= state.wasi_rng << 17;
                let bytes = state
                    .wasi_rng
                    .wrapping_mul(0x2545_F491_4F6C_DD1D)
                    .to_le_bytes();
                chunk.copy_from_slice(&bytes[..chunk.len()]);
            }
            Ok(SUCCESS)
        },
    )?;
    linker.func_wrap(
        WASI,
        "clock_time_get",
        |mut caller: Caller<'_, HostState>,
         _id: i32,
         _precision: i64,
         out: i32|
         -> wasmtime::Result<i32> {
            let memory = guest_memory(&mut caller)?;
            let (data, state) = memory.data_and_store_mut(&mut caller);
            // Frozen epoch plus a fixed step per read: monotonic for naive
            // elapsed-time code, identical on every run.
            state.wasi_clock_reads += 1;
            let now = WASI_EPOCH_NANOS + state.wasi_clock_reads * WASI_CLOCK_STEP_NANOS;
            let dst = guest_slice_mut(data, out, 8, "clock_time_get")?;
            dst.copy_from_slice(&now.to_le_bytes());
            Ok(SUCCESS)
        },
    )?;
    linker.func_wrap(
        WASI,
        "fd_write",
        |mut caller: Caller<'_, HostState>,
         _fd: i32,
         iovs: i32,
         iovs_len: i32,
         nwritten: i32|
         -> wasmtime::Result<i32> {
            // Accept and discard: the runner captures console output itself;
            // this only keeps libc's stdio from failing. The iovs count is
            // capped first — host loops run outside the fuel meter, and no
            // libc writev carries more than a handful of entries.
            if !(0..=1024).contains(&iovs_len) {
                return Err(wasmtime::Error::msg(format!(
                    "fd_write: iovs count {iovs_len} is outside 0..=1024"
                )));
            }
            let memory = guest_memory(&mut caller)?;
            let data = memory.data(&caller);
            let mut total: u32 = 0;
            for i in 0..iovs_len.max(0) as usize {
                let entry = (iovs as u32 as usize) + i * 8;
                let len_at = entry + 4;
                let len_bytes = data
                    .get(len_at..len_at + 4)
                    .ok_or_else(|| wasmtime::Error::msg("fd_write: out of bounds"))?;
                total = total.wrapping_add(u32::from_le_bytes(len_bytes.try_into().unwrap()));
            }
            memory.write(&mut caller, nwritten as u32 as usize, &total.to_le_bytes())?;
            Ok(SUCCESS)
        },
    )?;
    linker.func_wrap(
        WASI,
        "environ_sizes_get",
        |mut caller: Caller<'_, HostState>, count: i32, size: i32| -> wasmtime::Result<i32> {
            let memory = guest_memory(&mut caller)?;
            memory.write(&mut caller, count as u32 as usize, &0u32.to_le_bytes())?;
            memory.write(&mut caller, size as u32 as usize, &0u32.to_le_bytes())?;
            Ok(SUCCESS)
        },
    )?;
    linker.func_wrap(
        WASI,
        "environ_get",
        |_: Caller<'_, HostState>, _: i32, _: i32| SUCCESS,
    )?;
    linker.func_wrap(
        WASI,
        "proc_exit",
        |_: Caller<'_, HostState>, code: i32| -> wasmtime::Result<()> {
            Err(wasmtime::Error::msg(format!(
                "guest called proc_exit({code})"
            )))
        },
    )?;
    linker.func_wrap(WASI, "fd_close", |_: Caller<'_, HostState>, _: i32| BADF)?;
    linker.func_wrap(
        WASI,
        "fd_fdstat_get",
        |_: Caller<'_, HostState>, _: i32, _: i32| BADF,
    )?;
    linker.func_wrap(
        WASI,
        "fd_seek",
        |_: Caller<'_, HostState>, _: i32, _: i64, _: i32, _: i32| BADF,
    )?;
    Ok(())
}

/// The guest's exported memory, or an ABI-violation trap.
fn guest_memory(caller: &mut Caller<'_, HostState>) -> wasmtime::Result<Memory> {
    match caller.get_export("memory") {
        Some(wasmtime::Extern::Memory(memory)) => Ok(memory),
        _ => Err(wasmtime::Error::msg("guest exports no `memory`")),
    }
}

/// `data[ptr .. ptr + len]`, or an out-of-bounds trap. The guest controls
/// `ptr` and `len`, so a range outside its own memory is an ABI violation
/// (the `what` names the host function in the trap). `len` is clamped at zero
/// first: a negative length is the guest's, not a panic on the host side.
fn guest_slice<'a>(data: &'a [u8], ptr: i32, len: i32, what: &str) -> wasmtime::Result<&'a [u8]> {
    let start = ptr as u32 as usize;
    start
        .checked_add(len.max(0) as usize)
        .and_then(|end| data.get(start..end))
        .ok_or_else(|| wasmtime::Error::msg(format!("{what}: out of guest memory bounds")))
}

/// The mutable counterpart of [`guest_slice`].
fn guest_slice_mut<'a>(
    data: &'a mut [u8],
    ptr: i32,
    len: i32,
    what: &str,
) -> wasmtime::Result<&'a mut [u8]> {
    let start = ptr as u32 as usize;
    start
        .checked_add(len.max(0) as usize)
        .and_then(|end| data.get_mut(start..end))
        .ok_or_else(|| wasmtime::Error::msg(format!("{what}: out of guest memory bounds")))
}

/// A UTF-8 path read out of guest memory, or an ABI-violation trap. The
/// length is capped before anything is allocated: a path is a name, not a
/// payload.
fn guest_path(caller: &mut Caller<'_, HostState>, ptr: i32, len: i32) -> wasmtime::Result<String> {
    if !(0..=MAX_PATH_BYTES).contains(&len) {
        return Err(wasmtime::Error::msg(format!(
            "path length {len} is outside 0..={MAX_PATH_BYTES}"
        )));
    }
    let memory = guest_memory(caller)?;
    let mut bytes = vec![0u8; len as usize];
    memory.read(&*caller, ptr as u32 as usize, &mut bytes)?;
    String::from_utf8(bytes).map_err(|_| wasmtime::Error::msg("path is not UTF-8"))
}

/// A `Sandbox` error tagged with the failing compute step and the full
/// wasmtime cause chain (`{e:#}`). The closure form suits `Result::map_err`,
/// where most of these arise.
fn sandbox_err(step: &'static str) -> impl Fn(wasmtime::Error) -> ToolError {
    move |e| ToolError::Sandbox(format!("compute: {step}: {e:#}"))
}

/// FNV-1a 64 over bytes: the module cache key (cf.
/// `harness::session::content_digest`, which is string-shaped).
fn fnv64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
