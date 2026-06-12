# qjs-runner

A hermetic QuickJS interpreter for the Compute tier, compiled to
`wasm32-wasip1` and shipped as the committed artifact
`crates/harness-sandbox/modules/qjs.wasm`. It turns `run_js` tool calls into
deterministic JavaScript execution: the model writes JS (inline or as a
workspace file), and this module runs it under the same fuel bound, workspace
handle, and injected seed as any other Compute guest.

This crate is **outside** the main Cargo workspace (note the empty
`[workspace]` table in `Cargo.toml`). A normal `cargo build`/`cargo test` of
the harness never compiles it; only `build.sh` does.

## What the script sees

- `input` — the tool call's `input` value (or `undefined`).
- `console.log/warn/error/info` — captured, returned as `console`.
- `workspace.read(path) -> string | null`, `workspace.write(path, content) -> bool`
  — over the host capability handle (sandbox spec §3.1; paths outside the
  workspace are unrepresentable).
- `Math.random()` — seeded from the host seed (deterministic, sandbox §3.2 S2).
- `Date.now()` / `new Date()` — frozen epoch; time does not advance.
- Nothing else: no `fetch`, no timers, no Node APIs.

The script's completion value returns as `{result, console}`; a thrown
exception returns `{error, console}`. Both are tool *outcomes* the model
reads — only a fuel-exhausted or ABI-violating guest is a `ToolError`.

## Building

```sh
WASI_SDK_PATH=/path/to/wasi-sdk ./build.sh
```

Needs `rustup target add wasm32-wasip1`, a
[WASI SDK](https://github.com/WebAssembly/wasi-sdk/releases) (clang + wasm
sysroot, for rquickjs's quickjs-ng C compilation), and libclang (for
rquickjs's bindgen, since no pregenerated bindings ship for wasm32-wasip1).
The script copies the result over the committed `qjs.wasm`.

After building, `cargo test -p harness-sandbox --features quickjs` runs the
real-artifact suite: the imports allowlist (hermeticity guard), the S2
determinism differential (same seed reproduces the `Math.random` stream and
workspace effects, a different seed diverges), the frozen clock, and the
workspace/console/exception/fuel cases. Real JavaScript runs inside the
wasmtime sandbox throughout; there is no other JS runtime in the loop.

## Provenance

The committed `qjs.wasm` was built from:

- rquickjs: `0.9.0` (quickjs-ng)
- WASI SDK: `33.0`
- rustc: `1.95.0` (target `wasm32-wasip1`)
- `qjs.wasm` size: `696176` bytes

Rebuild with `build.sh` and update these on any change; the imports-allowlist
test in `harness-sandbox/tests/quickjs.rs` fails if a rebuild widens the host
surface.

## Imports (the hermeticity contract)

The artifact may import only:

- module `harness`: `seed`, `ws_size`, `ws_read`, `ws_write`;
- module `wasi_snapshot_preview1`: the subset the host stubs deterministically
  (`random_get`, `clock_time_get`, `fd_write`, `environ_sizes_get`,
  `environ_get`, `proc_exit`, `fd_close`, `fd_fdstat_get`, `fd_seek`).

`harness-sandbox/tests/quickjs.rs` pins this set. A wasi-libc upgrade that
imports more fails that test rather than silently widening the surface; the
fix is a deliberate addition to `wasi_stubs` in `compute.rs`.
