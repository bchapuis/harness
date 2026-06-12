# Compute-tier module artifacts

This directory holds prebuilt `.wasm` guests the provider embeds.

- `qjs.wasm` — the hermetic QuickJS runner (`run_js`), built from
  `guest/qjs-runner` by its `build.sh` and committed here. Embedded by the
  `quickjs` feature via `include_bytes!`. Rebuild with the WASI toolchain when
  the runner changes; provenance and the rebuild command are in
  `guest/qjs-runner/README.md`.

The default and `compute` feature sets do not need anything here.
