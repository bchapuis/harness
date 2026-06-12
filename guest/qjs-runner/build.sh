#!/usr/bin/env bash
# Build the QuickJS runner to wasm and install it as the committed artifact.
#
# Requirements (not needed for a normal `cargo build` of the harness):
#   - rustup target wasm32-wasip1
#   - a WASI SDK (clang + wasm sysroot), for rquickjs's quickjs-ng C build
#   - libclang, for rquickjs's bindgen on wasm32-wasip1
#
# Usage:
#   WASI_SDK_PATH=/path/to/wasi-sdk ./build.sh
#
# Records its toolchain versions into README.md's provenance block on success.
set -euo pipefail
cd "$(dirname "$0")"

TARGET=wasm32-wasip1
OUT=../../crates/harness-sandbox/modules/qjs.wasm

if [[ -z "${WASI_SDK_PATH:-}" ]]; then
  echo "set WASI_SDK_PATH to a WASI SDK install (https://github.com/WebAssembly/wasi-sdk/releases)" >&2
  exit 1
fi

rustup target add "$TARGET"

# rquickjs's cc/bindgen build uses the WASI SDK's clang and sysroot.
export CC_wasm32_wasip1="$WASI_SDK_PATH/bin/clang"
export CFLAGS_wasm32_wasip1="--sysroot=$WASI_SDK_PATH/share/wasi-sysroot"
export RQUICKJS_WASI_SDK="$WASI_SDK_PATH"

cargo build --release --target "$TARGET"

mkdir -p "$(dirname "$OUT")"
cp "target/$TARGET/release/qjs_runner.wasm" "$OUT"

SIZE=$(wc -c < "$OUT" | tr -d ' ')
echo "wrote $OUT ($SIZE bytes)"
echo "now run: cargo test -p harness-sandbox --features quickjs"
