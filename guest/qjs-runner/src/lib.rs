//! The QuickJS runner: a hermetic JavaScript interpreter exposed to the
//! Compute tier through the guest ABI of `harness-sandbox`'s compute.rs.
//!
//! The host hands this module `{script, input}` JSON and reads back
//! `{result, console}` (a completed script) or `{error, console}` (a thrown
//! exception). The JavaScript environment is deterministic by construction:
//! `Math.random()` and `Date` are seeded from the host's injected seed, and
//! the only I/O is `workspace.read`/`workspace.write` over the host's
//! capability handle. There is no network, no timers, and no Node API.
//!
//! Determinism (sandbox spec S2): every nondeterministic source a stock JS
//! engine would reach for is replaced by a function of the seed. The runner
//! itself reads no clock and no entropy beyond `harness.seed()`.
//!
//! Note: this crate is built only by `build.sh` against the wasm toolchain;
//! the main `cargo` workspace never compiles it. The rquickjs API calls below
//! are pinned to the version in Cargo.toml and may need a touch-up on a major
//! upgrade — the imports-allowlist and S2 tests in `harness-sandbox` are the
//! backstop.

use std::cell::RefCell;

use rquickjs::{Context, Function, Runtime, Value};

// The host capabilities, module `harness` (compute.rs). The host's
// deterministic WASI stubs cover wasi-libc's own imports separately.
#[link(wasm_import_module = "harness")]
unsafe extern "C" {
    fn seed() -> i64;
    fn ws_size(path_ptr: *const u8, path_len: i32) -> i64;
    fn ws_read(path_ptr: *const u8, path_len: i32, dst_ptr: *mut u8, dst_cap: i32) -> i64;
    fn ws_write(path_ptr: *const u8, path_len: i32, src_ptr: *const u8, src_len: i32) -> i64;
}

/// Allocate a guest buffer for the host to write input into (guest ABI).
/// The instance is discarded after one `run`, so leaking is correct: there
/// is no second call to free for.
#[unsafe(no_mangle)]
pub extern "C" fn alloc(len: i32) -> *mut u8 {
    let mut buf = vec![0u8; len.max(0) as usize];
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}

/// Run one script (guest ABI). Reads `len` bytes of `{script, input}` JSON at
/// `ptr`, evaluates, and returns the packed `(out_ptr << 32) | out_len` of
/// the `{result, console}` / `{error, console}` JSON output.
#[unsafe(no_mangle)]
pub extern "C" fn run(ptr: *const u8, len: i32) -> i64 {
    let input = unsafe { std::slice::from_raw_parts(ptr, len.max(0) as usize) };
    let bytes = run_inner(input).into_bytes();
    let out_ptr = bytes.as_ptr() as u64;
    let out_len = bytes.len() as u64;
    std::mem::forget(bytes);
    ((out_ptr << 32) | (out_len & 0xFFFF_FFFF)) as i64
}

thread_local! {
    static CONSOLE: RefCell<String> = const { RefCell::new(String::new()) };
}

fn run_inner(input: &[u8]) -> String {
    let request: serde_json::Value = match serde_json::from_slice(input) {
        Ok(value) => value,
        Err(e) => return error_json(&format!("malformed runner input: {e}"), ""),
    };
    let script = match request.get("script").and_then(|v| v.as_str()) {
        Some(s) => s.to_string(),
        None => return error_json("missing `script`", ""),
    };
    // `input` is re-serialized for the prelude's `JSON.parse`; absent stays
    // `undefined` in the script.
    let input_json = request.get("input").map(|v| v.to_string());

    CONSOLE.with(|b| b.borrow_mut().clear());

    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(e) => return error_json(&format!("runtime: {e}"), ""),
    };
    let ctx = match Context::full(&rt) {
        Ok(ctx) => ctx,
        Err(e) => return error_json(&format!("context: {e}"), ""),
    };

    ctx.with(|ctx| {
        let globals = ctx.globals();
        // The seed is handed to JS as a decimal string, losslessly: an f64
        // would collapse seeds differing below 2^-52, and host seeds span the
        // full 64-bit range. The prelude rebuilds it with BigInt.
        let _ = globals.set(
            "__seed",
            Function::new(ctx.clone(), || (unsafe { seed() } as u64).to_string()),
        );
        let _ = globals.set(
            "__ws_read",
            Function::new(ctx.clone(), |path: String| ws_read_rs(&path)),
        );
        let _ = globals.set(
            "__ws_write",
            Function::new(ctx.clone(), |path: String, content: String| {
                ws_write_rs(&path, &content)
            }),
        );
        let _ = globals.set(
            "__console_write",
            Function::new(ctx.clone(), |line: String| {
                CONSOLE.with(|b| {
                    let mut b = b.borrow_mut();
                    b.push_str(&line);
                    b.push('\n');
                });
            }),
        );
        let _ = globals.set("__input_json", input_json.clone());

        if let Err(e) = ctx.eval::<Value, _>(PRELUDE) {
            return error_json(&format!("prelude: {}", describe(&ctx, e)), &console_take());
        }
        match ctx.eval::<Value, _>(script.as_bytes()) {
            Ok(value) => {
                let result = ctx.json_stringify(&value).ok().flatten();
                let result = result
                    .and_then(|s| s.to_string().ok())
                    .unwrap_or_else(|| "null".to_string());
                result_json(&result, &console_take())
            }
            Err(e) => error_json(&describe(&ctx, e), &console_take()),
        }
    })
}

/// The deterministic JavaScript environment (seeded `Math.random`, frozen
/// `Date`, `console`, `workspace`). Kept in its own file as the single source
/// of truth: `tests/prelude_determinism.mjs` runs the same text through a
/// real engine to validate the determinism design independently of the wasm
/// build.
const PRELUDE: &str = include_str!("prelude.js");

// --- host helpers -----------------------------------------------------------

const READ_CAP: usize = 256 * 1024;

fn ws_read_rs(path: &str) -> Option<String> {
    let p = path.as_bytes();
    let size = unsafe { ws_size(p.as_ptr(), p.len() as i32) };
    if size < 0 {
        return None;
    }
    let want = (size as usize).min(READ_CAP);
    let mut buf = vec![0u8; want];
    let got = unsafe { ws_read(p.as_ptr(), p.len() as i32, buf.as_mut_ptr(), want as i32) };
    if got < 0 {
        return None;
    }
    buf.truncate(want.min(got as usize));
    String::from_utf8(buf).ok()
}

fn ws_write_rs(path: &str, content: &str) -> bool {
    let p = path.as_bytes();
    let c = content.as_bytes();
    unsafe { ws_write(p.as_ptr(), p.len() as i32, c.as_ptr(), c.len() as i32) >= 0 }
}

fn console_take() -> String {
    CONSOLE.with(|b| std::mem::take(&mut *b.borrow_mut()))
}

fn describe(ctx: &rquickjs::Ctx, err: rquickjs::Error) -> String {
    if matches!(err, rquickjs::Error::Exception) {
        let exc = ctx.catch();
        if let Some(obj) = exc.as_object() {
            if let Ok(message) = obj.get::<_, String>("message") {
                return message;
            }
        }
        if let Some(s) = exc.as_string().and_then(|s| s.to_string().ok()) {
            return s;
        }
    }
    err.to_string()
}

fn result_json(result: &str, console: &str) -> String {
    format!("{{\"result\":{},\"console\":{}}}", result, json_string(console))
}

fn error_json(message: &str, console: &str) -> String {
    format!(
        "{{\"error\":{},\"console\":{}}}",
        json_string(message),
        json_string(console)
    )
}

/// JSON string escaping for the messages and console we wrap by hand.
fn json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
