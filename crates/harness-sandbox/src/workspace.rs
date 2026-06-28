//! The `Workspace` tier (sandbox spec §3.1): typed tools over a capability
//! handle.
//!
//! Confinement is the [`Dir`] handle itself: every path in every tool
//! resolves through cap-std, which re-resolves each component inside the
//! handle — `../`, absolute paths, and out-pointing symlinks are
//! *unrepresentable*, not filtered (S1). No string sanitization appears
//! anywhere in this module, deliberately: sanitization filters a
//! representable escape, and the symlink/TOCTOU class survives filtering.
//!
//! The tools are pure functions of the call and the filesystem (S2's
//! discipline applied one tier down): no ambient clock or entropy reaches an
//! output — no mtimes, and `list_dir` is name-sorted — so they run
//! unmodified under the deterministic simulator (core spec §18.1).

use std::path::Path;

use cap_std::fs::Dir;
use harness::OnDangling;
use harness::Tier;
use harness::ToolDecl;
use harness::ToolError;
use serde_json::Value;
use serde_json::json;

/// Cap on `read_file` content, so one large file cannot blow up the journal
/// and the model context it feeds.
const READ_CAP: usize = 256 * 1024;

/// The workspace tier's tool declarations, ready for [`harness::Kind::tool`]:
/// every declaration is `Tier::Workspace`, and every tool is idempotent, so
/// blind re-execution after a crash is safe (`OnDangling::Reexecute`,
/// harness spec §5.5).
pub fn workspace_tools() -> Vec<ToolDecl> {
    let decl = |name: &str, description: &str, schema: Value| ToolDecl {
        name: name.to_string(),
        description: description.to_string(),
        input_schema: schema,
        tier: Tier::Workspace,
        on_dangling: OnDangling::Reexecute,
        timeout: None,
    };
    vec![
        decl(
            "read_file",
            "Read a UTF-8 file from the session workspace. Returns its content, \
             truncated past 256 KiB.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace-relative path."}
                },
                "required": ["path"]
            }),
        ),
        decl(
            "write_file",
            "Write a UTF-8 file into the session workspace, creating parent \
             directories as needed. Returns the byte count written.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace-relative path."},
                    "content": {"type": "string", "description": "The full file content."}
                },
                "required": ["path", "content"]
            }),
        ),
        decl(
            "list_dir",
            "List a workspace directory: name, kind, and size per entry, sorted \
             by name. Defaults to the workspace root.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace-relative path; defaults to \".\"."}
                }
            }),
        ),
        decl(
            "remove",
            "Remove a workspace file or directory. A missing path is success; a \
             non-empty directory needs `recursive`.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace-relative path."},
                    "recursive": {"type": "boolean", "description": "Remove a directory and its contents."}
                },
                "required": ["path"]
            }),
        ),
    ]
}

/// Execute one workspace tool against the session's capability handle.
pub(crate) fn call(dir: &Dir, name: &str, input: &Value) -> Result<Value, ToolError> {
    match name {
        "read_file" => read_file(dir, input),
        "write_file" => write_file(dir, input),
        "list_dir" => list_dir(dir, input),
        "remove" => remove(dir, input),
        other => Err(ToolError::Sandbox(format!(
            "tool not provided by this sandbox: {other}"
        ))),
    }
}

fn read_file(dir: &Dir, input: &Value) -> Result<Value, ToolError> {
    let path = required_str(input, "path")?;
    let bytes = dir
        .read(path)
        .map_err(|e| ToolError::Sandbox(format!("read_file: {path}: {e}")))?;
    Ok(cap_and_decode(&bytes))
}

/// Apply the 256 KiB read cap and clean UTF-8 truncation, returning the tier's
/// `{content, truncated}` shape. Shared by the cap-std and durable read tools.
pub(crate) fn cap_and_decode(bytes: &[u8]) -> Value {
    let truncated = bytes.len() > READ_CAP;
    let mut end = if truncated { READ_CAP } else { bytes.len() };
    // Never cut a multi-byte character in half: walk the cap back off any
    // UTF-8 continuation bytes, so truncation ends cleanly at the last whole
    // character instead of a replacement character.
    while truncated && end > 0 && (bytes[end] & 0b1100_0000) == 0b1000_0000 {
        end -= 1;
    }
    let content = String::from_utf8_lossy(&bytes[..end]).into_owned();
    json!({ "content": content, "truncated": truncated })
}

fn write_file(dir: &Dir, input: &Value) -> Result<Value, ToolError> {
    let path = required_str(input, "path")?;
    let content = required_str(input, "content")?;
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        dir.create_dir_all(parent)
            .map_err(|e| ToolError::Sandbox(format!("write_file: {}: {e}", parent.display())))?;
    }
    dir.write(path, content.as_bytes())
        .map_err(|e| ToolError::Sandbox(format!("write_file: {path}: {e}")))?;
    Ok(json!({ "bytes": content.len() }))
}

fn list_dir(dir: &Dir, input: &Value) -> Result<Value, ToolError> {
    let path = match input.get("path") {
        None | Some(Value::Null) => ".",
        Some(value) => value
            .as_str()
            .ok_or_else(|| ToolError::InvalidArguments("`path` must be a string".to_string()))?,
    };
    let read = dir
        .read_dir(path)
        .map_err(|e| ToolError::Sandbox(format!("list_dir: {path}: {e}")))?;
    let mut entries = Vec::new();
    for entry in read {
        let entry = entry.map_err(|e| ToolError::Sandbox(format!("list_dir: {path}: {e}")))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        let file_type = entry
            .file_type()
            .map_err(|e| ToolError::Sandbox(format!("list_dir: {name}: {e}")))?;
        let kind = if file_type.is_dir() {
            "dir"
        } else if file_type.is_symlink() {
            "symlink"
        } else {
            "file"
        };
        let size = if file_type.is_file() {
            entry
                .metadata()
                .map_err(|e| ToolError::Sandbox(format!("list_dir: {name}: {e}")))?
                .len()
        } else {
            0
        };
        entries.push((name, kind, size));
    }
    // Name-sorted: directory iteration order is OS state, not workspace
    // state, and a pure function leaks neither.
    entries.sort();
    let entries: Vec<Value> = entries
        .into_iter()
        .map(|(name, kind, size)| json!({ "name": name, "kind": kind, "size": size }))
        .collect();
    Ok(json!({ "entries": entries }))
}

fn remove(dir: &Dir, input: &Value) -> Result<Value, ToolError> {
    let path = required_str(input, "path")?;
    let recursive = input
        .get("recursive")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let result = match dir.remove_file(path) {
        Ok(()) => Ok(()),
        // Not a file: try it as a directory.
        Err(_) => {
            if recursive {
                dir.remove_dir_all(path)
            } else {
                dir.remove_dir(path)
            }
        }
    };
    match result {
        Ok(()) => Ok(json!({})),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(json!({})),
        Err(e) => Err(ToolError::Sandbox(format!("remove: {path}: {e}"))),
    }
}

pub(crate) fn required_str<'a>(input: &'a Value, key: &str) -> Result<&'a str, ToolError> {
    input
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ToolError::InvalidArguments(format!("`{key}` must be a string")))
}
