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
             truncated past 256 KiB. To page a large file, pass `offset` (1-based \
             line) and `limit` (line count).",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace-relative path."},
                    "offset": {"type": "integer", "description": "1-based first line to return; defaults to 1."},
                    "limit": {"type": "integer", "description": "Maximum number of lines to return; defaults to all."}
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
            "edit_file",
            "Replace an exact string in a workspace file. `old_string` must match \
             once unless `replace_all` is set; if it is missing or not unique the \
             edit fails untouched. Prefer this over rewriting the whole file.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Workspace-relative path."},
                    "old_string": {"type": "string", "description": "Exact text to replace."},
                    "new_string": {"type": "string", "description": "Replacement text."},
                    "replace_all": {"type": "boolean", "description": "Replace every occurrence; default false (requires a unique match)."}
                },
                "required": ["path", "old_string", "new_string"]
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
        "edit_file" => edit_file(dir, input),
        "list_dir" => list_dir(dir, input),
        "remove" => remove(dir, input),
        other => Err(ToolError::Sandbox(format!(
            "tool not provided by this sandbox: {other}"
        ))),
    }
}

fn read_file(dir: &Dir, input: &Value) -> Result<Value, ToolError> {
    let path = required_str(input, "path")?;
    let (offset, limit) = (optional_u64(input, "offset")?, optional_u64(input, "limit")?);
    let bytes = dir
        .read(path)
        .map_err(|e| ToolError::Sandbox(format!("read_file: {path}: {e}")))?;
    Ok(cap_and_decode(&bytes, offset, limit))
}

fn edit_file(dir: &Dir, input: &Value) -> Result<Value, ToolError> {
    let path = required_str(input, "path")?;
    let bytes = dir
        .read(path)
        .map_err(|e| ToolError::Sandbox(format!("edit_file: {path}: {e}")))?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| ToolError::Sandbox(format!("edit_file: {path}: not a UTF-8 text file")))?;
    let updated = edit_text(text, input, path)?;
    dir.write(path, updated.text.as_bytes())
        .map_err(|e| ToolError::Sandbox(format!("edit_file: {path}: {e}")))?;
    Ok(json!({ "replaced": updated.replaced }))
}

/// The result of one `edit_file` string replacement: the new content and how many
/// occurrences were replaced. Factored out so every edit tool enforces identical
/// match/uniqueness semantics.
pub(crate) struct Edit {
    pub text: String,
    pub replaced: usize,
}

/// Apply an `edit_file` call (`old_string` → `new_string`, optional `replace_all`)
/// to `text`. A pure function of the content and the call: an absent or — without
/// `replace_all` — non-unique `old_string` is an `InvalidArguments` error, so an
/// ambiguous edit fails loudly rather than touching the wrong site.
pub(crate) fn edit_text(text: &str, input: &Value, path: &str) -> Result<Edit, ToolError> {
    let old = required_str(input, "old_string")?;
    let new = required_str(input, "new_string")?;
    if old.is_empty() {
        return Err(ToolError::InvalidArguments(
            "`old_string` must be non-empty".to_string(),
        ));
    }
    if old == new {
        return Err(ToolError::InvalidArguments(
            "`old_string` and `new_string` are identical".to_string(),
        ));
    }
    let replace_all = input
        .get("replace_all")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let count = text.matches(old).count();
    if count == 0 {
        return Err(ToolError::InvalidArguments(format!(
            "`old_string` not found in {path}"
        )));
    }
    if count > 1 && !replace_all {
        return Err(ToolError::InvalidArguments(format!(
            "`old_string` occurs {count} times in {path}; pass replace_all or include more \
             surrounding context to make it unique"
        )));
    }
    let (text, replaced) = if replace_all {
        (text.replace(old, new), count)
    } else {
        (text.replacen(old, new, 1), 1)
    };
    Ok(Edit { text, replaced })
}

/// Decode `bytes` as UTF-8, optionally to a 1-based line window (`offset`/`limit`,
/// for paging past the cap), then apply the 256 KiB cap with clean truncation —
/// the tier's `{content, truncated}` shape. Factored out so every read tool pages
/// identically. Pure: a function of the bytes and the window.
pub(crate) fn cap_and_decode(bytes: &[u8], offset: Option<u64>, limit: Option<u64>) -> Value {
    let text = String::from_utf8_lossy(bytes);
    // The requested line window, if any. `split_inclusive` keeps each line's
    // trailing newline, so the slice rejoins to the exact original bytes.
    let content: String = if offset.is_some() || limit.is_some() {
        let lines: Vec<&str> = text.split_inclusive('\n').collect();
        let start = (offset.unwrap_or(1).max(1) - 1) as usize;
        let start = start.min(lines.len());
        let end = match limit {
            Some(n) => start.saturating_add(n as usize).min(lines.len()),
            None => lines.len(),
        };
        lines[start..end].concat()
    } else {
        text.into_owned()
    };
    let truncated = content.len() > READ_CAP;
    // Never cut a multi-byte character in half: walk the cap back to the last
    // whole character.
    let mut end = if truncated { READ_CAP } else { content.len() };
    while truncated && end > 0 && !content.is_char_boundary(end) {
        end -= 1;
    }
    json!({ "content": content[..end].to_string(), "truncated": truncated })
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

/// An optional non-negative integer argument (e.g. `read_file`'s `offset`/`limit`).
/// Absent or null is `None`; a present non-integer is an error.
pub(crate) fn optional_u64(input: &Value, key: &str) -> Result<Option<u64>, ToolError> {
    match input.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(|| {
            ToolError::InvalidArguments(format!("`{key}` must be a non-negative integer"))
        }),
    }
}
