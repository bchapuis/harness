//! Workspace-tier conformance: S1 (confinement by capability handle —
//! adversarial traversal) and S5 (per-tier release) of sandbox spec §6.

mod support;

use std::sync::Arc;

use futures::executor::block_on;
use harness::Sandbox;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use harness_sandbox::TieredSandboxes;
use serde_json::Value;
use serde_json::json;

fn open(provider: &TieredSandboxes, session: &str) -> Arc<dyn Sandbox> {
    block_on(provider.open(&SessionId::new(session), &SandboxProfile::default()))
        .expect("workspace open")
}

fn call(sandbox: &Arc<dyn Sandbox>, name: &str, input: Value) -> Result<Value, ToolError> {
    block_on(sandbox.call(Tier::Workspace, name, input))
}

// ---------------------------------------------------------------------------
// S1: adversarial traversal — escapes are unrepresentable, not filtered
// ---------------------------------------------------------------------------

#[test]
fn paths_outside_the_workspace_are_unrepresentable() {
    let dir = tempfile::tempdir().expect("tempdir");
    // A sentinel outside every workspace: confinement holding means no tool
    // invocation can read or touch it.
    let sentinel = dir.path().join("sentinel");
    std::fs::write(&sentinel, "outside").expect("sentinel");
    let provider = TieredSandboxes::new(dir.path().join("workspaces")).expect("provider");
    let sandbox = open(&provider, "s");

    for escape in [
        "../sentinel",
        "../../sentinel",
        "sub/../../sentinel",
        "/etc/passwd",
        "..",
    ] {
        let read = call(&sandbox, "read_file", json!({"path": escape}));
        assert!(
            matches!(read, Err(ToolError::Sandbox(_))),
            "read_file({escape}) must fail as an outcome, got {read:?}"
        );
        let write = call(
            &sandbox,
            "write_file",
            json!({"path": escape, "content": "evil"}),
        );
        assert!(
            matches!(write, Err(ToolError::Sandbox(_))),
            "write_file({escape}) must fail as an outcome, got {write:?}"
        );
        let list = call(&sandbox, "list_dir", json!({"path": escape}));
        assert!(
            matches!(list, Err(ToolError::Sandbox(_))),
            "list_dir({escape}) must fail as an outcome, got {list:?}"
        );
    }
    // The sentinel never moved: nothing escaped.
    assert_eq!(
        std::fs::read_to_string(&sentinel).expect("sentinel"),
        "outside"
    );
}

#[test]
fn an_out_pointing_symlink_is_not_followed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let sentinel = dir.path().join("sentinel");
    std::fs::write(&sentinel, "outside").expect("sentinel");
    let workspaces = dir.path().join("workspaces");
    let provider = TieredSandboxes::new(&workspaces).expect("provider");
    let sandbox = open(&provider, "s");

    // Plant the symlink with ambient authority, as an attacker who once had
    // a foothold would: the handle, not the directory's cleanliness, is the
    // boundary.
    let session_dir = std::fs::read_dir(&workspaces)
        .expect("workspaces")
        .next()
        .expect("session dir")
        .expect("entry")
        .path();
    std::os::unix::fs::symlink(&sentinel, session_dir.join("link")).expect("symlink");

    let read = call(&sandbox, "read_file", json!({"path": "link"}));
    assert!(
        matches!(read, Err(ToolError::Sandbox(_))),
        "reading through an out-pointing symlink must fail, got {read:?}"
    );
    let write = call(
        &sandbox,
        "write_file",
        json!({"path": "link", "content": "evil"}),
    );
    assert!(
        matches!(write, Err(ToolError::Sandbox(_))),
        "writing through an out-pointing symlink must fail, got {write:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&sentinel).expect("sentinel"),
        "outside"
    );
}

// ---------------------------------------------------------------------------
// The tools as pure functions of call + filesystem
// ---------------------------------------------------------------------------

#[test]
fn the_typed_tools_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "s");

    let written = call(
        &sandbox,
        "write_file",
        json!({"path": "notes/plan.md", "content": "hello"}),
    )
    .expect("write");
    assert_eq!(written, json!({"bytes": 5}));

    let read = call(&sandbox, "read_file", json!({"path": "notes/plan.md"})).expect("read");
    assert_eq!(read, json!({"content": "hello", "truncated": false}));

    let listed = call(&sandbox, "list_dir", json!({})).expect("list");
    assert_eq!(
        listed,
        json!({"entries": [{"name": "notes", "kind": "dir", "size": 0}]})
    );
    let listed = call(&sandbox, "list_dir", json!({"path": "notes"})).expect("list");
    assert_eq!(
        listed,
        json!({"entries": [{"name": "plan.md", "kind": "file", "size": 5}]})
    );

    call(
        &sandbox,
        "remove",
        json!({"path": "notes", "recursive": true}),
    )
    .expect("remove");
    let listed = call(&sandbox, "list_dir", json!({})).expect("list");
    assert_eq!(listed, json!({"entries": []}));
    // A missing path is success: the tool is idempotent, as its
    // `OnDangling::Reexecute` declaration asserts.
    call(
        &sandbox,
        "remove",
        json!({"path": "notes", "recursive": true}),
    )
    .expect("idempotent");
}

#[test]
fn malformed_arguments_are_invalid_not_sandbox_failures() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "s");

    let bad = call(&sandbox, "read_file", json!({"path": 42}));
    assert!(matches!(bad, Err(ToolError::InvalidArguments(_))));
    let bad = call(&sandbox, "write_file", json!({"path": "f"}));
    assert!(matches!(bad, Err(ToolError::InvalidArguments(_))));
    let bad = call(&sandbox, "no_such_tool", json!({}));
    assert!(matches!(bad, Err(ToolError::Sandbox(_))));
}

#[test]
fn tiers_not_offered_fail_as_outcomes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "s");

    let outcome = block_on(sandbox.call(Tier::Native, "anything", json!({})));
    assert!(
        matches!(outcome, Err(ToolError::Sandbox(_))),
        "an unoffered tier is a per-call failure (§5.4), got {outcome:?}"
    );
}

// ---------------------------------------------------------------------------
// S5: per-tier release, idempotent
// ---------------------------------------------------------------------------

#[test]
fn release_removes_the_workspace_and_is_idempotent() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "s");
    call(&sandbox, "write_file", json!({"path": "f", "content": "x"})).expect("write");
    assert_eq!(provider.stats.opened(), 1);

    block_on(sandbox.release());
    assert_eq!(provider.stats.released(), 1);
    assert_eq!(
        std::fs::read_dir(dir.path()).expect("root").count(),
        0,
        "the session's workspace directory is gone"
    );
    // Idempotent: a second release is a no-op, not an error.
    block_on(sandbox.release());
}

#[test]
fn sessions_are_scoped_apart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let a = open(&provider, "session-a");
    let b = open(&provider, "session-b");

    call(&a, "write_file", json!({"path": "f", "content": "a's"})).expect("write");
    let missing = call(&b, "read_file", json!({"path": "f"}));
    assert!(
        matches!(missing, Err(ToolError::Sandbox(_))),
        "b must not see a's file, got {missing:?}"
    );
    // Releasing b leaves a intact (H8's cross-session isolation, by
    // construction of the provider).
    block_on(b.release());
    let read = call(&a, "read_file", json!({"path": "f"})).expect("read");
    assert_eq!(read, json!({"content": "a's", "truncated": false}));
}

// ---------------------------------------------------------------------------
// Loss reporting (sandbox spec §4; harness spec §5.5)
// ---------------------------------------------------------------------------

#[test]
fn a_vanished_workspace_surfaces_as_environment_loss() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "s");
    call(&sandbox, "write_file", json!({"path": "f", "content": "x"})).expect("write");

    // The world loses the workspace behind the provider's back.
    let session_dir = std::fs::read_dir(dir.path())
        .expect("root")
        .next()
        .expect("session dir")
        .expect("entry")
        .path();
    std::fs::remove_dir_all(&session_dir).expect("external loss");

    // Not an ordinary tool failure: only EnvironmentLost engages the
    // harness's reset protocol (§5.5) — a Sandbox error here would leave the
    // model retrying against state that no longer exists.
    let lost = call(&sandbox, "read_file", json!({"path": "f"}));
    assert!(
        matches!(lost, Err(ToolError::EnvironmentLost(_))),
        "a vanished workspace is environment loss, got {lost:?}"
    );
    // A merely missing file in a live workspace stays an ordinary failure.
    let sandbox = open(&provider, "s2");
    let missing = call(&sandbox, "read_file", json!({"path": "absent"}));
    assert!(
        matches!(missing, Err(ToolError::Sandbox(_))),
        "a missing file is not environment loss, got {missing:?}"
    );
}

#[test]
fn truncation_never_splits_a_character() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "s");

    // 256 KiB - 1 of ASCII, then a 3-byte character straddling the cap.
    let content = format!("{}€tail", "a".repeat(256 * 1024 - 1));
    call(
        &sandbox,
        "write_file",
        json!({"path": "f", "content": content}),
    )
    .expect("write");
    let read = call(&sandbox, "read_file", json!({"path": "f"})).expect("read");
    assert_eq!(read["truncated"], true);
    let text = read["content"].as_str().expect("content");
    assert!(
        !text.contains('\u{FFFD}'),
        "truncation must end at a whole character, not a replacement character"
    );
    assert_eq!(
        text.len(),
        256 * 1024 - 1,
        "the straddling char is dropped whole"
    );
}
