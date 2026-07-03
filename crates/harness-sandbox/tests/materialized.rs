//! `MaterializedSandboxes`: a shell-capable workspace backed by a durable `Fs` grain
//! (granary §7.10; harness §5.5).
//!
//! Drives the provider through the `SandboxProvider`/`Sandbox` seam over the
//! `Workspace` tier alone — no container or microVM is needed to exercise the
//! durability seam, since `hydrate`/`sync_back` operate on the real directory the
//! inner provider manages. The native shell tier is the same `TieredSandboxes` call
//! path and is covered by `tests/native.rs`/`tests/firecracker.rs`.

#![cfg(feature = "durable")]

use actor_core::LocalSystemBuilder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::fs::Fs;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use harness_sandbox::DurabilityRules;
use harness_sandbox::MaterializedSandboxes;
use harness_sandbox::TieredSandboxes;
use serde_json::json;

fn provider() -> (Simulation, MaterializedSandboxes<SimSystem>, tempfile::TempDir) {
    let sim = Simulation::new(1);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let granary = system.granary::<Fs<SimSystem>>(GranaryConfig::default());
    let root = tempfile::tempdir().expect("workspaces tempdir");
    let inner = TieredSandboxes::new(root.path()).expect("inner provider");
    let provider =
        MaterializedSandboxes::new(inner, granary, root.path(), DurabilityRules::default())
            .expect("materialized provider");
    (sim, provider, root)
}

#[test]
fn the_provider_reports_a_durable_workspace() {
    let (_sim, provider, _root) = provider();
    assert!(provider.workspace_durable());
}

#[test]
fn durable_files_survive_release_and_reopen_excluded_trees_do_not() {
    let (sim, provider, _root) = provider();
    let session = SessionId::new("session-1");
    sim.block_on(async move {
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("open");

        // A durable write, and a write under an excluded (regenerable) tree.
        sb.call(
            Tier::Workspace,
            "write_file",
            json!({ "path": "src/main.rs", "content": "fn main() {}" }),
        )
        .await
        .expect("durable write");
        sb.call(
            Tier::Workspace,
            "write_file",
            json!({ "path": "target/debug/app", "content": "binary" }),
        )
        .await
        .expect("excluded write");

        // Release syncs the durable subtree into the grain and tears the local dir
        // down; reopen rehydrates from the grain alone.
        sb.release().await;
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("reopen");

        assert_eq!(
            sb.call(Tier::Workspace, "read_file", json!({ "path": "src/main.rs" }))
                .await
                .expect("durable read"),
            json!({ "content": "fn main() {}", "truncated": false }),
            "durable content must survive release+reopen",
        );
        assert!(
            matches!(
                sb.call(Tier::Workspace, "read_file", json!({ "path": "target/debug/app" })).await,
                Err(ToolError::Sandbox(_))
            ),
            "an excluded tree must not be persisted into the grain",
        );
    });
}

#[test]
fn an_oversized_durable_file_is_not_silently_persisted() {
    // sync_back caps the durable subtree (mirroring Firecracker's tar cap). A file
    // past the cap fails the sync — which `release` logs loudly and swallows (the
    // seam returns `()`), so the safe outcome is that it is simply not persisted,
    // never a silent partial write. Reopen confirms the grain holds nothing.
    let (sim, provider, _root) = provider();
    let session = SessionId::new("session-big");
    sim.block_on(async move {
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("open");
        // 64 MiB + 1: one byte past MAX_DURABLE.
        let big = "a".repeat((64 << 20) + 1);
        sb.call(Tier::Workspace, "write_file", json!({ "path": "huge.bin", "content": big }))
            .await
            .expect("write huge");
        sb.release().await;

        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("reopen");
        assert!(
            matches!(
                sb.call(Tier::Workspace, "read_file", json!({ "path": "huge.bin" })).await,
                Err(ToolError::Sandbox(_))
            ),
            "an over-cap file must not be persisted into the grain",
        );
    });
}

#[test]
fn edit_file_replaces_an_exact_string_and_guards_ambiguity() {
    let (sim, provider, _root) = provider();
    let session = SessionId::new("session-edit");
    sim.block_on(async move {
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("open");
        sb.call(Tier::Workspace, "write_file", json!({ "path": "a.txt", "content": "foo bar foo" }))
            .await
            .expect("write");

        // A non-unique match without replace_all fails, untouched.
        assert!(matches!(
            sb.call(Tier::Workspace, "edit_file",
                json!({ "path": "a.txt", "old_string": "foo", "new_string": "baz" })).await,
            Err(ToolError::InvalidArguments(_))
        ));

        // replace_all rewrites every occurrence.
        assert_eq!(
            sb.call(Tier::Workspace, "edit_file",
                json!({ "path": "a.txt", "old_string": "foo", "new_string": "baz", "replace_all": true }))
                .await
                .expect("edit"),
            json!({ "replaced": 2 }),
        );
        assert_eq!(
            sb.call(Tier::Workspace, "read_file", json!({ "path": "a.txt" })).await.unwrap(),
            json!({ "content": "baz bar baz", "truncated": false }),
        );

        // A missing old_string is an error, not a silent no-op.
        assert!(matches!(
            sb.call(Tier::Workspace, "edit_file",
                json!({ "path": "a.txt", "old_string": "nope", "new_string": "x" })).await,
            Err(ToolError::InvalidArguments(_))
        ));
    });
}

#[test]
fn read_file_pages_by_line_window() {
    let (sim, provider, _root) = provider();
    let session = SessionId::new("session-read");
    sim.block_on(async move {
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("open");
        sb.call(Tier::Workspace, "write_file",
            json!({ "path": "lines.txt", "content": "l1\nl2\nl3\nl4\nl5\n" }))
            .await
            .expect("write");

        // offset is 1-based; limit caps the line count. The window rejoins exactly.
        assert_eq!(
            sb.call(Tier::Workspace, "read_file",
                json!({ "path": "lines.txt", "offset": 2, "limit": 2 })).await.unwrap(),
            json!({ "content": "l2\nl3\n", "truncated": false }),
        );
        // No window reads the whole file (back-compat).
        assert_eq!(
            sb.call(Tier::Workspace, "read_file", json!({ "path": "lines.txt" })).await.unwrap(),
            json!({ "content": "l1\nl2\nl3\nl4\nl5\n", "truncated": false }),
        );
    });
}

#[test]
fn a_delete_on_disk_propagates_to_the_grain() {
    let (sim, provider, _root) = provider();
    let session = SessionId::new("session-2");
    sim.block_on(async move {
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("open");
        sb.call(Tier::Workspace, "write_file", json!({ "path": "keep.txt", "content": "a" }))
            .await
            .expect("write keep");
        sb.call(Tier::Workspace, "write_file", json!({ "path": "gone.txt", "content": "b" }))
            .await
            .expect("write gone");
        sb.release().await;

        // Reopen, delete one durable file, release again.
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("reopen");
        sb.call(Tier::Workspace, "remove", json!({ "path": "gone.txt" }))
            .await
            .expect("remove");
        sb.release().await;

        // Final reopen: the kept file is present, the removed one stays gone (prune
        // propagated the delete into the grain).
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("reopen 2");
        assert_eq!(
            sb.call(Tier::Workspace, "read_file", json!({ "path": "keep.txt" }))
                .await
                .expect("read keep"),
            json!({ "content": "a", "truncated": false }),
        );
        assert!(
            matches!(
                sb.call(Tier::Workspace, "read_file", json!({ "path": "gone.txt" })).await,
                Err(ToolError::Sandbox(_))
            ),
            "a file deleted on disk must not reappear from the grain",
        );
    });
}
