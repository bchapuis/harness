//! The durable workspace provider (granary §7.10; harness §5.5 reversal).
//!
//! Drives `DurableWorkspaces` through the `SandboxProvider`/`Sandbox` seam: the four
//! `Workspace` tools keep their JSON contract, durable paths are backed by the
//! filesystem grain and survive a sandbox release+reopen, non-durable (excluded) paths
//! live in the ephemeral overlay and are lost on release, `list_dir` merges the two,
//! and a durable-path failure is a transient `Sandbox` error — never `EnvironmentLost`
//! (so the harness's §5.5 reset is not entered for durable content).

#![cfg(feature = "durable")]

use actor_core::LocalSystemBuilder;
use actor_simulation::SimSystem;
use actor_simulation::Simulation;
use granary::GranaryConfig;
use granary::GranaryExt;
use granary::fs::Workspace;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use harness_sandbox::DurableWorkspaces;
use serde_json::json;

fn provider() -> (Simulation, DurableWorkspaces<SimSystem>, tempfile::TempDir) {
    let sim = Simulation::new(1);
    let system = LocalSystemBuilder::new(sim.clock(), sim.entropy(), sim.spawner()).build();
    let granary = system.granary::<Workspace<SimSystem>>(GranaryConfig::default());
    let overlay = tempfile::tempdir().expect("overlay tempdir");
    let provider = DurableWorkspaces::new(granary, overlay.path()).expect("provider");
    (sim, provider, overlay)
}

#[test]
fn durable_paths_survive_release_and_reopen_overlay_does_not() {
    let (sim, provider, _overlay) = provider();
    let session = SessionId::new("session-1");
    sim.block_on(async move {
        let sb = provider
            .open(&session, &SandboxProfile::default())
            .await
            .expect("open");

        // A durable write, and a non-durable write under an excluded tree.
        assert_eq!(
            sb.call(
                Tier::Workspace,
                "write_file",
                json!({ "path": "src/main.rs", "content": "fn main() {}" }),
            )
            .await
            .unwrap(),
            json!({ "bytes": 12 })
        );
        sb.call(
            Tier::Workspace,
            "write_file",
            json!({ "path": "node_modules/dep.js", "content": "module.exports = {}" }),
        )
        .await
        .unwrap();

        // The durable read round-trips with the tier's exact JSON shape.
        assert_eq!(
            sb.call(Tier::Workspace, "read_file", json!({ "path": "src/main.rs" })).await.unwrap(),
            json!({ "content": "fn main() {}", "truncated": false })
        );

        // `list_dir` at the root merges the durable child (src) and the overlay child
        // (node_modules), name-sorted.
        let listing = sb.call(Tier::Workspace, "list_dir", json!({ "path": "." })).await.unwrap();
        let names: Vec<&str> = listing["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, vec!["node_modules", "src"]);

        // Release (the §5.5 reversal): the overlay is dropped, the grain is not.
        sb.release().await;

        // Reopen the same session: the durable file survives; the overlay file is gone.
        let sb = provider
            .open(&session, &SandboxProfile::default())
            .await
            .expect("reopen");
        assert_eq!(
            sb.call(Tier::Workspace, "read_file", json!({ "path": "src/main.rs" })).await.unwrap(),
            json!({ "content": "fn main() {}", "truncated": false }),
            "durable content must survive release+reopen",
        );
        assert!(
            matches!(
                sb.call(Tier::Workspace, "read_file", json!({ "path": "node_modules/dep.js" })).await,
                Err(ToolError::Sandbox(_))
            ),
            "the non-durable overlay file must be gone after release",
        );
    });
}

#[test]
fn a_durable_failure_is_never_environment_lost() {
    // Reading a missing durable file is a transient `Sandbox` error, NOT
    // `EnvironmentLost` — so the harness's §5.5 reset is not triggered for durable
    // content (the grain is the durable source of truth).
    let (sim, provider, _overlay) = provider();
    let session = SessionId::new("session-2");
    sim.block_on(async move {
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("open");
        let result = sb.call(Tier::Workspace, "read_file", json!({ "path": "src/missing.rs" })).await;
        assert!(
            matches!(result, Err(ToolError::Sandbox(_))),
            "a missing durable file must be Sandbox, never EnvironmentLost: {result:?}",
        );
    });
}

#[test]
fn other_tiers_are_not_offered() {
    let (sim, provider, _overlay) = provider();
    let session = SessionId::new("session-3");
    sim.block_on(async move {
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("open");
        assert!(matches!(
            sb.call(Tier::Compute, "run_js", json!({})).await,
            Err(ToolError::Sandbox(_))
        ));
    });
}

#[test]
fn edit_file_and_ranged_read_route_through_the_grain() {
    // The new tools work on a durable path (the grain), with the same semantics as
    // the cap-std tier, so every sandbox mode behaves identically.
    let (sim, provider, _overlay) = provider();
    let session = SessionId::new("session-edit");
    sim.block_on(async move {
        let sb = provider.open(&session, &SandboxProfile::default()).await.expect("open");
        sb.call(Tier::Workspace, "write_file",
            json!({ "path": "src/lib.rs", "content": "a\nb\nc\nd\n" }))
            .await
            .expect("write");

        // Targeted edit on a durable path.
        assert_eq!(
            sb.call(Tier::Workspace, "edit_file",
                json!({ "path": "src/lib.rs", "old_string": "b\n", "new_string": "B\n" }))
                .await
                .expect("edit"),
            json!({ "replaced": 1 }),
        );
        // Ranged read of the durable file.
        assert_eq!(
            sb.call(Tier::Workspace, "read_file",
                json!({ "path": "src/lib.rs", "offset": 2, "limit": 2 })).await.unwrap(),
            json!({ "content": "B\nc\n", "truncated": false }),
        );
        // A missing old_string is a loud error, never a silent no-op.
        assert!(matches!(
            sb.call(Tier::Workspace, "edit_file",
                json!({ "path": "src/lib.rs", "old_string": "zzz", "new_string": "x" })).await,
            Err(ToolError::InvalidArguments(_))
        ));
    });
}
