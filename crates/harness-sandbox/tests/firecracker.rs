//! Native-tier conformance at the microVM grade (feature `firecracker`):
//! S1 (the guest reaches the host filesystem through the synced workspace
//! only, and no network device exists at all), S5 (the VM dies on release,
//! idempotently), and the loss conduct of sandbox spec §4 — the firecracker
//! face of the docker suite in `native.rs`.
//!
//! Tests that boot a VM need Linux, `/dev/kvm`, and the assets
//! `guest/fc-rootfs/build.sh` produces (vmlinux, rootfs.ext4, firecracker);
//! they skip (eprintln + return) where any is missing, so macOS and KVM-less
//! machines stay green. With `E2E_REQUIRE` set, a missing prerequisite
//! panics instead: the CI job that exists to run this suite must not read a
//! broken environment as a pass. Point `HARNESS_FC_ASSETS` at an assets
//! directory to override the default `guest/fc-rootfs/out`. The conduct
//! tests at the bottom run everywhere.

#![cfg(feature = "firecracker")]

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use harness::Sandbox;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use harness_sandbox::FirecrackerConfig;
use harness_sandbox::TieredSandboxes;
use serde_json::Value;
use serde_json::json;

/// Skip (`None`) on a missing prerequisite — or panic under `E2E_REQUIRE`
/// (module docs).
fn missing<T>(reason: String) -> Option<T> {
    assert!(
        std::env::var_os("E2E_REQUIRE").is_none(),
        "E2E_REQUIRE is set but {reason}"
    );
    eprintln!("skipping: {reason}");
    None
}

fn assets() -> Option<(FirecrackerConfig, String)> {
    if !cfg!(target_os = "linux") {
        return missing("firecracker runs on linux only".to_string());
    }
    if !Path::new("/dev/kvm").exists() {
        return missing("/dev/kvm is absent".to_string());
    }
    let dir = std::env::var("HARNESS_FC_ASSETS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../guest/fc-rootfs/out")
        });
    let (binary, kernel, rootfs) = (
        dir.join("firecracker"),
        dir.join("vmlinux"),
        dir.join("rootfs.ext4"),
    );
    if !(binary.exists() && kernel.exists() && rootfs.exists()) {
        return missing(format!(
            "assets missing under {} (run guest/fc-rootfs/build.sh)",
            dir.display()
        ));
    }
    Some((
        FirecrackerConfig::new(binary, kernel),
        rootfs.display().to_string(),
    ))
}

macro_rules! require_assets {
    () => {
        match assets() {
            Some(config) => config,
            None => return,
        }
    };
}

fn provider(config: FirecrackerConfig) -> TieredSandboxes {
    TieredSandboxes::new().with_firecracker(config)
}

async fn open(
    provider: &TieredSandboxes,
    session: &str,
    image: &str,
    workspace: &Path,
) -> Arc<dyn Sandbox> {
    provider
        .open(
            &SessionId::new(session),
            &SandboxProfile::image(image),
            workspace,
        )
        .await
        .expect("open")
}

async fn shell(sandbox: &Arc<dyn Sandbox>, command: &str) -> Result<Value, ToolError> {
    sandbox
        .call(Tier::Native, "shell", json!({"command": command}))
        .await
}

async fn workspace(
    sandbox: &Arc<dyn Sandbox>,
    name: &str,
    input: Value,
) -> Result<Value, ToolError> {
    sandbox.call(Tier::Workspace, name, input).await
}

/// The VM's host-side footprint: the firecracker process whose command line
/// names this session's control directory (the same digest the tier
/// derives), and the directory itself. The tier canonicalizes the workspace
/// path before digesting; match it.
fn control_dir(workspace: &Path) -> PathBuf {
    let workspace = std::fs::canonicalize(workspace)
        .expect("workspace exists")
        .display()
        .to_string();
    let digest = harness::session::content_digest(&workspace);
    std::env::temp_dir().join(format!("harness-fc-{digest:016x}"))
}

fn vm_pids(control: &Path) -> Vec<String> {
    let output = std::process::Command::new("pgrep")
        .args(["-f", &control.display().to_string()])
        .output()
        .expect("pgrep");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_string)
        .collect()
}

// ---------------------------------------------------------------------------
// The shell tool over the synced workspace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shell_round_trips_through_the_synced_workspace() {
    let (config, image) = require_assets!();
    let dir = tempfile::tempdir().expect("tempdir");
    let ws = dir.path().join("rt");
    let provider = provider(config);
    let sandbox = open(&provider, "rt", &image, &ws).await;

    // Guest → host: a file the shell writes is pulled back to the workspace
    // the Workspace-tier tools see.
    let written = shell(&sandbox, "printf hello > f.txt")
        .await
        .expect("shell");
    assert_eq!(written["exit_code"], 0, "shell write failed: {written}");
    let read = workspace(&sandbox, "read_file", json!({"path": "f.txt"}))
        .await
        .expect("read");
    assert_eq!(read["content"], "hello");

    // Host → guest: the next call's push carries the host-side write in.
    workspace(
        &sandbox,
        "write_file",
        json!({"path": "g.txt", "content": "from-host"}),
    )
    .await
    .expect("write");
    let cat = shell(&sandbox, "cat g.txt").await.expect("shell");
    assert_eq!(cat["exit_code"], 0);
    assert_eq!(cat["stdout"], "from-host");

    // Guest state *outside* the workspace persists for the activation.
    shell(&sandbox, "echo once > /tmp/state")
        .await
        .expect("shell");
    let state = shell(&sandbox, "cat /tmp/state").await.expect("shell");
    assert_eq!(state["stdout"], "once\n");

    sandbox.release().await;
}

#[tokio::test]
async fn confinement_smoke() {
    let (config, image) = require_assets!();
    let dir = tempfile::tempdir().expect("tempdir");
    // A sentinel beside the workspaces root: only the workspace is synced,
    // so no path the guest can spell reaches it (S1, microVM grade — the
    // sentinel is not even on the guest's filesystem).
    let sentinel = dir.path().join("sentinel");
    std::fs::write(&sentinel, "outside").expect("sentinel");
    let ws = dir.path().join("workspaces/conf");
    let provider = provider(config);
    let sandbox = open(&provider, "conf", &image, &ws).await;

    for escape in [
        "cat /workspace/../sentinel",
        "cat ../sentinel",
        "cat /sentinel",
    ] {
        let out = shell(&sandbox, escape).await.expect("shell");
        assert_ne!(out["exit_code"], 0, "{escape} must not reach the sentinel");
    }
    // No network device exists: egress fails fast, DNS or no DNS.
    let net = shell(&sandbox, "wget -T 2 -q -O - http://1.1.1.1")
        .await
        .expect("shell");
    assert_ne!(net["exit_code"], 0, "egress must be unreachable: {net}");

    // An absolute symlink the guest plants does not survive the pull.
    shell(&sandbox, "ln -s /etc/passwd leak")
        .await
        .expect("shell");
    let listed = workspace(&sandbox, "list_dir", json!({"path": "."}))
        .await
        .expect("list");
    assert!(
        !listed.to_string().contains("leak"),
        "an absolute symlink target must be dropped at the pull: {listed}"
    );

    assert_eq!(
        std::fs::read_to_string(&sentinel).expect("sentinel"),
        "outside"
    );
    sandbox.release().await;
}

#[tokio::test]
async fn failures_are_outcomes_not_errors() {
    let (config, image) = require_assets!();
    let dir = tempfile::tempdir().expect("tempdir");
    let ws = dir.path().join("fail");
    let provider = provider(config);
    let sandbox = open(&provider, "fail", &image, &ws).await;

    // A nonzero exit is an outcome the model reacts to, never a ToolError.
    let out = shell(&sandbox, "exit 3").await.expect("outcome");
    assert_eq!(out["exit_code"], 3);
    let bad = sandbox
        .call(Tier::Native, "shell", json!({"command": 42}))
        .await;
    assert!(matches!(bad, Err(ToolError::InvalidArguments(_))));
    let unknown = sandbox.call(Tier::Native, "no_such_tool", json!({})).await;
    assert!(matches!(unknown, Err(ToolError::Sandbox(_))));

    sandbox.release().await;
}

#[tokio::test]
async fn output_is_capped() {
    let (config, image) = require_assets!();
    let dir = tempfile::tempdir().expect("tempdir");
    let ws = dir.path().join("cap");
    let provider = provider(config);
    let sandbox = open(&provider, "cap", &image, &ws).await;

    let out = shell(&sandbox, "head -c 100000 /dev/zero | tr '\\0' 'a'")
        .await
        .expect("shell");
    let stdout = out["stdout"].as_str().expect("stdout");
    assert!(
        stdout.contains("[truncated"),
        "100 KB of stdout must truncate, got {} bytes",
        stdout.len()
    );
    sandbox.release().await;
}

// ---------------------------------------------------------------------------
// S5: lazy provisioning, release, idempotence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn provisioning_is_lazy_and_counted() {
    let (config, image) = require_assets!();
    let dir = tempfile::tempdir().expect("tempdir");
    let ws = dir.path().join("lazy");
    let provider = provider(config);
    let sandbox = open(&provider, "lazy", &image, &ws).await;

    // A Workspace call provisions nothing (sandbox spec §2.3 item 2).
    workspace(&sandbox, "write_file", json!({"path": "f", "content": "x"}))
        .await
        .expect("write");
    assert_eq!(provider.stats().native_built(), 0);

    shell(&sandbox, "true").await.expect("first");
    assert_eq!(provider.stats().native_built(), 1);
    shell(&sandbox, "true").await.expect("second");
    assert_eq!(provider.stats().native_built(), 1, "the VM is reused");

    sandbox.release().await;
}

#[tokio::test]
async fn release_kills_the_vm_and_is_idempotent() {
    let (config, image) = require_assets!();
    let dir = tempfile::tempdir().expect("tempdir");
    let ws = dir.path().join("rel");
    let provider = provider(config);
    let sandbox = open(&provider, "rel", &image, &ws).await;

    shell(&sandbox, "true").await.expect("shell");
    let control = control_dir(&ws);
    assert_eq!(vm_pids(&control).len(), 1, "one VM runs");

    sandbox.release().await;
    assert!(vm_pids(&control).is_empty(), "release kills the VM (S5)");
    assert!(!control.exists(), "the control directory is gone");
    assert!(
        ws.is_dir(),
        "release never deletes the caller-owned workspace directory"
    );
    // Idempotent: a second release is a no-op, not an error.
    sandbox.release().await;
}

// ---------------------------------------------------------------------------
// Loss conduct (sandbox spec §4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_externally_killed_vm_is_single_tier_loss() {
    let (config, image) = require_assets!();
    let dir = tempfile::tempdir().expect("tempdir");
    let ws = dir.path().join("stl");
    let provider = provider(config);
    let sandbox = open(&provider, "stl", &image, &ws).await;

    shell(&sandbox, "true").await.expect("first");
    let control = control_dir(&ws);
    let pids = vm_pids(&control);
    assert_eq!(pids.len(), 1);
    // The world kills the VMM behind the provider's back, while the
    // workspace survives.
    let killed = std::process::Command::new("kill")
        .args(["-9", &pids[0]])
        .status()
        .expect("kill");
    assert!(killed.success());

    // Single-tier loss: an ordinary ToolError, never EnvironmentLost, never
    // a silent re-grant on this call (sandbox spec §4).
    let lost = shell(&sandbox, "true").await;
    assert!(
        matches!(lost, Err(ToolError::Sandbox(_))),
        "a dead VM with a live workspace is single-tier loss, got {lost:?}"
    );
    // The next call MAY re-provision lazily under the acquisition this
    // activation already journaled.
    shell(&sandbox, "true").await.expect("re-provisioned");
    assert_eq!(provider.stats().native_built(), 2);

    sandbox.release().await;
}

#[tokio::test]
async fn a_vanished_workspace_escalates_to_environment_lost() {
    let (config, image) = require_assets!();
    let dir = tempfile::tempdir().expect("tempdir");
    let ws = dir.path().join("lost");
    let provider = provider(config);
    let sandbox = open(&provider, "lost", &image, &ws).await;

    shell(&sandbox, "true").await.expect("first");
    std::fs::remove_dir_all(&ws).expect("external loss");

    // Only EnvironmentLost engages the harness's reset protocol (§5.5).
    let lost = shell(&sandbox, "true").await;
    assert!(
        matches!(lost, Err(ToolError::EnvironmentLost(_))),
        "a vanished workspace is environment loss, got {lost:?}"
    );
    sandbox.release().await;
}

// ---------------------------------------------------------------------------
// Configuration conduct — these run everywhere (no KVM, no assets)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_missing_binary_fails_the_call_as_a_sandbox_outcome() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = provider(FirecrackerConfig::new(
        "/no/such/firecracker",
        "/no/vmlinux",
    ));
    let sandbox = open(&provider, "nobin", "/no/rootfs.ext4", &dir.path().join("nobin")).await;

    // Provisioning fails as a per-call outcome (harness spec §5.4) — and
    // lazily: the Workspace tier still works without firecracker anywhere.
    workspace(&sandbox, "write_file", json!({"path": "f", "content": "x"}))
        .await
        .expect("workspace tier works");
    let out = shell(&sandbox, "true").await;
    match out {
        Err(ToolError::Sandbox(e)) => assert!(
            e.contains("provision"),
            "the error must name provisioning: {e}"
        ),
        other => panic!("a missing binary is a Sandbox outcome, got {other:?}"),
    }
    assert_eq!(provider.stats().native_built(), 0, "nothing was provisioned");
    sandbox.release().await;
}

#[tokio::test]
async fn a_named_egress_allowlist_fails_open() {
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = provider(FirecrackerConfig::new("/usr/bin/firecracker", "/k"));

    // §2.2: holding Native implies Network's grants; a VM with no network
    // device delivers an empty allowlist only, so a profile naming egress
    // must fail loudly (the same conduct as the docker fallback).
    let egress = SandboxProfile {
        egress: vec!["api.example.com".to_string()],
        ..SandboxProfile::default()
    };
    let denied = provider
        .open(&SessionId::new("net"), &egress, &dir.path().join("net"))
        .await;
    assert!(denied.is_err(), "a named egress allowlist must fail open");

    let capped = SandboxProfile::default().cap([Tier::Workspace, Tier::Network]);
    let denied = provider
        .open(&SessionId::new("net2"), &capped, &dir.path().join("net2"))
        .await;
    assert!(denied.is_err(), "a Network-bearing cap must fail open");
}
