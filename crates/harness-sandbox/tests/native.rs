//! Native-tier conformance (feature `native`): S1 (the container reaches the
//! host filesystem through the workspace mount only, and no network) and S5
//! (the container is removed on release, idempotently), plus the loss
//! conduct of sandbox spec §4.
//!
//! Tests that drive docker probe for it first and skip (eprintln + return)
//! when unavailable, so a machine without docker stays green. The pinned
//! image is pulled on the first run (`docker pull alpine:3.20` to pre-warm).
//! Under colima, only `$HOME` and `/tmp/colima` are shared with the VM by
//! default — point `TMPDIR` somewhere under `$HOME` if provisioning fails
//! with a mount error. A test that panics mid-flight can stray a container:
//! `docker ps -a --filter name=harness-sb-` lists them.

#![cfg(feature = "native")]

use std::process::Stdio;
use std::sync::Arc;

use harness::Sandbox;
use harness::SandboxProfile;
use harness::SandboxProvider;
use harness::SessionId;
use harness::Tier;
use harness::ToolError;
use harness_sandbox::TieredSandboxes;
use serde_json::Value;
use serde_json::json;

const IMAGE: &str = "alpine:3.20";

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

macro_rules! require_docker {
    () => {
        if !docker_available() {
            eprintln!("skipping: docker unavailable");
            return;
        }
    };
}

async fn open(provider: &TieredSandboxes, session: &str) -> Arc<dyn Sandbox> {
    provider
        .open(&SessionId::new(session), &SandboxProfile::image(IMAGE))
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

/// The session's containers, by docker's substring name filter: the
/// provider's names are `harness-sb-<workspace>-<digest>`, so filtering on
/// the prefix finds them without knowing the digest.
fn containers(name_prefix: &str) -> Vec<String> {
    let output = std::process::Command::new("docker")
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("name={name_prefix}"),
            "--format",
            "{{.Names}}",
        ])
        .output()
        .expect("docker ps");
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::to_string)
        .collect()
}

// ---------------------------------------------------------------------------
// The shell tool over the bind-mounted workspace
// ---------------------------------------------------------------------------

#[tokio::test]
async fn shell_round_trips_through_the_bind_mounted_workspace() {
    require_docker!();
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "rt").await;

    // Container → host: a file the shell writes is visible to the
    // Workspace-tier tools over the same directory.
    let written = shell(&sandbox, "printf hello > f.txt")
        .await
        .expect("shell");
    assert_eq!(written["exit_code"], 0, "shell write failed: {written}");
    let read = workspace(&sandbox, "read_file", json!({"path": "f.txt"}))
        .await
        .expect("read");
    assert_eq!(read["content"], "hello");

    // Host → container: the reverse direction through the same mount.
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

    sandbox.release().await;
}

#[tokio::test]
async fn confinement_smoke() {
    require_docker!();
    let dir = tempfile::tempdir().expect("tempdir");
    // A sentinel beside the workspaces root: only the workspace itself is
    // mounted, so no path the guest can spell reaches it (S1, container
    // grade).
    let sentinel = dir.path().join("sentinel");
    std::fs::write(&sentinel, "outside").expect("sentinel");
    let provider = TieredSandboxes::new(dir.path().join("workspaces")).expect("provider");
    let sandbox = open(&provider, "conf").await;

    for escape in [
        "cat /workspace/../sentinel",
        "cat ../sentinel",
        "cat /sentinel",
    ] {
        let out = shell(&sandbox, escape).await.expect("shell");
        assert_ne!(out["exit_code"], 0, "{escape} must not reach the sentinel");
    }
    // --network none: egress fails fast, DNS or no DNS.
    let net = shell(&sandbox, "wget -T 2 -q -O - http://1.1.1.1")
        .await
        .expect("shell");
    assert_ne!(net["exit_code"], 0, "egress must be unreachable: {net}");

    assert_eq!(
        std::fs::read_to_string(&sentinel).expect("sentinel"),
        "outside"
    );
    sandbox.release().await;
}

#[tokio::test]
async fn failures_are_outcomes_not_errors() {
    require_docker!();
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "fail").await;

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
    require_docker!();
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "cap").await;

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
// Configuration conduct: no silent defaults, no silent withholding
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_empty_image_fails_the_call_as_a_sandbox_outcome() {
    // No docker required: the check fires before any docker invocation.
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = provider
        .open(&SessionId::new("noimg"), &SandboxProfile::default())
        .await
        .expect("open");

    let out = shell(&sandbox, "true").await;
    match out {
        Err(ToolError::Sandbox(e)) => {
            assert!(e.contains("image"), "the error must name the image: {e}")
        }
        other => panic!("an empty image is a Sandbox outcome, got {other:?}"),
    }
    assert_eq!(provider.stats.native_built(), 0, "nothing was provisioned");
    sandbox.release().await;
}

#[tokio::test]
async fn a_named_egress_allowlist_fails_open() {
    // No docker required: the validation is provider-side, at open.
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");

    // §2.2: holding Native implies Network's grants; --network none delivers
    // an empty allowlist only, so a profile naming egress must fail loudly.
    let mut egress = SandboxProfile::image(IMAGE);
    egress.egress = vec!["api.example.com".to_string()];
    let denied = provider.open(&SessionId::new("net"), &egress).await;
    assert!(denied.is_err(), "a named egress allowlist must fail open");

    let capped = SandboxProfile::image(IMAGE).cap([Tier::Workspace, Tier::Network]);
    let denied = provider.open(&SessionId::new("net2"), &capped).await;
    assert!(denied.is_err(), "a Network-bearing cap must fail open");
}

// ---------------------------------------------------------------------------
// S5: lazy provisioning, release, idempotence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn provisioning_is_lazy_and_counted() {
    require_docker!();
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "lazy").await;

    // A Workspace call provisions nothing (sandbox spec §2.3 item 2).
    workspace(&sandbox, "write_file", json!({"path": "f", "content": "x"}))
        .await
        .expect("write");
    assert_eq!(provider.stats.native_built(), 0);

    shell(&sandbox, "true").await.expect("first");
    assert_eq!(provider.stats.native_built(), 1);
    shell(&sandbox, "true").await.expect("second");
    assert_eq!(provider.stats.native_built(), 1, "the container is reused");

    sandbox.release().await;
}

#[tokio::test]
async fn release_removes_the_container_and_is_idempotent() {
    require_docker!();
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "rel").await;

    shell(&sandbox, "true").await.expect("shell");
    assert_eq!(containers("harness-sb-rel").len(), 1, "container exists");

    sandbox.release().await;
    assert_eq!(
        containers("harness-sb-rel").len(),
        0,
        "release removes the container (S5)"
    );
    assert_eq!(
        std::fs::read_dir(dir.path()).expect("root").count(),
        0,
        "the workspace directory is gone too"
    );
    // Idempotent: a second release is a no-op, not an error.
    sandbox.release().await;
}

// ---------------------------------------------------------------------------
// Loss conduct (sandbox spec §4)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_externally_killed_container_is_single_tier_loss() {
    require_docker!();
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "stl").await;

    shell(&sandbox, "true").await.expect("first");
    let names = containers("harness-sb-stl");
    assert_eq!(names.len(), 1);
    // The world removes the container behind the provider's back, while the
    // workspace survives.
    let killed = std::process::Command::new("docker")
        .args(["rm", "-f", &names[0]])
        .output()
        .expect("docker rm");
    assert!(killed.status.success());

    // Single-tier loss: an ordinary ToolError, never EnvironmentLost, never
    // a silent re-grant on this call (sandbox spec §4).
    let lost = shell(&sandbox, "true").await;
    assert!(
        matches!(lost, Err(ToolError::Sandbox(_))),
        "a lost container with a live workspace is single-tier loss, got {lost:?}"
    );
    // The next call MAY re-provision lazily under the acquisition this
    // activation already journaled.
    shell(&sandbox, "true").await.expect("re-provisioned");
    assert_eq!(provider.stats.native_built(), 2);

    sandbox.release().await;
}

#[tokio::test]
async fn a_vanished_workspace_escalates_to_environment_lost() {
    require_docker!();
    let dir = tempfile::tempdir().expect("tempdir");
    let provider = TieredSandboxes::new(dir.path()).expect("provider");
    let sandbox = open(&provider, "lost").await;

    shell(&sandbox, "true").await.expect("first");
    // Remove the container first (a live bind mount can keep the directory
    // serving inside the container), then the host directory: the whole
    // environment is gone.
    let names = containers("harness-sb-lost");
    assert_eq!(names.len(), 1);
    let killed = std::process::Command::new("docker")
        .args(["rm", "-f", &names[0]])
        .output()
        .expect("docker rm");
    assert!(killed.status.success());
    let session_dir = std::fs::read_dir(dir.path())
        .expect("root")
        .next()
        .expect("session dir")
        .expect("entry")
        .path();
    std::fs::remove_dir_all(&session_dir).expect("external loss");

    // Only EnvironmentLost engages the harness's reset protocol (§5.5).
    let lost = shell(&sandbox, "true").await;
    assert!(
        matches!(lost, Err(ToolError::EnvironmentLost(_))),
        "a vanished workspace is environment loss, got {lost:?}"
    );
    sandbox.release().await;
}
