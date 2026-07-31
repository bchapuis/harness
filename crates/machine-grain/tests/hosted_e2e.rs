//! The production binding, end to end (feature `guest`, machine §2.1): boot the
//! machine base image, reach the guest agent, round-trip the workspace volume
//! through the guest's tmpfs (machine §3), pause and resume at a quiescent point
//! (machine §4), kill.
//!
//! One body, run against **both** mechanisms, because that is the claim the
//! binding makes: a machine's durability does not depend on how its guest is
//! held. Each mechanism skips (eprintln + return) when its prerequisites are
//! absent, so any host stays green:
//!
//! - the microVM mechanism needs Linux, `/dev/kvm`, and `vmlinux` + `firecracker`
//!   + `machine.ext4` from `guest/machine-rootfs/build.sh`;
//! - the container mechanism needs a docker daemon, `machine.ext4`, and the
//!   runner image from `guest/machine-docker/build.sh` — so it runs on macOS,
//!   where the microVM mechanism cannot.
//!
//! With `E2E_REQUIRE` set, a missing prerequisite panics instead: the CI job that
//! exists to run this suite must not read a broken environment as a pass. Point
//! `HARNESS_MACHINE_ASSETS` at an assets directory to override the default
//! `guest/machine-rootfs/out`.
//!
//! Under the container mechanism the disk image is bind-mounted from the test's
//! temporary directory, which the container engine must be able to share: under
//! colima only `$HOME` and `/tmp/colima` are shared by default, so point
//! `TMPDIR` somewhere under `$HOME` if a boot fails with a mount error.

#![cfg(feature = "host")]

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use machine_grain::BootSpec;
use machine_grain::MachineRuntimeProvider;
use machine_grain::hosted::HostedRuntimeConfig;
use machine_grain::hosted::HostedRuntimeProvider;
use machine_host::MachineHost;

/// The runner image `guest/machine-docker/build.sh` builds.
const RUNNER_IMAGE: &str = "harness-machine-runner:1";

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

fn assets_dir() -> PathBuf {
    std::env::var("HARNESS_MACHINE_ASSETS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../guest/machine-rootfs/out")
        })
}

/// The base rootfs both mechanisms boot.
fn base_image() -> Option<PathBuf> {
    let image = assets_dir().join("machine.ext4");
    if !image.exists() {
        return missing(format!(
            "{} is absent (run guest/machine-rootfs/build.sh)",
            image.display()
        ));
    }
    Some(image)
}

/// The microVM mechanism, or `None` where this host cannot run one.
fn microvm() -> Option<Arc<dyn MachineHost>> {
    if !cfg!(target_os = "linux") {
        return missing("firecracker runs on linux only".to_string());
    }
    if !Path::new("/dev/kvm").exists() {
        return missing("/dev/kvm is absent".to_string());
    }
    let dir = assets_dir();
    let (binary, kernel) = (dir.join("firecracker"), dir.join("vmlinux"));
    if !(binary.exists() && kernel.exists()) {
        return missing(format!(
            "vmm assets missing under {} (run guest/machine-rootfs/build.sh)",
            dir.display()
        ));
    }
    Some(machine_grain::hosted::microvm_host(binary, kernel))
}

/// The container mechanism, or `None` where this host has no docker or no runner
/// image.
async fn container() -> Option<Arc<dyn MachineHost>> {
    if !probe(&["version"]).await {
        return missing("no docker daemon".to_string());
    }
    if !probe(&["image", "inspect", RUNNER_IMAGE]).await {
        return missing(format!(
            "{RUNNER_IMAGE} is absent (run guest/machine-docker/build.sh)"
        ));
    }
    Some(machine_grain::hosted::container_host(
        "docker",
        RUNNER_IMAGE,
    ))
}

/// Whether a `docker` invocation succeeds, for a prerequisite probe.
async fn probe(args: &[&str]) -> bool {
    #[allow(clippy::disallowed_methods)]
    tokio::process::Command::new("docker")
        .args(args)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success())
}

/// Boot a machine on `mechanism` and exercise everything the grain drives
/// through the binding seam.
async fn round_trip(mechanism: Arc<dyn MachineHost>, image: PathBuf, name: &str) {
    // The machine writes its drive in place (grain §7.15's departure); the test
    // copies the base image so reruns start clean, standing in for the disk
    // facet's materialization.
    let scratch = tempfile::tempdir().expect("tempdir");
    let disk = scratch.path().join("machine.img");
    std::fs::copy(&image, &disk).expect("copy image");

    let provider = HostedRuntimeProvider::new(
        mechanism,
        // Shorter than the default minute: a failure here should not stall the
        // suite, and both mechanisms serve well inside it on a working host.
        HostedRuntimeConfig {
            node: "e2e".to_string(),
            ready_timeout: Duration::from_secs(45),
            egress: None,
        },
    );
    let runtime = provider
        .boot(BootSpec {
            image: disk,
            vcpus: 1,
            mem_mib: 256,
            machine: granary::GrainName::new(machine_grain::MACHINE_TYPE, name),
            egress: machine_grain::EgressPolicy::Open,
        })
        .await
        .expect("boot: the guest agent must answer a channel");

    // The workspace volume (machine §3), end to end: push a tree into the
    // guest's tmpfs over the WsPush channel, pull it back over WsPull, and get
    // the bytes back — the rootfs's /workspace mount, the guest agent's sync
    // channels, and the host codec, all against a real guest.
    let ws = scratch.path().join("ws");
    std::fs::create_dir(&ws).expect("ws dir");
    std::fs::write(ws.join("hello.txt"), b"through the seam").expect("seed");
    runtime.push_ws(ws.clone()).await.expect("push_ws");
    std::fs::remove_file(ws.join("hello.txt")).expect("clear host side");
    runtime.pull_ws(ws.clone()).await.expect("pull_ws");
    assert_eq!(
        std::fs::read(ws.join("hello.txt")).expect("pulled file"),
        b"through the seam",
        "the workspace round-trips through the guest tmpfs"
    );

    // The capture command's quiescent point (machine §4).
    runtime.pause().await.expect("pause");
    runtime.resume().await.expect("resume");
    // Idempotent kill (the M5 path and on_passivate both call it).
    runtime.kill().await;
    runtime.kill().await;
}

#[tokio::test]
async fn a_microvm_guest_round_trips() {
    let Some(image) = base_image() else { return };
    let Some(mechanism) = microvm() else { return };
    round_trip(mechanism, image, "e2e-microvm").await;
}

#[tokio::test]
async fn a_container_guest_round_trips() {
    let Some(image) = base_image() else { return };
    let Some(mechanism) = container().await else {
        return;
    };
    round_trip(mechanism, image, "e2e-container").await;
}
