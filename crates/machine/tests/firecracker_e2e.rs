//! The real VM binding, end to end (feature `firecracker`, machine §2.1):
//! boot the machine base image on Firecracker, reach the guest agent over
//! vsock, round-trip the workspace volume through the guest's tmpfs
//! (machine §3), pause and resume at a quiescent point (machine §4), kill.
//!
//! Needs Linux, `/dev/kvm`, and the assets `guest/machine-rootfs/build.sh`
//! produces (vmlinux, machine.ext4, firecracker); skips (eprintln + return)
//! where any is missing, so macOS and KVM-less machines stay green — the
//! harness-sandbox firecracker suite's convention. With `E2E_REQUIRE` set, a
//! missing prerequisite panics instead: the CI job that exists to run this
//! suite must not read a broken environment as a pass. Point
//! `HARNESS_MACHINE_ASSETS` at an assets directory to override the default
//! `guest/machine-rootfs/out`.

#![cfg(feature = "firecracker")]

use std::path::Path;
use std::path::PathBuf;

use machine::MachineVmProvider;
use machine::VmSpec;
use machine::firecracker::FirecrackerMachineConfig;
use machine::firecracker::FirecrackerMachineProvider;

struct Assets {
    config: FirecrackerMachineConfig,
    image: PathBuf,
}

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

fn assets() -> Option<Assets> {
    if !cfg!(target_os = "linux") {
        return missing("firecracker runs on linux only".to_string());
    }
    if !Path::new("/dev/kvm").exists() {
        return missing("/dev/kvm is absent".to_string());
    }
    let dir = std::env::var("HARNESS_MACHINE_ASSETS")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../guest/machine-rootfs/out")
        });
    let (binary, kernel, image) = (
        dir.join("firecracker"),
        dir.join("vmlinux"),
        dir.join("machine.ext4"),
    );
    if !(binary.exists() && kernel.exists() && image.exists()) {
        return missing(format!(
            "assets missing under {} (run guest/machine-rootfs/build.sh)",
            dir.display()
        ));
    }
    Some(Assets {
        config: FirecrackerMachineConfig::new(binary, kernel),
        image,
    })
}

#[tokio::test]
async fn boot_ws_sync_pause_resume_kill_round_trips() {
    let Some(assets) = assets() else { return };
    // The machine writes its drive in place (grain §7.15's departure); the
    // test copies the base image so reruns start clean, standing in for the
    // disk facet's materialization.
    let scratch = tempfile::tempdir().expect("tempdir");
    let image = scratch.path().join("machine.img");
    std::fs::copy(&assets.image, &image).expect("copy image");

    let provider = FirecrackerMachineProvider::new(assets.config);
    let vm = provider
        .boot(VmSpec {
            image,
            vcpus: 1,
            mem_mib: 256,
            machine: granary::GrainName::new(machine::MACHINE_TYPE, "e2e-box"),
            egress: machine::EgressPolicy::Open,
        })
        .await
        .expect("boot: the guest agent must accept a vsock connection");

    // The workspace volume (machine §3), end to end: push a tree into the
    // guest's tmpfs over the WsPush channel, pull it back over WsPull, and
    // get the bytes back — the rootfs's /workspace mount, the guest agent's
    // sync channels, and the host codec, all against a real guest.
    let ws = scratch.path().join("ws");
    std::fs::create_dir(&ws).expect("ws dir");
    std::fs::write(ws.join("hello.txt"), b"over vsock").expect("seed");
    vm.push_ws(ws.clone()).await.expect("push_ws");
    std::fs::remove_file(ws.join("hello.txt")).expect("clear host side");
    vm.pull_ws(ws.clone()).await.expect("pull_ws");
    assert_eq!(
        std::fs::read(ws.join("hello.txt")).expect("pulled file"),
        b"over vsock",
        "the workspace round-trips through the guest tmpfs"
    );

    // The capture command's quiescent point (machine §4): pause stops the
    // vCPUs, resume restarts them; both are API-socket PATCHes.
    vm.pause().await.expect("pause");
    vm.resume().await.expect("resume");
    // Idempotent kill (the M5 path and on_passivate both call it).
    vm.kill().await;
    vm.kill().await;
}
