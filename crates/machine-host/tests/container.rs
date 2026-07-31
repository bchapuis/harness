//! The container mechanism against a real daemon: the lifecycle both consumers
//! drive it through, and the one distinction their loss conduct rests on —
//! `GuestError::Gone` (the container is gone; re-provision) versus a command
//! that merely failed (an outcome to report).
//!
//! Probes for docker first and skips (eprintln + return) when it is absent, so a
//! machine without it stays green — the convention of
//! `harness-sandbox/tests/native.rs`. The pinned image is pulled on the first run
//! (`docker pull alpine:3.20` to pre-warm). A test that panics mid-flight can
//! strand a container: `docker ps -a --filter name=isolation-test-` lists them.

use std::sync::Arc;

use machine_host::GuestError;
use machine_host::GuestKey;
use machine_host::GuestSpec;
use machine_host::MachineGuest;
use machine_host::MachineHost;
use machine_host::Network;
use machine_host::container::ContainerHost;
use machine_host::container::ContainerSpec;

const IMAGE: &str = "alpine:3.20";
const PREFIX: &str = "isolation-test";

async fn docker_available() -> bool {
    #[allow(clippy::disallowed_methods)]
    let ok = tokio::process::Command::new("docker")
        .args(["image", "inspect", IMAGE])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .await
        .is_ok_and(|status| status.success());
    if !ok {
        eprintln!("skipping: no docker daemon, or {IMAGE} is not pulled");
    }
    ok
}

/// A container that stays up until removed.
fn idle(name: &str) -> ContainerSpec {
    ContainerSpec::image(GuestKey::new("", name), IMAGE)
        .entrypoint("sleep", ["2147483647".to_string()])
}

#[tokio::test]
async fn a_container_execs_and_stops() {
    if !docker_available().await {
        return;
    }
    let host = ContainerHost::new("docker", PREFIX);
    let guest = host
        .start_container(idle("exec"))
        .await
        .expect("start a container");

    let output = guest
        .exec(&["/bin/sh", "-c", "echo hello"])
        .await
        .expect("exec");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "hello");

    // A command's own failure is an outcome, not a mechanism error: the caller
    // reports the exit code rather than re-provisioning.
    let output = guest
        .exec(&["/bin/sh", "-c", "exit 3"])
        .await
        .expect("a failing command is still Ok");
    assert_eq!(output.status.code(), Some(3));

    // Freeze and thaw: the quiescent point a capture takes (machine §4).
    guest.pause().await.expect("pause");
    guest.resume().await.expect("resume");

    guest.stop().await;
    // Idempotent: both consumers call stop on paths where the guest may already
    // be gone.
    guest.stop().await;
    assert!(!guest.running().await, "the container is gone after stop");
}

#[tokio::test]
async fn a_vanished_container_is_gone_not_a_failed_command() {
    if !docker_available().await {
        return;
    }
    let host = ContainerHost::new("docker", PREFIX);
    let guest = host
        .start_container(idle("vanished"))
        .await
        .expect("start a container");
    // Pull it out from under the guest, as a daemon restart or an operator's
    // `docker rm -f` would.
    host.remove(&GuestKey::new("", "vanished")).await;

    let error = guest
        .exec(&["/bin/sh", "-c", "echo still here"])
        .await
        .expect_err("a removed container cannot exec");
    assert!(
        matches!(error, GuestError::Gone(_)),
        "the container being gone must not read as a command failure: {error}"
    );
}

#[tokio::test]
async fn connecting_to_an_absent_agent_is_gone() {
    if !docker_available().await {
        return;
    }
    let host = ContainerHost::new("docker", PREFIX);
    let guest = host
        .start_container(idle("no-agent"))
        .await
        .expect("start a container");
    // A bare image ships no guest agent, so nothing binds the port's socket.
    // The consumer must hear "gone" — its re-provision signal — rather than a
    // host-side fault it would retry in place.
    let Err(error) = guest.connect(62).await else {
        panic!("no agent listens in a bare image, so a channel cannot open")
    };
    assert!(
        matches!(error, GuestError::Gone(_)),
        "an absent agent must read as gone: {error}"
    );
    guest.stop().await;
}

/// A guest this host is not running answers `Gone` — what a front door reports
/// for a machine whose activation is on another node (machine §5.1).
#[tokio::test]
async fn connect_by_key_reports_an_absent_guest() {
    if !docker_available().await {
        return;
    }
    let host = ContainerHost::new("docker", PREFIX);
    let Err(error) = host
        .connect_by_key(&GuestKey::new("n9", "never-started"), 62)
        .await
    else {
        panic!("there is no such container on this host")
    };
    assert!(matches!(error, GuestError::Gone(_)), "{error}");
}

/// The block-image path ([`MachineHost::start`]) refuses a NIC rather than
/// booting a guest whose journaled egress policy it silently dropped
/// (machine §5.2).
#[tokio::test]
async fn a_tap_is_refused_not_ignored() {
    let host: Arc<dyn MachineHost> =
        Arc::new(ContainerHost::new("docker", PREFIX).with_runner_image("unused:1"));
    let Err(error) = host
        .start(GuestSpec {
            key: GuestKey::new("n1", "tap"),
            disk: "/nonexistent.img".into(),
            vcpus: 1,
            mem_mib: 64,
            network: Network::Tap {
                interface: machine_host::microvm::NetIf {
                    iface_id: "eth0".to_string(),
                    host_dev_name: "tap0".to_string(),
                    guest_mac: None,
                },
                boot_arg: "ip=…".to_string(),
            },
        })
        .await
    else {
        panic!("a container cannot take a tap")
    };
    assert!(
        matches!(&error, GuestError::Host(e) if e.contains("tap")),
        "{error}"
    );
}
