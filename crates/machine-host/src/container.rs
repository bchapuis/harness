//! The container mechanism (sandbox spec §3.4's SHOULD grade, §3.5's
//! development fallback; machine spec §2.1): OCI containers driven through the
//! `docker` CLI.
//!
//! Confinement here is **shared-kernel**: the guest speaks to the host kernel,
//! so the guarantee is priced by kernel privilege-escalation bugs and
//! multi-tenant deployments SHOULD NOT rely on it alone. It is what a host
//! without `/dev/kvm` can run.
//!
//! Two ways in, because the two consumers hold different things:
//!
//! - [`ContainerHost::start_container`] runs any OCI image with mounts, a user,
//!   and an entrypoint of the caller's choosing — the shape the agent sandbox's
//!   `Native` tier needs, whose virtue is requiring *nothing* inside the image.
//! - [`MachineHost::start`] holds a **block image** guest: the runner image
//!   loop-mounts `spec.disk` and chroots into it, so the guest is the disk's own
//!   rootfs and its writes land in that very file (grain §7.15). The container
//!   half is `guest/machine-docker/init.sh`; `--privileged` is what `losetup`
//!   and `mount` cost, and the reason this mechanism is a development one.
//!
//! Reaching an agent inside differs from vsock: a unix socket the guest creates
//! is not one the host can dial — on macOS the container sits inside a VM of its
//! own — so [`MachineGuest::connect`] relays through `docker exec` and `socat`, then
//! speaks the same muxer handshake Firecracker performs host-side. One agent
//! binary therefore serves either mechanism unchanged.

use std::pin::Pin;
use std::process::Output;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::ReadBuf;
use tokio::process::Child;
use tokio::process::ChildStderr;
use tokio::process::ChildStdin;
use tokio::process::ChildStdout;
use tokio::process::Command;

use crate::BoxFuture;
use crate::Duplex;
use crate::GuestError;
use crate::GuestKey;
use crate::GuestSpec;
use crate::MachineGuest;
use crate::MachineHost;
use crate::MachineIsolation;
use crate::Network;

/// The directory a containerized guest binds its agent sockets in, one per port
/// (`/run/guest/62.sock`), reachable under this same path from the container's
/// own namespace.
///
/// This and [`GUEST_DISK`] are the whole contract between this mechanism and a
/// runner image, and it is stated *here*: [`MachineHost::start`] passes both to
/// the runner as environment (`MACHINE_SOCK_DIR`, `MACHINE_DISK`), so the shell
/// half derives them instead of repeating them and a change here cannot leave a
/// runner reading a path nothing binds.
const SOCKET_DIR: &str = "/run/guest";

/// Where [`MachineHost::start`] binds the guest's block image.
const GUEST_DISK: &str = "/machine/disk.img";

/// How long a started container is watched for an early exit before its guest is
/// called up. Long enough for the runner's mount-and-chroot to fail loudly,
/// short enough not to lengthen a boot that works.
const SETTLE: Duration = Duration::from_millis(300);
const SETTLE_TRIES: u32 = 6;

/// One container name prefix per consumer role, so `docker ps` distinguishes
/// them and neither sweeps the other's guests.
#[derive(Clone, Debug)]
pub struct ContainerHost {
    cli: String,
    prefix: String,
    /// The runner image [`MachineHost::start`] holds block images with; unused
    /// by [`start_container`](ContainerHost::start_container).
    runner_image: String,
}

impl ContainerHost {
    /// `cli` is the container CLI (`docker`; podman's compatible CLI and colima
    /// answer to the same vocabulary). `prefix` names this consumer's containers.
    pub fn new(cli: impl Into<String>, prefix: impl Into<String>) -> ContainerHost {
        ContainerHost {
            cli: cli.into(),
            prefix: prefix.into(),
            runner_image: String::new(),
        }
    }

    /// The runner image for [`MachineHost::start`], built by
    /// `guest/machine-docker/build.sh`. It must be the same architecture as the
    /// guests' rootfs: the chroot runs their binaries under this host's kernel.
    pub fn with_runner_image(mut self, image: impl Into<String>) -> ContainerHost {
        self.runner_image = image.into();
        self
    }

    /// This host's name for `key`'s container: stable per guest, so a start
    /// finds and sweeps the previous activation's debris, and distinct across
    /// guests and nodes ([`GuestKey`]).
    pub fn container_name(&self, key: &GuestKey) -> String {
        format!("{}-{}", self.prefix, key.slug())
    }

    /// Remove `key`'s container if it is there. Idempotent, and by *name*: it
    /// covers the orphan a dropped start left behind, whose guest object no
    /// caller ever received.
    pub async fn remove(&self, key: &GuestKey) {
        let _ = run(&self.cli, &["rm", "-f", &self.container_name(key)]).await;
    }

    /// Run a container and return its guest. The caller owns every policy in
    /// `spec`; this owns the CLI, the pre-run sweep, and the early-exit check.
    pub async fn start_container(&self, spec: ContainerSpec) -> Result<ContainerGuest, GuestError> {
        let name = self.container_name(&spec.key);
        // A container from a previous activation would answer for a guest its
        // consumer has moved on from, and hold the disk image's loop device.
        // Sweep it, as a microVM launch sweeps its control directory.
        self.remove(&spec.key).await;
        let output = run(&self.cli, &spec.run_args(&name)).await?;
        if !output.status.success() {
            return Err(classify(&output, &format!("run {}", spec.image)));
        }
        let guest = ContainerGuest {
            cli: self.cli.clone(),
            name,
            stopped: AtomicBool::new(false),
        };
        // A runner whose mount or chroot failed exits within moments of a
        // successful `run`; report that as the guest being gone, with the
        // container's own last words, rather than as a later connect timeout.
        for attempt in 0..SETTLE_TRIES {
            if guest.running().await {
                return Ok(guest);
            }
            if attempt + 1 < SETTLE_TRIES {
                tokio::time::sleep(SETTLE).await;
            }
        }
        let words = guest.console_tail().await;
        guest.stop().await;
        Err(GuestError::Gone(format!(
            "container {} exited during start: {words}",
            guest.name
        )))
    }
}

impl MachineHost for ContainerHost {
    fn isolation(&self) -> MachineIsolation {
        MachineIsolation::Container
    }

    fn start(
        &self,
        spec: GuestSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn MachineGuest>, GuestError>> {
        let host = self.clone();
        Box::pin(async move {
            if host.runner_image.is_empty() {
                return Err(GuestError::Host(
                    "no runner image: ContainerHost::with_runner_image names the image that \
                     loop-mounts a guest's disk (guest/machine-docker/build.sh)"
                        .to_string(),
                ));
            }
            if !matches!(spec.network, Network::None) {
                // Refused rather than ignored: a guest that silently lost the
                // NIC its consumer's policy asked for would be a policy
                // failure disguised as a working boot (machine §5.2).
                return Err(GuestError::Host(
                    "a container cannot take a host tap; only the microvm grade realizes egress"
                        .to_string(),
                ));
            }
            // Absolute: docker resolves a bind mount's source itself, and a
            // relative path would name a volume instead.
            let disk = spec.disk.canonicalize().map_err(|e| {
                GuestError::Host(format!("guest disk {}: {e}", spec.disk.display()))
            })?;
            let container = ContainerSpec::image(spec.key.clone(), &host.runner_image)
                .privileged()
                .mount(disk, GUEST_DISK, false)
                .env("MACHINE_DISK", GUEST_DISK)
                .env("MACHINE_SOCK_DIR", SOCKET_DIR)
                .sized(spec.vcpus, spec.mem_mib);
            Ok(Arc::new(host.start_container(container).await?) as Arc<dyn MachineGuest>)
        })
    }

    fn connect_by_key<'a>(
        &'a self,
        key: &'a GuestKey,
        port: u32,
    ) -> BoxFuture<'a, Result<Box<dyn Duplex>, GuestError>> {
        Box::pin(async move {
            let name = self.container_name(key);
            if !running(&self.cli, &name).await {
                return Err(GuestError::Gone(format!(
                    "no container {name} on this node"
                )));
            }
            connect(&self.cli, &name, port).await
        })
    }
}

/// One mount a container takes.
#[derive(Clone, Debug)]
pub struct Mount {
    pub host: std::path::PathBuf,
    pub guest: String,
    pub read_only: bool,
}

/// Everything one `docker run` needs. Container-level policy throughout: a
/// mechanism that cannot share a filesystem has no analogue for most of it,
/// which is why this type belongs to this module and not to [`GuestSpec`].
#[derive(Clone, Debug)]
pub struct ContainerSpec {
    pub key: GuestKey,
    pub image: String,
    pub mounts: Vec<Mount>,
    /// The container's working directory (`-w`).
    pub workdir: Option<String>,
    /// `--user`, as `uid:gid`. Without it, files the guest writes into a mount
    /// are root-owned on Linux and the host's later removal fails.
    pub user: Option<String>,
    /// `--pids-limit`: the fork-bomb guard.
    pub pids_limit: Option<u32>,
    pub mem_mib: Option<u32>,
    pub vcpus: Option<u8>,
    /// `--privileged`, which `losetup` and `mount` need and nothing else here
    /// does. A root-equivalent grant on the host.
    pub privileged: bool,
    /// Environment for the container, as `KEY=VALUE` pairs.
    pub env: Vec<(String, String)>,
    /// Override the image's entrypoint, with `args` following it.
    pub entrypoint: Option<String>,
    pub args: Vec<String>,
}

impl ContainerSpec {
    /// A container with no network, no mounts, and the image's own entrypoint.
    /// Egress is absent by construction in both consumers (sandbox §1.1;
    /// machine §5.2's degradation), so there is no knob to grant it.
    pub fn image(key: GuestKey, image: impl Into<String>) -> ContainerSpec {
        ContainerSpec {
            key,
            image: image.into(),
            mounts: Vec::new(),
            workdir: None,
            user: None,
            pids_limit: None,
            mem_mib: None,
            vcpus: None,
            privileged: false,
            env: Vec::new(),
            entrypoint: None,
            args: Vec::new(),
        }
    }

    pub fn env(mut self, key: impl Into<String>, value: impl Into<String>) -> ContainerSpec {
        self.env.push((key.into(), value.into()));
        self
    }

    pub fn mount(
        mut self,
        host: impl Into<std::path::PathBuf>,
        guest: impl Into<String>,
        read_only: bool,
    ) -> ContainerSpec {
        self.mounts.push(Mount {
            host: host.into(),
            guest: guest.into(),
            read_only,
        });
        self
    }

    pub fn workdir(mut self, dir: impl Into<String>) -> ContainerSpec {
        self.workdir = Some(dir.into());
        self
    }

    pub fn user(mut self, user: impl Into<String>) -> ContainerSpec {
        self.user = Some(user.into());
        self
    }

    pub fn pids_limit(mut self, limit: u32) -> ContainerSpec {
        self.pids_limit = Some(limit);
        self
    }

    pub fn sized(mut self, vcpus: u8, mem_mib: u32) -> ContainerSpec {
        self.vcpus = Some(vcpus.max(1));
        self.mem_mib = Some(mem_mib.max(64));
        self
    }

    pub fn privileged(mut self) -> ContainerSpec {
        self.privileged = true;
        self
    }

    pub fn entrypoint(
        mut self,
        entrypoint: impl Into<String>,
        args: impl IntoIterator<Item = String>,
    ) -> ContainerSpec {
        self.entrypoint = Some(entrypoint.into());
        self.args = args.into_iter().collect();
        self
    }

    /// The `docker run` argument vector. Built in one place so both consumers'
    /// containers differ only where their specs do.
    fn run_args(&self, name: &str) -> Vec<String> {
        let mut args: Vec<String> = ["run", "-d", "--name", name, "--network", "none"]
            .iter()
            .map(|a| a.to_string())
            .collect();
        if self.privileged {
            args.push("--privileged".to_string());
        }
        if let Some(user) = &self.user {
            args.push("--user".to_string());
            args.push(user.clone());
        }
        if let Some(limit) = self.pids_limit {
            args.push("--pids-limit".to_string());
            args.push(limit.to_string());
        }
        if let Some(mem) = self.mem_mib {
            args.push("--memory".to_string());
            args.push(format!("{mem}m"));
        }
        if let Some(vcpus) = self.vcpus {
            args.push("--cpus".to_string());
            args.push(vcpus.to_string());
        }
        for (key, value) in &self.env {
            args.push("-e".to_string());
            args.push(format!("{key}={value}"));
        }
        for mount in &self.mounts {
            args.push("-v".to_string());
            args.push(format!(
                "{}:{}{}",
                mount.host.display(),
                mount.guest,
                if mount.read_only { ":ro" } else { "" }
            ));
        }
        if let Some(dir) = &self.workdir {
            args.push("-w".to_string());
            args.push(dir.clone());
        }
        if let Some(entrypoint) = &self.entrypoint {
            args.push("--entrypoint".to_string());
            args.push(entrypoint.clone());
        }
        args.push(self.image.clone());
        args.extend(self.args.iter().cloned());
        args
    }
}

/// One live container.
pub struct ContainerGuest {
    cli: String,
    name: String,
    /// Latched by [`stop`](MachineGuest::stop) so `Drop` does not fire a second
    /// removal for a guest already gone.
    stopped: AtomicBool,
}

impl ContainerGuest {
    /// Run a command **in the container**, outside any chroot its runner made:
    /// this is the host acting on the container, never the guest's own shell —
    /// except for the one consumer whose guest *is* the container (the sandbox's
    /// `Native` tier), for which this is the whole mechanism.
    ///
    /// A nonzero exit is `Ok`: it is the command's outcome, and the caller
    /// reports it. Only the container being gone is an error
    /// ([`GuestError::Gone`]), because that is the one case the caller must act
    /// on rather than report.
    pub async fn exec(&self, argv: &[&str]) -> Result<Output, GuestError> {
        let mut args = vec!["exec", &self.name];
        args.extend_from_slice(argv);
        let output = run(&self.cli, &args).await?;
        let stderr = String::from_utf8_lossy(&output.stderr);
        if daemon_error(output.status.code(), &stderr) {
            return Err(GuestError::Gone(format!(
                "container {}: {}",
                self.name,
                stderr.trim()
            )));
        }
        Ok(output)
    }

    /// Whether the container is still up.
    pub async fn running(&self) -> bool {
        running(&self.cli, &self.name).await
    }

    fn remove_detached(&self) {
        // Deliberately not awaited: correctness needs the removal requested,
        // not observed, and the next start's sweep covers one that never
        // landed. Drop cannot await.
        #[allow(clippy::disallowed_methods)]
        let _ = std::process::Command::new(&self.cli)
            .args(["rm", "-f", &self.name])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn();
    }
}

impl Drop for ContainerGuest {
    /// A dropped guest must not leave a container running: it would hold its
    /// disk's loop device and keep writing to state its consumer no longer owns.
    fn drop(&mut self) {
        if !self.stopped.swap(true, Ordering::SeqCst) {
            self.remove_detached();
        }
    }
}

impl MachineGuest for ContainerGuest {
    fn alive(&self) -> BoxFuture<'_, bool> {
        Box::pin(async move { self.running().await })
    }

    fn connect(&self, port: u32) -> BoxFuture<'_, Result<Box<dyn Duplex>, GuestError>> {
        Box::pin(async move { connect(&self.cli, &self.name, port).await })
    }

    fn pause(&self) -> BoxFuture<'_, Result<(), GuestError>> {
        Box::pin(async move {
            // Freezing the container's processes stops new writes but flushes
            // none: the guest's blocks sit in this kernel's cache, not in the
            // image file a capture scans. So sync first — which makes this
            // mechanism's quiescent point filesystem-clean, not merely
            // crash-consistent (machine §2.2).
            self.exec(&["sync"]).await?;
            let output = run(&self.cli, &["pause", &self.name]).await?;
            if output.status.success() {
                return Ok(());
            }
            Err(classify(&output, &format!("pause {}", self.name)))
        })
    }

    fn resume(&self) -> BoxFuture<'_, Result<(), GuestError>> {
        Box::pin(async move {
            let output = run(&self.cli, &["unpause", &self.name]).await?;
            if output.status.success() {
                return Ok(());
            }
            Err(classify(&output, &format!("unpause {}", self.name)))
        })
    }

    fn stop(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            // `rm -f`, not `stop`: a paused container cannot be stopped
            // gracefully, and this path is reached from a forced step-down
            // too. The runner's own unmount is a nicety for `docker stop`, not
            // something durability rests on — the consumer's capture cadence
            // bounds exactly this loss (machine §4, M3).
            self.stopped.store(true, Ordering::SeqCst);
            let _ = run(&self.cli, &["rm", "-f", &self.name]).await;
        })
    }

    fn console_tail(&self) -> BoxFuture<'_, String> {
        Box::pin(async move {
            let Ok(output) = run(&self.cli, &["logs", "--tail", "10", &self.name]).await else {
                return String::new();
            };
            let mut text = String::from_utf8_lossy(&output.stdout).into_owned();
            text.push_str(&String::from_utf8_lossy(&output.stderr));
            // One line: this lands inside another error message.
            text.split_whitespace().collect::<Vec<_>>().join(" ")
        })
    }
}

/// Open one channel: relay a `docker exec` onto the guest's socket for `port`,
/// then the muxer handshake, so the caller reads and writes frames exactly as it
/// would over vsock.
async fn connect(cli: &str, name: &str, port: u32) -> Result<Box<dyn Duplex>, GuestError> {
    let mut channel = exec_channel(cli, name, port)
        .await
        .map_err(|e| GuestError::Host(format!("container {name}: exec relay: {e}")))?;
    match crate::vsock::handshake(&mut channel, port).await {
        Ok(()) => Ok(Box::new(channel)),
        // A relay that cannot reach the socket is the agent being absent, which
        // for a guest that should be serving means the guest is effectively
        // gone: the consumer re-provisions rather than retrying in place. The
        // relay's own stderr is what says *why* — a missing container reads as
        // the daemon's message, not as an unexplained short read — so it is
        // read here, on the one path that can use it.
        Err(e) => Err(GuestError::Gone(format!(
            "container {name}: agent on port {port}: {e}{}",
            channel.stderr_tail().await
        ))),
    }
}

/// `docker exec -i <name> socat - UNIX-CONNECT:<socket>`: the exec's stdin and
/// stdout are the two halves of one stream to the guest's listener.
async fn exec_channel(cli: &str, name: &str, port: u32) -> std::io::Result<ExecChannel> {
    #[allow(clippy::disallowed_methods)]
    let mut child = Command::new(cli)
        .args([
            "exec",
            "-i",
            name,
            "socat",
            "-",
            &format!("UNIX-CONNECT:{SOCKET_DIR}/{port}.sock"),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        // Captured, not inherited: a failed relay's complaint belongs in the
        // error the caller reports (`ExecChannel::stderr_tail`), never raw on
        // the node's stderr.
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| std::io::Error::other("exec: no stdin pipe"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("exec: no stdout pipe"))?;
    let stderr = child.stderr.take();
    Ok(ExecChannel {
        _child: child,
        stdin,
        stdout,
        stderr,
    })
}

/// One guest channel: the stdio of a relaying `docker exec`. Holds the child so
/// the exec lives exactly as long as the channel and dies with it
/// (`kill_on_drop`) — a dropped PTY channel must not leave a relay attached to
/// the agent. Private: callers receive it as a [`Duplex`].
struct ExecChannel {
    _child: Child,
    stdin: ChildStdin,
    stdout: ChildStdout,
    /// The relay's own complaints, read only when opening the channel failed
    /// ([`stderr_tail`](ExecChannel::stderr_tail)). Piped rather than inherited
    /// so they land in that error instead of raw on the host's stderr; a channel
    /// that opened keeps the pipe idle, which `socat` never fills — it writes
    /// here only on the way out.
    stderr: Option<ChildStderr>,
}

impl ExecChannel {
    /// Whatever the relay said, as a message fragment: empty when it said
    /// nothing, so a caller can always append it.
    async fn stderr_tail(&mut self) -> String {
        /// Enough for the daemon's one-line refusals; a relay that says more
        /// than this is not saying anything more useful.
        const CAP: usize = 512;
        let Some(stderr) = self.stderr.take() else {
            return String::new();
        };
        use tokio::io::AsyncReadExt;
        let mut buffer = Vec::new();
        // The relay has exited or is exiting on this path, so the read ends;
        // the cap bounds a stuck one.
        let read = stderr.take(CAP as u64).read_to_end(&mut buffer).await;
        match read {
            Ok(n) if n > 0 => format!(" ({})", String::from_utf8_lossy(&buffer).trim()),
            _ => String::new(),
        }
    }
}

impl AsyncRead for ExecChannel {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, buf)
    }
}

impl AsyncWrite for ExecChannel {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.stdin).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdin).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stdin).poll_shutdown(cx)
    }
}

/// Whether a named container is running.
async fn running(cli: &str, name: &str) -> bool {
    match run(cli, &["inspect", "-f", "{{.State.Running}}", name]).await {
        Ok(output) => String::from_utf8_lossy(&output.stdout).trim() == "true",
        Err(_) => false,
    }
}

/// One CLI invocation, output captured. `kill_on_drop`: a caller that abandons
/// its future (a tool timeout) must not leave the client behind.
async fn run(cli: &str, args: &[impl AsRef<std::ffi::OsStr>]) -> Result<Output, GuestError> {
    #[allow(clippy::disallowed_methods)]
    Command::new(cli)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await
        .map_err(|e| GuestError::Host(format!("spawn {cli}: {e}")))
}

/// Which arm a failed invocation belongs to.
fn classify(output: &Output, what: &str) -> GuestError {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let text = stderr.trim();
    let message = if text.is_empty() {
        format!("{what}: exit {}", output.status)
    } else {
        format!("{what}: {text}")
    };
    if daemon_error(output.status.code(), &stderr) {
        GuestError::Gone(message)
    } else {
        GuestError::Host(message)
    }
}

/// Whether a CLI failure means the *container* is gone, as opposed to a command
/// inside it having failed.
///
/// Discriminated by stderr, not by exit code: `docker exec` uses 125/126/127 for
/// its own failures, but a user's command can exit 125 too. A daemon-reported
/// error is what marks the container gone.
fn daemon_error(code: Option<i32>, stderr: &str) -> bool {
    if stderr.starts_with("Error response from daemon:")
        || stderr.starts_with("Cannot connect to the Docker daemon")
    {
        return true;
    }
    matches!(code, Some(125..=127))
        && (stderr.contains("Error response from daemon:")
            || stderr.contains("Cannot connect to the Docker daemon")
            || stderr.contains("No such container")
            || stderr.contains("is not running"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> GuestKey {
        GuestKey::new("n1", "abc123")
    }

    #[test]
    fn a_daemon_error_is_the_container_being_gone() {
        assert!(daemon_error(
            Some(1),
            "Error response from daemon: No such container: x"
        ));
        assert!(daemon_error(Some(126), "No such container: x"));
        // A user command that exits 125 with its own message is an outcome.
        assert!(!daemon_error(Some(125), "cp: cannot stat 'x'"));
        assert!(!daemon_error(Some(1), "make: *** No targets."));
    }

    #[test]
    fn run_args_carry_only_what_the_spec_sets() {
        let bare = ContainerSpec::image(key(), "alpine:3.20").run_args("c");
        assert_eq!(
            bare,
            vec![
                "run",
                "-d",
                "--name",
                "c",
                "--network",
                "none",
                "alpine:3.20"
            ]
        );
        let full = ContainerSpec::image(key(), "img")
            .privileged()
            .user("1000:1000")
            .pids_limit(512)
            .sized(2, 512)
            .mount("/h", "/g", true)
            .workdir("/g")
            .entrypoint("sleep", ["9".to_string()])
            .run_args("c");
        let joined = full.join(" ");
        assert!(joined.contains("--privileged"), "{joined}");
        assert!(joined.contains("--user 1000:1000"), "{joined}");
        assert!(joined.contains("--pids-limit 512"), "{joined}");
        assert!(joined.contains("--memory 512m"), "{joined}");
        assert!(joined.contains("--cpus 2"), "{joined}");
        assert!(joined.contains("-v /h:/g:ro"), "{joined}");
        assert!(joined.contains("-w /g"), "{joined}");
        assert!(joined.ends_with("--entrypoint sleep img 9"), "{joined}");
    }

    /// Sizing floors: a spec must never ask docker for zero cpus or a memory
    /// limit below what it accepts.
    #[test]
    fn sizing_has_floors() {
        let spec = ContainerSpec::image(key(), "img").sized(0, 0);
        assert_eq!(spec.vcpus, Some(1));
        assert_eq!(spec.mem_mib, Some(64));
    }

    /// The name is what a start sweeps and a front door dials; it must stay
    /// stable per guest and distinct per node.
    #[test]
    fn container_names_are_node_scoped() {
        let host = ContainerHost::new("docker", "harness-machine");
        assert_eq!(
            host.container_name(&GuestKey::new("n1", "abc")),
            "harness-machine-n1-abc"
        );
        assert_ne!(
            host.container_name(&GuestKey::new("n1", "abc")),
            host.container_name(&GuestKey::new("n2", "abc"))
        );
    }
}
