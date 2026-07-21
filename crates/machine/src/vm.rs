//! The machine's runtime binding seam (machine §1, §2.1): how the grain
//! reaches its live microVM.
//!
//! The activation *is* a running microVM (machine §1), but the VM is one of
//! the machine's two disposable things: stopping it loses no committed disk
//! block. The grain therefore drives the VM through this narrow seam — boot
//! against the rehydrated disk-facet image, pause for a capture's quiescent
//! point (machine §4), resume, kill — and never owns VMM mechanics. The
//! deterministic simulation binds [`fake::FakeVmProvider`]; production binds
//! the Firecracker `Native` mechanics the sandbox proved (sandbox §3.5).

use std::path::PathBuf;
use std::sync::Arc;

use actor_core::BoxFuture;
use granary::GrainName;

#[cfg(feature = "firecracker")]
pub mod firecracker;
#[cfg(feature = "firecracker")]
pub mod ws_proto;

/// A VM operation failed. An application-level outcome (the grain maps it into
/// replies or retries), never a durability failure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VmError(pub String);

impl std::fmt::Display for VmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "vm error: {}", self.0)
    }
}

impl std::error::Error for VmError {}

/// What a boot needs (machine §3): the disk-facet image to mount as the
/// rootfs, the journaled sizing, and the machine's name for attribution.
#[derive(Clone, Debug)]
pub struct VmSpec {
    /// The disk facet's materialized image (`ctx.disk().path()`), the VM's
    /// backing drive. The guest writes it in place between captures (grain
    /// §7.15's one departure).
    pub image: PathBuf,
    pub vcpus: u8,
    pub mem_mib: u32,
    /// The machine this VM belongs to — the attribution key (machine §5.2).
    pub machine: GrainName,
}

/// One live guest. Held by the activation and by nothing durable; dropped or
/// killed with the activation (machine §1).
pub trait MachineVm: Send + Sync + 'static {
    /// Pause the guest at a quiescent point (machine §4): once resolved, the
    /// guest issues no further writes to the image until [`resume`]
    /// (MachineVm::resume), so a capture's scan sees a stable image (grain
    /// §7.15's capture seam).
    fn pause(&self) -> BoxFuture<'_, Result<(), VmError>>;

    /// Resume a paused guest.
    fn resume(&self) -> BoxFuture<'_, Result<(), VmError>>;

    /// Stop the guest. Idempotent: the forced step-down path (machine §4, M5)
    /// and `on_passivate` both call it, possibly for a VM already gone; an
    /// implementation whose process handle outlives the call must also kill
    /// on drop, so a dropped activation can never leak a running guest.
    fn kill(&self) -> BoxFuture<'_, ()>;

    /// Replace the guest's `/workspace` (a tmpfs, machine §3) with the host
    /// workspace directory's contents. Called once per boot, before the first
    /// attach is answered; a failure means the guest must not serve (the
    /// grain kills the VM and fails the command, machine §4).
    fn push_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), VmError>>;

    /// Flush the guest and replace the host workspace directory's contents
    /// with the guest's `/workspace`. Must be called while the guest is
    /// *running* (a paused guest cannot answer), so the capture sequence is
    /// pull → pause → capture → resume (machine §4). On failure the host
    /// directory is left untouched, so nothing partial can be durably
    /// captured.
    fn pull_ws(&self, ws: PathBuf) -> BoxFuture<'_, Result<(), VmError>>;
}

/// Boots machines. One per node, injected into the grain factory
/// (`granary_named`), so each activation binds its node's VMM.
pub trait MachineVmProvider: Send + Sync + 'static {
    /// Boot a guest against `spec.image`. The image was rehydrated by the disk
    /// facet before the first command (grain §7.15), so the boot reads the
    /// committed rootfs.
    fn boot(&self, spec: VmSpec) -> BoxFuture<'static, Result<Arc<dyn MachineVm>, VmError>>;
}

/// The simulation's VM (machine §7): a "guest" whose activity is a
/// deterministic, seed-stable stream of block writes into the image file, so
/// captures have real dirty blocks and one seed reproduces a whole
/// attach–crash–failover–reconnect narrative byte-identically.
pub mod fake {
    use std::io::Seek;
    use std::io::Write;
    use std::sync::Arc;
    use std::sync::Weak;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use granary::GranarySystem;

    use super::MachineVm;
    use super::MachineVmProvider;
    use super::VmError;
    use super::VmSpec;

    /// Boots [`FakeVm`]s whose writer task ticks on the system's virtual
    /// clock: deterministic under simulation, quiescent under [`pause`]
    /// (MachineVm::pause), and dead the moment the VM is killed or dropped.
    pub struct FakeVmProvider<S: GranarySystem> {
        system: S,
        /// Virtual time between guest writes.
        tick: Duration,
    }

    impl<S: GranarySystem> FakeVmProvider<S> {
        pub fn new(system: S, tick: Duration) -> FakeVmProvider<S> {
            FakeVmProvider { system, tick }
        }
    }

    impl<S: GranarySystem> MachineVmProvider for FakeVmProvider<S> {
        fn boot(
            &self,
            spec: VmSpec,
        ) -> actor_core::BoxFuture<'static, Result<Arc<dyn MachineVm>, VmError>> {
            let system = self.system.clone();
            let tick = self.tick;
            Box::pin(async move {
                let vm = Arc::new(FakeVm {
                    image: spec.image.clone(),
                    paused: AtomicBool::new(false),
                    killed: AtomicBool::new(false),
                    guest_ws: std::sync::Mutex::new(std::collections::BTreeMap::new()),
                });
                // Seed the write stream from the machine's name, so each
                // machine's guest activity is distinct but seed-stable.
                let mut lcg: u64 = granary::BlobId::of(spec.machine.to_string().as_bytes())
                    .as_bytes()[..8]
                    .try_into()
                    .map(u64::from_le_bytes)
                    .expect("eight bytes");
                let weak: Weak<FakeVm> = Arc::downgrade(&vm);
                let clock = system.clone();
                system.launch(Box::pin(async move {
                    loop {
                        clock.sleep(tick).await;
                        let Some(vm) = weak.upgrade() else { break };
                        if vm.killed.load(Ordering::Relaxed) {
                            break;
                        }
                        if vm.paused.load(Ordering::Relaxed) {
                            continue;
                        }
                        lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
                        vm.write_once(lcg);
                        vm.ws_write_once(lcg);
                    }
                }));
                Ok(vm as Arc<dyn MachineVm>)
            })
        }
    }

    /// See [`FakeVmProvider`].
    pub struct FakeVm {
        image: std::path::PathBuf,
        paused: AtomicBool,
        killed: AtomicBool,
        /// The guest's `/workspace` tmpfs, as an in-memory `path → bytes`
        /// map: [`push_ws`](MachineVm::push_ws) fills it from the host
        /// directory, the writer task dirties `guest.log` seed-stably, and
        /// [`pull_ws`](MachineVm::pull_ws) writes it back.
        guest_ws: std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
    }

    impl FakeVm {
        /// One deterministic guest write: 64 bytes derived from `n`, at an
        /// offset derived from `n`, within the current image. The write is
        /// synchronous and the writer task holds no await between the pause
        /// check and the write, so a resolved `pause` really is quiescent
        /// under the simulation's cooperative scheduler.
        fn write_once(&self, n: u64) {
            let Ok(meta) = std::fs::metadata(&self.image) else {
                return;
            };
            let len = meta.len();
            if len < 64 {
                return;
            }
            let offset = n % (len - 64);
            let Ok(mut file) = std::fs::OpenOptions::new().write(true).open(&self.image) else {
                return;
            };
            let mut block = [0u8; 64];
            for (i, b) in block.iter_mut().enumerate() {
                *b = (n as u8).wrapping_add(i as u8);
            }
            if file.seek(std::io::SeekFrom::Start(offset)).is_ok() {
                let _ = file.write_all(&block);
            }
        }

        /// One deterministic guest *workspace* write: `guest.log` becomes 32
        /// bytes derived from `n`, under the same pause/kill guards as
        /// [`write_once`](FakeVm::write_once), so a quiescent point sees a
        /// settled workspace too.
        fn ws_write_once(&self, n: u64) {
            let mut bytes = Vec::with_capacity(32);
            for i in 0..32u8 {
                bytes.push((n as u8).wrapping_mul(31).wrapping_add(i));
            }
            self.lock_ws().insert("guest.log".to_string(), bytes);
        }

        fn lock_ws(
            &self,
        ) -> std::sync::MutexGuard<'_, std::collections::BTreeMap<String, Vec<u8>>> {
            self.guest_ws.lock().unwrap_or_else(|e| e.into_inner())
        }

        /// Walk a host directory into a `relative path → bytes` map (sorted
        /// by construction; regular files only, as the sim's guests write).
        fn read_dir_map(
            root: &std::path::Path,
        ) -> std::io::Result<std::collections::BTreeMap<String, Vec<u8>>> {
            fn walk(
                root: &std::path::Path,
                dir: &std::path::Path,
                map: &mut std::collections::BTreeMap<String, Vec<u8>>,
            ) -> std::io::Result<()> {
                for entry in std::fs::read_dir(dir)? {
                    let entry = entry?;
                    let path = entry.path();
                    let kind = entry.file_type()?;
                    if kind.is_dir() {
                        walk(root, &path, map)?;
                    } else if kind.is_file() {
                        let rel = path
                            .strip_prefix(root)
                            .expect("walk stays under root")
                            .to_string_lossy()
                            .into_owned();
                        map.insert(rel, std::fs::read(&path)?);
                    }
                }
                Ok(())
            }
            let mut map = std::collections::BTreeMap::new();
            walk(root, root, &mut map)?;
            Ok(map)
        }
    }

    impl MachineVm for FakeVm {
        fn pause(&self) -> actor_core::BoxFuture<'_, Result<(), VmError>> {
            self.paused.store(true, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        }

        fn resume(&self) -> actor_core::BoxFuture<'_, Result<(), VmError>> {
            self.paused.store(false, Ordering::Relaxed);
            Box::pin(async { Ok(()) })
        }

        fn kill(&self) -> actor_core::BoxFuture<'_, ()> {
            self.killed.store(true, Ordering::Relaxed);
            Box::pin(async {})
        }

        fn push_ws(
            &self,
            ws: std::path::PathBuf,
        ) -> actor_core::BoxFuture<'_, Result<(), VmError>> {
            Box::pin(async move {
                if self.killed.load(Ordering::Relaxed) {
                    return Err(VmError("fake vm killed".to_string()));
                }
                let map = FakeVm::read_dir_map(&ws).map_err(|e| VmError(e.to_string()))?;
                *self.lock_ws() = map;
                Ok(())
            })
        }

        fn pull_ws(
            &self,
            ws: std::path::PathBuf,
        ) -> actor_core::BoxFuture<'_, Result<(), VmError>> {
            Box::pin(async move {
                if self.killed.load(Ordering::Relaxed) {
                    return Err(VmError("fake vm killed".to_string()));
                }
                // The write-out is all synchronous IO, so the guard is held
                // across it — no clone of the file bytes.
                let map = self.lock_ws();
                // Replace, never merge — the guest's view is authoritative
                // across a pull, as for the real agent.
                for entry in std::fs::read_dir(&ws).map_err(|e| VmError(e.to_string()))? {
                    let entry = entry.map_err(|e| VmError(e.to_string()))?;
                    let kind = entry.file_type().map_err(|e| VmError(e.to_string()))?;
                    let result = if kind.is_dir() {
                        std::fs::remove_dir_all(entry.path())
                    } else {
                        std::fs::remove_file(entry.path())
                    };
                    result.map_err(|e| VmError(e.to_string()))?;
                }
                for (rel, bytes) in map.iter() {
                    let path = ws.join(rel);
                    if let Some(parent) = path.parent() {
                        std::fs::create_dir_all(parent).map_err(|e| VmError(e.to_string()))?;
                    }
                    std::fs::write(&path, bytes).map_err(|e| VmError(e.to_string()))?;
                }
                Ok(())
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::vm::MachineVm;

        fn fake() -> FakeVm {
            FakeVm {
                image: std::path::PathBuf::new(),
                paused: AtomicBool::new(false),
                killed: AtomicBool::new(false),
                guest_ws: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            }
        }

        #[tokio::test]
        async fn push_then_pull_round_trips_nested_files() {
            let vm = fake();
            let host = tempfile::tempdir().expect("host ws");
            std::fs::create_dir_all(host.path().join("d")).expect("mkdir");
            std::fs::write(host.path().join("d/f.txt"), b"deep").expect("write");
            std::fs::write(host.path().join("top.txt"), b"tip").expect("write");
            vm.push_ws(host.path().to_path_buf()).await.expect("push");

            // The guest dirties its log; the host files survive beside it.
            vm.ws_write_once(7);
            std::fs::remove_file(host.path().join("top.txt")).expect("hide");
            vm.pull_ws(host.path().to_path_buf()).await.expect("pull");

            assert_eq!(
                std::fs::read(host.path().join("d/f.txt")).expect("f"),
                b"deep"
            );
            assert_eq!(
                std::fs::read(host.path().join("top.txt")).expect("t"),
                b"tip"
            );
            assert!(host.path().join("guest.log").exists());
        }

        #[test]
        fn ws_writes_are_a_pure_function_of_the_seed() {
            let one = fake();
            let two = fake();
            one.ws_write_once(42);
            two.ws_write_once(42);
            assert_eq!(*one.lock_ws(), *two.lock_ws());
        }

        #[tokio::test]
        async fn a_killed_vm_refuses_the_sync() {
            let vm = fake();
            vm.kill().await;
            let host = tempfile::tempdir().expect("host ws");
            assert!(vm.push_ws(host.path().to_path_buf()).await.is_err());
            assert!(vm.pull_ws(host.path().to_path_buf()).await.is_err());
        }
    }
}
