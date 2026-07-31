use std::io::Seek;
use std::io::Write;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use granary::GranarySystem;

use super::BootSpec;
use super::MachineRuntime;
use super::MachineRuntimeProvider;
use super::RuntimeError;

/// Boots [`FakeRuntime`]s whose writer task ticks on the system's virtual clock:
/// deterministic under simulation, quiescent under [`pause`]
/// (MachineRuntime::pause), and dead the moment the guest is killed or dropped.
pub struct FakeRuntimeProvider<S: GranarySystem> {
    system: S,
    /// Virtual time between guest writes.
    tick: Duration,
}

impl<S: GranarySystem> FakeRuntimeProvider<S> {
    pub fn new(system: S, tick: Duration) -> FakeRuntimeProvider<S> {
        FakeRuntimeProvider { system, tick }
    }
}

impl<S: GranarySystem> MachineRuntimeProvider for FakeRuntimeProvider<S> {
    fn boot(
        &self,
        spec: BootSpec,
    ) -> actor_core::BoxFuture<'static, Result<Arc<dyn MachineRuntime>, RuntimeError>> {
        let system = self.system.clone();
        let tick = self.tick;
        Box::pin(async move {
            let runtime = Arc::new(FakeRuntime {
                image: spec.image.clone(),
                paused: AtomicBool::new(false),
                killed: AtomicBool::new(false),
                guest_ws: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            });
            // Seed the write stream from the machine's name, so each
            // machine's guest activity is distinct but seed-stable.
            let mut lcg: u64 = granary::BlobId::of(spec.machine.to_string().as_bytes()).as_bytes()
                [..8]
                .try_into()
                .map(u64::from_le_bytes)
                .expect("eight bytes");
            let weak: Weak<FakeRuntime> = Arc::downgrade(&runtime);
            let clock = system.clone();
            system.launch(Box::pin(async move {
                loop {
                    clock.sleep(tick).await;
                    let Some(runtime) = weak.upgrade() else { break };
                    if runtime.killed.load(Ordering::Relaxed) {
                        break;
                    }
                    if runtime.paused.load(Ordering::Relaxed) {
                        continue;
                    }
                    lcg = lcg.wrapping_mul(6364136223846793005).wrapping_add(1);
                    runtime.write_once(lcg);
                    runtime.ws_write_once(lcg);
                }
            }));
            Ok(runtime as Arc<dyn MachineRuntime>)
        })
    }
}

/// See [`FakeRuntimeProvider`].
pub struct FakeRuntime {
    image: std::path::PathBuf,
    paused: AtomicBool,
    killed: AtomicBool,
    /// The guest's `/workspace` tmpfs, as an in-memory `path → bytes`
    /// map: [`push_ws`](MachineRuntime::push_ws) fills it from the host
    /// directory, the writer task dirties `guest.log` seed-stably, and
    /// [`pull_ws`](MachineRuntime::pull_ws) writes it back.
    guest_ws: std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
}

impl FakeRuntime {
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
    /// [`write_once`](FakeRuntime::write_once), so a quiescent point sees a
    /// settled workspace too.
    fn ws_write_once(&self, n: u64) {
        let mut bytes = Vec::with_capacity(32);
        for i in 0..32u8 {
            bytes.push((n as u8).wrapping_mul(31).wrapping_add(i));
        }
        self.lock_ws().insert("guest.log".to_string(), bytes);
    }

    fn lock_ws(&self) -> std::sync::MutexGuard<'_, std::collections::BTreeMap<String, Vec<u8>>> {
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

impl MachineRuntime for FakeRuntime {
    fn pause(&self) -> actor_core::BoxFuture<'_, Result<(), RuntimeError>> {
        self.paused.store(true, Ordering::Relaxed);
        Box::pin(async { Ok(()) })
    }

    fn resume(&self) -> actor_core::BoxFuture<'_, Result<(), RuntimeError>> {
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
    ) -> actor_core::BoxFuture<'_, Result<(), RuntimeError>> {
        Box::pin(async move {
            if self.killed.load(Ordering::Relaxed) {
                return Err(RuntimeError::Transport("fake runtime killed".to_string()));
            }
            let map = FakeRuntime::read_dir_map(&ws)
                .map_err(|e| RuntimeError::Transport(e.to_string()))?;
            *self.lock_ws() = map;
            Ok(())
        })
    }

    fn pull_ws(
        &self,
        ws: std::path::PathBuf,
    ) -> actor_core::BoxFuture<'_, Result<(), RuntimeError>> {
        Box::pin(async move {
            if self.killed.load(Ordering::Relaxed) {
                return Err(RuntimeError::Transport("fake runtime killed".to_string()));
            }
            // All synchronous IO, so the guard is held across it — no clone
            // of the file bytes.
            let map = self.lock_ws();
            // Replace, never merge — the guest's view is authoritative
            // across a pull, as for the real agent.
            for entry in
                std::fs::read_dir(&ws).map_err(|e| RuntimeError::Transport(e.to_string()))?
            {
                let entry = entry.map_err(|e| RuntimeError::Transport(e.to_string()))?;
                let kind = entry
                    .file_type()
                    .map_err(|e| RuntimeError::Transport(e.to_string()))?;
                let result = if kind.is_dir() {
                    std::fs::remove_dir_all(entry.path())
                } else {
                    std::fs::remove_file(entry.path())
                };
                result.map_err(|e| RuntimeError::Transport(e.to_string()))?;
            }
            for (rel, bytes) in map.iter() {
                let path = ws.join(rel);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| RuntimeError::Transport(e.to_string()))?;
                }
                std::fs::write(&path, bytes).map_err(|e| RuntimeError::Transport(e.to_string()))?;
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::MachineRuntime;

    fn fake() -> FakeRuntime {
        FakeRuntime {
            image: std::path::PathBuf::new(),
            paused: AtomicBool::new(false),
            killed: AtomicBool::new(false),
            guest_ws: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    #[tokio::test]
    async fn push_then_pull_round_trips_nested_files() {
        let runtime = fake();
        let host = tempfile::tempdir().expect("host ws");
        std::fs::create_dir_all(host.path().join("d")).expect("mkdir");
        std::fs::write(host.path().join("d/f.txt"), b"deep").expect("write");
        std::fs::write(host.path().join("top.txt"), b"tip").expect("write");
        runtime
            .push_ws(host.path().to_path_buf())
            .await
            .expect("push");

        // The guest dirties its log; the host files survive beside it.
        runtime.ws_write_once(7);
        std::fs::remove_file(host.path().join("top.txt")).expect("hide");
        runtime
            .pull_ws(host.path().to_path_buf())
            .await
            .expect("pull");

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
        let runtime = fake();
        runtime.kill().await;
        let host = tempfile::tempdir().expect("host ws");
        assert!(runtime.push_ws(host.path().to_path_buf()).await.is_err());
        assert!(runtime.pull_ws(host.path().to_path_buf()).await.is_err());
    }
}
