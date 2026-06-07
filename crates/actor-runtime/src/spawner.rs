//! The production [`Spawner`]: tasks run on a tokio runtime (spec §4.6).
//!
//! Holds a [`tokio::runtime::Handle`] rather than the runtime itself, so the
//! spawner can be cloned freely and handed to the cluster system while the
//! runtime is owned elsewhere (typically by `#[tokio::main]` or a `Runtime` the
//! caller keeps alive).

use actor_core::BoxFuture;
use actor_core::Spawner;
use tokio::runtime::Handle;

/// A [`Spawner`] that launches tasks onto a tokio runtime.
#[derive(Clone)]
pub struct TokioSpawner {
    handle: Handle,
}

impl TokioSpawner {
    /// Bind to an explicit runtime handle.
    pub fn new(handle: Handle) -> TokioSpawner {
        TokioSpawner { handle }
    }

    /// Bind to the runtime this constructor is called from. Panics if no tokio
    /// runtime is running on the current thread.
    pub fn current() -> TokioSpawner {
        TokioSpawner {
            handle: Handle::current(),
        }
    }
}

impl Spawner for TokioSpawner {
    fn launch(&self, task: BoxFuture<'static, ()>) {
        self.handle.spawn(task);
    }
}
