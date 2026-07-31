//! The node's runtime binding, chosen at runtime (machine §2.1).
//!
//! `Machine<S, P>` fixes its provider in the type, so a node that could host
//! either binding would host two *different* grain types. This enum keeps one
//! `P` and dispatches inside it, so `--machine` is a flag rather than a build.
//!
//! Two arms, not three: the two real mechanisms — a Firecracker microVM and a
//! container holding the same rootfs — are one binding over
//! `machine_host::MachineHost`, and which of them a node holds guests with is
//! decided when that host is built (see `node::machine_host`).

use std::sync::Arc;

use actor_core::BoxFuture;
use actor_runtime::TcpCluster;
use machine_grain::BootSpec;
use machine_grain::MachineRuntime;
use machine_grain::MachineRuntimeProvider;
use machine_grain::RuntimeError;
use machine_grain::fake::FakeRuntimeProvider;

/// Which binding `--machine` selected.
pub enum NodeRuntimeProvider {
    /// The simulation's guest (machine §7): no mechanism, no guest agent — a
    /// deterministic stream of block writes into the image. Every durable
    /// property is exercised (provision, capture, hibernate, fail over), and
    /// nothing that needs a live guest is: an SSH channel has nothing to
    /// bridge to. What a machine with no way to hold a guest can honestly show.
    Fake(FakeRuntimeProvider<TcpCluster>),
    /// A real guest, held by whichever mechanism this node was configured with
    /// (`--machine firecracker` or `--machine docker`).
    #[cfg(feature = "host")]
    Hosted(machine_grain::hosted::HostedRuntimeProvider),
}

impl MachineRuntimeProvider for NodeRuntimeProvider {
    fn boot(
        &self,
        spec: BootSpec,
    ) -> BoxFuture<'static, Result<Arc<dyn MachineRuntime>, RuntimeError>> {
        match self {
            NodeRuntimeProvider::Fake(p) => p.boot(spec),
            #[cfg(feature = "host")]
            NodeRuntimeProvider::Hosted(p) => p.boot(spec),
        }
    }
}
