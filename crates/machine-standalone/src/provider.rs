//! The node's VM binding, chosen at runtime (machine §2.1).
//!
//! `Machine<S, P>` fixes its provider in the type, so a node that could host
//! either binding would host two *different* grain types. This enum keeps one
//! `P` and dispatches inside it, so `--vm` is a flag rather than a build.

use std::sync::Arc;

use actor_core::BoxFuture;
use actor_runtime::TcpCluster;
use machine::MachineVm;
use machine::MachineVmProvider;
use machine::VmError;
use machine::VmSpec;
use machine::fake::FakeVmProvider;

/// Which binding `--vm` selected.
pub enum NodeVmProvider {
    /// The simulation's guest (machine §7): no VMM, no guest agent — a
    /// deterministic stream of block writes into the image. Every durable
    /// property is exercised (provision, capture, hibernate, fail over), and
    /// nothing that needs a live guest is: an SSH channel has nothing to
    /// bridge to. What a machine without KVM can honestly show.
    Fake(FakeVmProvider<TcpCluster>),
    /// The real binding: a Firecracker microVM per machine, its rootfs the
    /// disk facet's image. Linux with `/dev/kvm`.
    #[cfg(feature = "firecracker")]
    Firecracker(machine::firecracker::FirecrackerMachineProvider),
}

impl MachineVmProvider for NodeVmProvider {
    fn boot(&self, spec: VmSpec) -> BoxFuture<'static, Result<Arc<dyn MachineVm>, VmError>> {
        match self {
            NodeVmProvider::Fake(p) => p.boot(spec),
            #[cfg(feature = "firecracker")]
            NodeVmProvider::Firecracker(p) => p.boot(spec),
        }
    }
}
