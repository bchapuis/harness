//! The front door's transport half (machine §5.1): one byte stream per channel
//! to the guest agent, over whatever this node holds its guests with.
//!
//! The door does not know the mechanism. It asks the node's
//! `machine_host::MachineHost` for a channel to a machine by name
//! (`machine_grain::hosted::open_channel`), which is a vsock connection under the
//! microVM grade and a relayed `docker exec` under the container grade. What is
//! left here is the diagnostic, because the *absence* of a guest is the one
//! failure a demo operator actually hits, and its two causes need different
//! answers:
//!
//! - The machine's activation is on **another node**. A guest is node-local and
//!   the cross-node relay is machine §8's deferred work, so this node's door
//!   cannot bridge it; the client should reach the leader's door.
//! - The node runs the **fake binding** (`--machine fake`). There is no guest
//!   at all: the fake runtime dirties disk blocks and answers pause/capture, so
//!   every durable property holds, but nothing listens.

use std::sync::Arc;

use granary::GrainName;
use machine_frontdoor::ChannelBackend;
use machine_frontdoor::ChannelKind;
use machine_frontdoor::Duplex;

/// Opens guest channels the way this node's binding allows.
pub struct LocalBackend {
    /// The node's machine host, or `None` under `--machine fake`, which has no
    /// guest to reach.
    host: Option<Arc<dyn machine_host::MachineHost>>,
    /// This node's id, scoping the guest names it may dial
    /// (`machine_grain::hosted::guest_key`).
    node: String,
}

impl LocalBackend {
    /// `host` is `None` for a node with no way to hold a guest.
    pub fn new(
        host: Option<Arc<dyn machine_host::MachineHost>>,
        node: impl Into<String>,
    ) -> LocalBackend {
        LocalBackend {
            host,
            node: node.into(),
        }
    }
}

impl ChannelBackend for LocalBackend {
    async fn open(
        &self,
        machine: &GrainName,
        kind: ChannelKind,
    ) -> std::io::Result<Box<dyn Duplex>> {
        #[cfg(feature = "host")]
        {
            let Some(host) = self.host.as_deref() else {
                return Err(no_guest_here(machine));
            };
            return machine_grain::hosted::open_channel(host, &self.node, machine, &kind)
                .await
                .map_err(|e| match e {
                    // The ordinary case, and not this node's fault: the machine
                    // is led elsewhere, so its guest is elsewhere.
                    machine_host::GuestError::Gone(e) => std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        format!(
                            "{machine}: {e} — this machine's activation is on another node, and a \
                             front door bridges only the machines its own node leads (machine \
                             §8's cross-node relay is not built). Reconnect to the leader's door."
                        ),
                    ),
                    machine_host::GuestError::Host(e) => std::io::Error::other(e),
                });
        }
        #[cfg(not(feature = "host"))]
        {
            let _ = (kind, &self.node, &self.host);
            Err(no_guest_here(machine))
        }
    }
}

/// A node with no guest to bridge to at all — `--machine fake`, or a build without
/// the binding. Distinct from a machine led by another node: durability works
/// here, sessions never will.
fn no_guest_here(machine: &GrainName) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!(
            "{machine}: this node runs --machine fake, which has no guest to bridge to. \
             Durability works; sessions do not. Run --machine docker (any host with docker) \
             or --machine firecracker (Linux with /dev/kvm) for a real machine."
        ),
    )
}
