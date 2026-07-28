//! The front door's transport half (machine §5.1): one byte stream per channel
//! to the guest agent, over the node-local vsock socket.
//!
//! `machine-frontdoor::VsockBackend` is the mechanism; this wraps it to say
//! something useful when the socket is not there, which is the one failure a
//! demo operator will actually hit. Two causes, and they need different
//! answers:
//!
//! - The machine's activation is on **another node**. The socket is node-local
//!   and the cross-node relay is machine §8's deferred work, so this node's
//!   door cannot bridge it; the client should reach the leader's door.
//! - The node runs the **fake binding** (`--vm fake`). There is no guest at
//!   all: a fake VM dirties disk blocks and answers pause/capture, so every
//!   durable property holds, but nothing listens on vsock.

use granary::GrainName;
use machine_frontdoor::ChannelBackend;
use machine_frontdoor::ChannelKind;
use machine_frontdoor::Duplex;

/// Opens guest channels over this node's own vsock sockets.
pub struct LocalVsockBackend {
    /// Whether this node's binding has a guest at all (see the module docs):
    /// only the diagnostic differs, never whether the dial is attempted.
    has_guest: bool,
}

impl LocalVsockBackend {
    pub fn new(has_guest: bool) -> LocalVsockBackend {
        LocalVsockBackend { has_guest }
    }
}

impl ChannelBackend for LocalVsockBackend {
    async fn open(
        &self,
        machine: &GrainName,
        kind: ChannelKind,
    ) -> std::io::Result<Box<dyn Duplex>> {
        #[cfg(feature = "firecracker")]
        {
            let socket = machine::firecracker::vsock_socket_path(machine);
            if !socket.exists() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    if self.has_guest {
                        format!(
                            "{machine}: no guest socket at {} — this machine's activation is on \
                             another node, and a front door bridges only the machines its own \
                             node leads (machine §8's cross-node relay is not built). Reconnect \
                             to the leader's door.",
                            socket.display()
                        )
                    } else {
                        format!(
                            "{machine}: this node runs --vm fake, which has no guest to bridge \
                             to. Durability works; sessions do not. Run --vm firecracker on \
                             Linux with /dev/kvm for a real machine."
                        )
                    },
                ));
            }
            let backend =
                machine_frontdoor::VsockBackend::new(machine::firecracker::vsock_socket_path);
            return backend.open(machine, kind).await;
        }
        #[cfg(not(feature = "firecracker"))]
        {
            let _ = (machine, kind, self.has_guest);
            Err(std::io::Error::other(
                "built without the `firecracker` feature: no guest transport",
            ))
        }
    }
}
