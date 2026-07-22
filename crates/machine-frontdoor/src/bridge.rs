//! The reference [`ChannelBackend`](crate::ChannelBackend): the guest agent
//! over vsock (machine §5.1).
//!
//! One channel is one vsock connection: the muxer handshake and framing the
//! shared transport provides ([`microvm::vsock`]), then the channel's
//! [`ChannelKind`](crate::ChannelKind) header, then [`machine_proto`] frames.
//! This assumes the guest socket is reachable from this node — the machine's
//! leader owns it, so a co-located front door reaches it directly; a front
//! door on another node supplies a relayed stream instead (the cross-node
//! relay is the seam's job, machine §8).

use std::path::PathBuf;

use granary::GrainName;
use machine_proto::AGENT_PORT;
use microvm::vsock;

use crate::ChannelBackend;
use crate::ChannelKind;
use crate::Duplex;

/// Opens channels to a guest agent over a Firecracker host-side vsock socket
/// this node can reach. `socket_for` maps a machine to its vsock socket path
/// (the leader's control directory).
pub struct VsockBackend<F> {
    socket_for: F,
}

impl<F> VsockBackend<F>
where
    F: Fn(&GrainName) -> PathBuf + Send + Sync + 'static,
{
    pub fn new(socket_for: F) -> VsockBackend<F> {
        VsockBackend { socket_for }
    }
}

impl<F> ChannelBackend for VsockBackend<F>
where
    F: Fn(&GrainName) -> PathBuf + Send + Sync + 'static,
{
    async fn open(
        &self,
        machine: &GrainName,
        kind: ChannelKind,
    ) -> std::io::Result<Box<dyn Duplex>> {
        let path = (self.socket_for)(machine);
        let mut stream = vsock::connect(&path, AGENT_PORT).await?;
        // The channel header, so the agent knows what to spawn.
        vsock::send_frame(&mut stream, &kind.header()).await?;
        Ok(Box::new(stream))
    }
}
