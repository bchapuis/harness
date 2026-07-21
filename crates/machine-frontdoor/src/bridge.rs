//! The reference [`ChannelBackend`](crate::ChannelBackend): the guest agent
//! over vsock (machine §5.1).
//!
//! One channel is one vsock connection: the Firecracker muxer's
//! `CONNECT <port>\n` → `OK <port>\n` line handshake, then the channel's
//! [`ChannelKind`](crate::ChannelKind) header, then [`proto`](crate::proto)
//! frames. This assumes the guest socket is reachable from this node — the
//! machine's leader owns it, so a co-located front door reaches it directly;
//! a front door on another node supplies a relayed stream instead (the
//! cross-node relay is the seam's job, machine §8).

use std::path::PathBuf;

use granary::GrainName;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

use crate::ChannelBackend;
use crate::ChannelKind;
use crate::Duplex;
use crate::proto::AGENT_PORT;
use crate::proto::send_frame;

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
        let mut stream = UnixStream::connect(&path).await?;
        // The muxer line handshake (machine §5.1).
        stream
            .write_all(format!("CONNECT {AGENT_PORT}\n").as_bytes())
            .await?;
        let mut line = Vec::with_capacity(16);
        loop {
            let byte = stream.read_u8().await?;
            if byte == b'\n' {
                break;
            }
            line.push(byte);
            if line.len() > 64 {
                return Err(std::io::Error::other("vsock handshake: oversized reply"));
            }
        }
        if !line.starts_with(b"OK ") {
            return Err(std::io::Error::other("vsock handshake failed"));
        }
        // The channel header, so the agent knows what to spawn.
        send_frame(&mut stream, &kind.header_json()).await?;
        Ok(Box::new(stream))
    }
}
