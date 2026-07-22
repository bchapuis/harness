//! The vsock transport (sandbox spec §3.5, mirrored by the guest agents):
//! Firecracker's host-side unix socket, the muxer's `CONNECT <port>\n` →
//! `OK <port>\n` line handshake, then `u32` little-endian length-prefixed
//! frames. Every receive is capped **before** it sizes anything: a frame
//! header's claim never becomes a host allocation (the sandbox §3.2 stance).

use std::path::Path;

use serde_json::Value;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

/// Connect to a guest listener through Firecracker's host-side vsock socket:
/// the muxer's `CONNECT <port>` line handshake, then frames.
pub async fn connect(uds: &Path, port: u32) -> Result<UnixStream, std::io::Error> {
    let mut stream = UnixStream::connect(uds).await?;
    stream
        .write_all(format!("CONNECT {port}\n").as_bytes())
        .await?;
    // The muxer answers one line, `OK <port>\n`; read to the newline and no
    // further — the bytes after it are frames.
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
        return Err(std::io::Error::other(format!(
            "vsock handshake: {}",
            String::from_utf8_lossy(&line)
        )));
    }
    Ok(stream)
}

/// Send one frame: a `u32` little-endian length, then the bytes. Generic over
/// the stream, so a relayed or split half frames identically to the socket.
pub async fn send_frame<W>(stream: &mut W, bytes: &[u8]) -> Result<(), std::io::Error>
where
    W: AsyncWrite + Unpin,
{
    stream
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .await?;
    stream.write_all(bytes).await?;
    stream.flush().await
}

/// Receive one frame of at most `cap` bytes, refusing an oversized header
/// before allocating.
pub async fn recv_frame<R>(stream: &mut R, cap: usize) -> Result<Vec<u8>, std::io::Error>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len) as usize;
    if len > cap {
        return Err(std::io::Error::other(format!(
            "frame of {len} bytes exceeds the {cap}-byte cap"
        )));
    }
    let mut bytes = vec![0u8; len];
    stream.read_exact(&mut bytes).await?;
    Ok(bytes)
}

/// Send one JSON frame.
pub async fn send_json<W>(stream: &mut W, value: &Value) -> Result<(), std::io::Error>
where
    W: AsyncWrite + Unpin,
{
    send_frame(stream, value.to_string().as_bytes()).await
}

/// Receive one JSON frame of at most `cap` bytes.
pub async fn recv_json<R>(stream: &mut R, cap: usize) -> Result<Value, std::io::Error>
where
    R: AsyncRead + Unpin,
{
    let bytes = recv_frame(stream, cap).await?;
    serde_json::from_slice(&bytes).map_err(|e| std::io::Error::other(format!("bad frame: {e}")))
}
