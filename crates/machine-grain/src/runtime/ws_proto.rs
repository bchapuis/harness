//! The workspace-sync channels, host side (machine spec §4): `WsPush`,
//! `WsPull`, and `Sync` drivers over the guest agent's channel protocol
//! ([`machine_proto`]) and the shared frame codec ([`machine_host::vsock`]).
//!
//! One connection per op. A push sends the header, `Data` chunks, `Eof`, and
//! reads a status; a pull sends the header and reads `Data` chunks to `Eof`
//! then a status, the accumulation capped at [`MAX_TAR`] before any byte
//! sizes a host allocation (sandbox spec §3.2).
//!
//! The drivers take an opened channel, not a socket path: the same three ops run
//! over a microVM's vsock and over a container's relayed pipe, because what
//! differs between the mechanisms is how the guest agent is reached
//! (`machine_host::Guest::connect`), not what it is told.

use machine_host::Duplex;
use machine_host::vsock;
use machine_host::ws_sync::MAX_TAR;
use machine_proto::Frame;
use machine_proto::MAX_FRAME;

/// Data chunks stay well under [`MAX_FRAME`] while keeping the frame count
/// (and its per-frame syscalls) low for a full-size stream.
const CHUNK: usize = 256 * 1024;

/// A ws-channel op failed, split by what still works: after a
/// [`Refused`](SyncError::Refused) refusal the guest and its transport still
/// serve; after a [`Transport`](SyncError::Transport) failure the guest may be
/// gone.
#[derive(Debug)]
pub enum SyncError {
    /// The agent answered with a non-zero status; its stderr is folded in.
    Refused(String),
    /// The vsock stream itself failed.
    Transport(std::io::Error),
}

impl std::fmt::Display for SyncError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncError::Refused(e) => write!(f, "guest agent: {e}"),
            SyncError::Transport(e) => write!(f, "vsock: {e}"),
        }
    }
}

impl From<std::io::Error> for SyncError {
    fn from(e: std::io::Error) -> SyncError {
        SyncError::Transport(e)
    }
}

/// Read frames until a terminal `ExitStatus`, folding `Data` into `tar`
/// (capped) and `Stderr` into the error message.
async fn read_to_status(
    stream: &mut impl Duplex,
    mut tar: Option<&mut Vec<u8>>,
) -> Result<(), SyncError> {
    let mut stderr = Vec::new();
    loop {
        let body = vsock::recv_frame(stream, MAX_FRAME).await?;
        // Bulk `Data` appends straight from the frame body — no decode copy.
        if let Some(bytes) = Frame::data_payload(&body) {
            if let Some(tar) = tar.as_deref_mut() {
                if tar.len() + bytes.len() > MAX_TAR {
                    return Err(SyncError::Transport(std::io::Error::other(format!(
                        "pulled workspace exceeds the {MAX_TAR}-byte sync cap"
                    ))));
                }
                tar.extend_from_slice(bytes);
            }
            continue;
        }
        match Frame::decode(&body) {
            Some(Frame::Stderr(bytes)) => stderr.extend_from_slice(&bytes),
            Some(Frame::Eof) => {}
            Some(Frame::ExitStatus(code)) => {
                if code != 0 {
                    return Err(SyncError::Refused(format!(
                        "status {code}: {}",
                        String::from_utf8_lossy(&stderr)
                    )));
                }
                return Ok(());
            }
            _ => {
                return Err(SyncError::Transport(std::io::Error::other(
                    "unexpected frame in a ws channel",
                )));
            }
        }
    }
}

/// `WsPush`: replace the guest's `/workspace` with `tar`, over an opened
/// `WsPush` channel.
pub async fn push(mut stream: impl Duplex, tar: &[u8]) -> Result<(), SyncError> {
    for chunk in tar.chunks(CHUNK) {
        vsock::send_frame(&mut stream, &Frame::encode_data(chunk)).await?;
    }
    vsock::send_frame(&mut stream, &Frame::Eof.encode()).await?;
    read_to_status(&mut stream, None).await
}

/// `WsPull`: pack the guest's `/workspace` and return the tar, over an opened
/// `WsPull` channel.
pub async fn pull(mut stream: impl Duplex) -> Result<Vec<u8>, SyncError> {
    let mut tar = Vec::new();
    read_to_status(&mut stream, Some(&mut tar)).await?;
    Ok(tar)
}

/// `Sync`: flush the guest's page cache (machine §2.2), so the pause that
/// follows a pull sees a filesystem-clean image.
pub async fn sync(mut stream: impl Duplex) -> Result<(), SyncError> {
    read_to_status(&mut stream, None).await
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::io::Write;
    use std::path::Path;

    use machine_proto::ChannelKind;
    use tokio::net::UnixStream;

    use super::*;

    /// Open a channel the way a mechanism would, against the fake socket below:
    /// connect, muxer handshake, header. The real bindings get their stream from
    /// `machine_host::Guest::connect`, which does the first two.
    async fn open_vsock(uds: &Path, port: u32, kind: &ChannelKind) -> std::io::Result<UnixStream> {
        let mut stream = vsock::connect(uds, port).await?;
        vsock::send_frame(&mut stream, &kind.header()).await?;
        Ok(stream)
    }

    /// A fake guest agent on a std thread: the muxer handshake, one header,
    /// then the ws grammar of [`machine_proto`].
    fn spawn_fake(
        behavior: fn(&mut std::os::unix::net::UnixStream, &ChannelKind),
    ) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().expect("sock dir");
        let sock = dir.path().join("v.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).expect("bind");
        // Test fixture only: the fake stands in for a whole VM.
        #[allow(clippy::disallowed_methods)]
        std::thread::spawn(move || {
            while let Ok((mut stream, _)) = listener.accept() {
                // Handshake.
                let mut line = Vec::new();
                loop {
                    let mut byte = [0u8; 1];
                    if stream.read_exact(&mut byte).is_err() {
                        return;
                    }
                    if byte[0] == b'\n' {
                        break;
                    }
                    line.push(byte[0]);
                }
                stream.write_all(b"OK 1024\n").expect("ok");
                // Header frame.
                let header =
                    machine_proto::recv_frame(&mut stream, MAX_FRAME).expect("header frame");
                let kind = ChannelKind::parse(&header).expect("header json");
                behavior(&mut stream, &kind);
            }
        });
        (dir, sock)
    }

    fn write_frame(stream: &mut std::os::unix::net::UnixStream, frame: &Frame) {
        machine_proto::send_frame(stream, &frame.encode()).expect("send frame");
    }

    /// The wire cap, the codec budget, and the ws facet's durable-tree cap
    /// agree only by convention across crates; a drift would make a
    /// facet-legal workspace fail at the sync boundary, and the capture path
    /// degrades silently. This is the one crate that sees all three.
    #[test]
    fn the_sync_caps_agree_across_the_crates() {
        assert_eq!(MAX_TAR as u64, granary::MAX_TREE_BYTES);
        assert_eq!(MAX_TAR, machine_proto::MAX_TAR);
    }

    #[tokio::test]
    async fn push_streams_chunks_and_reads_the_status() {
        let (_dir, sock) = spawn_fake(|stream, kind| {
            assert_eq!(*kind, ChannelKind::WsPush);
            // Drain Data to Eof, then answer 0.
            let mut got = Vec::new();
            loop {
                let body = machine_proto::recv_frame(stream, MAX_FRAME).expect("frame");
                match Frame::decode(&body) {
                    Some(Frame::Data(bytes)) => got.extend_from_slice(&bytes),
                    Some(Frame::Eof) => break,
                    other => panic!("unexpected {other:?}"),
                }
            }
            assert_eq!(got.len(), 100_000, "chunks reassemble");
            write_frame(stream, &Frame::ExitStatus(0));
        });
        let channel = open_vsock(&sock, 62, &ChannelKind::WsPush)
            .await
            .expect("open");
        push(channel, &vec![7u8; 100_000]).await.expect("push");
    }

    #[tokio::test]
    async fn pull_reassembles_the_stream() {
        let (_dir, sock) = spawn_fake(|stream, kind| {
            assert_eq!(*kind, ChannelKind::WsPull);
            for chunk in vec![9u8; 70_000].chunks(CHUNK) {
                write_frame(stream, &Frame::Data(chunk.to_vec()));
            }
            write_frame(stream, &Frame::Eof);
            write_frame(stream, &Frame::ExitStatus(0));
        });
        let channel = open_vsock(&sock, 62, &ChannelKind::WsPull)
            .await
            .expect("open");
        let tar = pull(channel).await.expect("pull");
        assert_eq!(tar, vec![9u8; 70_000]);
    }

    #[tokio::test]
    async fn a_guest_error_carries_its_stderr() {
        let (_dir, sock) = spawn_fake(|stream, _| {
            write_frame(stream, &Frame::Stderr(b"tmpfs full".to_vec()));
            write_frame(stream, &Frame::ExitStatus(1));
        });
        let channel = open_vsock(&sock, 62, &ChannelKind::WsPull)
            .await
            .expect("open");
        let err = pull(channel).await.expect_err("must fail");
        assert!(err.to_string().contains("tmpfs full"), "{err}");
    }

    #[tokio::test]
    async fn an_over_cap_pull_is_refused_before_allocation() {
        let (_dir, sock) = spawn_fake(|stream, _| {
            // Stream legal frames past MAX_TAR; never reach a status.
            let chunk = vec![0u8; CHUNK];
            loop {
                let body = Frame::Data(chunk.clone()).encode();
                if machine_proto::send_frame(stream, &body).is_err() {
                    return; // host hung up at the cap
                }
            }
        });
        let channel = open_vsock(&sock, 62, &ChannelKind::WsPull)
            .await
            .expect("open");
        let err = pull(channel).await.expect_err("must refuse");
        assert!(err.to_string().contains("cap"), "{err}");
    }
}
