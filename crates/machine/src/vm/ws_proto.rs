//! The workspace-sync channels, host side (machine spec §4): `WsPush` and
//! `WsPull` over the guest agent's vsock protocol. A third mirror-by-
//! convention peer of `guest/machine-agent/src/proto.rs` and the front door's
//! `machine-frontdoor/src/proto.rs` — the three MUST agree byte for byte.
//! This module carries only the subset a sync needs: the JSON header, the
//! `Data`/`Stderr`/`Eof`/`ExitStatus` tags, and the framing the shared
//! [`microvm::vsock`] transport provides.
//!
//! One connection per op. A push sends the header, `Data` chunks, `Eof`, and
//! reads a status; a pull sends the header and reads `Data` chunks to `Eof`
//! then a status, the accumulation capped at [`MAX_TAR`] before any byte
//! sizes a host allocation (sandbox spec §3.2's stance).

use std::path::Path;

use microvm::vsock;
use microvm::ws_sync::MAX_TAR;
use serde_json::json;
use tokio::net::UnixStream;

/// The frame-tag subset of the channel grammar (mirror, module docs).
const DATA: u8 = 0;
const STDERR: u8 = 1;
const EOF: u8 = 4;
const EXIT_STATUS: u8 = 5;

/// Cap on any single frame payload (mirrors the agent's 1 MiB).
const MAX_FRAME: usize = 1024 * 1024;

/// Data chunks stay well under [`MAX_FRAME`] while keeping the frame count
/// (and its per-frame syscalls) low for a full-size stream.
const CHUNK: usize = 256 * 1024;

/// Open one channel: connect, muxer handshake, send the JSON header.
async fn open(uds: &Path, port: u32, header: &str) -> std::io::Result<UnixStream> {
    let mut stream = vsock::connect(uds, port).await?;
    vsock::send_json(&mut stream, &json!(header)).await?;
    Ok(stream)
}

/// Read frames until a terminal `ExitStatus`, folding `Data` into `tar`
/// (capped) and `Stderr` into the error message.
async fn read_to_status(
    stream: &mut UnixStream,
    mut tar: Option<&mut Vec<u8>>,
) -> std::io::Result<()> {
    let mut stderr = Vec::new();
    loop {
        let body = vsock::recv_frame(stream, MAX_FRAME).await?;
        match body.split_first() {
            Some((&DATA, rest)) => {
                if let Some(tar) = tar.as_deref_mut() {
                    if tar.len() + rest.len() > MAX_TAR {
                        return Err(std::io::Error::other(format!(
                            "pulled workspace exceeds the {MAX_TAR}-byte sync cap"
                        )));
                    }
                    tar.extend_from_slice(rest);
                }
            }
            Some((&STDERR, rest)) => stderr.extend_from_slice(rest),
            Some((&EOF, _)) => {}
            Some((&EXIT_STATUS, rest)) => {
                let code = i32::from_le_bytes(
                    rest.get(0..4)
                        .and_then(|b| b.try_into().ok())
                        .ok_or_else(|| std::io::Error::other("malformed exit status"))?,
                );
                if code != 0 {
                    return Err(std::io::Error::other(format!(
                        "guest agent status {code}: {}",
                        String::from_utf8_lossy(&stderr)
                    )));
                }
                return Ok(());
            }
            _ => return Err(std::io::Error::other("unexpected frame in a ws channel")),
        }
    }
}

/// `WsPush`: replace the guest's `/workspace` with `tar`.
pub async fn push(uds: &Path, port: u32, tar: &[u8]) -> std::io::Result<()> {
    let mut stream = open(uds, port, "WsPush").await?;
    for chunk in tar.chunks(CHUNK) {
        let mut frame = Vec::with_capacity(1 + chunk.len());
        frame.push(DATA);
        frame.extend_from_slice(chunk);
        vsock::send_frame(&mut stream, &frame).await?;
    }
    vsock::send_frame(&mut stream, &[EOF]).await?;
    read_to_status(&mut stream, None).await
}

/// `WsPull`: pack the guest's `/workspace` and return the tar.
pub async fn pull(uds: &Path, port: u32) -> std::io::Result<Vec<u8>> {
    let mut stream = open(uds, port, "WsPull").await?;
    let mut tar = Vec::new();
    read_to_status(&mut stream, Some(&mut tar)).await?;
    Ok(tar)
}

/// `Sync`: flush the guest's page cache (machine §2.2), so the pause that
/// follows a pull sees a filesystem-clean image.
pub async fn sync(uds: &Path, port: u32) -> std::io::Result<()> {
    let mut stream = open(uds, port, "Sync").await?;
    read_to_status(&mut stream, None).await
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::io::Write;

    use super::*;

    /// A fake guest agent on a std thread: the muxer handshake, one header,
    /// then the ws grammar — protocol-identical to `guest/machine-agent`.
    fn spawn_fake(
        behavior: fn(&mut std::os::unix::net::UnixStream, &str),
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
                let header = read_frame(&mut stream);
                let kind: String = serde_json::from_slice(&header).expect("header json");
                behavior(&mut stream, &kind);
            }
        });
        (dir, sock)
    }

    fn read_frame(stream: &mut std::os::unix::net::UnixStream) -> Vec<u8> {
        let mut len = [0u8; 4];
        stream.read_exact(&mut len).expect("len");
        let mut bytes = vec![0u8; u32::from_le_bytes(len) as usize];
        stream.read_exact(&mut bytes).expect("body");
        bytes
    }

    fn write_frame(stream: &mut std::os::unix::net::UnixStream, body: &[u8]) {
        stream
            .write_all(&(body.len() as u32).to_le_bytes())
            .expect("len");
        stream.write_all(body).expect("body");
    }

    fn status_frame(code: i32) -> Vec<u8> {
        let mut body = vec![EXIT_STATUS];
        body.extend_from_slice(&code.to_le_bytes());
        body
    }

    /// The transport's stream cap and the ws facet's durable-tree cap agree
    /// only by convention across crates; a drift would make a facet-legal
    /// workspace fail at the sync boundary — and the capture path degrades
    /// silently, far from the constant that caused it.
    #[test]
    fn the_sync_cap_matches_the_facet_cap() {
        assert_eq!(MAX_TAR as u64, granary::MAX_TREE_BYTES);
    }

    #[tokio::test]
    async fn push_streams_chunks_and_reads_the_status() {
        let (_dir, sock) = spawn_fake(|stream, kind| {
            assert_eq!(kind, "WsPush");
            // Drain Data to Eof, then answer 0.
            let mut got = Vec::new();
            loop {
                let body = read_frame(stream);
                match body.split_first() {
                    Some((&DATA, rest)) => got.extend_from_slice(rest),
                    Some((&EOF, _)) => break,
                    other => panic!("unexpected {other:?}"),
                }
            }
            assert_eq!(got.len(), 100_000, "chunks reassemble");
            write_frame(stream, &status_frame(0));
        });
        push(&sock, 62, &vec![7u8; 100_000]).await.expect("push");
    }

    #[tokio::test]
    async fn pull_reassembles_the_stream() {
        let (_dir, sock) = spawn_fake(|stream, kind| {
            assert_eq!(kind, "WsPull");
            for chunk in vec![9u8; 70_000].chunks(CHUNK) {
                let mut body = vec![DATA];
                body.extend_from_slice(chunk);
                write_frame(stream, &body);
            }
            write_frame(stream, &[EOF]);
            write_frame(stream, &status_frame(0));
        });
        let tar = pull(&sock, 62).await.expect("pull");
        assert_eq!(tar, vec![9u8; 70_000]);
    }

    #[tokio::test]
    async fn a_guest_error_carries_its_stderr() {
        let (_dir, sock) = spawn_fake(|stream, _| {
            let mut body = vec![STDERR];
            body.extend_from_slice(b"tmpfs full");
            write_frame(stream, &body);
            write_frame(stream, &status_frame(1));
        });
        let err = pull(&sock, 62).await.expect_err("must fail");
        assert!(err.to_string().contains("tmpfs full"), "{err}");
    }

    #[tokio::test]
    async fn an_over_cap_pull_is_refused_before_allocation() {
        let (_dir, sock) = spawn_fake(|stream, _| {
            // Stream legal frames past MAX_TAR; never reach a status.
            let chunk = vec![0u8; CHUNK];
            loop {
                let mut body = vec![DATA];
                body.extend_from_slice(&chunk);
                let write = stream
                    .write_all(&(body.len() as u32).to_le_bytes())
                    .and_then(|()| stream.write_all(&body));
                if write.is_err() {
                    return; // host hung up at the cap
                }
            }
        });
        let err = pull(&sock, 62).await.expect_err("must refuse");
        assert!(err.to_string().contains("cap"), "{err}");
    }
}
