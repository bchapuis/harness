//! The guest-agent wire protocol, front-door side (machine spec §5.1). Mirrors
//! `guest/machine-agent/src/proto.rs` — the two MUST agree byte for byte.
//!
//! One [`ChannelBackend`](crate::ChannelBackend) byte stream carries one SSH
//! channel: a JSON **header** frame naming the kind, then tagged **frames**
//! (`u32` little-endian length, one tag byte, payload).

use serde_json::json;

/// The vsock port the machine's guest agent listens on (machine §5.1).
pub const AGENT_PORT: u32 = 62;

/// Cap on any single frame payload (mirrors the agent's 1 MiB, sandbox §3.2).
pub const MAX_FRAME: usize = 1024 * 1024;

/// What one channel does — the header frame, sent once before any data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ChannelKind {
    Pty {
        term: String,
        cols: u16,
        rows: u16,
        argv: Vec<String>,
    },
    Exec {
        argv: Vec<String>,
        env: Vec<(String, String)>,
    },
    Sftp,
    Sync,
    /// Replace the guest's `/workspace` with a host-sent tar stream:
    /// `Data` chunks then `Eof`; the agent replies `ExitStatus` (machine §4).
    WsPush,
    /// Pack the guest's `/workspace` back: the agent sends `Data` chunks,
    /// `Eof`, then `ExitStatus` (machine §4).
    WsPull,
}

impl ChannelKind {
    /// The JSON header bytes the agent decodes into its own `ChannelKind`.
    pub fn header_json(&self) -> Vec<u8> {
        let value = match self {
            ChannelKind::Pty {
                term,
                cols,
                rows,
                argv,
            } => json!({"Pty": {"term": term, "cols": cols, "rows": rows, "argv": argv}}),
            ChannelKind::Exec { argv, env } => json!({"Exec": {"argv": argv, "env": env}}),
            ChannelKind::Sftp => json!("Sftp"),
            ChannelKind::Sync => json!("Sync"),
            ChannelKind::WsPush => json!("WsPush"),
            ChannelKind::WsPull => json!("WsPull"),
        };
        serde_json::to_vec(&value).expect("header encodes")
    }
}

/// One framed message after the header. The tag byte discriminates; a tag's
/// meaning is direction-dependent (host→guest input vs guest→host output).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Data(Vec<u8>),
    Stderr(Vec<u8>),
    WindowChange { cols: u16, rows: u16 },
    Signal(String),
    Eof,
    ExitStatus(i32),
}

impl Frame {
    const DATA: u8 = 0;
    const STDERR: u8 = 1;
    const WINDOW_CHANGE: u8 = 2;
    const SIGNAL: u8 = 3;
    const EOF: u8 = 4;
    const EXIT_STATUS: u8 = 5;

    /// The tag byte plus payload (without the length prefix).
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        match self {
            Frame::Data(bytes) => {
                out.push(Frame::DATA);
                out.extend_from_slice(bytes);
            }
            Frame::Stderr(bytes) => {
                out.push(Frame::STDERR);
                out.extend_from_slice(bytes);
            }
            Frame::WindowChange { cols, rows } => {
                out.push(Frame::WINDOW_CHANGE);
                out.extend_from_slice(&cols.to_le_bytes());
                out.extend_from_slice(&rows.to_le_bytes());
            }
            Frame::Signal(name) => {
                out.push(Frame::SIGNAL);
                out.extend_from_slice(name.as_bytes());
            }
            Frame::Eof => out.push(Frame::EOF),
            Frame::ExitStatus(code) => {
                out.push(Frame::EXIT_STATUS);
                out.extend_from_slice(&code.to_le_bytes());
            }
        }
        out
    }

    /// Parse a tag-plus-payload body (length prefix already stripped).
    pub fn decode(body: &[u8]) -> Option<Frame> {
        let (&tag, rest) = body.split_first()?;
        match tag {
            Frame::DATA => Some(Frame::Data(rest.to_vec())),
            Frame::STDERR => Some(Frame::Stderr(rest.to_vec())),
            Frame::WINDOW_CHANGE => Some(Frame::WindowChange {
                cols: u16::from_le_bytes(rest.get(0..2)?.try_into().ok()?),
                rows: u16::from_le_bytes(rest.get(2..4)?.try_into().ok()?),
            }),
            Frame::SIGNAL => Some(Frame::Signal(String::from_utf8_lossy(rest).into_owned())),
            Frame::EOF => Some(Frame::Eof),
            Frame::EXIT_STATUS => Some(Frame::ExitStatus(i32::from_le_bytes(
                rest.get(0..4)?.try_into().ok()?,
            ))),
            _ => None,
        }
    }
}

/// Write one length-prefixed frame body.
pub async fn send_frame<W>(writer: &mut W, body: &[u8]) -> std::io::Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    writer.write_all(&(body.len() as u32).to_le_bytes()).await?;
    writer.write_all(body).await?;
    writer.flush().await
}

/// Read one length-prefixed frame body, capped before allocation.
pub async fn recv_frame<R>(reader: &mut R, cap: usize) -> std::io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    use tokio::io::AsyncReadExt;
    let len = reader.read_u32_le().await? as usize;
    if len > cap {
        return Err(std::io::Error::other(format!(
            "frame of {len} bytes exceeds the {cap}-byte cap"
        )));
    }
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip() {
        for frame in [
            Frame::Data(b"hi".to_vec()),
            Frame::Stderr(b"e".to_vec()),
            Frame::WindowChange {
                cols: 100,
                rows: 30,
            },
            Frame::Signal("INT".to_string()),
            Frame::Eof,
            Frame::ExitStatus(7),
        ] {
            assert_eq!(
                Frame::decode(&frame.encode()),
                Some(frame.clone()),
                "{frame:?}"
            );
        }
    }

    #[test]
    fn the_header_matches_the_agents_serde_shape() {
        // serde derives `{"Exec": {...}}` for a struct variant and `"Sftp"`
        // for a unit variant — the agent's `ChannelKind` must decode these.
        let exec = ChannelKind::Exec {
            argv: vec!["ls".into()],
            env: vec![],
        };
        let v: serde_json::Value = serde_json::from_slice(&exec.header_json()).unwrap();
        assert_eq!(v["Exec"]["argv"][0], "ls");
        let sftp: serde_json::Value =
            serde_json::from_slice(&ChannelKind::Sftp.header_json()).unwrap();
        assert_eq!(sftp, serde_json::json!("Sftp"));
        for (kind, name) in [
            (ChannelKind::WsPush, "WsPush"),
            (ChannelKind::WsPull, "WsPull"),
        ] {
            let v: serde_json::Value = serde_json::from_slice(&kind.header_json()).unwrap();
            assert_eq!(v, serde_json::json!(name));
        }
    }
}
