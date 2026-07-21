//! The machine guest agent's wire protocol (machine spec §5.1), mirrored by
//! the front door's channel relay (`crates/machine-frontdoor/src/proto.rs`).
//! The two ends MUST agree byte for byte.
//!
//! One vsock stream carries one SSH channel. After the muxer handshake the
//! host sends a single **header** frame naming the channel kind, then the two
//! ends exchange **frames**: a `u32` little-endian length, then a one-byte
//! tag, then the payload. Data and control (window change, signal, exit
//! status) share the stream through the tag, so one channel's PTY resize
//! never blocks another's bytes.

use serde::Deserialize;
use serde::Serialize;

/// The vsock port the agent listens on. Distinct from the sandbox agent's 52
/// (machine spec §5.1): the protocols differ, and a machine image running the
/// wrong agent must not read as ready.
pub const PORT: u32 = 62;

/// Cap on any single frame payload, mirroring the sandbox transport's 1 MiB
/// (sandbox §3.2): a frame header's claim never becomes an unbounded guest
/// allocation.
pub const MAX_FRAME: usize = 1024 * 1024;

/// Cap on one workspace tar stream, either direction, accumulated across its
/// [`Frame::Data`] chunks (machine §4; the workspace facet's own 64 MiB cap).
pub const MAX_TAR: usize = 64 * 1024 * 1024;

/// What one channel does. The header frame, sent once by the host before any
/// data flows.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChannelKind {
    /// An interactive session on a pseudo-terminal. `argv` empty means the
    /// user's login shell.
    Pty {
        term: String,
        cols: u16,
        rows: u16,
        argv: Vec<String>,
    },
    /// A non-interactive command with piped stdio.
    Exec { argv: Vec<String>, env: Vec<(String, String)> },
    /// The SFTP subsystem: exec the rootfs's `sftp-server`, relay its stdio.
    Sftp,
    /// Flush the guest's page cache to its block device (`sync(2)`). The host
    /// issues it before a capture's `pause` so the quiescent image is
    /// filesystem-clean, not merely crash-consistent (machine §2.2, M3). No
    /// data follows; the agent replies with one [`Frame::ExitStatus`] `0`.
    Sync,
    /// Replace the guest's `/workspace` (a tmpfs, machine §3) with a tar
    /// stream the host sends: [`Frame::Data`] chunks then [`Frame::Eof`],
    /// accumulated under [`MAX_TAR`]. The agent clears the directory's
    /// children (the mount survives), unpacks, and replies
    /// [`Frame::ExitStatus`] `0` — or [`Frame::Stderr`] then `ExitStatus` 1.
    WsPush,
    /// Pack the guest's `/workspace` and send it to the host as
    /// [`Frame::Data`] chunks then [`Frame::Eof`] then [`Frame::ExitStatus`]
    /// `0`, the pack budgeted under [`MAX_TAR`] — or [`Frame::Stderr`] then
    /// `ExitStatus` 1 with no data.
    WsPull,
}

/// One framed message after the header. The tag byte discriminates; the
/// meaning of a tag is direction-dependent (host→guest input vs guest→host
/// output), the way an SSH channel's data and extended-data are distinct
/// streams over one channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    /// Channel data. Host→guest: stdin / PTY input. Guest→host: stdout / PTY
    /// output.
    Data(Vec<u8>),
    /// Guest→host: stderr (SSH extended data). Never sent for a PTY (a
    /// terminal merges the streams).
    Stderr(Vec<u8>),
    /// Host→guest: the terminal resized (`cols`, `rows`) — a PTY channel only.
    WindowChange { cols: u16, rows: u16 },
    /// Host→guest: deliver a signal to the process group by name (e.g.
    /// `"TERM"`, `"INT"`), the SSH `signal` request.
    Signal(String),
    /// Host→guest: no more input will follow (the peer closed its write half).
    Eof,
    /// Guest→host: the process exited with this code (or 255 if killed by a
    /// signal). Terminal for the channel.
    ExitStatus(i32),
}

impl Frame {
    pub(crate) const DATA: u8 = 0;
    const STDERR: u8 = 1;
    const WINDOW_CHANGE: u8 = 2;
    const SIGNAL: u8 = 3;
    pub(crate) const EOF: u8 = 4;
    const EXIT_STATUS: u8 = 5;

    /// The tag byte plus payload of this frame (without the length prefix).
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

    /// Parse a tag-plus-payload body (the length prefix already stripped).
    pub fn decode(body: &[u8]) -> Option<Frame> {
        let (&tag, rest) = body.split_first()?;
        match tag {
            Frame::DATA => Some(Frame::Data(rest.to_vec())),
            Frame::STDERR => Some(Frame::Stderr(rest.to_vec())),
            Frame::WINDOW_CHANGE => {
                let cols = u16::from_le_bytes(rest.get(0..2)?.try_into().ok()?);
                let rows = u16::from_le_bytes(rest.get(2..4)?.try_into().ok()?);
                Some(Frame::WindowChange { cols, rows })
            }
            Frame::SIGNAL => Some(Frame::Signal(String::from_utf8_lossy(rest).into_owned())),
            Frame::EOF => Some(Frame::Eof),
            Frame::EXIT_STATUS => {
                Some(Frame::ExitStatus(i32::from_le_bytes(rest.get(0..4)?.try_into().ok()?)))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frames_round_trip_through_encode_decode() {
        let cases = [
            Frame::Data(b"hello".to_vec()),
            Frame::Stderr(b"oops".to_vec()),
            Frame::WindowChange { cols: 120, rows: 40 },
            Frame::Signal("TERM".to_string()),
            Frame::Eof,
            Frame::ExitStatus(-1),
            Frame::ExitStatus(0),
        ];
        for frame in cases {
            assert_eq!(Frame::decode(&frame.encode()), Some(frame.clone()), "{frame:?}");
        }
    }

    #[test]
    fn an_unknown_tag_is_rejected() {
        assert_eq!(Frame::decode(&[99, 1, 2, 3]), None);
        assert_eq!(Frame::decode(&[]), None);
    }

    #[test]
    fn the_header_round_trips_as_json() {
        let header = ChannelKind::Pty {
            term: "xterm-256color".to_string(),
            cols: 80,
            rows: 24,
            argv: vec![],
        };
        let bytes = serde_json::to_vec(&header).expect("encode");
        let back: ChannelKind = serde_json::from_slice(&bytes).expect("decode");
        assert_eq!(back, header);
    }
}
