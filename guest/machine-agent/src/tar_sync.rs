//! The guest half of the workspace sync (machine spec §4): pack and unpack
//! `/workspace` as a tar stream, mirroring the host-side codec
//! (`crates/microvm/src/ws_sync.rs`) by convention, as `guest/fc-agent`
//! mirrors the sandbox's. Deterministic walk (sorted entries, zero mtimes);
//! symlinks archived as symlinks (following one would copy its target *into*
//! the stream); fifos, sockets, and devices skipped *before* any open, so one
//! `mkfifo` cannot wedge a pull. The host drops what it cannot represent
//! anyway (absolute paths, `..`, absolute symlink targets, suid bits).

use std::io::Read;
use std::io::Write;
use std::path::Path;

use crate::proto::Frame;
use crate::proto::MAX_FRAME;
use crate::proto::MAX_TAR;

/// Replace the workspace's contents with a pushed tar stream. Children only:
/// the directory itself is a tmpfs mount that must survive.
pub fn unpack(workspace: &Path, tar: &[u8]) -> std::io::Result<()> {
    for entry in std::fs::read_dir(workspace)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            std::fs::remove_dir_all(entry.path())?;
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    tar::Archive::new(tar).unpack(workspace)
}

/// Per-entry budget charge beyond file contents (header, name extensions,
/// padding), mirroring the host codec's `TAR_ENTRY_OVERHEAD`.
const TAR_ENTRY_OVERHEAD: usize = 1024;

/// Tar the workspace, budgeted against [`MAX_TAR`] *before* bytes accumulate
/// — the host codec's discipline, mirrored: an over-cap workspace refuses
/// mid-walk rather than after materializing the whole stream.
pub fn pack(workspace: &Path) -> std::io::Result<Vec<u8>> {
    let mut builder = tar::Builder::new(Vec::new());
    let mut budget = MAX_TAR;
    append_dir(&mut builder, workspace, Path::new(""), &mut budget)?;
    builder.into_inner()
}

fn charge(budget: &mut usize, cost: usize) -> std::io::Result<()> {
    *budget = budget.checked_sub(cost).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("workspace exceeds the {MAX_TAR}-byte sync cap"),
        )
    })?;
    Ok(())
}

fn append_dir(
    builder: &mut tar::Builder<Vec<u8>>,
    dir: &Path,
    prefix: &Path,
    budget: &mut usize,
) -> std::io::Result<()> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = prefix.join(entry.file_name());
        let full = entry.path();
        // symlink_metadata: classify without following — or opening.
        let meta = std::fs::symlink_metadata(&full)?;
        let kind = meta.file_type();
        charge(budget, TAR_ENTRY_OVERHEAD)?;
        if kind.is_dir() {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            header.set_mtime(0);
            builder.append_data(&mut header, &path, std::io::empty())?;
            append_dir(builder, &full, &path, budget)?;
        } else if kind.is_symlink() {
            let target = std::fs::read_link(&full)?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_mode(0o777);
            header.set_size(0);
            header.set_mtime(0);
            builder.append_link(&mut header, &path, &target)?;
        } else if kind.is_file() {
            use std::os::unix::fs::PermissionsExt;
            charge(budget, meta.len() as usize)?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Regular);
            header.set_mode(meta.permissions().mode() & 0o777);
            header.set_size(meta.len());
            header.set_mtime(0);
            builder.append_data(&mut header, &path, std::fs::File::open(&full)?)?;
        }
    }
    Ok(())
}

/// Data chunks stay well under [`MAX_FRAME`] while keeping the frame count
/// (and its per-frame syscalls) low; the host side uses the same size.
const CHUNK: usize = 256 * 1024;

/// Receive a tar as `Data` chunks terminated by `Eof`, accumulated under
/// [`MAX_TAR`] before any of it is unpacked. Tags are matched on the raw
/// body — no intermediate [`Frame`] allocation per chunk.
pub fn recv_tar(stream: &mut impl Read) -> std::io::Result<Vec<u8>> {
    let mut tar = Vec::new();
    loop {
        let body = crate::recv_frame(stream, MAX_FRAME)?;
        match body.split_first() {
            Some((&Frame::DATA, rest)) => {
                if tar.len() + rest.len() > MAX_TAR {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("pushed workspace exceeds the {MAX_TAR}-byte sync cap"),
                    ));
                }
                tar.extend_from_slice(rest);
            }
            Some((&Frame::EOF, _)) => return Ok(tar),
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "unexpected frame in a workspace stream",
                ));
            }
        }
    }
}

/// Send a tar as `Data` chunks terminated by `Eof`. Frame bodies are built
/// in place — one copy per chunk, no intermediate [`Frame`].
pub fn send_tar(stream: &mut impl Write, tar: &[u8]) -> std::io::Result<()> {
    for chunk in tar.chunks(CHUNK) {
        let mut body = Vec::with_capacity(1 + chunk.len());
        body.push(Frame::DATA);
        body.extend_from_slice(chunk);
        crate::send_frame(stream, &body)?;
    }
    crate::send_frame(stream, &[Frame::EOF])
}
