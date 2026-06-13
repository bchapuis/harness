//! The guest half of harness-sandbox's Firecracker `Native` tier: pid 1 in
//! the microVM, serving the wire protocol documented in
//! `crates/harness-sandbox/src/firecracker.rs` (the host side; the two files
//! must agree byte for byte on the protocol).
//!
//! As init it mounts `/proc`, `/dev`, and `/tmp`, then listens on vsock port
//! 52. Each connection serves sequential framed requests: `ping`, `push`
//! (replace `/workspace` with a tar stream), `exec` (`/bin/sh -c`, cwd
//! `/workspace`), `pull` (tar `/workspace` back). An op the agent cannot
//! honor answers `{"error":…}` over the working transport; transport
//! failures just drop the connection — the host treats those as single-tier
//! loss and re-provisions.
//!
//! Connections are served on threads, one each: the host enforces tool
//! timeouts by dropping its client, and a command that runs on regardless
//! must wedge only its own connection, never the next call's. The pull walks
//! the workspace manually — files, directories, and symlinks only — because
//! a blanket archive would `open` whatever it finds, and opening a fifo a
//! guest command left behind blocks forever.
//!
//! `--uds <path>` listens on a unix socket instead and *also* speaks the
//! `CONNECT`/`OK` line handshake Firecracker's muxer would perform, so the
//! host-side client exercises an identical byte stream in tests without a
//! VM. `--workspace <dir>` overrides `/workspace` for the same reason.

use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::RwLock;

use serde_json::json;
use serde_json::Value;

/// Coordinates `exec` with zombie reaping. As pid 1 the agent inherits every
/// orphan, but a bare `waitpid(-1)` from one thread could steal the exit
/// status `Command::output` is waiting on in another. `exec` holds the read
/// side across spawn-to-wait; the reaper runs only when it can take the
/// write side without blocking — a stuck command starves reaping (zombies
/// accumulate, bounded by the guest's own pid space), never the other way.
static EXEC_GATE: RwLock<()> = RwLock::new(());

/// The protocol constants, mirrored from the host module.
const PORT: u32 = 52;
const MAX_TAR: usize = 64 * 1024 * 1024;
const MAX_FRAME: usize = 1024 * 1024;
/// Guest-side cap per captured stream; the host re-caps for the journal.
const OUTPUT_CAP: usize = 256 * 1024;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let workspace = PathBuf::from(flag("--workspace").unwrap_or_else(|| "/workspace".into()));
    if std::process::id() == 1 {
        setup_as_init();
    }
    let _ = std::fs::create_dir_all(&workspace);
    match flag("--uds") {
        Some(path) => serve_uds(&path, &workspace),
        None => serve_vsock(&workspace),
    }
}

/// Mount the pseudo-filesystems a shell expects. Errors are ignored: a
/// missing mount degrades some commands, it does not take the agent down.
fn setup_as_init() {
    for dir in ["/proc", "/dev", "/tmp"] {
        let _ = std::fs::create_dir_all(dir);
    }
    mount("proc", "/proc", "proc");
    mount("devtmpfs", "/dev", "devtmpfs");
    mount("tmpfs", "/tmp", "tmpfs");
    std::env::set_var("PATH", "/usr/sbin:/usr/bin:/sbin:/bin");
    std::env::set_var("HOME", "/root");
}

#[cfg(target_os = "linux")]
fn mount(source: &str, target: &str, fstype: &str) {
    use std::ffi::CString;
    let source = CString::new(source).expect("source");
    let target = CString::new(target).expect("target");
    let fstype = CString::new(fstype).expect("fstype");
    unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            fstype.as_ptr(),
            0,
            std::ptr::null(),
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn mount(_source: &str, _target: &str, _fstype: &str) {}

/// Orphans reparent to pid 1 and zombify on exit; reap them between
/// requests. (`Command::output` waits for its own child, so blanket
/// `SIGCHLD` ignoring would break it — explicit reaping instead, and only
/// when no `exec` is mid-wait; see [`EXEC_GATE`].)
fn reap_zombies() {
    #[cfg(target_os = "linux")]
    {
        let Ok(_exclusive) = EXEC_GATE.try_write() else {
            return;
        };
        unsafe {
            let mut status = 0;
            while libc::waitpid(-1, &mut status, libc::WNOHANG) > 0 {}
        }
    }
}

/// Production listener: vsock port 52, raw streams (Firecracker's muxer
/// performs the host-side handshake; none of it reaches the guest).
#[cfg(target_os = "linux")]
fn serve_vsock(workspace: &Path) -> ! {
    use std::os::fd::FromRawFd;
    unsafe {
        let fd = libc::socket(libc::AF_VSOCK, libc::SOCK_STREAM, 0);
        assert!(fd >= 0, "vsock socket");
        let mut addr: libc::sockaddr_vm = std::mem::zeroed();
        addr.svm_family = libc::AF_VSOCK as libc::sa_family_t;
        addr.svm_port = PORT;
        addr.svm_cid = libc::VMADDR_CID_ANY;
        let bound = libc::bind(
            fd,
            &addr as *const libc::sockaddr_vm as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_vm>() as libc::socklen_t,
        );
        assert_eq!(bound, 0, "vsock bind port {PORT}");
        assert_eq!(libc::listen(fd, 8), 0, "vsock listen");
        loop {
            reap_zombies();
            let conn = libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut());
            if conn < 0 {
                continue;
            }
            // A socket fd reads and writes like any fd; File is the wrapper
            // that closes it on drop. One thread per connection (module
            // docs): a wedged command wedges only its own connection.
            let stream = std::fs::File::from_raw_fd(conn);
            let workspace = workspace.to_path_buf();
            std::thread::spawn(move || {
                let _ = serve(&stream, &workspace);
            });
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn serve_vsock(_workspace: &Path) -> ! {
    eprintln!("vsock is linux-only; use --uds <path>");
    std::process::exit(1);
}

/// Test listener: a unix socket, with the muxer's line handshake faked so
/// the host client speaks identical bytes (module docs).
fn serve_uds(path: &str, workspace: &Path) -> ! {
    let _ = std::fs::remove_file(path);
    let listener = std::os::unix::net::UnixListener::bind(path).expect("bind uds");
    loop {
        reap_zombies();
        let Ok((mut stream, _)) = listener.accept() else {
            continue;
        };
        let workspace = workspace.to_path_buf();
        std::thread::spawn(move || {
            if handshake(&mut stream).is_ok() {
                let _ = serve(&stream, &workspace);
            }
        });
    }
}

fn handshake(stream: &mut (impl Read + Write)) -> std::io::Result<()> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
        if line.len() > 64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "oversized CONNECT line",
            ));
        }
    }
    if line == format!("CONNECT {PORT}").as_bytes() {
        stream.write_all(b"OK 1024\n")
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad CONNECT line",
        ))
    }
}

/// Serve framed requests on one connection until EOF.
fn serve(mut stream: impl Read + Write, workspace: &Path) -> std::io::Result<()> {
    loop {
        reap_zombies();
        let request: Value = match recv_frame(&mut stream, MAX_FRAME) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?,
            Err(_) => return Ok(()), // EOF or framing failure: connection done
        };
        let reply = match request["op"].as_str() {
            Some("ping") => json!({"ok": true}),
            Some("push") => {
                let tar = recv_frame(&mut stream, MAX_TAR)?;
                match push(workspace, &tar) {
                    Ok(()) => json!({"ok": true}),
                    Err(e) => json!({"error": format!("push: {e}")}),
                }
            }
            Some("exec") => match request["command"].as_str() {
                Some(command) => exec(workspace, command),
                None => json!({"error": "exec: `command` must be a string"}),
            },
            Some("pull") => match pull(workspace) {
                Ok(tar) => {
                    send_json(&mut stream, &json!({"ok": true}))?;
                    send_frame(&mut stream, &tar)?;
                    continue;
                }
                Err(e) => json!({"error": format!("pull: {e}")}),
            },
            other => json!({"error": format!("unknown op {other:?}")}),
        };
        send_json(&mut stream, &reply)?;
    }
}

/// Replace the workspace's contents with the pushed tar stream.
fn push(workspace: &Path, tar: &[u8]) -> std::io::Result<()> {
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

/// Tar the workspace back: a manual walk over files, directories, and
/// symlinks, mirroring the host side. Symlinks are archived as symlinks —
/// following them would copy whatever they point at *into* the stream — and
/// everything else (fifos, sockets, devices) is skipped *before* any open:
/// opening a fifo with no writer blocks forever, and one `mkfifo` must not
/// wedge every later pull. The host drops what it cannot represent anyway.
fn pull(workspace: &Path) -> std::io::Result<Vec<u8>> {
    let mut builder = tar::Builder::new(Vec::new());
    append_dir(&mut builder, workspace, Path::new(""))?;
    let tar = builder.into_inner()?;
    if tar.len() > MAX_TAR {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("workspace exceeds the {MAX_TAR}-byte sync cap"),
        ));
    }
    Ok(tar)
}

fn append_dir(
    builder: &mut tar::Builder<Vec<u8>>,
    dir: &Path,
    prefix: &Path,
) -> std::io::Result<()> {
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let path = prefix.join(entry.file_name());
        let full = entry.path();
        // symlink_metadata: classify without following — or opening.
        let meta = std::fs::symlink_metadata(&full)?;
        let kind = meta.file_type();
        if kind.is_dir() {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            header.set_mtime(0);
            builder.append_data(&mut header, &path, std::io::empty())?;
            append_dir(builder, &full, &path)?;
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

fn exec(workspace: &Path, command: &str) -> Value {
    // Read-held across spawn-to-wait so the reaper cannot steal this child's
    // exit status (see EXEC_GATE).
    let _spawned = EXEC_GATE.read().unwrap_or_else(|e| e.into_inner());
    match Command::new("/bin/sh")
        .args(["-c", command])
        .current_dir(workspace)
        .output()
    {
        Ok(output) => json!({
            "exit_code": output.status.code(),
            "stdout": capped(&output.stdout),
            "stderr": capped(&output.stderr),
        }),
        Err(e) => json!({"error": format!("exec: {e}")}),
    }
}

fn capped(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    if text.len() <= OUTPUT_CAP {
        return text.into_owned();
    }
    let mut end = OUTPUT_CAP;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… [truncated {} bytes]", &text[..end], text.len() - end)
}

fn send_frame(stream: &mut impl Write, bytes: &[u8]) -> std::io::Result<()> {
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(bytes)?;
    stream.flush()
}

fn send_json(stream: &mut impl Write, value: &Value) -> std::io::Result<()> {
    send_frame(stream, value.to_string().as_bytes())
}

fn recv_frame(stream: &mut impl Read, cap: usize) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let len = u32::from_le_bytes(len) as usize;
    if len > cap {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame of {len} bytes exceeds the {cap}-byte cap"),
        ));
    }
    let mut bytes = vec![0u8; len];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}
