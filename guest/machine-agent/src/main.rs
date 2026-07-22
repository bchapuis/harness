//! The persistent machine's guest agent (machine spec §5.1): a channel broker
//! inside the microVM, serving the wire protocol in [`machine_proto`] — the
//! same crate the front door compiles, so the two ends cannot drift.
//!
//! Unlike harness-sandbox's `fc-agent`, this is **not** pid 1: it is an
//! ordinary service the *user's own rootfs* ships (machine §2.1), started by
//! the rootfs's init. So it mounts nothing and assumes a booted system. It
//! listens on vsock port 62 and serves each connection — one SSH channel — on
//! its own thread, so a wedged PTY never blocks the next channel.
//!
//! vsock is reachable only from the host (machine §5.1): possession of the
//! bridged stream *is* the host's authority, so the agent performs no
//! authentication of its own. A user who removes or breaks the agent severs
//! only their own front-door access; the machine keeps running and capturing,
//! and recovery is restore-from-checkpoint (machine §8).
//!
//! `--uds <path>` listens on a unix socket and speaks the `CONNECT 62\n` →
//! `OK <port>\n` muxer handshake Firecracker performs host-side, so the relay
//! is exercised over an identical byte stream in tests without a VM.

use machine_proto as proto;

mod tar_sync;

use std::io::Read;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
#[cfg(target_os = "linux")]
use std::os::fd::OwnedFd;
use std::os::fd::RawFd;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Mutex;

use proto::AGENT_PORT as PORT;
use proto::ChannelKind;
use proto::Frame;
use proto::MAX_FRAME;
use proto::recv_frame;
use proto::send_frame;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let flag = |name: &str| {
        args.iter()
            .position(|a| a == name)
            .and_then(|i| args.get(i + 1))
            .cloned()
    };
    let workspace = PathBuf::from(flag("--workspace").unwrap_or_else(|| "/workspace".to_string()));
    match flag("--uds") {
        Some(path) => serve_uds(&path, workspace),
        None => serve_vsock(workspace),
    }
}

/// Production listener: vsock port 62, one thread per connection.
#[cfg(target_os = "linux")]
fn serve_vsock(workspace: PathBuf) -> ! {
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
        assert_eq!(libc::listen(fd, 16), 0, "vsock listen");
        loop {
            let conn = libc::accept(fd, std::ptr::null_mut(), std::ptr::null_mut());
            if conn < 0 {
                continue;
            }
            let stream = std::fs::File::from_raw_fd(conn);
            let workspace = workspace.clone();
            std::thread::spawn(move || {
                let _ = serve(stream, &workspace);
            });
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn serve_vsock(_workspace: PathBuf) -> ! {
    eprintln!("vsock is linux-only; use --uds <path>");
    std::process::exit(1);
}

/// Test listener: a unix socket, with the muxer's line handshake faked so the
/// relay speaks identical bytes (module docs).
fn serve_uds(path: &str, workspace: PathBuf) -> ! {
    let _ = std::fs::remove_file(path);
    let listener = std::os::unix::net::UnixListener::bind(path).expect("bind uds");
    loop {
        let Ok((mut stream, _)) = listener.accept() else {
            continue;
        };
        let workspace = workspace.clone();
        std::thread::spawn(move || {
            if handshake(&mut stream).is_ok() {
                let _ = serve(stream, &workspace);
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

/// Serve one channel: read the header, dispatch by kind. `stream` is cloned so
/// a reader thread and the main writer share the fd.
fn serve(
    stream: impl Read + Write + AsRawFd + Send + 'static,
    workspace: &std::path::Path,
) -> std::io::Result<()> {
    let mut stream = stream;
    let header = recv_frame(&mut stream, MAX_FRAME)?;
    let kind: ChannelKind = serde_json::from_slice(&header)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    match kind {
        ChannelKind::Sync => {
            sync_all();
            send_frame(&mut stream, &Frame::ExitStatus(0).encode())
        }
        ChannelKind::WsPush => {
            // Receive fully (capped) before touching the directory, so an
            // aborted stream never half-clears the workspace.
            match tar_sync::recv_tar(&mut stream)
                .and_then(|tar| tar_sync::unpack(workspace, &tar))
            {
                Ok(()) => send_frame(&mut stream, &Frame::ExitStatus(0).encode()),
                Err(e) => {
                    send_frame(&mut stream, &Frame::Stderr(e.to_string().into_bytes()).encode())?;
                    send_frame(&mut stream, &Frame::ExitStatus(1).encode())
                }
            }
        }
        ChannelKind::WsPull => match tar_sync::pack(workspace) {
            Ok(tar) => {
                tar_sync::send_tar(&mut stream, &tar)?;
                send_frame(&mut stream, &Frame::ExitStatus(0).encode())
            }
            Err(e) => {
                send_frame(&mut stream, &Frame::Stderr(e.to_string().into_bytes()).encode())?;
                send_frame(&mut stream, &Frame::ExitStatus(1).encode())
            }
        },
        ChannelKind::Exec { argv, env } => serve_piped(stream, argv, env),
        ChannelKind::Sftp => serve_piped(stream, vec![sftp_server_path()], Vec::new()),
        ChannelKind::Pty {
            term,
            cols,
            rows,
            argv,
        } => serve_pty(stream, term, cols, rows, argv),
    }
}

/// `sync(2)`: flush the guest page cache before the host pauses for a capture
/// (machine §2.2).
fn sync_all() {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::sync();
    }
}

/// The rootfs's sftp-server. OpenSSH ships it here on most distributions; the
/// exec fails cleanly (relayed as an exit status) if the rootfs lacks it.
fn sftp_server_path() -> String {
    for path in ["/usr/lib/ssh/sftp-server", "/usr/libexec/sftp-server"] {
        if std::path::Path::new(path).exists() {
            return path.to_string();
        }
    }
    "/usr/lib/ssh/sftp-server".to_string()
}

/// A shared stream-writer so the reader thread (guest→host) and the input
/// handler cannot interleave a frame.
type SharedWriter<W> = Arc<Mutex<W>>;

fn write_frame<W: Write>(writer: &SharedWriter<W>, frame: &Frame) -> std::io::Result<()> {
    let mut w = writer.lock().unwrap_or_else(|e| e.into_inner());
    send_frame(&mut *w, &frame.encode())
}

/// Serve an `Exec` or `Sftp` channel: spawn the process with piped stdio,
/// pump stdout/stderr to the host as frames, feed host `Data` to stdin, and
/// send the exit status. `argv[0]` is the program.
fn serve_piped<S: Read + Write + AsRawFd + Send + 'static>(
    stream: S,
    argv: Vec<String>,
    env: Vec<(String, String)>,
) -> std::io::Result<()> {
    if argv.is_empty() {
        return send_frame(&mut { stream }, &Frame::ExitStatus(255).encode());
    }
    let mut command = Command::new(&argv[0]);
    command
        .args(&argv[1..])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Its own process group, so an SSH signal reaches a `sh -c` pipeline's
    // members, not just the shell.
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    for (k, v) in &env {
        command.env(k, v);
    }
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => {
            let mut stream = stream;
            return send_frame(&mut stream, &Frame::ExitStatus(127).encode());
        }
    };
    let reader = split_reader(&stream);
    let writer: SharedWriter<S> = Arc::new(Mutex::new(stream));

    let mut stdin = child.stdin.take();
    let mut stdout = child.stdout.take().expect("piped stdout");
    let mut stderr = child.stderr.take().expect("piped stderr");

    // stdout → host Data.
    let out_writer = Arc::clone(&writer);
    let out = std::thread::spawn(move || pump(&mut stdout, &out_writer, Frame::Data));
    // stderr → host Stderr.
    let err_writer = Arc::clone(&writer);
    let err = std::thread::spawn(move || pump(&mut stderr, &err_writer, Frame::Stderr));

    // host frames → stdin / signals, until Eof or the stream closes.
    let mut reader = reader;
    while let Ok(body) = recv_frame(&mut reader, MAX_FRAME) {
        match Frame::decode(&body) {
            Some(Frame::Data(bytes)) => {
                if let Some(stdin) = stdin.as_mut() {
                    if stdin.write_all(&bytes).is_err() {
                        break;
                    }
                }
            }
            Some(Frame::Eof) => {
                stdin.take();
            }
            Some(Frame::Signal(name)) => signal_group(child.id() as i32, &name),
            _ => {}
        }
    }
    let status = child.wait().map(exit_code).unwrap_or(255);
    let _ = out.join();
    let _ = err.join();
    write_frame(&writer, &Frame::ExitStatus(status))
}

/// Serve a `Pty` channel: `forkpty` a login shell, relay the master fd both
/// ways, apply window changes, and send the exit status. A terminal merges
/// stdout and stderr, so no `Stderr` frames are produced.
#[cfg(target_os = "linux")]
fn serve_pty<S: Read + Write + AsRawFd + Send + 'static>(
    stream: S,
    term: String,
    cols: u16,
    rows: u16,
    argv: Vec<String>,
) -> std::io::Result<()> {
    let (master, pid) = match unsafe { fork_pty(cols, rows, &term, &argv) } {
        Ok(pair) => pair,
        Err(_) => {
            let mut stream = stream;
            return send_frame(&mut stream, &Frame::ExitStatus(255).encode());
        }
    };
    let master = Arc::new(master);
    let reader = split_reader(&stream);
    let writer: SharedWriter<S> = Arc::new(Mutex::new(stream));

    // master → host Data.
    let out_master = Arc::clone(&master);
    let out_writer = Arc::clone(&writer);
    let out = std::thread::spawn(move || {
        let mut file = unsafe { std::fs::File::from_raw_fd(dup_fd(out_master.as_raw_fd())) };
        pump(&mut file, &out_writer, Frame::Data);
    });

    // host frames → master / winsize / signal.
    let mut reader = reader;
    let mut master_write = unsafe { std::fs::File::from_raw_fd(dup_fd(master.as_raw_fd())) };
    while let Ok(body) = recv_frame(&mut reader, MAX_FRAME) {
        match Frame::decode(&body) {
            Some(Frame::Data(bytes)) => {
                if master_write.write_all(&bytes).is_err() {
                    break;
                }
            }
            Some(Frame::WindowChange { cols, rows }) => set_winsize(master.as_raw_fd(), cols, rows),
            Some(Frame::Signal(name)) => {
                // Signal the terminal's foreground job, as the line discipline
                // would for an interrupt character; the session leader's group
                // is the fallback when no job holds the terminal.
                let fg = unsafe { libc::tcgetpgrp(master.as_raw_fd()) };
                signal_group(if fg > 0 { fg } else { pid }, &name);
            }
            Some(Frame::Eof) => {}
            _ => {}
        }
    }
    let status = wait_pid(pid);
    let _ = out.join();
    write_frame(&writer, &Frame::ExitStatus(status))
}

#[cfg(not(target_os = "linux"))]
fn serve_pty<S: Read + Write + AsRawFd + Send + 'static>(
    stream: S,
    _term: String,
    _cols: u16,
    _rows: u16,
    _argv: Vec<String>,
) -> std::io::Result<()> {
    let mut stream = stream;
    send_frame(&mut stream, &Frame::ExitStatus(255).encode())
}

/// Copy a child stream to the host as `frame(bytes)` frames until EOF.
fn pump<R: Read, W: Write>(src: &mut R, writer: &SharedWriter<W>, frame: fn(Vec<u8>) -> Frame) {
    let mut buf = [0u8; 32 * 1024];
    loop {
        match src.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => {
                if write_frame(writer, &frame(buf[..n].to_vec())).is_err() {
                    break;
                }
            }
        }
    }
}

/// A second handle onto the connection for the reader thread. On Linux a
/// socket fd is dup'd; the two halves read and write the same connection.
fn split_reader<S: AsRawFd>(stream: &S) -> std::fs::File {
    unsafe { std::fs::File::from_raw_fd(dup_fd(stream.as_raw_fd())) }
}

fn dup_fd(fd: RawFd) -> RawFd {
    unsafe { libc::dup(fd) }
}

fn exit_code(status: std::process::ExitStatus) -> i32 {
    status.code().unwrap_or(255)
}

/// Deliver a named signal (the SSH `signal` request) to the process group led
/// by `pgid` — a piped child's own group, or the PTY's foreground job.
fn signal_group(pgid: i32, name: &str) {
    #[cfg(target_os = "linux")]
    {
        let sig = match name {
            "TERM" => libc::SIGTERM,
            "INT" => libc::SIGINT,
            "HUP" => libc::SIGHUP,
            "KILL" => libc::SIGKILL,
            "QUIT" => libc::SIGQUIT,
            _ => return,
        };
        unsafe {
            libc::kill(-pgid, sig);
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (pgid, name);
}

#[cfg(target_os = "linux")]
fn set_winsize(fd: RawFd, cols: u16, rows: u16) {
    let ws = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    unsafe {
        libc::ioctl(fd, libc::TIOCSWINSZ, &ws);
    }
}

/// `forkpty` a login shell (or `argv`), returning the master fd and child
/// pid. The child sets `$TERM` and execs; the parent keeps the master.
#[cfg(target_os = "linux")]
unsafe fn fork_pty(
    cols: u16,
    rows: u16,
    term: &str,
    argv: &[String],
) -> std::io::Result<(OwnedFd, libc::pid_t)> {
    let mut master: RawFd = -1;
    let ws = libc::winsize {
        ws_row: rows,
        ws_col: cols,
        ws_xpixel: 0,
        ws_ypixel: 0,
    };
    let pid = libc::forkpty(
        &mut master,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
        &ws as *const libc::winsize as *mut libc::winsize,
    );
    if pid < 0 {
        return Err(std::io::Error::last_os_error());
    }
    if pid == 0 {
        // Child: become the session's shell.
        std::env::set_var("TERM", term);
        let program = argv
            .first()
            .cloned()
            .unwrap_or_else(|| std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string()));
        let err = Command::new(&program).args(argv.iter().skip(1)).exec_replace();
        // exec failed; exit so the parent sees a status.
        eprintln!("machine-agent: exec {program}: {err}");
        std::process::exit(127);
    }
    Ok((OwnedFd::from_raw_fd(master), pid))
}

#[cfg(target_os = "linux")]
fn wait_pid(pid: libc::pid_t) -> i32 {
    let mut status = 0;
    unsafe {
        libc::waitpid(pid, &mut status, 0);
    }
    if libc::WIFEXITED(status) {
        libc::WEXITSTATUS(status)
    } else {
        255
    }
}

/// `Command::exec` without the std `CommandExt` import churn: replace this
/// process image, returning the error if it fails.
#[cfg(target_os = "linux")]
trait ExecReplace {
    fn exec_replace(&mut self) -> std::io::Error;
}

#[cfg(target_os = "linux")]
impl ExecReplace for Command {
    fn exec_replace(&mut self) -> std::io::Error {
        use std::os::unix::process::CommandExt;
        self.exec()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::io::Write;

    use super::*;

    /// Spawn the uds listener against a private workspace; return the socket
    /// path. The listener thread runs for the test process's lifetime.
    fn spawn_agent(workspace: &std::path::Path) -> PathBuf {
        let dir = tempfile::tempdir().expect("sock dir");
        let sock = dir.path().join("agent.sock");
        // Leak the tempdir so the socket path outlives this function.
        std::mem::forget(dir);
        let path = sock.to_str().expect("utf8 path").to_string();
        let ws = workspace.to_path_buf();
        std::thread::spawn(move || serve_uds(&path, ws));
        while !sock.exists() {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        sock
    }

    /// Connect and complete the muxer handshake, as the host does.
    fn connect(sock: &std::path::Path) -> std::os::unix::net::UnixStream {
        let mut stream = std::os::unix::net::UnixStream::connect(sock).expect("connect");
        stream
            .write_all(format!("CONNECT {PORT}\n").as_bytes())
            .expect("connect line");
        let mut byte = [0u8; 1];
        let mut line = Vec::new();
        loop {
            stream.read_exact(&mut byte).expect("ok line");
            if byte[0] == b'\n' {
                break;
            }
            line.push(byte[0]);
        }
        assert!(line.starts_with(b"OK "), "handshake reply");
        stream
    }

    fn send_header(stream: &mut impl Write, kind: &ChannelKind) {
        let json = serde_json::to_vec(kind).expect("header");
        send_frame(stream, &json).expect("send header");
    }

    fn read_status(stream: &mut impl Read) -> (i32, Vec<u8>) {
        let mut stderr = Vec::new();
        loop {
            let body = recv_frame(stream, MAX_FRAME).expect("frame");
            match Frame::decode(&body).expect("decode") {
                Frame::ExitStatus(code) => return (code, stderr),
                Frame::Stderr(bytes) => stderr.extend_from_slice(&bytes),
                other => panic!("unexpected frame {other:?}"),
            }
        }
    }

    #[test]
    fn ws_push_then_pull_round_trips() {
        let workspace = tempfile::tempdir().expect("workspace");
        let sock = spawn_agent(workspace.path());

        // Push a small tree.
        let src = tempfile::tempdir().expect("src");
        std::fs::create_dir(src.path().join("d")).expect("mkdir");
        std::fs::write(src.path().join("d/f.txt"), b"hello").expect("write");
        std::fs::write(src.path().join("top.txt"), b"tip").expect("write");
        let tar = tar_sync::pack(src.path()).expect("pack");

        let mut stream = connect(&sock);
        send_header(&mut stream, &ChannelKind::WsPush);
        tar_sync::send_tar(&mut stream, &tar).expect("send tar");
        let (code, stderr) = read_status(&mut stream);
        assert_eq!(code, 0, "{}", String::from_utf8_lossy(&stderr));
        assert_eq!(
            std::fs::read(workspace.path().join("d/f.txt")).expect("pushed file"),
            b"hello"
        );

        // Pull it back and unpack into a third directory.
        let mut stream = connect(&sock);
        send_header(&mut stream, &ChannelKind::WsPull);
        let tar = tar_sync::recv_tar(&mut stream).expect("recv tar");
        let (code, _) = read_status(&mut stream);
        assert_eq!(code, 0);
        let dst = tempfile::tempdir().expect("dst");
        tar_sync::unpack(dst.path(), &tar).expect("unpack");
        assert_eq!(std::fs::read(dst.path().join("d/f.txt")).expect("f"), b"hello");
        assert_eq!(std::fs::read(dst.path().join("top.txt")).expect("t"), b"tip");
    }

    #[test]
    fn a_push_replaces_the_workspace_children() {
        let workspace = tempfile::tempdir().expect("workspace");
        std::fs::write(workspace.path().join("stale.txt"), b"old").expect("seed");
        let sock = spawn_agent(workspace.path());

        let src = tempfile::tempdir().expect("src");
        std::fs::write(src.path().join("fresh.txt"), b"new").expect("write");
        let tar = tar_sync::pack(src.path()).expect("pack");

        let mut stream = connect(&sock);
        send_header(&mut stream, &ChannelKind::WsPush);
        tar_sync::send_tar(&mut stream, &tar).expect("send tar");
        let (code, _) = read_status(&mut stream);
        assert_eq!(code, 0);
        assert!(!workspace.path().join("stale.txt").exists());
        assert!(workspace.path().join("fresh.txt").exists());
        // The workspace directory itself survived (it stands in for the
        // tmpfs mount).
        assert!(workspace.path().exists());
    }

    #[test]
    fn an_oversized_push_is_refused_with_a_status() {
        let workspace = tempfile::tempdir().expect("workspace");
        let sock = spawn_agent(workspace.path());

        let mut stream = connect(&sock);
        send_header(&mut stream, &ChannelKind::WsPush);
        // Stream more Data than MAX_TAR admits, in legal-sized frames. The
        // agent may sever the stream mid-send (it refused at the cap), so
        // sends past that point are best-effort.
        let chunk = vec![0u8; 512 * 1024];
        for _ in 0..(proto::MAX_TAR / chunk.len() + 2) {
            let frame = Frame::Data(chunk.clone()).encode();
            if send_frame(&mut stream, &frame).is_err() {
                break;
            }
        }
        let _ = send_frame(&mut stream, &Frame::Eof.encode());
        // The load-bearing assertion: nothing was unpacked.
        assert_eq!(
            std::fs::read_dir(workspace.path()).expect("dir").count(),
            0,
            "an over-cap push must not touch the workspace"
        );
    }
}
