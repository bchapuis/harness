//! Protocol and sync tests that run without KVM, docker, or firecracker:
//! a fake guest serves the module's wire protocol over a plain unix socket —
//! including the muxer's `CONNECT` handshake, so the host-side client code
//! path is byte-identical to production — and executes `/bin/sh` against a
//! private "guest workspace" directory. The full push → exec → pull bracket
//! is therefore exercised end to end; only the VMM lifecycle itself needs
//! the KVM-gated suite (`tests/firecracker.rs`).

use std::io::Read;
use std::io::Write;
use std::path::PathBuf;

use cap_std::fs::Dir;
use serde_json::json;

use super::*;

/// A fake guest agent on a std thread: one connection at a time, blocking
/// IO, the exact frame protocol of the module docs.
struct FakeGuest {
    /// The guest's `/workspace` equivalent.
    workspace: tempfile::TempDir,
    /// Holds the socket directory alive.
    _sock_dir: tempfile::TempDir,
    sock: PathBuf,
}

impl FakeGuest {
    fn spawn() -> FakeGuest {
        let workspace = tempfile::tempdir().expect("guest workspace");
        let sock_dir = tempfile::tempdir().expect("sock dir");
        let sock = sock_dir.path().join("v.sock");
        let listener = std::os::unix::net::UnixListener::bind(&sock).expect("bind");
        let guest_dir = workspace.path().to_path_buf();
        // Test fixture only: the fake guest stands in for a whole VM, which
        // the §18.1 no-OS-threads discipline was never meant to cover.
        #[allow(clippy::disallowed_methods)]
        std::thread::spawn(move || {
            while let Ok((stream, _)) = listener.accept() {
                // A protocol error on one connection must not kill the fake.
                let _ = serve(stream, &guest_dir);
            }
        });
        FakeGuest {
            workspace,
            _sock_dir: sock_dir,
            sock,
        }
    }
}

fn serve(
    mut stream: std::os::unix::net::UnixStream,
    workspace: &std::path::Path,
) -> std::io::Result<()> {
    // The muxer handshake, guest-faked: read the CONNECT line, answer OK.
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
    }
    assert_eq!(
        String::from_utf8_lossy(&line),
        format!("CONNECT {VSOCK_PORT}"),
        "the host must request the agent's port"
    );
    stream.write_all(b"OK 1024\n")?;
    loop {
        let request: Value = match read_frame(&mut stream) {
            Ok(bytes) => serde_json::from_slice(&bytes).expect("json request"),
            Err(_) => return Ok(()), // EOF: connection done
        };
        match request["op"].as_str() {
            Some("ping") => write_frame(&mut stream, json!({"ok": true}).to_string().as_bytes())?,
            Some("push") => {
                let tar = read_frame(&mut stream)?;
                for entry in std::fs::read_dir(workspace)? {
                    let entry = entry?;
                    if entry.file_type()?.is_dir() {
                        std::fs::remove_dir_all(entry.path())?;
                    } else {
                        std::fs::remove_file(entry.path())?;
                    }
                }
                tar::Archive::new(&tar[..]).unpack(workspace)?;
                write_frame(&mut stream, json!({"ok": true}).to_string().as_bytes())?;
            }
            Some("exec") => {
                let command = request["command"].as_str().expect("command");
                match command {
                    // Test hooks the protocol alone can't reach: an agent
                    // error reply, and a frame whose header lies about size.
                    "@error" => write_frame(
                        &mut stream,
                        json!({"error": "refused for the test"})
                            .to_string()
                            .as_bytes(),
                    )?,
                    "@hugeframe" => {
                        stream.write_all(&u32::MAX.to_le_bytes())?;
                    }
                    _ => {
                        let output = std::process::Command::new("/bin/sh")
                            .args(["-c", command])
                            .current_dir(workspace)
                            .output()?;
                        write_frame(
                            &mut stream,
                            json!({
                                "exit_code": output.status.code(),
                                "stdout": String::from_utf8_lossy(&output.stdout),
                                "stderr": String::from_utf8_lossy(&output.stderr),
                            })
                            .to_string()
                            .as_bytes(),
                        )?;
                    }
                }
            }
            Some("pull") => {
                let mut builder = tar::Builder::new(Vec::new());
                // As the real agent: archive the symlink, never its target —
                // following one here would copy host files *into* the tar.
                builder.follow_symlinks(false);
                builder.append_dir_all("", workspace)?;
                let tar = builder.into_inner()?;
                write_frame(&mut stream, json!({"ok": true}).to_string().as_bytes())?;
                write_frame(&mut stream, &tar)?;
            }
            other => panic!("unexpected op {other:?}"),
        }
    }
}

fn read_frame(stream: &mut std::os::unix::net::UnixStream) -> std::io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len)?;
    let mut bytes = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn write_frame(stream: &mut std::os::unix::net::UnixStream, bytes: &[u8]) -> std::io::Result<()> {
    stream.write_all(&(bytes.len() as u32).to_le_bytes())?;
    stream.write_all(bytes)
}

fn workspace() -> (tempfile::TempDir, Dir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = Dir::open_ambient_dir(tmp.path(), cap_std::ambient_authority()).expect("open");
    (tmp, dir)
}

// ---------------------------------------------------------------------------
// The bracket, end to end against the fake guest
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_bracket_round_trips_the_workspace() {
    let guest = FakeGuest::spawn();
    let (tmp, dir) = workspace();
    dir.write("in.txt", b"forty-two").expect("seed");

    let outcome = exec_bracket(&dir, tmp.path(), &guest.sock, "cat in.txt > out.txt && echo done")
        .await
        .unwrap_or_else(|_| panic!("bracket failed"));
    assert_eq!(outcome["exit_code"], 0);
    assert_eq!(outcome["stdout"], "done\n");

    // Pull: the guest's write is in the host workspace, via the handle.
    let mut pulled = String::new();
    dir.open("out.txt")
        .expect("pulled file")
        .read_to_string(&mut pulled)
        .expect("read");
    assert_eq!(pulled, "forty-two");
    // Push: the seed file made it to the guest's workspace too.
    assert_eq!(
        std::fs::read_to_string(guest.workspace.path().join("in.txt")).expect("guest file"),
        "forty-two"
    );
}

#[tokio::test]
async fn nested_directories_the_exec_bit_and_relative_symlinks_survive() {
    let guest = FakeGuest::spawn();
    let (tmp, dir) = workspace();
    dir.create_dir_all("a/b").expect("dirs");
    dir.write("a/b/f.txt", b"deep").expect("seed");

    let outcome = exec_bracket(
        &dir,
        tmp.path(),
        &guest.sock,
        "printf '#!/bin/sh\\necho ran' > run.sh && chmod +x run.sh && \
         ln -s a/b/f.txt rel && ln -s /etc/passwd abs",
    )
    .await
    .unwrap_or_else(|_| panic!("bracket failed"));
    assert_eq!(outcome["exit_code"], 0);

    // The exec bit round-tripped: a second call runs the pulled script
    // (which the push carried back into the guest).
    let ran = exec_bracket(&dir, tmp.path(), &guest.sock, "./run.sh")
        .await
        .unwrap_or_else(|_| panic!("bracket failed"));
    assert_eq!(ran["exit_code"], 0, "exec bit must survive: {ran}");
    assert_eq!(ran["stdout"], "ran\n");

    // The relative symlink survives as a symlink; the absolute one is
    // dropped at the pull (module docs, S1).
    assert!(
        dir.symlink_metadata("rel")
            .expect("rel exists")
            .file_type()
            .is_symlink(),
        "rel must round-trip as a symlink, not a copy"
    );
    let mut linked = String::new();
    dir.open("rel")
        .expect("relative symlink resolves")
        .read_to_string(&mut linked)
        .expect("read");
    assert_eq!(linked, "deep");
    assert!(
        dir.symlink_metadata("abs").is_err(),
        "an absolute symlink target must not survive the pull"
    );
}

#[tokio::test]
async fn suid_bits_do_not_survive_the_pull() {
    let guest = FakeGuest::spawn();
    let (tmp, dir) = workspace();

    let outcome = exec_bracket(&dir, tmp.path(), &guest.sock, "touch s.bin && chmod 4755 s.bin")
        .await
        .unwrap_or_else(|_| panic!("bracket failed"));
    assert_eq!(outcome["exit_code"], 0);

    let mode = cap_std::fs::PermissionsExt::mode(
        &dir.symlink_metadata("s.bin").expect("pulled").permissions(),
    );
    assert_eq!(
        mode & 0o7000,
        0,
        "a guest must not mint suid/sgid/sticky host files, got {mode:o}"
    );
    assert_eq!(mode & 0o777, 0o755, "the rwx bits round-trip, got {mode:o}");
}

#[tokio::test]
async fn a_deleted_guest_file_stays_deleted_after_the_pull() {
    let guest = FakeGuest::spawn();
    let (tmp, dir) = workspace();
    dir.write("stale.txt", b"x").expect("seed");

    let outcome = exec_bracket(&dir, tmp.path(), &guest.sock, "rm stale.txt")
        .await
        .unwrap_or_else(|_| panic!("bracket failed"));
    assert_eq!(outcome["exit_code"], 0);
    assert!(
        dir.symlink_metadata("stale.txt").is_err(),
        "the pull replaces the workspace, it does not merge into it"
    );
}

// ---------------------------------------------------------------------------
// Error discrimination (module docs: agent error vs transport loss)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn an_agent_error_is_not_transport_loss() {
    let guest = FakeGuest::spawn();
    let (tmp, dir) = workspace();
    match exec_bracket(&dir, tmp.path(), &guest.sock, "@error").await {
        Err(BracketError::Agent(e)) => assert!(e.contains("refused"), "{e}"),
        other => panic!(
            "an error reply over a working transport is an agent error, got {:?}",
            other.map(|_| ())
        ),
    }
}

#[tokio::test]
async fn an_oversized_frame_header_is_refused_before_allocation() {
    let guest = FakeGuest::spawn();
    let (tmp, dir) = workspace();
    match exec_bracket(&dir, tmp.path(), &guest.sock, "@hugeframe").await {
        Err(BracketError::Transport(e)) => {
            assert!(e.contains("cap"), "the cap must be named: {e}")
        }
        other => panic!(
            "a frame over the cap is transport loss, got {:?}",
            other.map(|_| ())
        ),
    }
}

#[tokio::test]
async fn a_dead_socket_is_transport_loss() {
    let (tmp, dir) = workspace();
    let gone = tmp.path().join("no-such.sock");
    assert!(matches!(
        exec_bracket(&dir, tmp.path(), &gone, "true").await,
        Err(BracketError::Transport(_))
    ));
}

// The codec itself (pack determinism, budgeting, round trips) is tested where
// it lives: `microvm::ws_sync`. This suite covers the protocol around it.

#[test]
fn the_config_document_pins_the_shape_firecracker_boots_from() {
    let config = FirecrackerConfig::new("/usr/bin/firecracker", "/k/vmlinux");
    let document = microvm::config_json(
        &vm_config(&config, std::path::Path::new("/ctl/rootfs.ext4")),
        std::path::Path::new("/ctl"),
    );
    assert_eq!(document["boot-source"]["kernel_image_path"], "/k/vmlinux");
    assert!(
        document["boot-source"]["boot_args"]
            .as_str()
            .expect("boot args")
            .contains("init=/sbin/fc-agent"),
        "the agent must be pid 1"
    );
    assert_eq!(document["drives"][0]["path_on_host"], "/ctl/rootfs.ext4");
    assert_eq!(document["drives"][0]["is_root_device"], true);
    assert_eq!(document["machine-config"]["smt"], false);
    assert_eq!(document["vsock"]["guest_cid"], 3);
    assert_eq!(document["vsock"]["uds_path"], "/ctl/v.sock");
    assert!(
        document.get("network-interfaces").is_none(),
        "a sandboxed guest has no NIC by construction (sandbox spec §1.1)"
    );
}
