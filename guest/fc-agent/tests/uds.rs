//! The agent binary over `--uds`: the same protocol bytes a Firecracker
//! host-side connection would carry (the agent fakes the muxer's handshake
//! in this mode), against a real `/bin/sh`. Runs on any unix — this is the
//! test docker executes for the Linux build during `fc-rootfs/build.sh`.

use std::io::Read;
use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::Path;

use serde_json::json;
use serde_json::Value;

struct Agent {
    child: std::process::Child,
    _dir: tempfile::TempDir,
    sock: std::path::PathBuf,
    workspace: std::path::PathBuf,
}

impl Drop for Agent {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn() -> Agent {
    let dir = tempfile::tempdir().expect("tempdir");
    let sock = dir.path().join("v.sock");
    let workspace = dir.path().join("ws");
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_fc-agent"))
        .args(["--uds", sock.to_str().expect("sock path")])
        .args(["--workspace", workspace.to_str().expect("ws path")])
        .spawn()
        .expect("spawn agent");
    // Wait for the listener.
    for _ in 0..100 {
        if sock.exists() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Agent {
        child,
        _dir: dir,
        sock,
        workspace,
    }
}

fn connect(sock: &Path) -> UnixStream {
    let mut stream = UnixStream::connect(sock).expect("connect");
    stream.write_all(b"CONNECT 52\n").expect("connect line");
    let mut reply = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).expect("handshake byte");
        if byte[0] == b'\n' {
            break;
        }
        reply.push(byte[0]);
    }
    assert!(
        reply.starts_with(b"OK "),
        "handshake: {}",
        String::from_utf8_lossy(&reply)
    );
    stream
}

fn send_frame(stream: &mut UnixStream, bytes: &[u8]) {
    stream
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .expect("len");
    stream.write_all(bytes).expect("frame");
}

fn send_json(stream: &mut UnixStream, value: Value) {
    send_frame(stream, value.to_string().as_bytes());
}

fn recv_frame(stream: &mut UnixStream) -> Vec<u8> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).expect("len");
    let mut bytes = vec![0u8; u32::from_le_bytes(len) as usize];
    stream.read_exact(&mut bytes).expect("frame");
    bytes
}

fn recv_json(stream: &mut UnixStream) -> Value {
    serde_json::from_slice(&recv_frame(stream)).expect("json")
}

#[test]
fn ping_push_exec_pull_round_trip() {
    let agent = spawn();
    let mut stream = connect(&agent.sock);

    send_json(&mut stream, json!({"op": "ping"}));
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));

    // Push a workspace holding one file.
    let mut builder = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(5);
    header.set_mode(0o644);
    header.set_mtime(0);
    builder
        .append_data(&mut header, "in.txt", &b"hello"[..])
        .expect("append");
    let tar = builder.into_inner().expect("tar");
    send_json(&mut stream, json!({"op": "push"}));
    send_frame(&mut stream, &tar);
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));

    // The shell sees it, transforms it, and a stale survivor is gone after
    // the next push (push replaces, never merges).
    send_json(
        &mut stream,
        json!({"op": "exec", "command": "tr a-z A-Z < in.txt > out.txt && echo done"}),
    );
    let outcome = recv_json(&mut stream);
    assert_eq!(outcome["exit_code"], 0, "{outcome}");
    assert_eq!(outcome["stdout"], "done\n");

    send_json(&mut stream, json!({"op": "pull"}));
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));
    let pulled = recv_frame(&mut stream);
    let mut archive = tar::Archive::new(&pulled[..]);
    let mut contents = std::collections::BTreeMap::new();
    for entry in archive.entries().expect("entries") {
        let mut entry = entry.expect("entry");
        let path = entry.path().expect("path").display().to_string();
        let mut body = String::new();
        let _ = entry.read_to_string(&mut body);
        contents.insert(path, body);
    }
    assert_eq!(contents.get("out.txt").map(String::as_str), Some("HELLO"));

    // Push an empty workspace: everything is gone.
    send_json(&mut stream, json!({"op": "push"}));
    let empty = tar::Builder::new(Vec::new()).into_inner().expect("tar");
    send_frame(&mut stream, &empty);
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));
    assert!(
        !agent.workspace.join("out.txt").exists(),
        "push must replace the workspace"
    );
}

#[test]
fn errors_are_replies_and_the_connection_survives_them() {
    let agent = spawn();
    let mut stream = connect(&agent.sock);

    send_json(&mut stream, json!({"op": "no-such-op"}));
    let reply = recv_json(&mut stream);
    assert!(reply["error"].as_str().expect("error").contains("unknown"));

    send_json(&mut stream, json!({"op": "exec", "command": 42}));
    let reply = recv_json(&mut stream);
    assert!(reply["error"].as_str().expect("error").contains("string"));

    // Still serving after two errors.
    send_json(&mut stream, json!({"op": "ping"}));
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));

    // A nonzero exit is an outcome, not an error.
    send_json(&mut stream, json!({"op": "exec", "command": "exit 7"}));
    assert_eq!(recv_json(&mut stream)["exit_code"], 7);
}

#[test]
fn a_slow_exec_does_not_wedge_other_connections() {
    let agent = spawn();

    // Connection A starts a command that outlives the host's patience (the
    // harness would drop its client here); connection B must still be
    // served while A's command runs.
    let mut slow = connect(&agent.sock);
    send_json(&mut slow, json!({"op": "exec", "command": "sleep 5"}));

    let started = std::time::Instant::now();
    let mut quick = connect(&agent.sock);
    send_json(&mut quick, json!({"op": "ping"}));
    assert_eq!(recv_json(&mut quick), json!({"ok": true}));
    send_json(&mut quick, json!({"op": "exec", "command": "echo alive"}));
    let outcome = recv_json(&mut quick);
    assert_eq!(outcome["stdout"], "alive\n");
    assert!(
        started.elapsed() < std::time::Duration::from_secs(4),
        "a second connection must not wait out the first's command"
    );
}

#[test]
fn a_fifo_is_skipped_not_opened_by_the_pull() {
    let agent = spawn();
    let mut stream = connect(&agent.sock);

    let empty = tar::Builder::new(Vec::new()).into_inner().expect("tar");
    send_json(&mut stream, json!({"op": "push"}));
    send_frame(&mut stream, &empty);
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));

    // A fifo with no writer: opening it for read would block forever.
    send_json(
        &mut stream,
        json!({"op": "exec", "command": "mkfifo wedge && echo ok > beside.txt"}),
    );
    assert_eq!(recv_json(&mut stream)["exit_code"], 0);

    send_json(&mut stream, json!({"op": "pull"}));
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));
    let pulled = recv_frame(&mut stream);
    let names: Vec<String> = tar::Archive::new(&pulled[..])
        .entries()
        .expect("entries")
        .map(|e| e.expect("entry").path().expect("path").display().to_string())
        .collect();
    assert!(
        names.contains(&"beside.txt".to_string()),
        "the regular file beside the fifo is in the stream: {names:?}"
    );
    assert!(
        !names.contains(&"wedge".to_string()),
        "the fifo is skipped, never opened: {names:?}"
    );
}

#[test]
fn symlinks_are_archived_not_followed() {
    let agent = spawn();
    let mut stream = connect(&agent.sock);

    let empty = tar::Builder::new(Vec::new()).into_inner().expect("tar");
    send_json(&mut stream, json!({"op": "push"}));
    send_frame(&mut stream, &empty);
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));

    send_json(
        &mut stream,
        json!({"op": "exec", "command": "ln -s /etc/hosts leak"}),
    );
    assert_eq!(recv_json(&mut stream)["exit_code"], 0);

    send_json(&mut stream, json!({"op": "pull"}));
    assert_eq!(recv_json(&mut stream), json!({"ok": true}));
    let pulled = recv_frame(&mut stream);
    let mut archive = tar::Archive::new(&pulled[..]);
    let entry = archive
        .entries()
        .expect("entries")
        .map(|e| e.expect("entry"))
        .find(|e| e.path().expect("path").display().to_string() == "leak")
        .expect("the symlink is in the stream");
    assert_eq!(
        entry.header().entry_type(),
        tar::EntryType::Symlink,
        "the symlink itself, never its target's bytes"
    );
}
