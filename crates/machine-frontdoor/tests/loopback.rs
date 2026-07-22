//! The front door end to end over a loopback: an in-process russh client
//! authenticates against a fake [`MachineAuthority`], and an exec channel
//! bridges to a fake guest agent through an in-memory [`ChannelBackend`]
//! (machine §5.1). Proves M4's auth decision (an authorized key attaches, an
//! unauthorized one is rejected) and the channel bridge (guest output and
//! exit status reach the client). No VM, no vsock, no SSH transport beyond a
//! `tokio::io::duplex` pair.

use std::sync::Arc;
use std::sync::Mutex;

use granary::GrainName;
use machine_frontdoor::ChannelBackend;
use machine_frontdoor::ChannelKind;
use machine_frontdoor::Duplex;
use machine_frontdoor::FrontDoorError;
use machine_frontdoor::MachineAuthority;
use machine_frontdoor::host_key_from_seed;
use machine_proto::Frame;
use microvm::vsock::send_frame;
use machine_frontdoor::serve_connection;
use russh::keys::PrivateKey;
use russh::keys::PrivateKeyWithHashAlg;
use russh::keys::PublicKey;

/// The authorized client key (its public half is the machine's one journaled
/// key); possession of this key attaches (M4).
const AUTHORIZED_KEY: &str = "\
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACBJM2WHRMLw0fKPGWP2mmVBRD3DTAcGK6jCtWL0yXqzaAAAAKAu8iAILvIg
CAAAAAtzc2gtZWQyNTUxOQAAACBJM2WHRMLw0fKPGWP2mmVBRD3DTAcGK6jCtWL0yXqzaA
AAAEAVrEEWPp1lhv6G3mtHwBtk0BlYyjvb7fRAfrxjWL6L4UkzZYdEwvDR8o8ZY/aaZUFE
PcNMBwYrqMK1YvTJerNoAAAAFm1hY2hpbmUtZnJvbnRkb29yLXRlc3QBAgMEBQYH
-----END OPENSSH PRIVATE KEY-----
";

/// The machine's journaled host-key material (machine §3): a raw 32-byte
/// ed25519 seed, expanded exactly as a real authority does.
const HOST_KEY_SEED: [u8; 32] = [7; 32];

/// A key the machine's policy does not authorize.
const UNAUTHORIZED_KEY: &str = "\
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACCTfV+36VnaeUenY9GYyqzbuxopHNufuQwx/lic05fqZAAAAJAZuAviGbgL
4gAAAAtzc2gtZWQyNTUxOQAAACCTfV+36VnaeUenY9GYyqzbuxopHNufuQwx/lic05fqZA
AAAEA9VtUelLgNyMIysLgL4Lvi83hTCyRjWcrPJELCgg9Q3pN9X7fpWdp5R6dj0ZjKrNu7
Gikc25+5DDH+WJzTl+pkAAAACXdyb25nLWtleQECAwQ=
-----END OPENSSH PRIVATE KEY-----
";

/// A fake authority: one journaled key, a fixed host key, an attachment
/// counter recording attach/detach so the test can assert the journal.
struct FakeAuthority {
    authorized: PublicKey,
    host_key: PrivateKey,
    attaches: Mutex<Vec<String>>,
    detaches: Mutex<Vec<u64>>,
}

impl MachineAuthority for FakeAuthority {
    async fn host_key(&self, _machine: &GrainName) -> Result<PrivateKey, FrontDoorError> {
        Ok(self.host_key.clone())
    }

    async fn authorizes(&self, _machine: &GrainName, key: &PublicKey) -> bool {
        // Compare key material, not the whole `PublicKey` — the wire key
        // carries no comment, so a `==` including the comment would never
        // match. A real authority compares fingerprints against the journaled
        // set (M4).
        key.key_data() == self.authorized.key_data()
    }

    async fn attach(&self, _machine: &GrainName, principal: &str) -> Result<u64, FrontDoorError> {
        let mut attaches = self.attaches.lock().unwrap();
        attaches.push(principal.to_string());
        Ok(attaches.len() as u64)
    }

    async fn detach(&self, _machine: &GrainName, attachment: u64) {
        self.detaches.lock().unwrap().push(attachment);
    }
}

/// A fake backend: each channel is an in-memory duplex to a fake agent that
/// emits deterministic output for the channel kind, standing in for the guest
/// over vsock.
struct FakeBackend;

impl ChannelBackend for FakeBackend {
    async fn open(
        &self,
        _machine: &GrainName,
        kind: ChannelKind,
    ) -> std::io::Result<Box<dyn Duplex>> {
        let (front, agent) = tokio::io::duplex(64 * 1024);
        tokio::spawn(fake_agent(agent, kind));
        Ok(Box::new(front))
    }
}

/// The fake guest agent: echo the command back as output, then exit 0.
async fn fake_agent(mut agent: tokio::io::DuplexStream, kind: ChannelKind) {
    let reply = match &kind {
        ChannelKind::Exec { argv, .. } | ChannelKind::Pty { argv, .. } => {
            format!("guest ran: {}\n", argv.join(" "))
        }
        ChannelKind::Sftp => "sftp\n".to_string(),
        // The front door never opens workspace-sync channels (they are the
        // machine grain's own, machine §4); an empty reply keeps the fake
        // total.
        ChannelKind::Sync | ChannelKind::WsPush | ChannelKind::WsPull => String::new(),
    };
    let _ = send_frame(&mut agent, &Frame::Data(reply.into_bytes()).encode()).await;
    let _ = send_frame(&mut agent, &Frame::ExitStatus(0).encode()).await;
}

fn authority() -> Arc<FakeAuthority> {
    let authorized = PrivateKey::from_openssh(AUTHORIZED_KEY)
        .expect("parse authorized key")
        .public_key()
        .clone();
    let host_key = host_key_from_seed(&HOST_KEY_SEED).expect("host key from seed");
    Arc::new(FakeAuthority {
        authorized,
        host_key,
        attaches: Mutex::new(Vec::new()),
        detaches: Mutex::new(Vec::new()),
    })
}

/// A client handler that pins the server's host key (proving the front door
/// presents the machine's journaled host key at KEX).
struct Client {
    server_key: Arc<Mutex<Option<PublicKey>>>,
}

impl russh::client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(&mut self, key: &PublicKey) -> Result<bool, Self::Error> {
        *self.server_key.lock().unwrap() = Some(key.clone());
        Ok(true)
    }
}

/// Drive one client session: connect over `client_side`, authenticate with
/// `key`, and (if authenticated) exec a command, returning the auth result,
/// the collected stdout, and the exit status.
async fn run_client(
    client_side: tokio::io::DuplexStream,
    key: PrivateKey,
) -> (bool, String, Option<u32>, Option<PublicKey>) {
    let server_key = Arc::new(Mutex::new(None));
    let config = Arc::new(russh::client::Config::default());
    let mut handle = russh::client::connect_stream(
        config,
        client_side,
        Client {
            server_key: Arc::clone(&server_key),
        },
    )
    .await
    .expect("client handshake");

    let auth = handle
        .authenticate_publickey("alice", PrivateKeyWithHashAlg::new(Arc::new(key), None))
        .await
        .expect("auth call");
    let host_key = server_key.lock().unwrap().clone();
    if !auth.success() {
        return (false, String::new(), None, host_key);
    }

    let mut channel = handle.channel_open_session().await.expect("open session");
    channel.exec(true, "echo hi").await.expect("exec");
    let mut stdout = Vec::new();
    let mut exit = None;
    loop {
        match channel.wait().await {
            Some(russh::ChannelMsg::Data { data }) => stdout.extend_from_slice(&data),
            Some(russh::ChannelMsg::ExitStatus { exit_status }) => exit = Some(exit_status),
            Some(russh::ChannelMsg::Eof) | Some(russh::ChannelMsg::Close) | None => break,
            _ => {}
        }
    }
    (
        true,
        String::from_utf8_lossy(&stdout).into_owned(),
        exit,
        host_key,
    )
}

#[tokio::test]
async fn an_authorized_key_attaches_and_bridges_an_exec_channel() {
    let authority = authority();
    let machine = GrainName::new("machine", "dev-box");
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);

    let server = tokio::spawn(serve_connection(
        server_side,
        machine,
        Arc::clone(&authority),
        Arc::new(FakeBackend),
    ));

    let key = PrivateKey::from_openssh(AUTHORIZED_KEY).expect("client key");
    let (authed, stdout, exit, host_key) = run_client(client_side, key).await;

    assert!(authed, "the authorized key must attach (M4)");
    assert_eq!(exit, Some(0), "the guest exit status must reach the client");
    assert!(
        stdout.contains("guest ran: /bin/sh -c echo hi"),
        "the guest output must bridge to the client, got {stdout:?}"
    );
    // The presented host key is the seed-expanded machine identity: the same
    // journaled 32 bytes always yield this key, so the client's `known_hosts`
    // pin survives hibernation, migration, and failover (machine §5.1).
    let presented = host_key.expect("the front door must present a host key at KEX");
    assert_eq!(
        presented.key_data(),
        host_key_from_seed(&HOST_KEY_SEED)
            .expect("host key from seed")
            .public_key()
            .key_data(),
        "the presented host key must be the machine's seed-expanded identity"
    );

    // The attachment was journaled with its principal, and detach fired on
    // close (M4).
    let _ = server.await;
    assert_eq!(&*authority.attaches.lock().unwrap(), &["alice".to_string()]);
    assert_eq!(
        authority.detaches.lock().unwrap().len(),
        1,
        "detach on close"
    );
}

#[tokio::test]
async fn an_unauthorized_key_is_rejected() {
    let authority = authority();
    let machine = GrainName::new("machine", "dev-box");
    let (client_side, server_side) = tokio::io::duplex(64 * 1024);

    let server = tokio::spawn(serve_connection(
        server_side,
        machine,
        Arc::clone(&authority),
        Arc::new(FakeBackend),
    ));

    let key = PrivateKey::from_openssh(UNAUTHORIZED_KEY).expect("client key");
    let (authed, _, _, _) = run_client(client_side, key).await;

    assert!(
        !authed,
        "a key the policy does not authorize must be rejected (M4)"
    );
    let _ = server.await;
    assert!(
        authority.attaches.lock().unwrap().is_empty(),
        "a rejected key never attaches"
    );
}
