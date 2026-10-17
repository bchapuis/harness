//! The SSH server: a `russh` handler that terminates SSH against the
//! machine's journaled policy and bridges each channel to the guest agent
//! (machine §2.5).
//!
//! Authentication is publickey-only, checked at [`Handler::auth_publickey`]
//! against [`MachineAuthority::authorizes`] (M4). The attachment it journals
//! names the authenticating key, not the client's word ([`principal`]). Two
//! refusals stay distinguishable to the client: a key the policy does not
//! authorize is an ordinary publickey failure, while a refusal by
//! [`MachineAuthority::attach`] (this node does not lead the machine, the
//! machine is unreachable) also disconnects with the authority's reason, so
//! an authorized user is told where to go rather than "permission denied".
//!
//! A channel becomes live on its `exec`/`shell`/`subsystem` request: the
//! handler opens a [`ChannelBackend`] stream, spawns a guest→host reader and a
//! host→guest writer, and thereafter forwards
//! `data`/`eof`/`window-change`/`signal` callbacks as
//! [`proto`](machine_proto) frames. The guest's exit status closes the channel.

use std::collections::HashMap;
use std::sync::Arc;

use entity::EntityName;
use russh::Channel;
use russh::ChannelId;
use russh::Disconnect;
use russh::Sig;
use russh::keys::HashAlg;
use russh::keys::PublicKey;
use russh::server::Auth;
use russh::server::Config;
use russh::server::Handler;
use russh::server::Msg;
use russh::server::Session;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

use machine_host::vsock::recv_frame;
use machine_host::vsock::send_frame;
use machine_proto::Frame;
use machine_proto::MAX_FRAME;

use crate::ChannelBackend;
use crate::ChannelKind;
use crate::FrontDoorError;
use crate::MachineAuthority;

/// Terminate SSH on `stream` for `machine`, bridging channels to its guest
/// agent (machine §2.5). Presents the machine's journaled host key at KEX and
/// runs to connection close.
pub async fn serve_connection<A, B, S>(
    stream: S,
    machine: EntityName,
    authority: Arc<A>,
    backend: Arc<B>,
) -> Result<(), FrontDoorError>
where
    A: MachineAuthority,
    B: ChannelBackend,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    let host_key = authority.host_key(&machine).await?;
    let mut methods = russh::MethodSet::empty();
    methods.push(russh::MethodKind::PublicKey);
    let config = Arc::new(Config {
        methods,
        keys: vec![host_key],
        ..Config::default()
    });
    // The attachment id is shared with the handler so the entrypoint can
    // journal the detach after the connection closes (the handler is consumed
    // by `run_stream`, machine §2.5).
    let attachment: Arc<std::sync::Mutex<Option<u64>>> = Arc::new(std::sync::Mutex::new(None));
    // The session handle exists only once `run_stream` returns, after the
    // handler has moved into the session, so it reaches the handler through a
    // oneshot the handler awaits only when it has a refusal to deliver.
    let (session_tx, session_rx) = oneshot::channel();
    let handler = FrontDoorHandler {
        machine: machine.clone(),
        authority: Arc::clone(&authority),
        backend,
        attachment: Arc::clone(&attachment),
        session: Some(session_rx),
        pty: None,
        env: Vec::new(),
        channels: HashMap::new(),
    };
    let session = russh::server::run_stream(config, stream, handler)
        .await
        .map_err(|e| FrontDoorError(format!("ssh handshake: {e}")))?;
    let _ = session_tx.send(session.handle());
    let outcome = session.await;
    // The connection closed (cleanly or with an error): journal the detach.
    // Take the id out from under the lock before awaiting, so no guard spans
    // the await (the future must stay `Send`).
    let id = *attachment.lock().expect("attachment lock");
    if let Some(id) = id {
        authority.detach(&machine, id).await;
    }
    outcome.map_err(|e| FrontDoorError(format!("ssh session: {e}")))?;
    Ok(())
}

/// The pending pseudo-terminal parameters a `pty-req` sets before the
/// `shell`/`exec` that consumes them.
struct PtyParams {
    term: String,
    cols: u16,
    rows: u16,
}

/// The principal an attachment journals for a connection authenticated by
/// `key` (M4): the key's SHA-256 fingerprint, the form the machine's
/// authorized-key set is keyed by, so the journal joins each attachment to the
/// key that made it. The SSH user name is the client's own assertion and rides
/// beside it, in parentheses, for the operator reading the journal; it never
/// stands alone, because the client chooses it freely.
fn principal(user: &str, key: &PublicKey) -> String {
    let fingerprint = key.fingerprint(HashAlg::Sha256);
    if user.is_empty() {
        fingerprint.to_string()
    } else {
        format!("{fingerprint} ({user})")
    }
}

struct FrontDoorHandler<A, B> {
    machine: EntityName,
    authority: Arc<A>,
    backend: Arc<B>,
    attachment: Arc<std::sync::Mutex<Option<u64>>>,
    /// The connection's session handle, delivered once `run_stream` returns;
    /// taken by the first refusal that disconnects with a reason.
    session: Option<oneshot::Receiver<russh::server::Handle>>,
    pty: Option<PtyParams>,
    /// `env` requests accepted so far, consumed by the `exec` that follows.
    env: Vec<(String, String)>,
    /// One host→guest frame queue per live channel.
    channels: HashMap<ChannelId, mpsc::Sender<Frame>>,
}

impl<A: MachineAuthority, B: ChannelBackend> FrontDoorHandler<A, B> {
    /// Open a backend channel of `kind`, wire it to `channel`, and remember
    /// its frame queue. Shared by exec/shell/subsystem.
    async fn start_channel(
        &mut self,
        channel: ChannelId,
        kind: ChannelKind,
        session: &mut Session,
    ) -> Result<(), russh::Error> {
        let stream =
            self.backend.open(&self.machine, kind).await.map_err(|e| {
                russh::Error::from(std::io::Error::other(format!("open channel: {e}")))
            })?;
        let (mut reader, mut writer) = tokio::io::split(stream);

        // host→guest: drain the frame queue to the backend. The mpsc receiver
        // is the sole writer, so the write half needs no lock.
        let (tx, mut rx) = mpsc::channel::<Frame>(256);
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if send_frame(&mut writer, &frame.encode()).await.is_err() {
                    break;
                }
            }
        });

        // guest→host: relay frames to the SSH channel via the session handle.
        let handle = session.handle();
        tokio::spawn(async move {
            loop {
                let body = match recv_frame(&mut reader, MAX_FRAME).await {
                    Ok(body) => body,
                    Err(_) => {
                        // The guest end went away without reporting an exit:
                        // a killed guest, a crashed agent, a severed vsock. The
                        // client is waiting on `exit-status` and would hang
                        // until some unrelated timeout, so say what happened:
                        // 255 is what SSH itself reports for a connection that
                        // failed rather than a command that ran.
                        let _ = handle.exit_status_request(channel, 255).await;
                        let _ = handle.close(channel).await;
                        break;
                    }
                };
                match Frame::decode(&body) {
                    Some(Frame::Data(bytes)) => {
                        let sent = handle.data(channel, bytes::Bytes::from(bytes)).await;
                        if sent.is_err() {
                            break;
                        }
                    }
                    Some(Frame::Stderr(bytes)) => {
                        let _ = handle
                            .extended_data(channel, 1, bytes::Bytes::from(bytes))
                            .await;
                    }
                    Some(Frame::ExitStatus(code)) => {
                        let _ = handle.exit_status_request(channel, code as u32).await;
                        let _ = handle.close(channel).await;
                        break;
                    }
                    _ => {}
                }
            }
        });

        self.channels.insert(channel, tx);
        session.channel_success(channel)?;
        Ok(())
    }

    /// The [`ChannelKind`] a `shell`/`exec` request opens: a PTY when one was
    /// requested (consuming the pending params), otherwise piped stdio with
    /// the accepted `env` requests.
    fn channel_kind(&mut self, argv: Vec<String>) -> ChannelKind {
        match self.pty.take() {
            Some(p) => ChannelKind::Pty {
                term: p.term,
                cols: p.cols,
                rows: p.rows,
                argv,
            },
            None => ChannelKind::Exec {
                argv,
                env: std::mem::take(&mut self.env),
            },
        }
    }

    async fn push(&self, channel: ChannelId, frame: Frame) {
        if let Some(tx) = self.channels.get(&channel) {
            let _ = tx.send(frame).await;
        }
    }
}

impl<A: MachineAuthority, B: ChannelBackend> Handler for FrontDoorHandler<A, B> {
    type Error = russh::Error;

    async fn auth_publickey(
        &mut self,
        user: &str,
        public_key: &PublicKey,
    ) -> Result<Auth, Self::Error> {
        // Possession is proven by russh (the signature); the front door
        // checks *membership* against the machine's journaled set (M4). No
        // key material enters the guest.
        if !self.authority.authorizes(&self.machine, public_key).await {
            return Ok(Auth::reject());
        }
        let principal = principal(user, public_key);
        match self.authority.attach(&self.machine, &principal).await {
            Ok(attachment) => {
                *self.attachment.lock().expect("attachment lock") = Some(attachment);
                Ok(Auth::Accept)
            }
            Err(refusal) => {
                // The key is authorized, so a bare rejection would tell its
                // holder the wrong thing. Disconnect with the authority's
                // reason after the rejection goes out. Spawned, because the
                // session delivers the disconnect from the same loop that is
                // awaiting this callback.
                if let Some(session) = self.session.take() {
                    tokio::spawn(async move {
                        if let Ok(handle) = session.await {
                            let _ = handle
                                .disconnect(
                                    Disconnect::ByApplication,
                                    refusal.to_string(),
                                    String::new(),
                                )
                                .await;
                        }
                    });
                }
                Ok(Auth::reject())
            }
        }
    }

    async fn channel_open_session(
        &mut self,
        _channel: Channel<Msg>,
        reply: russh::server::ChannelOpenHandle,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        reply.accept().await;
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: ChannelId,
        term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.pty = Some(PtyParams {
            term: term.to_string(),
            cols: col_width as u16,
            rows: row_height as u16,
        });
        let _ = session.channel_success(channel);
        Ok(())
    }

    async fn env_request(
        &mut self,
        channel: ChannelId,
        variable_name: &str,
        variable_value: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        // Accepted unfiltered: the key holder already has arbitrary exec on
        // this machine, so an env variable grants nothing it lacks.
        self.env
            .push((variable_name.to_string(), variable_value.to_string()));
        let _ = session.channel_success(channel);
        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let kind = self.channel_kind(vec![]);
        self.start_channel(channel, kind, session).await
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let command = String::from_utf8_lossy(data).into_owned();
        let kind = self.channel_kind(vec!["/bin/sh".into(), "-c".into(), command]);
        self.start_channel(channel, kind, session).await
    }

    async fn subsystem_request(
        &mut self,
        channel: ChannelId,
        name: &str,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        if name != "sftp" {
            let _ = session.channel_failure(channel);
            return Ok(());
        }
        self.start_channel(channel, ChannelKind::Sftp, session)
            .await
    }

    async fn data(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.push(channel, Frame::Data(data.to_vec())).await;
        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.push(
            channel,
            Frame::WindowChange {
                cols: col_width as u16,
                rows: row_height as u16,
            },
        )
        .await;
        Ok(())
    }

    async fn channel_eof(
        &mut self,
        channel: ChannelId,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        // The client closed stdin: forward it so the guest closes the child's
        // stdin pipe; `echo x | ssh m wc -c` cannot finish without this.
        self.push(channel, Frame::Eof).await;
        Ok(())
    }

    async fn signal(
        &mut self,
        channel: ChannelId,
        signal: Sig,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        let name = match signal {
            Sig::TERM => "TERM",
            Sig::INT => "INT",
            Sig::HUP => "HUP",
            Sig::KILL => "KILL",
            Sig::QUIT => "QUIT",
            _ => return Ok(()),
        };
        self.push(channel, Frame::Signal(name.to_string())).await;
        Ok(())
    }
}
