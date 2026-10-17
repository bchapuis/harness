//! The production TCP [`Transport`] (spec §2.13).
//!
//! Frames travel length-delimited over TCP (see [`crate::wire`]). Each *directed*
//! pair of nodes uses its own connection, and outbound traffic always goes over
//! the connection this node dialed, so per-pair FIFO ordering holds end to end
//! over one association (spec §3.3, §4). Accepted connections are receive-only: a
//! reply travels back over the replier's own dialed connection, not the request's
//! socket.
//!
//! Before any actor traffic, the two ends exchange a [`Hello`] handshake (the
//! wire version, node identity, codec name, cluster secret) and reject the
//! association on any mismatch (spec §2.14). When a [`TlsConfig`] is present the
//! handshake runs over a mutually-authenticated TLS stream and the node allowlist
//! is enforced (§9).
//!
//! Addresses live in a runtime book **seeded** from [`TcpConfig::peers`] and
//! grown **dynamically** (spec §6.3): every handshake teaches both ends the
//! peer's advertised address, and nodes periodically gossip their known
//! `(node, address)` table, so addresses propagate to peers a node never
//! directly contacted. An unknown peer, a failed dial, or a closed connection
//! surfaces as [`TransportError::Unreachable`].

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::net::SocketAddr;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::Frame;
use actor_cluster::Transport;
use actor_cluster::TransportError;
use actor_cluster::WIRE;
use actor_core::NodeId;
use actor_serialization::Codec;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::pki_types::PrivateKeyDer;
use tokio_rustls::rustls::pki_types::ServerName;

use crate::association::Admitted;
use crate::association::Association;
use crate::association::Book;
use crate::association::Slots;
use crate::wire::Hello;
use crate::wire::Wire;
use crate::wire::read_hello;
use crate::wire::read_wire;
use crate::wire::write_hello;
use crate::wire::write_wire;

/// The transport address of node `id`: `host` at port `port_base + id - 1`.
///
/// The one place a node id becomes a port. A deployment configures its roster as
/// hosts plus a single port base rather than as a list of addresses, so every
/// process that stands up a [`TcpTransport`] (a voter, a non-voting client
/// gateway) has to derive the same port from the same id. A second copy of this
/// arithmetic is how one of them ends up dialing a port nobody is listening on,
/// with a plain `Unreachable` as the only symptom.
///
/// `host` may be an IP literal, used as-is, or a name (a container or pod DNS
/// name like `agent-0.agent`) resolved through the system resolver, which is
/// what lets a roster span machines instead of loopback.
///
/// An id whose port would not fit is an error, not a panic: ids reach here from an
/// operator's `--peer`/`--client` flag, so they are input to validate rather than
/// an invariant to assert.
pub fn node_addr(host: &str, port_base: u16, id: u64) -> Result<SocketAddr, String> {
    let port = id
        .checked_sub(1)
        .and_then(|offset| u16::try_from(offset).ok())
        .and_then(|offset| port_base.checked_add(offset))
        .ok_or_else(|| {
            format!(
                "node id {id} has no transport port at base {port_base}: ids start at 1, and \
                 base + id - 1 must fit a port"
            )
        })?;
    (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("resolve {host}:{port}: {e}"))?
        .next()
        .ok_or_else(|| format!("resolve {host}:{port}: no address"))
}

/// The TLS material for mutually-authenticated associations (spec §9). Both
/// the acceptor and the connector require a peer certificate, so every
/// association is mutually authenticated against the trusted roots baked into
/// these configs. [`mutual`](TlsConfig::mutual) assembles them from the raw
/// material; producing that material (CA, certs, keys) is the caller's job,
/// and the fields stay public as the escape hatch for a deployment that needs
/// a different rustls assembly.
#[derive(Clone)]
pub struct TlsConfig {
    /// Wraps inbound (accepted) connections; configured to demand client auth.
    pub acceptor: TlsAcceptor,
    /// Wraps outbound (dialed) connections; carries this node's client cert.
    pub connector: TlsConnector,
    /// The server name to request when dialing. Peer certificates are issued
    /// for this name, so all nodes can share one cluster identity.
    pub server_name: ServerName<'static>,
}

impl TlsConfig {
    /// Assemble the mutual-TLS material from one CA, this node's leaf `cert`
    /// and `key`, and the cluster's shared `server_name`: the CA becomes both
    /// the client-auth verifier and the dial-side root store, and the leaf
    /// serves both directions.
    pub fn mutual(
        ca: CertificateDer<'static>,
        cert: CertificateDer<'static>,
        key: PrivateKeyDer<'static>,
        server_name: ServerName<'static>,
    ) -> Result<TlsConfig, rustls::Error> {
        let mut roots = rustls::RootCertStore::empty();
        roots.add(ca)?;
        let verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots.clone()))
            .build()
            .map_err(|e| rustls::Error::General(e.to_string()))?;
        let server = rustls::ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![cert.clone()], key.clone_key())?;
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(vec![cert], key)?;
        Ok(TlsConfig {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            connector: TlsConnector::from(Arc::new(client)),
            server_name,
        })
    }
}

/// The erased connection stream (TCP, or TLS over it), so the dial and accept
/// sequences are written once per side rather than once per encryption mode.
trait Duplex: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Duplex for T {}

/// Static configuration for a [`TcpTransport`] (spec §2.14, §9).
#[derive(Clone)]
pub struct TcpConfig {
    /// This node's identity, announced in the handshake.
    pub node: NodeId,
    /// This node's own reachable address, announced so peers learn how to dial
    /// back (the bound address may be a wildcard or ephemeral port).
    pub advertised: SocketAddr,
    /// Seed addresses: enough peers to reach the cluster once. The runtime
    /// address book starts here and grows by handshake and gossip (spec §6.3).
    pub peers: BTreeMap<NodeId, SocketAddr>,
    /// How often to gossip the known endpoint table to peers (spec §6.3).
    /// 1 second from [`TcpConfig::new`].
    pub endpoint_gossip_interval: Duration,
    /// How long a TCP connect may take before it is `Unreachable` (spec §2.13), so
    /// a dial to a black-holed peer cannot stall the detector or gossip.
    /// 5 seconds from [`TcpConfig::new`].
    pub connect_timeout: Duration,
    /// How long the association handshake may take before it is torn down (spec
    /// §2.13, §9), so a silent peer cannot tie up a task. 5 seconds from
    /// [`TcpConfig::new`].
    pub handshake_timeout: Duration,
    /// Per-peer outbound queue depth before sends fail fast with backpressure
    /// (spec §3.3, §4): a bounded queue means a peer too slow to drain it costs
    /// failed sends, never unbounded memory. 1024 from [`TcpConfig::new`].
    pub outbound_capacity: usize,
    /// The wire codec; both ends must agree (spec §2.11).
    pub codec: Arc<dyn Codec>,
    /// A shared secret that guards against accidental cross-cluster association
    /// (spec §9). Peers presenting a different secret are rejected.
    pub cluster_secret: String,
    /// If set, only these node identities may associate (spec §9). `None`
    /// permits any peer that clears the version/codec/secret checks.
    pub allowlist: Option<BTreeSet<NodeId>>,
    /// How this association is protected on the wire (spec §9).
    ///
    /// Deliberately not an `Option<TlsConfig>` defaulting to `None`: the plaintext
    /// choice is a security decision every deployment must make on purpose, and an
    /// `Option` lets it be made by omission. The [`Encryption`] enum has no default,
    /// so a construction site has to name which it wants.
    pub encryption: Encryption,
}

impl TcpConfig {
    /// A config over a deployment's genuine choices, with everything that has a
    /// sane default (the timeouts, the queue capacity, the gossip cadence, no
    /// allowlist) filled in. The required arguments are the ones no default may
    /// choose: identity and addressing, the codec both ends must agree on, and
    /// the two security decisions (the secret, and [`Encryption`]; see the
    /// field's docs). The fields stay public, so a caller that needs a different
    /// timing overrides the one field it means to change.
    pub fn new(
        node: NodeId,
        advertised: SocketAddr,
        peers: BTreeMap<NodeId, SocketAddr>,
        codec: Arc<dyn Codec>,
        cluster_secret: String,
        encryption: Encryption,
    ) -> TcpConfig {
        TcpConfig {
            node,
            advertised,
            peers,
            endpoint_gossip_interval: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(5),
            outbound_capacity: 1024,
            codec,
            cluster_secret,
            allowlist: None,
            encryption,
        }
    }
}

/// How a transport association is protected (spec §9).
///
/// The `cluster_secret` is what authorizes membership, and it crosses the wire in
/// the association handshake. Under [`Encryption::PlaintextTrusted`] anyone who can
/// observe one handshake learns it and can join the cluster as a replica. That is
/// acceptable on loopback or a genuinely private network and nowhere else, which is
/// why the variant says so in its name.
#[derive(Clone)]
pub enum Encryption {
    /// Mutual TLS: both ends present certificates and the association runs
    /// encrypted. The production choice for any link that leaves a trusted network.
    MutualTls(Box<TlsConfig>),
    /// Plaintext, on a network the operator asserts is trusted: loopback, a private
    /// cluster network, or a test.
    PlaintextTrusted,
}

/// Shared transport state behind an `Arc`, so the handle clones cheaply.
struct Shared {
    config: TcpConfig,
    inbound: async_channel::Sender<(NodeId, Frame)>,
    /// The per-peer association slots ([`Slots`]): outbound connections this
    /// node dialed, one live per peer. Held only briefly to decide; dialing
    /// happens with the lock released. The queue is bounded (backpressure,
    /// spec §3.3). The arbitration and eviction *rules* live socket-free in
    /// [`crate::association`], where they are deterministically tested.
    slots: Mutex<Slots<Conn>>,
    /// The runtime address book ([`Book`], spec §6.3): seeded from config,
    /// then grown as handshakes and gossip reveal more peers.
    book: Mutex<Book>,
    /// Flipped to `true` on [`shutdown`](TcpTransport::shutdown); the accept loop,
    /// gossip loop, and connection tasks watch it and stop (spec §6.3).
    shutdown: watch::Sender<bool>,
}

/// One live outbound association: the queue frames leave by.
#[derive(Clone)]
struct Conn {
    tx: mpsc::Sender<Wire>,
}

impl Association for Conn {
    fn is_live(&self) -> bool {
        !self.tx.is_closed()
    }

    fn is_same(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }
}

impl Shared {
    fn hello(&self) -> Hello {
        Hello {
            version: WIRE.version(),
            node: self.config.node,
            advertised: self.config.advertised,
            codec_name: self.config.codec.name().to_string(),
            cluster_secret: self.config.cluster_secret.clone(),
        }
    }

    /// Record `node`'s address in the [`Book`] (which ignores claims about
    /// ourselves: our advertised address is authoritative).
    fn learn(&self, node: NodeId, addr: SocketAddr) {
        self.book
            .lock()
            .expect("book mutex poisoned")
            .learn(node, addr);
    }

    /// Merge a gossiped `(node, address)` table.
    fn learn_all(&self, table: Vec<(NodeId, SocketAddr)>) {
        self.book
            .lock()
            .expect("book mutex poisoned")
            .learn_all(table);
    }

    /// Look up a peer's address from the runtime book.
    fn address_of(&self, node: NodeId) -> Option<SocketAddr> {
        self.book
            .lock()
            .expect("book mutex poisoned")
            .address_of(node)
    }

    /// Validate a peer's handshake against our policy, returning the peer's node
    /// id. `expected` is `Some` when we dialed a specific peer and want to confirm
    /// we reached it.
    fn accept_hello(&self, hello: &Hello, expected: Option<NodeId>) -> Result<NodeId, String> {
        // Version first, so a build skew is never reported as a security failure
        // (compatibility §3.3).
        WIRE.admit(hello.version).map_err(|e| e.to_string())?;
        if hello.codec_name != self.config.codec.name() {
            return Err(format!("codec mismatch: {}", hello.codec_name));
        }
        if hello.cluster_secret != self.config.cluster_secret {
            return Err("cluster secret mismatch".to_string());
        }
        if let Some(allow) = &self.config.allowlist
            && !allow.contains(&hello.node)
        {
            return Err(format!("node {} not in allowlist", hello.node));
        }
        if let Some(expected) = expected
            && hello.node != expected
        {
            return Err(format!(
                "dialed {expected} but peer identified as {}",
                hello.node
            ));
        }
        Ok(hello.node)
    }
}

/// A TCP [`Transport`]. Clone it freely; clones share one set of connections.
#[derive(Clone)]
pub struct TcpTransport {
    shared: Arc<Shared>,
}

impl TcpTransport {
    /// Start the transport on an already-bound `listener` (binding separately
    /// lets a caller discover an ephemeral port before wiring the address book).
    /// Spawns the accept loop; returns the handle plus the inbound frame
    /// receiver to hand to [`ClusterSystem::start`](actor_cluster::ClusterSystem).
    pub fn start(
        config: TcpConfig,
        listener: TcpListener,
    ) -> (TcpTransport, async_channel::Receiver<(NodeId, Frame)>) {
        let (inbound_tx, inbound_rx) = async_channel::unbounded();
        // Seed the runtime address book from the configured peers.
        let book = Book::new(
            config.node,
            config.advertised,
            config.peers.iter().map(|(n, a)| (*n, *a)),
        );
        let gossip_interval = config.endpoint_gossip_interval;
        let shared = Arc::new(Shared {
            config,
            inbound: inbound_tx,
            slots: Mutex::new(Slots::new()),
            book: Mutex::new(book),
            shutdown: watch::channel(false).0,
        });
        tokio::spawn(accept_loop(listener, Arc::clone(&shared)));
        let transport = TcpTransport { shared };
        tokio::spawn(endpoint_gossip(transport.clone(), gossip_interval));
        (transport, inbound_rx)
    }

    /// Get a sender to `peer`'s outbound queue, dialing and handshaking if no
    /// live connection exists. Connect and handshake are bounded by timeouts, so
    /// an unresponsive peer surfaces as `Unreachable` (spec §2.13).
    async fn connection(&self, peer: NodeId) -> Result<Conn, TransportError> {
        // Fast path: reuse a live connection.
        if let Some(conn) = self.lookup(peer) {
            return Ok(conn);
        }

        // Resolve the address from the runtime book (seeded + learned).
        let addr = self
            .shared
            .address_of(peer)
            .ok_or(TransportError::Unreachable)?;

        let tcp =
            tokio::time::timeout(self.shared.config.connect_timeout, TcpStream::connect(addr))
                .await
                .map_err(|_| TransportError::Unreachable)? // timed out
                .map_err(|_| TransportError::Unreachable)?; // connect failed
        let _ = tcp.set_nodelay(true);
        let handshake_timeout = self.shared.config.handshake_timeout;

        // Wrap in TLS if configured, then run the application handshake over the
        // (possibly encrypted) stream, all under one handshake deadline. The
        // stream type is erased first, so the deadline/handshake/registration
        // sequence exists once for both encryption modes.
        let handshake = async {
            let stream: Box<dyn Duplex> = match &self.shared.config.encryption {
                Encryption::MutualTls(tls) => {
                    Box::new(tls.connector.connect(tls.server_name.clone(), tcp).await?)
                }
                Encryption::PlaintextTrusted => Box::new(tcp),
            };
            client_handshake(stream, &self.shared, peer).await
        };
        let stream = tokio::time::timeout(handshake_timeout, handshake)
            .await
            .map_err(|_| TransportError::Unreachable)?
            .map_err(|_| TransportError::Unreachable)?;
        Ok(self.register_dialed(peer, stream))
    }

    fn lookup(&self, peer: NodeId) -> Option<Conn> {
        self.shared
            .slots
            .lock()
            .expect("slots mutex poisoned")
            .reuse(peer)
    }

    /// Register a freshly dialed connection, or, if a concurrent dial already
    /// won, keep the existing one and let this stream drop (clean teardown).
    fn register_dialed<S>(&self, peer: NodeId, stream: S) -> Conn
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(self.shared.config.outbound_capacity);
        let conn = Conn { tx: tx.clone() };
        // The arbitration is [`Slots::admit`]'s: a concurrent dial that won is the
        // association whose frames will travel, so its `Conn` is kept whole and
        // this stream is dropped without ever starting.
        let admitted = self
            .shared
            .slots
            .lock()
            .expect("slots mutex poisoned")
            .admit(peer, conn);
        match admitted {
            Admitted::Won(conn) => {
                tokio::spawn(dialed_connection(
                    stream,
                    rx,
                    peer,
                    Arc::clone(&self.shared),
                    conn.clone(),
                ));
                conn
            }
            Admitted::Lost(winner) => winner,
        }
    }
}

impl Transport for TcpTransport {
    async fn send(&self, peer: NodeId, frame: Frame) -> Result<(), TransportError> {
        let conn = self.connection(peer).await?;
        // Non-blocking enqueue with backpressure: a full queue (peer too slow) or
        // a closed one (connection died) both surface as `Unreachable`, and the
        // dead/saturated entry is dropped so the next send re-dials. At-most-once:
        // this frame is never resent (spec §4), and a dropped frame an `ask` was
        // awaiting completes it as `Unreachable`, so nothing is silently lost.
        conn.tx.try_send(Wire::Frame(frame)).map_err(|_| {
            self.shared
                .slots
                .lock()
                .expect("slots mutex poisoned")
                .evict_if_same(peer, &conn);
            TransportError::Unreachable
        })
    }

    /// Stop the node's transport (spec §6.3): signal the accept, gossip, and
    /// connection tasks to exit (releasing the listener and open sockets), close
    /// the inbound path so the receive loop ends, and drop the outbound queues.
    /// Idempotent.
    fn shutdown(&self) {
        let _ = self.shared.shutdown.send(true);
        self.shared.inbound.close();
        self.shared
            .slots
            .lock()
            .expect("slots mutex poisoned")
            .clear();
    }
}

/// Accept inbound connections, handshaking each and serving it receive-only.
async fn accept_loop(listener: TcpListener, shared: Arc<Shared>) {
    let mut shutdown = shared.shutdown.subscribe();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let Ok((stream, _addr)) = accepted else {
                    return; // listener gone
                };
                let _ = stream.set_nodelay(true);
                tokio::spawn(serve_accepted(stream, Arc::clone(&shared)));
            }
            _ = shutdown.changed() => return, // node stopping (spec §6.3)
        }
    }
}

/// Handshake an accepted connection, then read frames from it until it closes.
/// A failed TLS or application handshake tears down the association, never the
/// node (spec §2.13, §9).
async fn serve_accepted(tcp: TcpStream, shared: Arc<Shared>) {
    // Bound the whole accept-side handshake: a peer that connects but never
    // speaks must not tie up this task (spec §2.13, §9). As on the dial side,
    // the stream type is erased first so the sequence exists once for both
    // encryption modes.
    let handshake_timeout = shared.config.handshake_timeout;
    let handshake = async {
        let stream: Box<dyn Duplex> = match shared.config.encryption.clone() {
            Encryption::MutualTls(tls) => Box::new(tls.acceptor.accept(tcp).await?),
            Encryption::PlaintextTrusted => Box::new(tcp),
        };
        server_handshake(stream, &shared).await
    };
    if let Ok(Ok((stream, peer))) = tokio::time::timeout(handshake_timeout, handshake).await {
        read_loop(stream, peer, &shared).await;
    }
}

/// A dialed connection, run as two independent halves over a split stream: a
/// **writer** drains the outbound queue, and a **reader** consumes anything the
/// peer sends (normally only its close; replies and gossip come over the peer's
/// own dialed connection). Splitting avoids a `select!` that would cancel a
/// partially-read frame and desync the stream. The connection ends when either
/// half finishes (queue closed, or socket error/EOF), and the peer is then
/// deregistered so the next send re-dials.
async fn dialed_connection<S>(
    stream: S,
    mut rx: mpsc::Receiver<Wire>,
    peer: NodeId,
    shared: Arc<Shared>,
    self_conn: Conn,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let codec_w = shared.config.codec.clone();
    let codec_r = shared.config.codec.clone();
    let reader_shared = Arc::clone(&shared);

    let writer = async move {
        while let Some(msg) = rx.recv().await {
            if write_wire(&mut write_half, codec_w.as_ref(), &msg)
                .await
                .is_err()
            {
                break;
            }
        }
    };
    let reader = async move {
        // Reads until a malformed frame or the peer closes (the `Err` exit).
        while let Ok(msg) = read_wire(&mut read_half, codec_r.as_ref()).await {
            if !deliver(&reader_shared, peer, msg).await {
                break;
            }
        }
    };

    let mut shutdown = shared.shutdown.subscribe();
    tokio::pin!(writer, reader);
    tokio::select! {
        _ = &mut writer => {}
        _ = &mut reader => {}
        _ = shutdown.changed() => {} // node stopping (spec §6.3)
    }

    // Deregister so the next send re-dials. `evict_if_same` leaves a successor
    // alone.
    shared
        .slots
        .lock()
        .expect("slots mutex poisoned")
        .evict_if_same(peer, &self_conn);
}

/// Read messages from a receive-only (accepted) connection until it closes.
async fn read_loop<S>(mut stream: S, peer: NodeId, shared: &Arc<Shared>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut shutdown = shared.shutdown.subscribe();
    loop {
        tokio::select! {
            framed = read_wire(&mut stream, shared.config.codec.as_ref()) => match framed {
                Ok(msg) => {
                    if !deliver(shared, peer, msg).await {
                        return;
                    }
                }
                Err(_) => return, // malformed frame or peer closed
            },
            _ = shutdown.changed() => return, // node stopping (spec §6.3)
        }
    }
}

/// Dispatch an inbound [`Wire`] message: actor frames go up to the cluster's
/// receive loop; an endpoint table is merged into the address book and consumed
/// here (spec §6.3). Returns `false` if the cluster's receive side is gone.
async fn deliver(shared: &Arc<Shared>, peer: NodeId, msg: Wire) -> bool {
    match msg {
        Wire::Frame(frame) => shared.inbound.send((peer, frame)).await.is_ok(),
        Wire::Endpoints(table) => {
            shared.learn_all(table);
            true
        }
    }
}

/// Dialer side of the handshake: announce ourselves, then read and validate the
/// peer's `Hello`, confirming it is the node we meant to reach, and learn its
/// advertised address (spec §6.3).
async fn client_handshake<S>(mut stream: S, shared: &Arc<Shared>, expected: NodeId) -> io::Result<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    write_hello(&mut stream, &shared.hello()).await?;
    let peer = read_hello(&mut stream).await?;
    shared
        .accept_hello(&peer, Some(expected))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    shared.learn(peer.node, peer.advertised);
    Ok(stream)
}

/// Acceptor side of the handshake: read and validate the peer's `Hello` first,
/// then announce ourselves. Returns the verified peer identity and learns its
/// advertised address (spec §6.3).
async fn server_handshake<S>(mut stream: S, shared: &Arc<Shared>) -> io::Result<(S, NodeId)>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    let peer = read_hello(&mut stream).await?;
    let node = shared
        .accept_hello(&peer, None)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    write_hello(&mut stream, &shared.hello()).await?;
    shared.learn(peer.node, peer.advertised);
    Ok((stream, node))
}

/// Endpoint anti-entropy (spec §6.3): every `interval`, push the known
/// `(node, address)` table to each known peer, dialing it if not already
/// connected. Gossip is the bootstrap, so it must be able to dial.
///
/// The per-peer sends run **concurrently** (each dial bounded by the configured
/// connect timeout), so one black-holed peer cannot stall gossip to the live
/// ones; awaiting them together caps in-flight dials at one per peer, with no
/// cross-interval pile-up. A non-blocking `try_send` keeps a slow but connected
/// peer from stalling the loop.
async fn endpoint_gossip(transport: TcpTransport, interval: Duration) {
    let mut shutdown = transport.shared.shutdown.subscribe();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => return, // node stopping (spec §6.3)
        }
        // One lock: the table to push and who to push it to (the book minus
        // ourselves, connected or not) are one consistent snapshot.
        let (table, targets) = {
            let book = transport.shared.book.lock().expect("book mutex poisoned");
            (book.table(), book.gossip_targets())
        };
        let sends = targets.into_iter().map(|peer| {
            let transport = transport.clone();
            let table = table.clone();
            async move {
                if let Ok(conn) = transport.connection(peer).await {
                    let _ = conn.tx.try_send(Wire::Endpoints(table));
                }
            }
        });
        futures::future::join_all(sends).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `Shared` with no listener and no tasks: enough to exercise
    /// [`Shared::accept_hello`]'s policy, which reads only the config.
    fn shared(secret: &str) -> Shared {
        Shared {
            config: TcpConfig::new(
                NodeId::new(1),
                "127.0.0.1:1".parse().expect("a literal address"),
                BTreeMap::new(),
                Arc::new(actor_serialization::JsonCodec),
                secret.to_string(),
                Encryption::PlaintextTrusted,
            ),
            inbound: async_channel::unbounded().0,
            slots: Mutex::new(Slots::new()),
            book: Mutex::new(Book::new(
                NodeId::new(1),
                "127.0.0.1:1".parse().expect("a literal address"),
                [],
            )),
            shutdown: watch::channel(false).0,
        }
    }

    fn hello(version: compat::Version) -> Hello {
        Hello {
            version,
            node: NodeId::new(2),
            advertised: "127.0.0.1:2".parse().expect("a literal address"),
            codec_name: "json".to_string(),
            cluster_secret: "secret".to_string(),
        }
    }

    #[test]
    fn a_node_id_lands_on_its_own_port_above_the_base() {
        // The convention the roster is configured against: id 1 sits *on* the base,
        // and each id after it one port higher. Every process standing up a
        // transport derives its dial map this way, so the mapping is the contract.
        for (id, expected) in [(1, 7401), (2, 7402), (9, 7409)] {
            assert_eq!(
                node_addr("127.0.0.1", 7401, id),
                Ok(format!("127.0.0.1:{expected}")
                    .parse()
                    .expect("a literal address"))
            );
        }
    }

    #[test]
    fn an_id_with_no_port_is_refused_rather_than_wrapped() {
        // Ids arrive from `--peer`/`--client`, so these are operator typos, not
        // invariants: each must come back as the error the caller already handles,
        // never as a panic or a silently wrapped port pointing at some other node.
        for (base, id) in [(7401u16, 0u64), (7401, 1 << 20), (u16::MAX, 2)] {
            assert!(node_addr("127.0.0.1", base, id).is_err());
        }
    }

    #[test]
    fn a_peer_on_this_builds_wire_version_associates() {
        let shared = shared("secret");
        assert_eq!(
            shared.accept_hello(&hello(WIRE.version()), None),
            Ok(NodeId::new(2)),
        );
    }

    #[test]
    fn a_peer_on_another_wire_version_is_refused_by_name() {
        let shared = shared("secret");
        // A peer from another release. Refused, and the message names both
        // versions so an operator knows which end to move (**V2**).
        let err = shared
            .accept_hello(&hello(compat::Version(9)), None)
            .expect_err("another version must not associate");
        assert!(
            err.contains("actor.wire") && err.contains("v9") && err.contains("v1"),
            "the refusal must name the format and both versions: {err}"
        );
    }

    #[test]
    fn the_version_check_precedes_the_secret_check() {
        // An unreadable peer is turned away before its secret is compared, so a
        // version skew is never reported as a security failure.
        let shared = shared("secret");
        let mut peer = hello(compat::Version(9));
        peer.cluster_secret = "wrong".to_string();
        let err = shared
            .accept_hello(&peer, None)
            .expect_err("a peer on another version must be refused");
        assert!(err.contains("actor.wire"), "version first: {err}");
    }
}
