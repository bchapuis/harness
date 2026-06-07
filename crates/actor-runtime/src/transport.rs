//! The production TCP [`Transport`] (spec §7).
//!
//! Frames travel length-delimited over TCP (see [`crate::wire`]). Each *directed*
//! pair of nodes uses its own connection — outbound traffic always goes over the
//! connection this node dialed — so per-pair FIFO ordering holds end to end over
//! one association (spec §6, §7.2). Accepted connections are receive-only: a
//! reply travels back over the replier's own dialed connection, not the request's
//! socket.
//!
//! Before any actor traffic, the two ends exchange a [`Hello`] handshake
//! (protocol version, node identity, codec name, cluster secret) and reject the
//! association on any mismatch (spec §7.1). When a [`TlsConfig`] is present the
//! handshake runs over a mutually-authenticated TLS stream and the node
//! allowlist is enforced (spec §15); the connection logic is generic over the
//! byte stream, so it serves both plaintext and TLS unchanged.
//!
//! Addresses live in a runtime book that is **seeded** from [`TcpConfig::peers`]
//! and grown **dynamically** (spec §9.3): every handshake teaches both ends the
//! peer's advertised address, and nodes periodically gossip their known
//! `(node, address)` table, so addresses propagate to peers a node never
//! directly contacted. A node therefore needs only enough seeds to reach the
//! cluster once. An unknown peer, a failed dial, or a closed connection surfaces
//! as [`TransportError::Unreachable`] (until the address is learned).

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use actor_cluster::Frame;
use actor_cluster::Transport;
use actor_cluster::TransportError;
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
use tokio_rustls::rustls::pki_types::ServerName;

use crate::wire::Hello;
use crate::wire::Wire;
use crate::wire::read_hello;
use crate::wire::read_wire;
use crate::wire::write_hello;
use crate::wire::write_wire;

/// The protocol version this build speaks (spec §7.1). A peer announcing a
/// different version is rejected at the handshake.
pub const PROTO_VERSION: u32 = 1;

/// Default for [`TcpConfig::connect_timeout`]: how long to wait for a TCP
/// connect before giving up (spec §7). Bounds a dial to a black-holed peer, so
/// it cannot stall the detector or gossip.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default for [`TcpConfig::handshake_timeout`]: how long to wait for the (TLS +)
/// `Hello` handshake before tearing the association down (spec §7, §15). Stops a
/// silent peer from tying up a task.
pub const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Default for [`TcpConfig::outbound_capacity`]: per-peer outbound queue depth. A
/// bounded queue applies backpressure: when a peer is too slow to drain it,
/// further sends fail fast with `Unreachable` rather than growing memory without
/// bound (spec §6, §7.2).
pub const DEFAULT_OUTBOUND_CAPACITY: usize = 1024;

/// The TLS material for mutually-authenticated associations (spec §15). Both
/// the acceptor and the connector require a peer certificate, so every
/// association is mutually authenticated against the trusted roots baked into
/// these configs. Constructing them (CA, certs, keys) is the caller's job; this
/// crate stays policy-light and simply wraps each connection.
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

/// Static configuration for a [`TcpTransport`] (spec §7.1, §15).
#[derive(Clone)]
pub struct TcpConfig {
    /// This node's identity, announced in the handshake.
    pub node: NodeId,
    /// This node's own reachable address, announced so peers learn how to dial
    /// back (the bound address may be a wildcard or ephemeral port).
    pub advertised: SocketAddr,
    /// Seed addresses: enough peers to reach the cluster once. The runtime
    /// address book starts here and grows by handshake + gossip (spec §9.3).
    pub peers: BTreeMap<NodeId, SocketAddr>,
    /// How often to gossip the known endpoint table to peers (spec §9.3).
    pub endpoint_gossip_interval: Duration,
    /// How long a TCP connect may take before it is `Unreachable` (spec §7).
    /// See [`DEFAULT_CONNECT_TIMEOUT`].
    pub connect_timeout: Duration,
    /// How long the association handshake may take before it is torn down (spec
    /// §7, §15). See [`DEFAULT_HANDSHAKE_TIMEOUT`].
    pub handshake_timeout: Duration,
    /// Per-peer outbound queue depth before sends fail fast with backpressure
    /// (spec §6). See [`DEFAULT_OUTBOUND_CAPACITY`].
    pub outbound_capacity: usize,
    /// The wire codec; both ends must agree (spec §5).
    pub codec: Arc<dyn Codec>,
    /// A shared secret that guards against accidental cross-cluster association
    /// (spec §15). Peers presenting a different secret are rejected.
    pub cluster_secret: String,
    /// If set, only these node identities may associate (spec §15). `None`
    /// permits any peer that clears the version/codec/secret checks.
    pub allowlist: Option<BTreeSet<NodeId>>,
    /// Mutual-TLS material (spec §15). `None` runs plaintext — intended for
    /// trusted networks and tests; production SHOULD set it.
    pub tls: Option<TlsConfig>,
}

/// Shared transport state behind an `Arc`, so the handle clones cheaply.
struct Shared {
    config: TcpConfig,
    inbound: async_channel::Sender<(NodeId, Frame)>,
    /// Outbound connections this node dialed, one per peer. Held only briefly to
    /// look up or insert; dialing happens with the lock released. The queue is
    /// bounded (backpressure, spec §6).
    conns: Mutex<HashMap<NodeId, mpsc::Sender<Wire>>>,
    /// The runtime address book (spec §9.3): seeded from config, then grown as
    /// handshakes and gossip reveal more peers.
    endpoints: Mutex<BTreeMap<NodeId, SocketAddr>>,
    /// Flipped to `true` on [`shutdown`](TcpTransport::shutdown); the accept loop,
    /// gossip loop, and connection tasks watch it and stop (spec §9.3).
    shutdown: watch::Sender<bool>,
}

impl Shared {
    fn hello(&self) -> Hello {
        Hello {
            proto_version: PROTO_VERSION,
            node: self.config.node,
            advertised: self.config.advertised,
            codec_name: self.config.codec.name().to_string(),
            cluster_secret: self.config.cluster_secret.clone(),
        }
    }

    /// Record `node`'s address, unless it is ourselves under a different address
    /// (our own advertised address is authoritative).
    fn learn(&self, node: NodeId, addr: SocketAddr) {
        if node == self.config.node {
            return;
        }
        self.endpoints
            .lock()
            .expect("endpoints mutex poisoned")
            .insert(node, addr);
    }

    /// Merge a gossiped `(node, address)` table.
    fn learn_all(&self, table: Vec<(NodeId, SocketAddr)>) {
        for (node, addr) in table {
            self.learn(node, addr);
        }
    }

    /// Look up a peer's address from the runtime book.
    fn address_of(&self, node: NodeId) -> Option<SocketAddr> {
        self.endpoints
            .lock()
            .expect("endpoints mutex poisoned")
            .get(&node)
            .copied()
    }

    /// The full known table, including our own advertised address, for gossip.
    fn endpoint_table(&self) -> Vec<(NodeId, SocketAddr)> {
        let mut table = vec![(self.config.node, self.config.advertised)];
        table.extend(
            self.endpoints
                .lock()
                .expect("endpoints mutex poisoned")
                .iter()
                .map(|(n, a)| (*n, *a)),
        );
        table
    }

    /// Validate a peer's handshake against our policy, returning the peer's node
    /// id. `expected` is `Some` when we dialed a specific peer and want to
    /// confirm we reached it.
    fn accept_hello(&self, hello: &Hello, expected: Option<NodeId>) -> Result<NodeId, String> {
        if hello.proto_version != PROTO_VERSION {
            return Err(format!(
                "protocol version mismatch: {}",
                hello.proto_version
            ));
        }
        if hello.codec_name != self.config.codec.name() {
            return Err(format!("codec mismatch: {}", hello.codec_name));
        }
        if hello.cluster_secret != self.config.cluster_secret {
            return Err("cluster secret mismatch".to_string());
        }
        if let Some(allow) = &self.config.allowlist {
            if !allow.contains(&hello.node) {
                return Err(format!("node {} not in allowlist", hello.node));
            }
        }
        if let Some(expected) = expected {
            if hello.node != expected {
                return Err(format!(
                    "dialed {expected} but peer identified as {}",
                    hello.node
                ));
            }
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
    /// Spawns the accept loop and returns the handle plus the inbound frame
    /// receiver to hand to [`ClusterSystem::start`](actor_cluster::ClusterSystem).
    pub fn start(
        config: TcpConfig,
        listener: TcpListener,
    ) -> (TcpTransport, async_channel::Receiver<(NodeId, Frame)>) {
        let (inbound_tx, inbound_rx) = async_channel::unbounded();
        // Seed the runtime address book from the configured peers.
        let endpoints: BTreeMap<NodeId, SocketAddr> = config
            .peers
            .iter()
            .filter(|(n, _)| **n != config.node)
            .map(|(n, a)| (*n, *a))
            .collect();
        let gossip_interval = config.endpoint_gossip_interval;
        let shared = Arc::new(Shared {
            config,
            inbound: inbound_tx,
            conns: Mutex::new(HashMap::new()),
            endpoints: Mutex::new(endpoints),
            shutdown: watch::channel(false).0,
        });
        tokio::spawn(accept_loop(listener, Arc::clone(&shared)));
        tokio::spawn(endpoint_gossip(Arc::clone(&shared), gossip_interval));
        (TcpTransport { shared }, inbound_rx)
    }

    /// Get a sender to `peer`'s outbound queue, dialing and handshaking if no
    /// live connection exists. The connect and handshake are bounded by timeouts
    /// so a black-holed or silent peer surfaces as `Unreachable` instead of
    /// hanging the caller (spec §7).
    async fn connection(&self, peer: NodeId) -> Result<mpsc::Sender<Wire>, TransportError> {
        // Fast path: reuse a live connection.
        if let Some(tx) = self.lookup(peer) {
            return Ok(tx);
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
        // (possibly encrypted) stream — all under one handshake deadline. Either
        // way the connection task is generic over the resulting stream type.
        match &self.shared.config.tls {
            Some(tls) => {
                let handshake = async {
                    let stream = tls.connector.connect(tls.server_name.clone(), tcp).await?;
                    client_handshake(stream, &self.shared, peer).await
                };
                let stream = tokio::time::timeout(handshake_timeout, handshake)
                    .await
                    .map_err(|_| TransportError::Unreachable)?
                    .map_err(|_| TransportError::Unreachable)?;
                Ok(self.register_dialed(peer, stream))
            }
            None => {
                let stream = tokio::time::timeout(
                    handshake_timeout,
                    client_handshake(tcp, &self.shared, peer),
                )
                .await
                .map_err(|_| TransportError::Unreachable)?
                .map_err(|_| TransportError::Unreachable)?;
                Ok(self.register_dialed(peer, stream))
            }
        }
    }

    fn lookup(&self, peer: NodeId) -> Option<mpsc::Sender<Wire>> {
        let conns = self.shared.conns.lock().expect("conns mutex poisoned");
        conns.get(&peer).filter(|tx| !tx.is_closed()).cloned()
    }

    /// Register a freshly dialed connection, or, if a concurrent dial already
    /// won, keep the existing one and let this stream drop (clean teardown).
    fn register_dialed<S>(&self, peer: NodeId, stream: S) -> mpsc::Sender<Wire>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (tx, rx) = mpsc::channel(self.shared.config.outbound_capacity);
        let mut conns = self.shared.conns.lock().expect("conns mutex poisoned");
        if let Some(existing) = conns.get(&peer).filter(|tx| !tx.is_closed()) {
            return existing.clone();
        }
        conns.insert(peer, tx.clone());
        drop(conns);
        tokio::spawn(dialed_connection(
            stream,
            rx,
            peer,
            Arc::clone(&self.shared),
            tx.clone(),
        ));
        tx
    }
}

impl Transport for TcpTransport {
    async fn send(&self, peer: NodeId, frame: Frame) -> Result<(), TransportError> {
        let tx = self.connection(peer).await?;
        // Non-blocking enqueue with backpressure: a full queue (peer too slow) or
        // a closed one (connection died) both surface as `Unreachable`, and the
        // dead/saturated entry is dropped so the next send re-dials. At-most-once
        // — we never resend this frame (spec §7.2); a dropped frame an `ask` was
        // awaiting completes it as `Unreachable`, so nothing is silently lost.
        tx.try_send(Wire::Frame(frame)).map_err(|_| {
            let mut conns = self.shared.conns.lock().expect("conns mutex poisoned");
            if conns.get(&peer).is_some_and(|cur| cur.same_channel(&tx)) {
                conns.remove(&peer);
            }
            TransportError::Unreachable
        })
    }

    /// Stop the node's transport (spec §9.3): signal the accept, gossip, and
    /// connection tasks to exit (releasing the listener and open sockets), close
    /// the inbound path so the receive loop ends, and drop the outbound queues.
    /// Idempotent.
    fn shutdown(&self) {
        let _ = self.shared.shutdown.send(true);
        self.shared.inbound.close();
        self.shared
            .conns
            .lock()
            .expect("conns mutex poisoned")
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
            _ = shutdown.changed() => return, // node stopping (spec §9.3)
        }
    }
}

/// Handshake an accepted connection, then read frames from it until it closes.
/// A failed TLS or application handshake tears down the association, never the
/// node (spec §7, §15).
async fn serve_accepted(tcp: TcpStream, shared: Arc<Shared>) {
    // Bound the whole accept-side handshake so a peer that connects but never
    // speaks (slowloris) cannot tie up this task (spec §7, §15).
    let handshake_timeout = shared.config.handshake_timeout;
    match shared.config.tls.clone() {
        Some(tls) => {
            let handshake = async {
                let stream = tls.acceptor.accept(tcp).await.map_err(|_| ())?;
                server_handshake(stream, &shared).await.map_err(|_| ())
            };
            if let Ok(Ok((stream, peer))) = tokio::time::timeout(handshake_timeout, handshake).await
            {
                read_loop(stream, peer, &shared).await;
            }
        }
        None => {
            if let Ok(Ok((stream, peer))) =
                tokio::time::timeout(handshake_timeout, server_handshake(tcp, &shared)).await
            {
                read_loop(stream, peer, &shared).await;
            }
        }
    }
}

/// A dialed connection, run as two independent halves over a split stream: a
/// **writer** drains the outbound queue, and a **reader** consumes anything the
/// peer sends (normally only its close — replies and gossip come over the peer's
/// own dialed connection). Splitting avoids a `select!` that would cancel a
/// partially-read frame and desync the stream. The connection ends when either
/// half finishes (queue closed, or socket error/EOF), and the peer is then
/// deregistered so the next send re-dials.
async fn dialed_connection<S>(
    stream: S,
    mut rx: mpsc::Receiver<Wire>,
    peer: NodeId,
    shared: Arc<Shared>,
    self_tx: mpsc::Sender<Wire>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let codec_w = shared.config.codec.clone();
    let codec_r = shared.config.codec.clone();
    let reader_shared = Arc::clone(&shared);

    let writer = async move {
        while let Some(msg) = rx.recv().await {
            if write_wire(&mut write_half, &codec_w, &msg).await.is_err() {
                break;
            }
        }
    };
    let reader = async move {
        // Reads until a malformed frame or the peer closes (the `Err` exit).
        while let Ok(msg) = read_wire(&mut read_half, &codec_r).await {
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
        _ = shutdown.changed() => {} // node stopping (spec §9.3)
    }

    // Deregister so the next send re-dials.
    let mut conns = shared.conns.lock().expect("conns mutex poisoned");
    if conns
        .get(&peer)
        .is_some_and(|cur| cur.same_channel(&self_tx))
    {
        conns.remove(&peer);
    }
}

/// Read messages from a receive-only (accepted) connection until it closes.
async fn read_loop<S>(mut stream: S, peer: NodeId, shared: &Arc<Shared>)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let codec = shared.config.codec.clone();
    let mut shutdown = shared.shutdown.subscribe();
    loop {
        tokio::select! {
            framed = read_wire(&mut stream, &codec) => match framed {
                Ok(msg) => {
                    if !deliver(shared, peer, msg).await {
                        return;
                    }
                }
                Err(_) => return, // malformed frame or peer closed
            },
            _ = shutdown.changed() => return, // node stopping (spec §9.3)
        }
    }
}

/// Dispatch an inbound [`Wire`] message: actor frames go up to the cluster's
/// receive loop; an endpoint table is merged into the address book and consumed
/// here (spec §9.3). Returns `false` if the cluster's receive side is gone.
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
/// peer's `Hello`, confirming it is the node we meant to reach — and learn its
/// advertised address (spec §9.3).
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
/// advertised address (spec §9.3).
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

/// Endpoint anti-entropy (spec §9.3): every `interval`, push the known
/// `(node, address)` table to each known peer, dialing it if not already
/// connected. A node that holds only a seed or two thereby learns the addresses
/// of peers it never directly contacted, so the address book self-assembles
/// across the cluster — gossip is the bootstrap, so it must be able to dial.
///
/// The per-peer sends run **concurrently** (and each dial is bounded by
/// [`CONNECT_TIMEOUT`]), so one black-holed peer can never stall gossip to the
/// live ones; awaiting them together caps in-flight dials at one per peer, with
/// no cross-interval pile-up. A non-blocking `try_send` keeps a slow but
/// connected peer from stalling the loop.
async fn endpoint_gossip(shared: Arc<Shared>, interval: Duration) {
    let transport = TcpTransport {
        shared: Arc::clone(&shared),
    };
    let mut shutdown = shared.shutdown.subscribe();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => return, // node stopping (spec §9.3)
        }
        let table = shared.endpoint_table();
        let sends = table
            .iter()
            .map(|(n, _)| *n)
            .filter(|n| *n != shared.config.node)
            .map(|peer| {
                let transport = transport.clone();
                let table = table.clone();
                async move {
                    if let Ok(tx) = transport.connection(peer).await {
                        let _ = tx.try_send(Wire::Endpoints(table));
                    }
                }
            });
        futures::future::join_all(sends).await;
    }
}
