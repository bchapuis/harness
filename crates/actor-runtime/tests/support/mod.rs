//! Shared support for the production-runtime integration tests: a couple of
//! generic actors and helpers that stand up TCP-wired cluster nodes on loopback.

#![allow(dead_code)]

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::DowningPolicy;
use actor_cluster::GossipMode;
use actor_cluster::MembershipMode;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Manifest;
use actor_core::Message;
use actor_core::NodeId;
use actor_runtime::OsEntropy;
use actor_runtime::TcpCluster;
use actor_runtime::TcpConfig;
use actor_runtime::TcpTransport;
use actor_runtime::TlsConfig;
use actor_runtime::TokioClock;
use actor_runtime::TokioSpawner;
use actor_serialization::Codec;
use actor_serialization::JsonCodec;
use serde::Deserialize;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;
use tokio_rustls::rustls::RootCertStore;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::CertificateDer;
use tokio_rustls::rustls::pki_types::PrivateKeyDer;
use tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::server::WebPkiClientVerifier;

// --- Messages and actors (generic over the system, as the spec allows) -------

#[derive(Serialize, Deserialize)]
pub struct Greet {
    pub name: String,
}
impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("rt.Greet");
}

#[derive(Serialize, Deserialize)]
pub struct Inc;
impl Message for Inc {
    type Reply = ();
    const MANIFEST: Manifest = Manifest::new("rt.Inc");
}

#[derive(Serialize, Deserialize)]
pub struct Get;
impl Message for Get {
    type Reply = u64;
    const MANIFEST: Manifest = Manifest::new("rt.Get");
}

pub struct Greeter<S> {
    pub greeting: String,
    _system: PhantomData<fn() -> S>,
}
impl<S> Greeter<S> {
    pub fn new(greeting: impl Into<String>) -> Greeter<S> {
        Greeter {
            greeting: greeting.into(),
            _system: PhantomData,
        }
    }
}
impl<S: ActorSystem> Actor for Greeter<S> {
    type System = S;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Greet>();
    }
}
impl<S: ActorSystem> Handler<Greet> for Greeter<S> {
    async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> String {
        format!("{}, {}!", self.greeting, msg.name)
    }
}

pub struct Counter<S> {
    pub count: u64,
    _system: PhantomData<fn() -> S>,
}
impl<S> Counter<S> {
    pub fn new() -> Counter<S> {
        Counter {
            count: 0,
            _system: PhantomData,
        }
    }
}
impl<S> Default for Counter<S> {
    fn default() -> Self {
        Counter::new()
    }
}
impl<S: ActorSystem> Actor for Counter<S> {
    type System = S;
    fn register(r: &mut HandlerRegistry<Self>) {
        r.accept::<Inc>();
        r.accept::<Get>();
    }
}
impl<S: ActorSystem> Handler<Inc> for Counter<S> {
    async fn handle(&mut self, _msg: Inc, _ctx: &Ctx<Self>) {
        self.count += 1;
    }
}
impl<S: ActorSystem> Handler<Get> for Counter<S> {
    async fn handle(&mut self, _msg: Get, _ctx: &Ctx<Self>) -> u64 {
        self.count
    }
}

/// A JSON codec under a chosen `name`, so a test can stand up two ends that
/// disagree on the codec (spec §5 #2, §7.1). Encoding and decoding delegate to
/// the bundled JSON codec; only the advertised name differs.
pub struct NamedJson(pub &'static str);

impl Codec for NamedJson {
    fn name(&self) -> &'static str {
        self.0
    }
    fn encode_erased(
        &self,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Vec<u8>, actor_serialization::CodecError> {
        JsonCodec.encode_erased(value)
    }
    fn with_deserializer(
        &self,
        bytes: &[u8],
        f: &mut dyn FnMut(&mut dyn erased_serde::Deserializer),
    ) {
        JsonCodec.with_deserializer(bytes, f)
    }
}

// --- Node wiring -------------------------------------------------------------

/// The shared cluster secret the helpers use unless a test overrides it.
pub const SECRET: &str = "test-cluster";

/// Bind an ephemeral loopback listener and report its resolved address.
pub async fn bind_local() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

/// Build a `TcpConfig` with the bundled JSON codec and the shared secret.
pub fn tcp_config(node: NodeId, peers: BTreeMap<NodeId, SocketAddr>) -> TcpConfig {
    let advertised = *peers.get(&node).expect("self must be in the seed peers");
    TcpConfig {
        node,
        advertised,
        peers,
        endpoint_gossip_interval: Duration::from_millis(50),
        connect_timeout: actor_runtime::DEFAULT_CONNECT_TIMEOUT,
        handshake_timeout: actor_runtime::DEFAULT_HANDSHAKE_TIMEOUT,
        outbound_capacity: actor_runtime::DEFAULT_OUTBOUND_CAPACITY,
        codec: Arc::new(JsonCodec),
        cluster_secret: SECRET.to_string(),
        allowlist: None,
        tls: None,
    }
}

/// Start a cluster node on `listener` with the production seam and a TCP
/// transport built from `cfg`. SWIM is disabled.
pub fn start_node(cfg: TcpConfig, listener: TcpListener) -> TcpCluster {
    start_node_cfg(cfg, listener, None)
}

/// Like [`start_node`], but with SWIM failure detection enabled.
pub fn start_node_swim(cfg: TcpConfig, listener: TcpListener, swim: SwimConfig) -> TcpCluster {
    start_node_cfg(cfg, listener, Some(swim))
}

fn start_node_cfg(cfg: TcpConfig, listener: TcpListener, swim: Option<SwimConfig>) -> TcpCluster {
    let node = cfg.node;
    let codec: Arc<dyn Codec> = Arc::clone(&cfg.codec);
    let (transport, inbound) = TcpTransport::start(cfg, listener);
    let membership = match swim {
        Some(swim) => MembershipMode::Gossip(GossipMode {
            swim,
            downing: DowningPolicy::Conservative,
        }),
        None => MembershipMode::Static { detector: None },
    };
    let config = ClusterConfig {
        codec,
        mailbox_capacity: 64,
        events: Arc::new(()),
        membership,
        joining: false,
        authorizer: None,
    };
    ClusterSystem::start(
        node,
        TokioClock::new(),
        OsEntropy::new(),
        TokioSpawner::current(),
        transport,
        inbound,
        config,
    )
}

// --- TLS material (spec §15) -------------------------------------------------

/// The DNS name every node certificate is issued for, so all nodes can share one
/// cluster identity and dial each other under a single server name.
pub const CLUSTER_DNS: &str = "cluster.local";

/// A self-signed certificate authority plus one leaf cert/key issued by it,
/// generated fresh for a test. Two independent authorities model nodes that do
/// not trust each other (the untrusted-cert case).
pub struct TlsAuthority {
    ca_der: CertificateDer<'static>,
    leaf_der: CertificateDer<'static>,
    key_der: Vec<u8>,
}

impl TlsAuthority {
    /// Generate a fresh CA and a leaf certificate (SAN = [`CLUSTER_DNS`]) signed
    /// by it.
    pub fn generate() -> TlsAuthority {
        // The process-wide crypto provider must be installed before building any
        // rustls config; harmless if another test already did it.
        let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

        let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "actor-cluster-ca");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let mut leaf_params = rcgen::CertificateParams::new(vec![CLUSTER_DNS.to_string()]).unwrap();
        leaf_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "actor-cluster-node");
        let leaf_key = rcgen::KeyPair::generate().unwrap();
        let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

        TlsAuthority {
            ca_der: ca_cert.der().clone(),
            leaf_der: leaf_cert.der().clone(),
            key_der: leaf_key.serialize_der(),
        }
    }

    fn private_key(&self) -> PrivateKeyDer<'static> {
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone()))
    }

    fn roots(&self) -> RootCertStore {
        let mut roots = RootCertStore::empty();
        roots.add(self.ca_der.clone()).unwrap();
        roots
    }

    /// A [`TlsConfig`] that presents this authority's leaf cert and trusts only
    /// this authority's CA.
    pub fn tls_config(&self) -> TlsConfig {
        let verifier = WebPkiClientVerifier::builder(Arc::new(self.roots()))
            .build()
            .unwrap();
        let server = ServerConfig::builder()
            .with_client_cert_verifier(verifier)
            .with_single_cert(vec![self.leaf_der.clone()], self.private_key())
            .unwrap();
        let client = ClientConfig::builder()
            .with_root_certificates(self.roots())
            .with_client_auth_cert(vec![self.leaf_der.clone()], self.private_key())
            .unwrap();
        TlsConfig {
            acceptor: TlsAcceptor::from(Arc::new(server)),
            connector: TlsConnector::from(Arc::new(client)),
            server_name: ServerName::try_from(CLUSTER_DNS).unwrap(),
        }
    }
}

/// Stand up two mutually-aware loopback nodes (ids 1 and 2), each config built
/// by the given closure from its node id and the shared address book. Lets a
/// test give each node distinct TLS material, secrets, or allowlists.
pub async fn two_nodes_with<FA, FB>(cfg_a: FA, cfg_b: FB) -> (TcpCluster, TcpCluster)
where
    FA: FnOnce(NodeId, BTreeMap<NodeId, SocketAddr>) -> TcpConfig,
    FB: FnOnce(NodeId, BTreeMap<NodeId, SocketAddr>) -> TcpConfig,
{
    let (la, addr_a) = bind_local().await;
    let (lb, addr_b) = bind_local().await;
    let node_a = NodeId::new(1);
    let node_b = NodeId::new(2);
    let peers: BTreeMap<NodeId, SocketAddr> = BTreeMap::from([(node_a, addr_a), (node_b, addr_b)]);

    let sys_a = start_node(cfg_a(node_a, peers.clone()), la);
    let sys_b = start_node(cfg_b(node_b, peers), lb);
    sys_a.add_member(node_b);
    sys_b.add_member(node_a);
    (sys_a, sys_b)
}

/// Two plaintext loopback nodes with the default config.
pub async fn two_nodes() -> (TcpCluster, TcpCluster) {
    two_nodes_with(tcp_config, tcp_config).await
}
