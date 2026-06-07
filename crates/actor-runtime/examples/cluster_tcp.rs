//! A real TCP cluster on the production runtime (spec §4.6, §7, §15).
//!
//! Brings up three nodes on loopback, wired with the tokio seam
//! ([`TokioClock`]/[`OsEntropy`]/[`TokioSpawner`]) and a mutual-TLS
//! [`TcpTransport`], with SWIM failure detection enabled. It registers a greeter
//! on one node and `ask`s it from another over the encrypted wire, then leaves a
//! configured-but-absent fourth peer down so the failure detector is seen moving
//! it to `unreachable`.
//!
//! Run with: `cargo run -p actor-runtime --example cluster_tcp`

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::Reachability;
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
use tokio_rustls::rustls::pki_types::PrivateKeyDer;
use tokio_rustls::rustls::pki_types::PrivatePkcs8KeyDer;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::server::WebPkiClientVerifier;

const CLUSTER_DNS: &str = "cluster.local";
const SECRET: &str = "demo-cluster-secret";

#[derive(Serialize, Deserialize)]
struct Greet {
    name: String,
}
impl Message for Greet {
    type Reply = String;
    const MANIFEST: Manifest = Manifest::new("demo.Greet");
}

struct Greeter<S> {
    greeting: String,
    _system: PhantomData<fn() -> S>,
}
impl<S> Greeter<S> {
    fn new(greeting: impl Into<String>) -> Greeter<S> {
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

/// Generate a CA and one shared cluster leaf cert, returning TLS material every
/// node can use (it presents the cluster cert and trusts the cluster CA).
fn cluster_tls() -> TlsConfig {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut ca_params = rcgen::CertificateParams::new(Vec::new()).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let mut leaf_params = rcgen::CertificateParams::new(vec![CLUSTER_DNS.to_string()]).unwrap();
    leaf_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "actor-cluster-node");
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();
    let leaf_der = leaf_cert.der().clone();
    let key_der = leaf_key.serialize_der();
    let key = || PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der.clone()));

    let mut roots = RootCertStore::empty();
    roots.add(ca_cert.der().clone()).unwrap();

    let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
        .build()
        .unwrap();
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![leaf_der.clone()], key())
        .unwrap();
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![leaf_der], key())
        .unwrap();

    TlsConfig {
        acceptor: TlsAcceptor::from(Arc::new(server)),
        connector: TlsConnector::from(Arc::new(client)),
        server_name: ServerName::try_from(CLUSTER_DNS).unwrap(),
    }
}

fn start_node(
    node: NodeId,
    peers: BTreeMap<NodeId, SocketAddr>,
    tls: TlsConfig,
    listener: TcpListener,
) -> TcpCluster {
    let codec: Arc<dyn Codec> = Arc::new(JsonCodec);
    let advertised = peers[&node];
    let config = TcpConfig {
        node,
        advertised,
        peers,
        endpoint_gossip_interval: Duration::from_millis(500),
        connect_timeout: actor_runtime::DEFAULT_CONNECT_TIMEOUT,
        handshake_timeout: actor_runtime::DEFAULT_HANDSHAKE_TIMEOUT,
        outbound_capacity: actor_runtime::DEFAULT_OUTBOUND_CAPACITY,
        codec: Arc::clone(&codec),
        cluster_secret: SECRET.to_string(),
        allowlist: None,
        tls: Some(tls),
    };
    let (transport, inbound) = TcpTransport::start(config, listener);
    ClusterSystem::start(
        node,
        TokioClock::new(),
        OsEntropy::new(),
        TokioSpawner::current(),
        transport,
        inbound,
        ClusterConfig {
            codec,
            mailbox_capacity: 64,
            events: Arc::new(()),
            swim: Some(SwimConfig {
                probe_interval: Duration::from_millis(200),
                rtt: Duration::from_millis(150),
                suspect_timeout: Duration::from_millis(600),
                ..SwimConfig::default()
            }),
            joining: false,
            authorizer: None,
        },
    )
}

#[tokio::main]
async fn main() {
    let tls = cluster_tls();

    // Three real nodes (1, 2, 3) plus a configured-but-absent node 4.
    let mut listeners = Vec::new();
    let mut peers: BTreeMap<NodeId, SocketAddr> = BTreeMap::new();
    for uid in 1..=3u64 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        peers.insert(NodeId::new(uid), listener.local_addr().unwrap());
        listeners.push(listener);
    }
    // Node 4: claim a port, then drop the listener so dials are refused.
    let absent = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let absent_node = NodeId::new(4);
    peers.insert(absent_node, absent.local_addr().unwrap());
    drop(absent);

    let mut nodes = Vec::new();
    for (uid, listener) in (1..=3u64).zip(listeners) {
        nodes.push(start_node(
            NodeId::new(uid),
            peers.clone(),
            tls.clone(),
            listener,
        ));
    }
    // Wire every node's roster: all four are members (node 4 will be probed and
    // found unreachable).
    for sys in &nodes {
        for uid in 1..=4u64 {
            if NodeId::new(uid) != sys.node() {
                sys.add_member(NodeId::new(uid));
            }
        }
    }

    // Register a greeter on node 3 and ask it from node 1 over TLS TCP.
    let greeter = nodes[2].spawn(Greeter::<TcpCluster>::new("Hello"));
    let from_node_1 = nodes[0].resolve::<Greeter<TcpCluster>>(greeter.id().clone());
    let reply = from_node_1
        .ask(Greet {
            name: "cluster".into(),
        })
        .await;
    println!("node 1 asked the greeter on node 3 over TLS → {reply:?}");

    // Watch SWIM move the absent node 4 to `unreachable` from node 1's view.
    let view = nodes[0].membership();
    let mut last = None;
    for _ in 0..50 {
        let now = view.reachability(absent_node);
        if now != last {
            println!("node 1 sees node 4 as {now:?}");
            last = now;
        }
        if now == Some(Reachability::Unreachable) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    println!("done.");
}
