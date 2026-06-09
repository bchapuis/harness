//! A single cluster node as its own OS process, talking over mutual-TLS TCP
//! (spec §4.6, §7, §15). Run several copies to form a real multi-process
//! cluster; they discover each other's actors through the receptionist (§13).
//!
//! All nodes share one TLS identity (CA + cert), so generate it once and point
//! every process at the same directory:
//!
//! ```text
//! # 1. Generate shared TLS material into ./certs
//! cargo run -p actor-runtime --example node -- gen-certs ./certs
//!
//! # 2. Start a server node (id 1) that hosts and registers a greeter
//! cargo run -p actor-runtime --example node -- run \
//!     --id 1 --bind 127.0.0.1:9001 --certs ./certs \
//!     --peer 1=127.0.0.1:9001 --peer 2=127.0.0.1:9002 \
//!     --serve "Hello"
//!
//! # 3. In another terminal, a node (id 2) *joins* knowing only the seed (node
//! #    1), is admitted to the cluster by the leader, then looks the greeter up
//! #    and asks it — all over the wire.
//! cargo run -p actor-runtime --example node -- run \
//!     --id 2 --bind 127.0.0.1:9002 --certs ./certs \
//!     --peer 1=127.0.0.1:9001 --peer 2=127.0.0.1:9002 \
//!     --join --ask "world"
//! ```
//!
//! The joiner prints that it is `joining`, then that it was `admitted … as Up`
//! (the leader, node 1, admitted it on convergence, spec §9.3), then
//! `Hello, world!` — obtained from the greeter in the other process over a
//! TLS-encrypted TCP association. Drop `--join` to start as a founding member
//! instead.

// A production-runtime demo: it legitimately reads the wall clock for its own
// run deadline (it is not part of the deterministic simulation build). The
// workspace determinism lint (§18.1) is allowed here for that reason.
#![allow(clippy::disallowed_methods)]

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::Path as FsPath;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actor_cluster::ClusterConfig;
use actor_cluster::ClusterSystem;
use actor_cluster::DowningPolicy;
use actor_cluster::GossipMode;
use actor_cluster::MemberStatus;
use actor_cluster::MembershipMode;
use actor_cluster::SwimConfig;
use actor_core::Actor;
use actor_core::ActorSystem;
use actor_core::Clock;
use actor_core::Ctx;
use actor_core::Handler;
use actor_core::HandlerRegistry;
use actor_core::Key;
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

const CLUSTER_DNS: &str = "cluster.local";
const SECRET: &str = "demo-cluster-secret";

/// The well-known receptionist key the greeter registers under, so any node can
/// discover it without knowing its `ActorId` up front (spec §13).
const GREETERS: Key<Greeter<TcpCluster>> = Key::new("greeters");

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

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("gen-certs") => {
            let dir = args
                .get(2)
                .unwrap_or_else(|| usage("gen-certs needs a directory"));
            gen_certs(FsPath::new(dir));
        }
        Some("run") => run(&args[2..]).await,
        _ => usage("expected a subcommand: `gen-certs` or `run`"),
    }
}

fn usage(msg: &str) -> ! {
    eprintln!("error: {msg}\n");
    eprintln!("usage:");
    eprintln!("  node gen-certs <dir>");
    eprintln!(
        "  node run --id <uid> --bind <addr> --certs <dir> \\\n           --peer <uid=addr> [--peer ...] [--join] (--serve <greeting> | --ask <name> | --relay)"
    );
    std::process::exit(2);
}

// --- run mode ----------------------------------------------------------------

async fn run(args: &[String]) {
    let opts = Options::parse(args);
    let tls = load_tls(&opts.certs);

    let listener = TcpListener::bind(opts.bind)
        .await
        .unwrap_or_else(|e| panic!("cannot bind {}: {e}", opts.bind));
    let codec: Arc<dyn Codec> = Arc::new(JsonCodec);
    let advertised = opts.bind;
    let config = TcpConfig {
        node: opts.id,
        advertised,
        peers: opts.peers.clone(),
        endpoint_gossip_interval: Duration::from_secs(1),
        connect_timeout: actor_runtime::DEFAULT_CONNECT_TIMEOUT,
        handshake_timeout: actor_runtime::DEFAULT_HANDSHAKE_TIMEOUT,
        outbound_capacity: actor_runtime::DEFAULT_OUTBOUND_CAPACITY,
        codec: Arc::clone(&codec),
        cluster_secret: SECRET.to_string(),
        allowlist: None,
        tls: Some(tls),
    };
    let (transport, inbound) = TcpTransport::start(config, listener);
    let system = ClusterSystem::start(
        opts.id,
        TokioClock::new(),
        OsEntropy::new(),
        TokioSpawner::current(),
        transport,
        inbound,
        ClusterConfig {
            codec,
            mailbox_capacity: 64,
            events: Arc::new(()),
            membership: MembershipMode::Gossip(GossipMode {
                swim: SwimConfig {
                    probe_interval: Duration::from_millis(500),
                    rtt: Duration::from_millis(250),
                    suspect_timeout: Duration::from_secs(2),
                    ..SwimConfig::default()
                },
                downing: DowningPolicy::Conservative,
            }),
            // A joiner enters Joining and is admitted to Up by the coordinator once it
            // gossips itself in via its seeds (spec §9.3).
            joining: opts.join,
            authorizer: None,
        },
    );
    // Every configured peer is a member from the start: for a founding node the
    // full roster, for a joiner just its seed(s). The failure detector probes
    // them and gossip/registrations flow to them.
    for &peer in opts.peers.keys() {
        if peer != opts.id {
            system.add_member(peer);
        }
    }

    // When joining, narrate the admission so it can be watched over the wire.
    if opts.join {
        println!("node {} joining via its seed(s)…", system.node());
        let watch = system.clone();
        tokio::spawn(async move {
            loop {
                if watch.membership().self_status() == MemberStatus::Up {
                    println!(
                        "node {} admitted to the cluster as Up (leader: {:?})",
                        watch.node(),
                        watch.leader(),
                    );
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
    }

    match opts.mode {
        Mode::Serve(greeting) => serve(system, greeting).await,
        Mode::Ask(name) => ask(system, name).await,
        Mode::Relay => {
            println!(
                "node {} relaying (membership + endpoint gossip)",
                system.node()
            );
            std::future::pending().await
        }
    }
}

/// Host a greeter, register it once, and stay alive serving requests until
/// killed. A single registration is enough: the receptionist's anti-entropy
/// (spec §13) pushes it to peers that join later or missed the initial
/// broadcast, so no periodic re-registration is needed.
async fn serve(system: TcpCluster, greeting: String) -> ! {
    let greeter = system.spawn(Greeter::<TcpCluster>::new(greeting));
    system.receptionist().register(GREETERS, &greeter);
    println!(
        "node {} serving greeter {} — registered under {:?}",
        system.node(),
        greeter.id(),
        "greeters"
    );
    std::future::pending().await
}

/// Discover the greeter through the receptionist (polling until it replicates
/// here), ask it, print the reply, and exit.
async fn ask(system: TcpCluster, name: String) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let listing = system.receptionist().lookup(GREETERS);
        if let Some(greeter) = listing.first() {
            match greeter.ask(Greet { name: name.clone() }).await {
                Ok(reply) => {
                    println!("{reply}");
                    return;
                }
                Err(e) => eprintln!("ask failed ({e:?}), retrying…"),
            }
        }
        if std::time::Instant::now() >= deadline {
            eprintln!("no greeter discovered within the timeout");
            std::process::exit(1);
        }
        system.clock().sleep(Duration::from_millis(500)).await;
    }
}

// --- options -----------------------------------------------------------------

enum Mode {
    Serve(String),
    Ask(String),
    /// Run purely as a seed/relay: stay alive so membership and endpoint gossip
    /// flow through this node, without hosting or asking for any actor.
    Relay,
}

struct Options {
    id: NodeId,
    bind: SocketAddr,
    certs: PathBuf,
    peers: BTreeMap<NodeId, SocketAddr>,
    mode: Mode,
    /// Start as a joiner (Joining → admitted to Up by the leader, spec §9.3)
    /// rather than a founding Up member.
    join: bool,
}

impl Options {
    fn parse(args: &[String]) -> Options {
        let mut id = None;
        let mut bind = None;
        let mut certs = None;
        let mut peers = BTreeMap::new();
        let mut mode = None;
        let mut join = false;

        let mut i = 0;
        while i < args.len() {
            let flag = args[i].as_str();
            let mut value = || {
                i += 1;
                args.get(i)
                    .cloned()
                    .unwrap_or_else(|| usage(&format!("{flag} needs a value")))
            };
            match flag {
                "--id" => {
                    id = Some(NodeId::new(
                        value()
                            .parse()
                            .unwrap_or_else(|_| usage("--id must be a number")),
                    ))
                }
                "--bind" => {
                    bind = Some(
                        value()
                            .parse()
                            .unwrap_or_else(|_| usage("--bind must be host:port")),
                    )
                }
                "--certs" => certs = Some(PathBuf::from(value())),
                "--peer" => {
                    let spec = value();
                    let (uid, addr) = spec
                        .split_once('=')
                        .unwrap_or_else(|| usage("--peer must be <uid>=<host:port>"));
                    let uid: u64 = uid
                        .parse()
                        .unwrap_or_else(|_| usage("--peer uid must be a number"));
                    let addr: SocketAddr = addr
                        .parse()
                        .unwrap_or_else(|_| usage("--peer addr must be host:port"));
                    peers.insert(NodeId::new(uid), addr);
                }
                "--serve" => mode = Some(Mode::Serve(value())),
                "--ask" => mode = Some(Mode::Ask(value())),
                "--relay" => mode = Some(Mode::Relay),
                "--join" => join = true,
                other => usage(&format!("unknown flag: {other}")),
            }
            i += 1;
        }

        Options {
            id: id.unwrap_or_else(|| usage("--id is required")),
            bind: bind.unwrap_or_else(|| usage("--bind is required")),
            certs: certs.unwrap_or_else(|| usage("--certs is required")),
            peers: {
                if peers.is_empty() {
                    usage("at least one --peer is required");
                }
                peers
            },
            mode: mode.unwrap_or_else(|| usage("one of --serve, --ask, or --relay is required")),
            join,
        }
    }
}

// --- TLS material on disk ----------------------------------------------------

/// Generate a CA and one shared cluster leaf cert/key, writing them as DER files
/// into `dir`. Every node loads the same three files.
fn gen_certs(dir: &FsPath) {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    std::fs::create_dir_all(dir).expect("cannot create certs dir");

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

    std::fs::write(dir.join("ca.der"), ca_cert.der()).unwrap();
    std::fs::write(dir.join("node.der"), leaf_cert.der()).unwrap();
    std::fs::write(dir.join("node-key.der"), leaf_key.serialize_der()).unwrap();
    println!("wrote ca.der, node.der, node-key.der to {}", dir.display());
}

/// Build the mutual-TLS config from the DER files written by `gen-certs`.
fn load_tls(dir: &FsPath) -> TlsConfig {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let read = |name: &str| {
        std::fs::read(dir.join(name)).unwrap_or_else(|e| panic!("cannot read {name}: {e}"))
    };
    let ca = CertificateDer::from(read("ca.der"));
    let leaf = CertificateDer::from(read("node.der"));
    let key_bytes = read("node-key.der");
    let key = || PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_bytes.clone()));

    let mut roots = RootCertStore::empty();
    roots.add(ca).unwrap();

    let verifier = WebPkiClientVerifier::builder(Arc::new(roots.clone()))
        .build()
        .unwrap();
    let server = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![leaf.clone()], key())
        .unwrap();
    let client = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(vec![leaf], key())
        .unwrap();

    TlsConfig {
        acceptor: TlsAcceptor::from(Arc::new(server)),
        connector: TlsConnector::from(Arc::new(client)),
        server_name: ServerName::try_from(CLUSTER_DNS).unwrap(),
    }
}
