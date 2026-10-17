//! The production runtime for the actor framework: the host environment, the
//! TCP transport, durable Raft storage, and node bring-up and departure.
//!
//! # Seams
//!
//! This crate implements the seams the core and cluster crates define, so a
//! [`ClusterSystem`] runs across processes and hosts with nothing else in the
//! framework changed; the simulator (`actor-simulation`) implements the same
//! seams deterministically.
//!
//! - [`TokioClock`], [`OsEntropy`], [`TokioSpawner`]: the [`Clock`],
//!   [`Entropy`], and [`Spawner`] of the host environment (spec §2.12.1).
//! - [`TcpTransport`]: the [`Transport`] (spec §2.13), over TCP with an optional
//!   mutual-TLS association ([`Encryption`], [`TlsConfig`]), a cluster secret,
//!   and a node allowlist (spec §9).
//! - [`FileRaftWAL`]: the `RaftWAL` of the leader-based control plane, on the
//!   local filesystem (spec §6.4.3).
//!
//! [`join()`] and [`depart`] are the composed bring-up and stop of one
//! production node, [`TcpCluster`] its type.
//!
//! # Mechanism
//!
//! Frames travel length-delimited over TCP under a size cap (`wire`), after a
//! handshake that checks the wire version first and then the codec, secret,
//! allowlist, and expected identity (`transport`). Each directed pair of nodes
//! uses the connection the sender dialed, which keeps per-pair order; the
//! address book is seeded from configuration and grows through handshakes and
//! a periodic endpoint gossip (`transport`, with the decisions in
//! `association`). [`FileRaftWAL`] writes a
//! checksummed, append-only log plus term and snapshot files, fsyncing before
//! it acknowledges, and poisons itself on the first failed write (`storage`).
//!
//! tokio and rustls live here and only here, so the core and cluster crates stay
//! free of any specific async runtime.
//!
//! # Example
//!
//! An actor reachable over the network, a node per process, and a call that
//! finds its target through the receptionist:
//!
//! ```ignore
//! struct Greeter { greeting: String }
//! impl Actor for Greeter {
//!     type System = TcpCluster;
//!     fn register(r: &mut HandlerRegistry<Self>) { r.accept::<Greet>(); }
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Greet { name: String }
//! impl Message for Greet {
//!     type Reply = String;
//!     const MANIFEST: Manifest = Manifest::new("myapp.Greet");
//! }
//! impl Handler<Greet> for Greeter {
//!     async fn handle(&mut self, msg: Greet, _ctx: &Ctx<Self>) -> String {
//!         format!("{}, {}!", self.greeting, msg.name)
//!     }
//! }
//!
//! const GREETERS: Key<Greeter> = Key::new("greeters");
//!
//! async fn node(id: u64) -> Result<TcpCluster, String> {
//!     actor_runtime::join(JoinOptions {
//!         node: id,
//!         voters: 3,                       // the roster is nodes 1..=3
//!         peer_hosts: BTreeMap::new(),     // loopback here
//!         clients: BTreeMap::new(),
//!         bind_host: "127.0.0.1".into(),
//!         advertise_host: None,
//!         port_base: 7000,                 // node `i` listens on 7000 + i - 1
//!         secret: "cluster-secret".into(),
//!         codec: Arc::new(PostcardCodec),
//!         events: Arc::new(()),
//!         encryption: Encryption::PlaintextTrusted,
//!         membership: Box::new(|roster| MembershipMode::Leader(LeaderMode {
//!             swim: SwimConfig::default(),
//!             raft: RaftConfig::new(roster.to_vec()),
//!             downing: DowningPolicy::Conservative,
//!         })),
//!     })
//!     .await
//! }
//!
//! // On the host node:
//! let system = node(1).await?;
//! let greeter = system.spawn(Greeter { greeting: "Hello".into() });
//! system.receptionist().register(GREETERS, &greeter);
//!
//! // On a client node: the same call site whether `greeter` is local or remote.
//! let system = node(2).await?;
//! if let Some(greeter) = system.receptionist().lookup(GREETERS).first() {
//!     match greeter.ask(Greet { name: "world".into() }).await {
//!         Ok(reply) => println!("{reply}"),
//!         Err(CallError::Unreachable) => { /* the peer is down */ }
//!         Err(e) => eprintln!("call failed: {e}"),
//!     }
//! }
//! ```
//!
//! Specified by `docs/distributed-actor-spec.md`.
//!
//! [`Clock`]: actor_core::Clock
//! [`Entropy`]: actor_core::Entropy
//! [`Spawner`]: actor_core::Spawner
//! [`Transport`]: actor_cluster::Transport
//! [`ClusterSystem`]: actor_cluster::ClusterSystem

// This crate IS the host seam (spec §2.12.1, §2.12.2): the one place allowed to read
// the wall clock, spawn OS/tokio tasks, and seed from the OS. The workspace
// `clippy.toml` forbids those host APIs everywhere else to keep the simulation
// build deterministic; this allow is the determinism boundary.
#![allow(clippy::disallowed_methods)]

mod association;
mod clock;
mod entropy;
mod join;
mod lifecycle;
mod spawner;
mod storage;
mod transport;
mod wire;

pub use clock::TokioClock;
pub use entropy::OsEntropy;
pub use join::JoinOptions;
pub use join::join;
pub use join::wait_for_cluster;
pub use lifecycle::depart;
pub use lifecycle::shutdown_signal;
pub use spawner::TokioSpawner;
pub use storage::FileRaftWAL;
pub use transport::Encryption;
pub use transport::TcpConfig;
pub use transport::TcpTransport;
pub use transport::TlsConfig;
pub use transport::node_addr;

/// A cluster node wired to the production runtime: tokio time and scheduling,
/// OS-seeded entropy, and the TCP transport. The production counterpart of
/// `actor_simulation::SimNode`.
pub type TcpCluster =
    actor_cluster::ClusterSystem<TokioClock, OsEntropy, TokioSpawner, TcpTransport>;
