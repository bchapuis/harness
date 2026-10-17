//! Cluster runtime for the distributed actor framework: [`ClusterSystem`], the
//! networked [`ActorSystem`](actor_core::ActorSystem), with its membership
//! control planes, failure detector, consensus engine, and the cluster
//! utilities layered on it.
//!
//! # Seams
//!
//! Implemented here, for the layers above:
//!
//! - [`ClusterSystem`] implements `actor-core`'s `ActorSystem` over a
//!   [`Transport`], built on `actor_core::host::LocalHost`.
//! - [`RaftConsensus`]: the application consensus capability (create Raft
//!   groups, propose commands, subscribe to commits, read terms and leaders),
//!   which entity uses for its shard map and per-shard groups.
//! - [`Membership`], read through [`ClusterSystem::membership`]: the local
//!   view, its serving set, and the rotation filter.
//! - The utilities: [`placement`] (rendezvous owner and ranking), [`Router`],
//!   and [`ClusterSystem::singleton`] with its [`SingletonProxy`].
//!
//! Defined here, for a deployment or the simulator to implement or choose:
//!
//! - [`Transport`]: carries [`Frame`]s between nodes; the TCP transport
//!   (`actor-runtime`) and the in-memory network (`actor-simulation`)
//!   implement it. [`WIRE`] versions the frame vocabulary.
//! - [`MembershipMode`] with [`SwimConfig`], [`RaftConfig`], and
//!   [`DowningPolicy`]: which control plane decides the member set.
//! - [`RegistryClient`]: the external registry of registry-based mode.
//! - [`RaftWAL`]: a voter's durable Raft state, answering each write with a
//!   [`WalAck`]; `actor-runtime` supplies the file-backed one.
//! - [`Authorizer`]: per-message admission of inbound envelopes.
//!
//! # Mechanism
//!
//! A node runs one receive loop over the transport's inbound channel
//! (`system`): it gates envelopes through the authorizer, hands them to the
//! local host, relays replies, answers probes, and applies death-watch,
//! receptionist, and Raft frames. Outbound asks wait on a correlator keyed by
//! call id, so the node-down cascade can complete every pending call to a
//! downed node at once. A SWIM detector loop (`system`, with the view and its
//! merge rules in `membership`) probes one member per interval, directly and
//! then through helpers, piggybacks a membership digest on every probe, and
//! confirms or refutes suspicion by incarnation. Receptionist registrations
//! are broadcast on change and reconciled by periodic anti-entropy.
//!
//! The control plane runs as one more loop per mode: a registry sync loop, a
//! Raft driver over the multi-group engine (`raft`, one group per
//! [`GroupId`], with heartbeats to one peer batched into one frame), or the
//! gossip coordinator's lifecycle pass. Every timer and random draw comes from
//! the system's `Clock` and `Entropy`, so the whole crate runs unchanged under
//! simulation.
//!
//! Specified by `docs/distributed-actor-spec.md` and, for the utilities,
//! `docs/cluster-utilities-spec.md`.

mod consensus;
mod correlator;
mod membership;
pub mod placement;
mod protocol;
mod raft;
mod registry;
mod router;
mod singleton;
mod system;
mod transport;

pub use consensus::RaftConsensus;
pub use membership::DowningPolicy;
pub use membership::GossipMode;
pub use membership::LeaderMode;
pub use membership::MemberStatus;
pub use membership::Membership;
pub use membership::MembershipCommand;
pub use membership::MembershipMode;
pub use membership::Reachability;
pub use membership::RegistryMode;
pub use membership::SwimConfig;
pub use protocol::CallId;
pub use protocol::Frame;
pub use protocol::WIRE;
pub use raft::Committed;
pub use raft::EntryPayload;
pub use raft::GroupId;
pub use raft::InMemoryRaftWAL;
pub use raft::PersistedRaft;
pub use raft::RaftConfig;
pub use raft::RaftEntry;
pub use raft::RaftWAL;
pub use raft::WalAck;
pub use registry::RegistryClient;
pub use registry::RegistryEntry;
pub use registry::RegistryError;
pub use registry::RegistrySnapshot;
pub use registry::RegistryState;
pub use router::RouteStrategy;
pub use router::Router;
pub use singleton::SingletonProxy;
pub use system::Authorizer;
pub use system::ClusterConfig;
pub use system::ClusterSystem;
pub use transport::Transport;
pub use transport::TransportError;
