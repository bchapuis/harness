//! Cluster runtime for the distributed actor framework (spec §7–§13).
//!
//! Provides [`ClusterSystem`], the networked reference
//! [`ActorSystem`](actor_core::ActorSystem), the [`Transport`] boundary it
//! routes over, and a SWIM failure detector that maintains [`Membership`]
//! reachability and drives the node-down cascade (spec §4, §7, §8.1, §10). It
//! also disseminates membership by gossip, refutes suspicion by incarnation,
//! prunes via death watch, and runs the receptionist with broadcast-on-change
//! plus periodic anti-entropy (spec §9.2, §12, §13). Failure detection uses
//! direct and indirect SWIM probing (spec §10).
//!
//! Who decides the member set is the configurable [`MembershipMode`] (spec
//! §9.4): a fixed **static** roster (§9.4.1), an external **registry** behind
//! the [`RegistryClient`] seam (§9.4.2), a self-hosted Raft log behind an
//! elected **leader** (§9.4.3), or peer-to-peer **gossip** with a deterministic
//! coordinator (§9.4.4).

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

pub use consensus::RaftLog;
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
pub use protocol::ReceptionistEntry;
pub use raft::Committed;
pub use raft::EntryPayload;
pub use raft::GroupId;
pub use raft::InMemoryRaftStorage;
pub use raft::PersistedRaft;
pub use raft::RaftConfig;
pub use raft::RaftEntry;
pub use raft::RaftStorage;
pub use registry::InMemoryRegistry;
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
