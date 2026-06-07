//! Cluster runtime for the distributed actor framework (spec §7–§13).
//!
//! Provides [`ClusterSystem`], the networked reference
//! [`ActorSystem`](actor_core::ActorSystem), the [`Transport`] boundary it
//! routes over, and a SWIM failure detector that maintains [`Membership`]
//! reachability and drives the node-down cascade (spec §4, §7, §8.1, §10). It
//! also disseminates membership by gossip, refutes suspicion by incarnation,
//! prunes via death watch, and runs the receptionist with broadcast-on-change
//! plus periodic anti-entropy (spec §9.2, §12, §13). Indirect SWIM probing and
//! leader-based up/down (spec §10, §9.2) arrive in later slices.

mod membership;
mod system;
mod transport;

pub use membership::DowningPolicy;
pub use membership::MemberStatus;
pub use membership::Membership;
pub use membership::Reachability;
pub use membership::SwimConfig;
pub use system::Authorizer;
pub use system::ClusterConfig;
pub use system::ClusterSystem;
pub use transport::CallId;
pub use transport::Frame;
pub use transport::ReceptionistEntry;
pub use transport::Transport;
pub use transport::TransportError;
