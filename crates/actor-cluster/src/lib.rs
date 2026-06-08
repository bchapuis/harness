//! Cluster runtime for the distributed actor framework (spec §7–§13).
//!
//! Provides [`ClusterSystem`], the networked reference
//! [`ActorSystem`](actor_core::ActorSystem), the [`Transport`] boundary it
//! routes over, and a SWIM failure detector that maintains [`Membership`]
//! reachability and drives the node-down cascade (spec §4, §7, §8.1, §10). It
//! also disseminates membership by gossip, refutes suspicion by incarnation,
//! prunes via death watch, and runs the receptionist with broadcast-on-change
//! plus periodic anti-entropy (spec §9.2, §12, §13). Failure detection uses
//! direct and indirect SWIM probing (spec §10), and the join/leave lifecycle is
//! leader-gated (spec §9.2, §9.3); full seen-by gossip-convergence detection
//! remains a follow-up.

mod correlator;
mod membership;
mod protocol;
mod system;
mod transport;

pub use membership::DowningPolicy;
pub use membership::MemberStatus;
pub use membership::Membership;
pub use membership::MembershipMode;
pub use membership::Reachability;
pub use membership::SwimConfig;
pub use system::Authorizer;
pub use system::ClusterConfig;
pub use system::ClusterSystem;
pub use protocol::CallId;
pub use protocol::Frame;
pub use protocol::ReceptionistEntry;
pub use transport::Transport;
pub use transport::TransportError;
