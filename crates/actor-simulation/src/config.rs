//! The control-plane timings a simulated cluster runs under (spec §18.2).
//!
//! [`SwimConfig`] and [`RaftConfig`] default to production cadences (seconds of
//! probe interval, a one-second election timeout) chosen for a real network where
//! a false suspicion costs a rehost. A simulated run wants the opposite trade: the
//! scenarios are failovers and partitions, virtual time only advances when the
//! clock is asked to, and a test that waits three seconds of virtual time for a
//! suspicion to mature spends its budget on the detector rather than on the
//! property it is checking.
//!
//! So every clustered suite in the tree runs the same tightened numbers. They were
//! written out once per suite (forty-odd copies of two struct literals) until one
//! of them silently lost its `heartbeat_interval` and nothing noticed, which is the
//! failure mode of a constant kept in forty places. They live here now, beside the
//! seams they configure, so tuning them is one edit and a suite that needs its own
//! numbers says so by not calling these.
//!
//! These are the *floor*, not a guarantee: a debug build is slow enough that a
//! cluster under load still flaps if a suite pushes the timings lower.

use std::time::Duration;

use actor_cluster::RaftConfig;
use actor_cluster::SwimConfig;
use actor_core::NodeId;

/// SWIM timings for a simulated cluster: probe every 100 ms, 50 ms for an ack,
/// and a suspicion matures in 300 ms.
///
/// The suspect timeout is the one that shows up in test runtimes, because it is
/// what a partition scenario waits out before the roster moves. It scales
/// logarithmically with cluster size (utilities §10), so it is a base rather than
/// a bound.
pub fn swim_config() -> SwimConfig {
    SwimConfig {
        probe_interval: Duration::from_millis(100),
        rtt: Duration::from_millis(50),
        suspect_timeout: Duration::from_millis(300),
        indirect_count: 2,
    }
}

/// The election timeout a simulated control group runs with.
///
/// Exposed because [`ClusterModeSpec::Leader`](crate::ClusterModeSpec) takes the
/// two timings as fields rather than a whole [`RaftConfig`], so a swarm and a
/// hand-built network must be able to name the same numbers.
pub const ELECTION_TIMEOUT: Duration = Duration::from_millis(500);

/// The leader heartbeat cadence a simulated control group runs with. See
/// [`ELECTION_TIMEOUT`].
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// Raft timings for a simulated control group over `voters`: [`ELECTION_TIMEOUT`]
/// and [`HEARTBEAT_INTERVAL`], over the in-memory storage [`RaftConfig::new`]
/// installs.
///
/// The ratio is what matters (heartbeats an order of magnitude under the election
/// timeout, so an idle leader is never mistaken for a lost one), and 500 ms leaves
/// room for the jitter each election round draws on top of it.
pub fn raft_config(voters: Vec<NodeId>) -> RaftConfig {
    let mut config = RaftConfig::new(voters);
    config.election_timeout = ELECTION_TIMEOUT;
    config.heartbeat_interval = HEARTBEAT_INTERVAL;
    config
}
