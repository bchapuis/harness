//! The front door's grain-cluster half (machine §5.1), over a real
//! `Granary<Machine>`.
//!
//! `machine-frontdoor` states the seam and ships fakes for its tests; this is
//! the production wiring. Every method is one grain command routed to the
//! machine's current leader, so the front door holds no machine state of its
//! own: the host key it presents, the key set it verifies against, and the
//! attachment journal are all reads and writes of the machine's journal.

use std::time::Duration;

use actor_core::ActorId;
use actor_runtime::TcpCluster;
use granary::GrainName;
use granary::Granary;
use machine_frontdoor::FrontDoorError;
use machine_frontdoor::MachineAuthority;
use machine_frontdoor::host_key_from_seed;
use machine_grain::Attach;
use machine_grain::Detach;
use machine_grain::DoorPolicy;
use machine_grain::DoorPolicyReply;
use machine_grain::Machine;
use russh::keys::PrivateKey;
use russh::keys::PublicKey;

use crate::provider::NodeRuntimeProvider;

/// The grain type this node hosts and this authority addresses.
pub type NodeMachine = Machine<TcpCluster, NodeRuntimeProvider>;

/// A [`MachineAuthority`] backed by the cluster (machine §5.1).
pub struct GrainAuthority {
    machines: Granary<NodeMachine>,
    /// The front-door actor holding these connections — the machine's
    /// death-watch target, so a front door that dies without detaching has
    /// its attachments reaped (`Detached { FrontDoorLost }`).
    door: ActorId,
    /// How long a front-door command waits. A machine mid-failover redirects
    /// within granary's bounded retry; past this the connection is refused
    /// rather than hung, and the client reconnects (machine §6).
    timeout: Duration,
}

impl GrainAuthority {
    pub fn new(machines: Granary<NodeMachine>, door: ActorId, timeout: Duration) -> GrainAuthority {
        GrainAuthority {
            machines,
            door,
            timeout,
        }
    }

    /// The machine's journaled door policy, read fresh per connection so a
    /// revoked key stops authorizing the next attach (M4).
    async fn policy(&self, machine: &GrainName) -> Result<DoorPolicyReply, FrontDoorError> {
        self.machines
            .grain(machine.key())
            .ask_timeout(DoorPolicy, self.timeout)
            .await
            .map_err(|e| FrontDoorError(format!("{machine}: {e}")))?
            .map_err(|e| FrontDoorError(format!("{machine}: {e}")))
    }
}

impl MachineAuthority for GrainAuthority {
    async fn host_key(&self, machine: &GrainName) -> Result<PrivateKey, FrontDoorError> {
        host_key_from_seed(&self.policy(machine).await?.host_key)
    }

    async fn authorizes(&self, machine: &GrainName, key: &PublicKey) -> bool {
        let Ok(policy) = self.policy(machine).await else {
            // An unreachable machine authorizes nothing: fail closed.
            return false;
        };
        // Compare key *material*, never the whole `PublicKey`: the wire key
        // carries no comment, so an `==` including the journaled line's
        // comment would never match.
        policy.authorized_keys.values().any(|line| {
            PublicKey::from_openssh(line)
                .is_ok_and(|authorized| authorized.key_data() == key.key_data())
        })
    }

    async fn attach(&self, machine: &GrainName, principal: &str) -> Result<u64, FrontDoorError> {
        self.machines
            .grain(machine.key())
            .ask_timeout(
                Attach {
                    principal: principal.to_string(),
                    front_door: self.door.clone(),
                },
                self.timeout,
            )
            .await
            .map_err(|e| FrontDoorError(format!("{machine}: attach: {e}")))?
            .map(|reply| reply.attachment)
            .map_err(|e| FrontDoorError(format!("{machine}: attach: {e}")))
    }

    async fn detach(&self, machine: &GrainName, attachment: u64) {
        // Best-effort by contract: a lost detach is reconciled by the
        // machine's death watch on this front door (machine §5.1).
        let _ = self
            .machines
            .grain(machine.key())
            .ask_timeout(Detach { attachment }, self.timeout)
            .await;
    }
}

/// The front-door member itself (machine §5.1): an actor whose only job is to
/// *exist*, so each attachment names something the machine can death-watch.
/// The bytes flow through `serve_connection`, never through this mailbox.
#[derive(Default)]
pub struct FrontDoor;

impl actor_core::Actor for FrontDoor {
    type System = TcpCluster;
}
