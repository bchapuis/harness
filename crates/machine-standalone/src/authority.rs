//! The front door's entity-cluster half (machine §5.1), over a real
//! `Entities<Machine>`.
//!
//! `machine-frontdoor` states the seam and ships fakes for its tests; this is
//! the production wiring. Every method is one entity command routed to the
//! machine's current leader, so the front door holds no machine state of its
//! own: the host key it presents, the key set it verifies against, and the
//! attachment journal are all reads and writes of the machine's journal.

use std::time::Duration;

use actor_core::ActorId;
use actor_core::NodeId;
use actor_runtime::TcpCluster;
use entity::EntityName;
use entity::Entities;
use machine_frontdoor::FrontDoorError;
use machine_frontdoor::MachineAuthority;
use machine_frontdoor::host_key_from_seed;
use machine_entity::Attach;
use machine_entity::Detach;
use machine_entity::DoorPolicy;
use machine_entity::DoorPolicyReply;
use machine_entity::Machine;
use russh::keys::PrivateKey;
use russh::keys::PublicKey;

/// The entity type this node hosts and this authority addresses.
pub type NodeMachine = Machine<TcpCluster>;

/// A [`MachineAuthority`] backed by the cluster (machine §5.1).
pub struct EntityAuthority {
    machines: Entities<NodeMachine>,
    /// The front-door actor holding these connections: the machine's
    /// death-watch target, so a front door that dies without detaching has
    /// its attachments reaped (`Detached { FrontDoorLost }`).
    door: ActorId,
    /// This node, so [`attach`](EntityAuthority::attach) can refuse a machine
    /// this node does not lead before journaling anything (machine §5.1).
    node: NodeId,
    /// How long a front-door command waits. A machine mid-failover redirects
    /// within entity's bounded retry; past this the connection is refused
    /// rather than hung, and the client reconnects (machine §7).
    timeout: Duration,
}

impl EntityAuthority {
    pub fn new(
        machines: Entities<NodeMachine>,
        door: ActorId,
        node: NodeId,
        timeout: Duration,
    ) -> EntityAuthority {
        EntityAuthority {
            machines,
            door,
            node,
            timeout,
        }
    }

    /// The machine's journaled door policy, read fresh per connection so a
    /// revoked key stops authorizing the next attach (M4).
    async fn policy(&self, machine: &EntityName) -> Result<DoorPolicyReply, FrontDoorError> {
        self.machines
            .entity(machine.key())
            .ask_timeout(DoorPolicy, self.timeout)
            .await
            .map_err(|e| FrontDoorError(format!("{machine}: {e}")))?
            .map_err(|e| FrontDoorError(format!("{machine}: {e}")))
    }
}

impl MachineAuthority for EntityAuthority {
    async fn host_key(&self, machine: &EntityName) -> Result<PrivateKey, FrontDoorError> {
        host_key_from_seed(&self.policy(machine).await?.host_key)
    }

    async fn authorizes(&self, machine: &EntityName, key: &PublicKey) -> bool {
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

    async fn attach(&self, machine: &EntityName, principal: &str) -> Result<u64, FrontDoorError> {
        // The bridge is node-local (machine §5.1): a channel ends at a guest
        // this node holds, so a door that does not lead the machine cannot
        // serve it whatever the entity replies. Refuse here, before the
        // `Attach` journals an attachment and boots a guest on the leader that
        // the first channel open would then have to abandon. A routing
        // observation, not a guarantee (leadership can move a moment later),
        // which is why the channel backend still reports the loss; `None` is a
        // shard mid-election, left to the ordinary bounded retry below.
        if let Some(leader) = self.machines.leader(machine.key())
            && leader != self.node
        {
            return Err(FrontDoorError(format!(
                "{machine}: led by {leader}, and a front door bridges only the machines its own \
                 node leads (status §11's cross-node relay is not built). Reconnect to that \
                 node's door."
            )));
        }
        self.machines
            .entity(machine.key())
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

    async fn detach(&self, machine: &EntityName, attachment: u64) {
        // Best-effort by contract: a lost detach is reconciled by the
        // machine's death watch on this front door (machine §5.1).
        let _ = self
            .machines
            .entity(machine.key())
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
