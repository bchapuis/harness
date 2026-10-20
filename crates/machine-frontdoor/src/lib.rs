//! The persistent machine's SSH front door: the ingress half of the machine's
//! network seam (M4).
//!
//! A front door is a cluster member, the ingress analogue of the agent runtime
//! gateway (agent §7.2), that **terminates SSH in-process** (`russh`) and
//! bridges each authenticated channel to the machine's guest agent. It
//! authenticates the client by public key against the machine's journaled
//! authorized-key set, presents the machine's own journaled host key so one SSH
//! identity survives hibernation, migration, and failover, journals each
//! attachment under the authenticating key's fingerprint, and never lets
//! guest-side material govern what it bridges: possession of the channel *is*
//! the host's authority.
//!
//! The crate defines two plug points, both supplied by the deployment, so the
//! cluster and the transport stay out of the SSH code:
//!
//! - [`MachineAuthority`], the entity-cluster half: the host key, the
//!   authorized-key check, and the journaled `Attach`/`Detach`. A deployment
//!   wires it to the machine entity's `DoorPolicy`, `Attach`, and `Detach`
//!   commands on the machine's leader.
//! - [`ChannelBackend`], the transport half: one byte stream per channel to the
//!   guest agent, header sent. A deployment supplies the node's
//!   `machine_host::MachineHost`, which reaches the guest however that node
//!   holds it. The stream speaks `machine_proto`'s frames, the one wire this
//!   crate shares with the guest agent and the machine's workspace sync.
//!
//! [`serve_connection`] is the whole server: one call per accepted connection,
//! running to its close and journaling the detach on the way out. The `ssh`
//! module holds the `russh` handler: publickey-only authentication, the
//! principal it journals, the disconnect that carries an authority's refusal,
//! and the per-channel reader and writer tasks that translate SSH callbacks to
//! frames and back.
//!
//! A backend assumes the guest is reachable from this node. The cross-node
//! relay that would carry a channel's bytes from any door to the leader that
//! holds the guest is deferred (status §11); it would also need credit-based
//! flow control across the actor transport.
//!
//! Specified by `docs/machine-spec.md`.

mod ssh;

pub use machine_proto::ChannelKind;
pub use ssh::serve_connection;

use std::future::Future;

use entity::EntityName;
use russh::keys::PrivateKey;
use russh::keys::PublicKey;
use russh::keys::ssh_key::private::Ed25519Keypair;
use russh::keys::ssh_key::private::KeypairData;

/// A front-door operation failed.
#[derive(Debug, Clone)]
pub struct FrontDoorError(pub String);

impl std::fmt::Display for FrontDoorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "front door: {}", self.0)
    }
}

impl std::error::Error for FrontDoorError {}

/// Expand the machine's journaled host-key material into the SSH host key
/// presented at KEX (machine §4, §5.1). The material is the raw 32-byte ed25519
/// seed the machine drew from system entropy at provisioning, read through its
/// `DoorPolicy` command ([`MachineAuthority::host_key`]); expanding it here
/// keeps russh out of the machine crate. Pure: the same seed always yields the
/// same key, so the client's `known_hosts` pin survives hibernation,
/// migration, and failover.
pub fn host_key_from_seed(seed: &[u8]) -> Result<PrivateKey, FrontDoorError> {
    let seed: &[u8; 32] = seed.try_into().map_err(|_| {
        FrontDoorError(format!(
            "host-key material: expected a 32-byte ed25519 seed, got {} bytes",
            seed.len()
        ))
    })?;
    PrivateKey::new(KeypairData::Ed25519(Ed25519Keypair::from_seed(seed)), "")
        .map_err(|e| FrontDoorError(format!("host key: {e}")))
}

/// A bidirectional byte stream to a guest agent for one channel (the header
/// already sent by [`ChannelBackend::open`]). The front door writes and reads
/// [`machine_proto`] frames on it.
///
/// One definition, `machine-host`'s: a channel comes from whichever mechanism holds
/// the guest, and a second identical trait here would make every backend box its
/// stream twice to cross the seam.
pub use machine_host::Duplex;

/// The entity-cluster half of the front door (machine §3.4, §5.1). Production wires
/// this over an `EntityRef<Machine>` to the machine's leader; tests supply a
/// fake. Every method is per-connection or per-channel: nothing here holds
/// cluster state.
pub trait MachineAuthority: Send + Sync + 'static {
    /// The machine's journaled host key (machine §4), presented at KEX (see
    /// [`host_key_from_seed`]).
    fn host_key(
        &self,
        machine: &EntityName,
    ) -> impl Future<Output = Result<PrivateKey, FrontDoorError>> + Send;

    /// Whether the machine's *current* journaled policy authorizes `key`
    /// (M4): a revoked key stops authorizing the next attach. Verified at the
    /// front door; no key material enters the guest.
    fn authorizes(&self, machine: &EntityName, key: &PublicKey)
    -> impl Future<Output = bool> + Send;

    /// Journal the attachment with its principal (M4) and boot the guest if
    /// needed; returns the attachment id used to detach.
    ///
    /// `principal` is the door's attribution, derived from the authenticating
    /// key: its SHA-256 fingerprint, then the client-asserted SSH user name in
    /// parentheses. An implementation journals it as given.
    ///
    /// An implementation MAY refuse before journaling anything: the bridge is
    /// node-local (machine §5.1), so the production authority checks that this
    /// node leads the machine and fails the attach when it does not, rather
    /// than booting a guest the first channel open could never reach. The
    /// refusal's message reaches the client as the SSH disconnect reason, so it
    /// should say what the client can do (which door to reach instead).
    fn attach(
        &self,
        machine: &EntityName,
        principal: &str,
    ) -> impl Future<Output = Result<u64, FrontDoorError>> + Send;

    /// Journal the detachment (machine §5.1). Best-effort: a lost detach is
    /// reconciled by the machine's death watch on this front door.
    fn detach(&self, machine: &EntityName, attachment: u64) -> impl Future<Output = ()> + Send;
}

/// The transport half of the front door (machine §3.4): open one byte stream
/// per channel to the machine's guest agent, having already performed the
/// vsock handshake and sent the channel [`ChannelKind`] header, so the caller
/// exchanges [`machine_proto`] frames directly.
pub trait ChannelBackend: Send + Sync + 'static {
    fn open(
        &self,
        machine: &EntityName,
        kind: ChannelKind,
    ) -> impl Future<Output = std::io::Result<Box<dyn Duplex>>> + Send;
}
