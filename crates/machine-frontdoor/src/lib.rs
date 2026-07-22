//! The persistent machine's SSH front door (machine spec §5.1).
//!
//! A front door is a cluster member — the ingress analogue of the harness
//! gateway (harness §7.3) — that **terminates SSH in-process** (with `russh`,
//! a pure-Rust server) and bridges each authenticated channel to the
//! machine's guest agent over vsock. It authenticates the connecting client
//! by public key against the machine's journaled authorized-key set, presents
//! the machine's own journaled host key so one SSH identity survives
//! hibernation/migration/failover, and never lets guest-side material govern
//! what it bridges — possession of the vsock channel *is* the host's
//! authority (M4).
//!
//! Two seams keep the cluster and the transport out of the SSH code:
//!
//! - [`MachineAuthority`] — the grain-cluster half (production: a `GrainRef`
//!   to the machine over its leader; tests: a fake). It supplies the host
//!   key, the authorized-key check, and the journaled `Attach`/`Detach`.
//! - [`ChannelBackend`] — the transport half. It opens one byte stream per
//!   channel to the guest agent (production: [`bridge::VsockBackend`] over
//!   the leader node's vsock; tests: an in-memory duplex to a fake agent).
//!
//! What is **not** here, because this sandbox cannot verify it end to end: the
//! cross-node relay that carries a channel's bytes from the front-door member
//! to the leader that owns the vsock socket (front door and leader may be
//! different nodes). [`ChannelBackend`] is exactly that seam; the reference
//! [`bridge::VsockBackend`] assumes the guest socket is reachable from this
//! node (co-located, or the caller supplies a relayed stream). Credit-based
//! flow control across the actor transport is future work (machine §8).

mod bridge;
mod ssh;

pub use bridge::VsockBackend;
pub use machine_proto::AGENT_PORT;
pub use machine_proto::ChannelKind;
pub use ssh::serve_connection;

use std::future::Future;

use granary::GrainName;
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
/// presented at KEX (machine §3, §5.1). The material — `MachineState::
/// host_key`, carried on the attach reply — is the raw 32-byte ed25519 seed
/// the machine drew from system entropy at provisioning; expanding it here,
/// the one decoder of that encoding, keeps russh out of the machine crate.
/// Pure: the same seed always yields the same key, so the client's
/// `known_hosts` pin survives hibernation, migration, and failover.
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
pub trait Duplex: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}

impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> Duplex for T {}

/// The grain-cluster half of the front door (machine §5.1). Production wires
/// this over a `GrainRef<Machine>` to the machine's leader; tests supply a
/// fake. Every method is per-connection or per-channel, so nothing here holds
/// cluster state.
pub trait MachineAuthority: Send + Sync + 'static {
    /// The machine's journaled host key (machine §3), presented at KEX so the
    /// client's `known_hosts` pin survives hibernation, migration, and
    /// failover.
    fn host_key(
        &self,
        machine: &GrainName,
    ) -> impl Future<Output = Result<PrivateKey, FrontDoorError>> + Send;

    /// Whether the machine's *current* journaled policy authorizes `key`
    /// (M4): a revoked key stops authorizing the next attach. Verified at the
    /// front door; no key material enters the guest.
    fn authorizes(&self, machine: &GrainName, key: &PublicKey)
    -> impl Future<Output = bool> + Send;

    /// Journal the attachment with its principal (M4) and boot the microVM if
    /// needed; returns the attachment id used to detach.
    fn attach(
        &self,
        machine: &GrainName,
        principal: &str,
    ) -> impl Future<Output = Result<u64, FrontDoorError>> + Send;

    /// Journal the detachment (machine §5.1). Best-effort: a lost detach is
    /// reconciled by the machine's death watch on this front door.
    fn detach(&self, machine: &GrainName, attachment: u64) -> impl Future<Output = ()> + Send;
}

/// The transport half of the front door (machine §5.1): open one byte stream
/// per channel to the machine's guest agent, having already performed the
/// vsock handshake and sent the channel [`ChannelKind`] header, so the caller
/// exchanges [`machine_proto`] frames directly.
pub trait ChannelBackend: Send + Sync + 'static {
    fn open(
        &self,
        machine: &GrainName,
        kind: ChannelKind,
    ) -> impl Future<Output = std::io::Result<Box<dyn Duplex>>> + Send;
}
