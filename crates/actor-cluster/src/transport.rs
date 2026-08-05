//! The transport trait (spec §7).
//!
//! A [`Transport`] is the pluggable *mechanism* that carries [`Frame`]s between
//! nodes (spec §7); the default TCP transport and the simulator's in-memory
//! network are two implementations of the one trait. The frames it carries — the
//! wire *protocol* — live in [`crate::protocol`], so the carrier stays decoupled
//! from any subsystem's payload.

use std::future::Future;

use actor_core::NodeId;

use crate::protocol::Frame;

/// A transport-level failure (spec §7, §14). Surfaced to callers as
/// `CallError::Unreachable`.
#[derive(Clone, Debug)]
pub enum TransportError {
    /// No association to the peer, or the peer is unknown/down.
    Unreachable,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::Unreachable => f.write_str("peer unreachable"),
        }
    }
}

impl std::error::Error for TransportError {}

/// A pluggable transport (spec §7). Cloneable so the system can hand copies to
/// the per-reply forwarding tasks. Inbound frames are delivered out of band into
/// the system's receive loop (the constructor wires the inbound channel).
pub trait Transport: Clone + Send + Sync + 'static {
    /// Send one frame to `peer` over its association. At-most-once (spec §7.2):
    /// the transport never transparently retransmits.
    fn send(
        &self,
        peer: NodeId,
        frame: Frame,
    ) -> impl Future<Output = Result<(), TransportError>> + Send;

    /// The wire revision settled with `peer` on the association this node would
    /// **send** over, or `None` when there is no such association (spec §7.1,
    /// compatibility spec §3.1).
    ///
    /// This is the *send-side gate*, and the reason the negotiated revision has to
    /// leave the handshake at all. A build whose window spans two revisions must
    /// not write the higher one to a peer that settled on the lower — negotiation
    /// alone only lets that be detected, on the receiving end, as an association
    /// torn down. Gating needs the value here, above the transport, where the
    /// frame is composed.
    ///
    /// `None` means **not yet known**, never "anything goes": a caller must then
    /// write what the oldest peer in its own accepted range could read. Guessing
    /// in the other direction is precisely the misparse **V2** exists to prevent,
    /// and the first send to a peer always lands here, because the association is
    /// what [`send`](Transport::send) establishes.
    ///
    /// The answer belongs to the **association**, not to the peer. A revision is
    /// what two ends settled on when they handshook, so it lives and dies with the
    /// connection frames travel over; a value cached per peer would outlive that
    /// peer being restarted onto a narrower window, which is the case a gate exists
    /// for in the first place.
    fn peer_version(&self, peer: NodeId) -> Option<compat::Version>;

    /// Release the transport's resources — background tasks, listeners, and open
    /// associations — on a graceful node stop (spec §9.3). Closing the inbound
    /// path also ends the system's receive loop. The default is a no-op, for a
    /// transport that holds nothing to release.
    fn shutdown(&self) {}
}
