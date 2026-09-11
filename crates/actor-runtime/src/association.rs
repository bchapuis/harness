//! The transport's protocol decisions, socket-free (spec §9.3): the runtime
//! **address book** and the per-peer **association slots**.
//!
//! `TcpTransport` interleaves two different things: sockets (dial, handshake,
//! read, write, all asynchronous and timing-dependent), and *decisions* about
//! them: whose address is believed, which live association serves a peer, who
//! wins a concurrent dial, when an entry is evicted, who gossip goes to. The
//! simulator exercises none of this file's caller (it routes by `NodeId` with
//! no addresses), so without this split the decisions would be testable only
//! through non-deterministic loopback runs. Here they are synchronous functions
//! over plain data: every race the transport can experience (two dials
//! completing in either order, an eviction against a replaced entry) is a
//! deterministic sequence of calls a unit test can spell out.
//!
//! This module decides, the transport executes. Nothing here blocks, sleeps, or
//! touches a socket; [`Slots`] is generic over the association handle so a test
//! can drive it with a toy.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::net::SocketAddr;

use actor_core::NodeId;

/// The runtime address book (spec §9.3): who this node believes lives where.
/// Seeded from the configured dial map, then grown as handshakes and endpoint
/// gossip reveal more peers. One rule guards it: claims about **this node**
/// are ignored; our own advertised address is authoritative, and neither a
/// peer's handshake nor third-hand gossip may move us.
pub(crate) struct Book {
    node: NodeId,
    advertised: SocketAddr,
    endpoints: BTreeMap<NodeId, SocketAddr>,
}

impl Book {
    /// Seed from the configured peers, dropping any entry claiming to be us.
    pub(crate) fn new(
        node: NodeId,
        advertised: SocketAddr,
        peers: impl IntoIterator<Item = (NodeId, SocketAddr)>,
    ) -> Book {
        Book {
            node,
            advertised,
            endpoints: peers.into_iter().filter(|(n, _)| *n != node).collect(),
        }
    }

    /// Record `node`'s address (a handshake's self-advertisement, or one entry
    /// of a gossiped table). Last claim wins; a claim about ourselves is
    /// ignored.
    pub(crate) fn learn(&mut self, node: NodeId, addr: SocketAddr) {
        if node == self.node {
            return;
        }
        self.endpoints.insert(node, addr);
    }

    /// Merge a gossiped `(node, address)` table.
    pub(crate) fn learn_all(&mut self, table: impl IntoIterator<Item = (NodeId, SocketAddr)>) {
        for (node, addr) in table {
            self.learn(node, addr);
        }
    }

    /// Where to dial `node`, if known.
    pub(crate) fn address_of(&self, node: NodeId) -> Option<SocketAddr> {
        self.endpoints.get(&node).copied()
    }

    /// The full known table for gossip, our own advertised address first: the
    /// one entry only we are authoritative for, and the reason gossip
    /// propagates a moved node's new address.
    pub(crate) fn table(&self) -> Vec<(NodeId, SocketAddr)> {
        let mut table = vec![(self.node, self.advertised)];
        table.extend(self.endpoints.iter().map(|(n, a)| (*n, *a)));
        table
    }

    /// Who a gossip round pushes the table to: every known peer (never
    /// ourselves). Gossip is the bootstrap, so the set is the book itself:
    /// a peer we can name is a peer we push to, connected or not.
    pub(crate) fn gossip_targets(&self) -> Vec<NodeId> {
        self.endpoints.keys().copied().collect()
    }
}

/// A per-peer association handle, as [`Slots`] needs to judge it. The two
/// methods are identity questions, not I/O: production's handle answers them
/// from its queue (`is_closed`, `same_channel`), a test's from a token.
pub(crate) trait Association: Clone {
    /// A dead association (its queue closed) is replaced rather than reused.
    fn is_live(&self) -> bool;

    /// Whether `other` is this very association, identity, not equality.
    /// What makes eviction ABA-safe: an eviction carries the association it
    /// means to evict, and a slot re-taken by a successor no longer matches.
    fn is_same(&self, other: &Self) -> bool;
}

/// What [`Slots::admit`] decided about a freshly created association.
pub(crate) enum Admitted<A> {
    /// The candidate now serves the peer: start its I/O.
    Won(A),
    /// A live association already serves the peer (a concurrent dial won the
    /// race); the returned handle is the winner's, and the candidate's
    /// resources should be dropped without ever starting.
    Lost(A),
}

#[cfg(test)]
impl<A> Admitted<A> {
    /// The association that serves the peer either way (a test convenience;
    /// production always branches on won/lost, since only a winner starts I/O).
    fn serving(self) -> A {
        match self {
            Admitted::Won(a) | Admitted::Lost(a) => a,
        }
    }
}

/// The per-peer association registry: at most one *live* outbound association
/// per peer, with the arbitration and eviction rules the transport's races
/// need decided in one place.
pub(crate) struct Slots<A> {
    slots: HashMap<NodeId, A>,
}

impl<A: Association> Slots<A> {
    pub(crate) fn new() -> Slots<A> {
        Slots {
            slots: HashMap::new(),
        }
    }

    /// The live association serving `peer`, if any. A dead entry is not
    /// returned (its slot awaits replacement by the next dial).
    pub(crate) fn reuse(&self, peer: NodeId) -> Option<A> {
        self.slots.get(&peer).filter(|a| a.is_live()).cloned()
    }

    /// Offer a freshly created association for `peer`. If a live one already
    /// holds the slot (a concurrent dial won) it is kept whole (its
    /// negotiated revision is the one whose frames will travel) and the
    /// candidate loses; otherwise the candidate takes the slot.
    pub(crate) fn admit(&mut self, peer: NodeId, candidate: A) -> Admitted<A> {
        if let Some(existing) = self.slots.get(&peer).filter(|a| a.is_live()) {
            return Admitted::Lost(existing.clone());
        }
        self.slots.insert(peer, candidate.clone());
        Admitted::Won(candidate)
    }

    /// Evict `peer`'s slot **iff** it still holds `association`: the dead or
    /// saturated one the caller failed on, or the one whose I/O just ended.
    /// A slot already re-taken by a successor is left alone, so a stale
    /// failure can never tear down its replacement. Returns whether it
    /// evicted.
    pub(crate) fn evict_if_same(&mut self, peer: NodeId, association: &A) -> bool {
        if self
            .slots
            .get(&peer)
            .is_some_and(|cur| cur.is_same(association))
        {
            self.slots.remove(&peer);
            return true;
        }
        false
    }

    /// Drop every slot (shutdown, spec §9.3).
    pub(crate) fn clear(&mut self) {
        self.slots.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().expect("addr")
    }

    const SELF: NodeId = NodeId::new(1);
    const B: NodeId = NodeId::new(2);
    const C: NodeId = NodeId::new(3);

    fn book() -> Book {
        Book::new(SELF, addr(7400), [(B, addr(7401)), (SELF, addr(9999))])
    }

    // ---- the address book ----

    #[test]
    fn the_book_never_holds_a_claim_about_ourselves() {
        // Seeding drops a self entry (the 9999 above), and neither a direct
        // claim nor a gossiped table can move us: our advertised address is
        // authoritative.
        let mut book = book();
        book.learn(SELF, addr(8888));
        book.learn_all([(SELF, addr(7777)), (C, addr(7403))]);
        assert_eq!(book.address_of(SELF), None);
        assert_eq!(book.table()[0], (SELF, addr(7400)));
        assert_eq!(book.address_of(C), Some(addr(7403)));
    }

    #[test]
    fn the_last_claim_about_a_peer_wins() {
        // A handshake's self-advertisement and a gossiped entry use the same
        // rule: latest write. A restarted peer's new address replaces the old
        // as soon as either channel reports it.
        let mut book = book();
        book.learn(B, addr(7501));
        assert_eq!(book.address_of(B), Some(addr(7501)));
        book.learn_all([(B, addr(7601))]);
        assert_eq!(book.address_of(B), Some(addr(7601)));
    }

    #[test]
    fn the_gossip_table_leads_with_our_own_address_and_targets_every_peer() {
        let mut book = book();
        book.learn(C, addr(7403));
        // The table is what a peer merges: us first (the entry only we are
        // authoritative for), then everyone we know.
        assert_eq!(
            book.table(),
            vec![(SELF, addr(7400)), (B, addr(7401)), (C, addr(7403))]
        );
        // The push set is the book minus us: reachability is not required;
        // gossip is the bootstrap, so it dials.
        assert_eq!(book.gossip_targets(), vec![B, C]);
    }

    // ---- the association slots ----

    /// A toy association: liveness is a flag, identity an `Arc` pointer.
    #[derive(Clone)]
    struct Toy {
        live: Arc<AtomicBool>,
    }

    impl Toy {
        fn new() -> Toy {
            Toy {
                live: Arc::new(AtomicBool::new(true)),
            }
        }

        fn die(&self) {
            self.live.store(false, Ordering::Relaxed);
        }
    }

    impl Association for Toy {
        fn is_live(&self) -> bool {
            self.live.load(Ordering::Relaxed)
        }

        fn is_same(&self, other: &Self) -> bool {
            Arc::ptr_eq(&self.live, &other.live)
        }
    }

    #[test]
    fn a_concurrent_dial_race_keeps_the_first_winner_whole() {
        // Two dials to the same peer complete back to back: the interleaving
        // the loopback tests could only hope to hit, spelled out. The second
        // loses and is told the winner's handle; the slot never flaps.
        let mut slots = Slots::new();
        let first = Toy::new();
        let second = Toy::new();
        assert!(matches!(slots.admit(B, first.clone()), Admitted::Won(_)));
        match slots.admit(B, second) {
            Admitted::Lost(winner) => assert!(winner.is_same(&first)),
            Admitted::Won(_) => panic!("the loser of the race must not replace the winner"),
        }
        assert!(slots.reuse(B).expect("live").is_same(&first));
    }

    #[test]
    fn a_dead_association_is_not_reused_and_loses_its_slot_to_the_next_dial() {
        let mut slots = Slots::new();
        let dead = Toy::new();
        slots.admit(B, dead.clone()).serving();
        dead.die();
        // Reuse skips it: the next send dials afresh…
        assert!(slots.reuse(B).is_none());
        // …and the fresh association takes the slot even though it is held.
        let fresh = Toy::new();
        assert!(matches!(slots.admit(B, fresh.clone()), Admitted::Won(_)));
        assert!(slots.reuse(B).expect("live").is_same(&fresh));
    }

    #[test]
    fn eviction_is_aba_safe_a_stale_failure_cannot_tear_down_the_successor() {
        // The race: a send fails on the old association while a re-dial has
        // already replaced it. The failure's eviction names the association it
        // failed on, so the successor survives.
        let mut slots = Slots::new();
        let old = Toy::new();
        slots.admit(B, old.clone()).serving();
        old.die();
        let successor = Toy::new();
        slots.admit(B, successor.clone()).serving();
        assert!(!slots.evict_if_same(B, &old), "stale eviction must miss");
        assert!(slots.reuse(B).expect("live").is_same(&successor));
        // The successor's own failure still evicts.
        assert!(slots.evict_if_same(B, &successor));
        assert!(slots.reuse(B).is_none());
    }

    #[test]
    fn shutdown_clears_every_slot() {
        let mut slots = Slots::new();
        slots.admit(B, Toy::new()).serving();
        slots.admit(C, Toy::new()).serving();
        slots.clear();
        assert!(slots.reuse(B).is_none());
        assert!(slots.reuse(C).is_none());
    }
}
