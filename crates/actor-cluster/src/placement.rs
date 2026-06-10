//! Deterministic placement by rendezvous hashing (utilities spec §2).
//!
//! Answers one question identically on every node: *given a key, which member
//! owns it?* Each candidate is scored independently — `weight(tag, key)` — and
//! the highest weight wins, so removing a member reassigns only the keys it
//! owned and adding one moves only the keys it now owns (utilities spec §2.2
//! item 5, invariant U1). Everything here is a pure function of its arguments:
//! no state, no clock, no entropy (core spec §18.1).
//!
//! The hash is normative and version-stable (utilities spec §2.2 item 3):
//! FNV-1a 64 over `tag ‖ key`, finished with the splitmix64 mixer. `std::hash`
//! and other unstable hashers are ruled out because two framework versions (or
//! platforms) must agree on every owner; the finalizer compensates for FNV-1a's
//! weak avalanche on short inputs. The constants are pinned by known-answer
//! tests below so an accidental change fails the build.
//!
//! Candidates come from [`Membership::serving_members`](crate::Membership::serving_members)
//! (utilities spec §2.1): `up` ∧ `reachable`, including the local node iff `up`.
//! Callers with divergent views may disagree on an owner until views converge —
//! placement is a routing function, not a lease (utilities spec §2.3).

use actor_core::ActorId;
use actor_core::NodeId;

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64 over the concatenation of `chunks` (no per-chunk framing — the
/// caller's tag/key split is positional, not self-describing).
fn fnv1a64(chunks: &[&[u8]]) -> u64 {
    let mut hash = FNV_OFFSET;
    for chunk in chunks {
        for &byte in *chunk {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
    }
    hash
}

/// The splitmix64 finalizer: full-avalanche mixing of a 64-bit value.
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x
}

/// The rendezvous weight of one candidate (by its `tag`) for `key` (utilities
/// spec §2.2): `mix64(fnv1a64(tag ‖ key))`. Pure and version-stable.
pub fn weight(tag: &[u8], key: &[u8]) -> u64 {
    mix64(fnv1a64(&[tag, key]))
}

/// The normative placement tag of a node (utilities spec §2.2): its uid as 8
/// little-endian bytes.
pub fn node_tag(node: NodeId) -> [u8; 8] {
    node.uid().to_le_bytes()
}

/// The placement tag of an actor: owner-node tag ‖ path bytes ‖ incarnation
/// (little-endian). Used by routers to rank routees for a key (utilities spec §3).
pub fn actor_tag(id: &ActorId) -> Vec<u8> {
    let path = id.path().as_str().as_bytes();
    let mut tag = Vec::with_capacity(8 + path.len() + 8);
    tag.extend_from_slice(&node_tag(id.node()));
    tag.extend_from_slice(path);
    tag.extend_from_slice(&id.incarnation().to_le_bytes());
    tag
}

/// The owner of `key` among `nodes` (utilities spec §2.2): the highest
/// rendezvous weight, ties to the lower [`NodeId`]. `None` when `nodes` is
/// empty. Equal inputs yield equal owners on every node (invariant U1).
pub fn owner(nodes: &[NodeId], key: &[u8]) -> Option<NodeId> {
    nodes
        .iter()
        .copied()
        .max_by_key(|&node| (weight(&node_tag(node), key), std::cmp::Reverse(node)))
}

/// The `n` highest-weight nodes for `key`, in descending weight order with the
/// same tie rule as [`owner`] — `top(nodes, key, 1)` equals `owner(nodes, key)`.
/// Provided for replica/shard placement (utilities spec §7); returns fewer than
/// `n` entries when the candidate set is smaller.
pub fn top(nodes: &[NodeId], key: &[u8], n: usize) -> Vec<NodeId> {
    let mut ranked: Vec<(u64, NodeId)> = nodes
        .iter()
        .map(|&node| (weight(&node_tag(node), key), node))
        .collect();
    // Descending weight; within a tie, the lower NodeId first.
    ranked.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    ranked.into_iter().take(n).map(|(_, node)| node).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The published FNV-1a 64 test vectors pin the constants: a change to the
    /// offset basis or prime is a wire-visible placement change (utilities spec
    /// §2.2 item 3) and must fail loudly.
    #[test]
    fn fnv1a64_known_answers() {
        assert_eq!(fnv1a64(&[b""]), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a64(&[b"a"]), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(fnv1a64(&[b"foobar"]), 0x8594_4171_f739_67e8);
        // Chunking is positional, not framed: split input hashes identically.
        assert_eq!(fnv1a64(&[b"foo", b"bar"]), fnv1a64(&[b"foobar"]));
    }

    /// Pinned end-to-end weights: any drift in the hash, the mixer, or the tag
    /// encoding breaks these.
    #[test]
    fn weight_known_answers() {
        assert_eq!(
            weight(&node_tag(NodeId::new(1)), b"key"),
            0xa814_7c24_bd36_5456
        );
        assert_eq!(
            weight(&node_tag(NodeId::new(2)), b"order-42"),
            0xad3b_202b_0a52_8c14
        );
    }

    #[test]
    fn node_tag_is_little_endian_uid() {
        assert_eq!(node_tag(NodeId::new(0x0102_0304)), [4, 3, 2, 1, 0, 0, 0, 0]);
    }
}
