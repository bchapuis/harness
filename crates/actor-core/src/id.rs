//! Actor and node identity (spec §3.6).
//!
//! An [`ActorId`] is the cluster-unique, serializable name the system assigns:
//! `{ node, path, incarnation }`.

use serde::Deserialize;
use serde::Serialize;

/// A cluster node identity (spec §3.6). In production this carries a uid plus a
/// network endpoint; under simulation the uid alone suffices, as the in-memory
/// network routes by it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
    /// Construct a node id from its uid.
    pub const fn new(uid: u64) -> NodeId {
        NodeId(uid)
    }

    /// The node's unique identifier.
    pub const fn uid(&self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

/// A hierarchical actor name, e.g. `/user/greeter` (spec §3.6).
///
/// The string is refcounted rather than owned because [`ActorId`] is cloned several times
/// per message — once to resolve the target's mailbox, then once more for each event the
/// send emits — and the path is the only part of an id that is not `Copy`. Sharing it
/// makes those clones an atomic increment instead of an allocation and a copy.
///
/// `Arc<str>` and not `Arc<String>`: one pointer to one allocation, no second indirection.
/// Every derived trait below forwards to `str`, so ordering and hashing are exactly what
/// they were when this held a `String` — which matters, because ids are `BTreeMap` keys in
/// the host registry and the map's iteration order is observable in the event stream.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Path(std::sync::Arc<str>);

impl Path {
    /// Wrap a path string. Callers supply already-normalized paths such as
    /// `"/user/greeter"`.
    ///
    /// Takes anything that becomes an `Arc<str>` rather than anything that becomes a
    /// `String`: the latter would allocate a `String` for a `&'static str` caller and then
    /// allocate again to copy it into the `Arc`. `&str` converts in one allocation, and an
    /// owned `String` still converts, so no caller has to change.
    pub fn new(path: impl Into<std::sync::Arc<str>>) -> Path {
        Path(path.into())
    }

    /// The path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// Written out rather than derived so the wire form stays a plain string — the same bytes
// a `String` field produced — without enabling serde's `rc` feature, which would add
// `Arc`/`Rc` impls across every crate in the tree for the sake of this one field.
//
// Deserializing shares nothing between ids: two that arrive with equal paths get separate
// allocations. The sharing this type exists for is within a process, where ids are cloned.
impl Serialize for Path {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Path {
    // A visitor rather than `String::deserialize(..).map(Path::new)`: every remote envelope
    // carries an `ActorId`, so this is the one per-message path that builds a `Path`, and
    // going through `String` would allocate it only to copy it into the `Arc` and free it.
    // Formats that can hand over a borrowed `&str` — postcard and serde_json both do —
    // reach `visit_str` and allocate once.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Path, D::Error> {
        struct PathVisitor;

        impl serde::de::Visitor<'_> for PathVisitor {
            type Value = Path;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("an actor path string")
            }

            fn visit_str<E: serde::de::Error>(self, path: &str) -> Result<Path, E> {
                Ok(Path::new(path))
            }

            fn visit_string<E: serde::de::Error>(self, path: String) -> Result<Path, E> {
                Ok(Path::new(path))
            }
        }

        deserializer.deserialize_str(PathVisitor)
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A cluster-unique actor identity (spec §3.6).
///
/// Two actors are equal iff their `ActorId`s are equal; `Hash`/`Eq` on an
/// `ActorRef` derive from this. The `node` makes locality classifiable without
/// contacting another node (spec §4.3); the `incarnation` ensures a reused path
/// on a fresh actor never collides with a resigned predecessor.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct ActorId {
    node: NodeId,
    path: Path,
    incarnation: u64,
}

impl ActorId {
    /// Construct an id from its parts.
    pub fn new(node: NodeId, path: Path, incarnation: u64) -> ActorId {
        ActorId {
            node,
            path,
            incarnation,
        }
    }

    /// The node that owns this actor — the locality key (spec §3.6, §4.3).
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// This actor's hierarchical path, e.g. `/user/greeter` (spec §3.6).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The incarnation (spec §3.6).
    pub fn incarnation(&self) -> u64 {
        self.incarnation
    }
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}#{}", self.node, self.path, self.incarnation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actor_serialization::Codec;
    use actor_serialization::JsonCodec;
    use actor_serialization::PostcardCodec;
    use actor_serialization::encode;

    #[test]
    fn a_path_serializes_as_the_plain_string_it_wraps() {
        // How `Path` stores its bytes is an in-process decision — it is refcounted so
        // that cloning an id is an atomic increment — and this is what keeps that
        // decision from reaching the wire. An id crosses nodes inside every remote
        // envelope, so a representation change that altered its encoding would be a
        // silent compatibility break between builds rather than a local optimization.
        let path = Path::new("/user/greeter");
        assert_eq!(
            encode(&JsonCodec as &dyn Codec, &path).expect("encodes"),
            br#""/user/greeter""#.to_vec(),
        );
        assert_eq!(
            encode(&PostcardCodec as &dyn Codec, &path).expect("encodes"),
            encode(&PostcardCodec as &dyn Codec, &"/user/greeter".to_string()).expect("encodes"),
        );
    }

    #[test]
    fn an_actor_id_round_trips_through_its_wire_form() {
        let id = ActorId::new(NodeId::new(7), Path::new("/user/greeter"), 3);
        let bytes = encode(&PostcardCodec as &dyn Codec, &id).expect("encodes");
        let back: ActorId =
            actor_serialization::decode(&PostcardCodec as &dyn Codec, &bytes).expect("decodes");
        assert_eq!(back, id);
    }

    #[test]
    fn cloning_a_path_shares_its_bytes_rather_than_copying_them() {
        // The property the refcount buys, stated where it can regress: two clones of one
        // path point at one allocation. A `Path` rebuilt from the same text does not —
        // sharing comes from cloning, which is what the message path actually does.
        let path = Path::new("/user/greeter");
        let clone = path.clone();
        assert_eq!(path.as_str().as_ptr(), clone.as_str().as_ptr());
    }
}
