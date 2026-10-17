//! Actor and node identity (spec §2.2).
//!
//! An [`ActorId`] is the cluster-unique, serializable name the system assigns:
//! `{ node, path, incarnation }`.

use serde::Deserialize;
use serde::Serialize;

/// A cluster node identity (spec §2.2): a bare uid. It carries no network
/// endpoint: the transport's address book maps a node to where it is reached,
/// so the same id routes over TCP in production and over the in-memory network
/// under simulation.
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

/// A hierarchical actor name, e.g. `/user/greeter` (spec §2.2).
///
/// The string is refcounted because [`ActorId`] is cloned several times per message
/// (once to resolve the target's mailbox, then once per event the send emits) and the
/// path is the only part of an id that is not `Copy`; sharing it makes those clones an
/// atomic increment.
///
/// `Arc<str>` and not `Arc<String>`: one pointer to one allocation, no second
/// indirection. Every derived trait below forwards to `str`, so ordering and hashing
/// are what a `String` gave, which matters because ids are `BTreeMap` keys in the host
/// registry and the map's iteration order is observable in the event stream.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct Path(std::sync::Arc<str>);

impl Path {
    /// Wrap a path string. Callers supply already-normalized paths such as
    /// `"/user/greeter"`.
    ///
    /// Takes anything that becomes an `Arc<str>` rather than anything that becomes a
    /// `String`: the latter would allocate twice for a `&'static str` caller. An owned
    /// `String` still converts, so no caller has to change.
    pub fn new(path: impl Into<std::sync::Arc<str>>) -> Path {
        Path(path.into())
    }

    /// The path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

// Written out rather than derived so the wire form is a plain string without enabling serde's `rc` feature, which would add
// `Arc`/`Rc` impls across every crate in the tree for the sake of this one field.
//
// Deserializing shares nothing between ids: two that arrive with equal paths get separate
// allocations.
impl Serialize for Path {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for Path {
    // A visitor rather than `String::deserialize(..).map(Path::new)`: every remote envelope
    // carries an `ActorId`, so this is the one per-message path that builds a `Path`, and
    // going through `String` would allocate it only to copy it into the `Arc` and free it.
    // Formats that hand over a borrowed `&str` (postcard and serde_json both do) reach
    // `visit_str` and allocate once.
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

/// A cluster-unique actor identity (spec §2.2).
///
/// Two actors are equal iff their `ActorId`s are equal; `Hash`/`Eq` on an
/// `ActorRef` derive from this. The `node` makes locality classifiable without
/// contacting another node (spec §3.2); the `path` comes from a counter the
/// host never reuses, so a fresh actor never collides with a resigned one; the
/// `incarnation` is the owning **process** incarnation, so an id issued before a
/// process restart never resolves against the actor that now holds the same
/// path (see `LocalHost::with_incarnation`).
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

    /// The node that owns this actor: the locality key (spec §2.2, §3.2).
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// This actor's hierarchical path, e.g. `/user/greeter` (spec §2.2).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The incarnation (spec §2.2).
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
    use actor_serialization::PostcardCodec;
    use actor_serialization::encode;

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
        // path point at one allocation. A `Path` rebuilt from the same text does not;
        // sharing comes from cloning, which is what the message path does.
        let path = Path::new("/user/greeter");
        let clone = path.clone();
        assert_eq!(path.as_str().as_ptr(), clone.as_str().as_ptr());
    }
}
