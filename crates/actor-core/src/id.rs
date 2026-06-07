//! Actor and node identity (spec §3.6).
//!
//! An [`ActorId`] is the cluster-unique, serializable name the system assigns:
//! `{ node, path, incarnation }`. The `node` lets any node classify a target as
//! local or remote from the id alone, with no network round-trip (spec §4.3).
//! The `incarnation` distinguishes a fresh actor from a resigned one that reused
//! the same name.

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
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct Path(String);

impl Path {
    /// Wrap a path string. Callers supply already-normalized paths such as
    /// `"/user/greeter"`.
    pub fn new(path: impl Into<String>) -> Path {
        Path(path.into())
    }

    /// The path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
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
    pub node: NodeId,
    pub path: Path,
    pub incarnation: u64,
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
}

impl std::fmt::Display for ActorId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}#{}", self.node, self.path, self.incarnation)
    }
}
