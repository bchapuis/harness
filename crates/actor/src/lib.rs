//! Umbrella crate for the distributed actor framework.
//!
//! Re-exports the layered crates described in Appendix B of the spec. The
//! cluster runtime (`actor-cluster`) and the deterministic simulator
//! (`actor-simulation`) are pulled in directly by downstream code as they
//! mature; this umbrella currently surfaces the stable core.

pub use actor_core as core;
pub use actor_serialization as serialization;
