//! Retry-safe caller-side coordination over the public World Relation Port.
//! The selected Port retains storage, authorization and semantic authority.
pub mod relation;
pub use relation::WorldRelationClient;
