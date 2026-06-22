//! Shared visibility helpers for boards and aggregated boards.
//!
//! Visibility is stored in Postgres as a lowercase string and exposed in Protobuf
//! as `BoardVisibility`. This module keeps the mapping in one place.

const VIS_PRIVATE: &str = "private";
const VIS_INTERNAL: &str = "internal";
const VIS_PUBLIC: &str = "public";

/// Default visibility for newly created boards / aggregates.
pub const DEFAULT_VISIBILITY: &str = VIS_PRIVATE;

/// Convert a proto `BoardVisibility` enum value to the Postgres string.
pub fn proto_to_db(v: i32) -> &'static str {
    match v {
        2 => VIS_INTERNAL,
        3 => VIS_PUBLIC,
        _ => VIS_PRIVATE,
    }
}

/// Convert a Postgres visibility string to a proto `BoardVisibility` enum value.
pub fn db_to_proto(s: &str) -> i32 {
    match s {
        VIS_INTERNAL => 2,
        VIS_PUBLIC => 3,
        _ => 1, // private, including unknown values
    }
}

/// True when the visibility level is visible to any authenticated user.
pub fn is_public_or_internal(s: &str) -> bool {
    s == VIS_PUBLIC || s == VIS_INTERNAL
}

/// True when the visibility level is public (visible without authentication).
pub fn is_public(s: &str) -> bool {
    s == VIS_PUBLIC
}
