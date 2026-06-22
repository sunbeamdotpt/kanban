// SPDX-License-Identifier: AGPL-3.0-or-later
//! Visibility helpers shared by boards and aggregated boards.
//!
//! Postgres stores visibility as a lowercase string, while protobuf represents it
//! as `BoardVisibility`. This module keeps the two representations in sync.

const VIS_PRIVATE: &str = "private";
const VIS_INTERNAL: &str = "internal";
const VIS_PUBLIC: &str = "public";

/// Default visibility for new boards and aggregated boards.
pub const DEFAULT_VISIBILITY: &str = VIS_PRIVATE;

/// Map a protobuf `BoardVisibility` value to its Postgres string.
pub fn proto_to_db(v: i32) -> &'static str {
    match v {
        2 => VIS_INTERNAL,
        3 => VIS_PUBLIC,
        _ => VIS_PRIVATE,
    }
}

/// Map a Postgres visibility string back to a protobuf `BoardVisibility` value.
pub fn db_to_proto(s: &str) -> i32 {
    match s {
        VIS_INTERNAL => 2,
        VIS_PUBLIC => 3,
        _ => 1, // private, including unknown values
    }
}

/// Returns true if the visibility is public or internal.
pub fn is_public_or_internal(s: &str) -> bool {
    s == VIS_PUBLIC || s == VIS_INTERNAL
}

/// Returns true if the visibility is public.
pub fn is_public(s: &str) -> bool {
    s == VIS_PUBLIC
}
