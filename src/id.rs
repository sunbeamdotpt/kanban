// SPDX-License-Identifier: AGPL-3.0-or-later
//! Domain identifier type.
//!
//! Kanban uses ULIDs for all primary and foreign keys. `Id` stores a 128-bit
//! identifier and remembers whether it was originally encoded as a ULID or a
//! legacy UUID. This lets existing deployments migrate their schema without
//! regenerating every row: old UUID strings keep their canonical form while
//! new identifiers are always generated as ULIDs.

use std::fmt;
use std::str::FromStr;

/// Error returned when a string is not a valid ULID or legacy UUID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InvalidId;

impl fmt::Display for InvalidId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid id, expected ULID or legacy UUID")
    }
}

impl std::error::Error for InvalidId {}

/// Canonical encoding of a stored identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum IdKind {
    Ulid,
    Uuid,
}

/// A Kanban domain identifier.
///
/// New identifiers are ULIDs. The type also round-trips legacy UUIDs so that
/// existing rows keep their original string form after the schema migration.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Id {
    value: u128,
    kind: IdKind,
}

impl Id {
    /// Generate a new identifier (ULID).
    pub fn new() -> Self {
        let ulid = ulid::Ulid::new();
        Self {
            value: ulid.0,
            kind: IdKind::Ulid,
        }
    }

    /// Build a deterministic ULID from a legacy UUID string and a creation
    /// timestamp. The UUID's lower 80 bits become the ULID randomness portion
    /// and `created_at_ms` becomes the timestamp portion.
    ///
    /// This is used by the system migration that rewrites existing UUID primary
    /// keys to ULIDs while preserving the original time-ordering of each row.
    pub fn from_uuid_with_timestamp(uuid_str: &str, created_at_ms: u64) -> Result<Self, InvalidId> {
        let uuid_value = parse_uuid(uuid_str).ok_or(InvalidId)?;
        let timestamp = (created_at_ms as u128) << 80;
        let randomness = uuid_value & ((1u128 << 80) - 1);
        Ok(Self {
            value: timestamp | randomness,
            kind: IdKind::Ulid,
        })
    }

    /// Returns true if the supplied string is a canonical ULID.
    pub fn is_ulid(s: &str) -> bool {
        parse_ulid(s).is_some()
    }
}

impl Default for Id {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            IdKind::Ulid => write!(f, "{}", ulid::Ulid(self.value)),
            IdKind::Uuid => write!(f, "{}", format_uuid(self.value)),
        }
    }
}

impl FromStr for Id {
    type Err = InvalidId;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(value) = parse_ulid(s) {
            Ok(Self {
                value,
                kind: IdKind::Ulid,
            })
        } else if let Some(value) = parse_uuid(s) {
            Ok(Self {
                value,
                kind: IdKind::Uuid,
            })
        } else {
            Err(InvalidId)
        }
    }
}

fn parse_ulid(s: &str) -> Option<u128> {
    if s.len() != 26 {
        return None;
    }
    s.parse::<ulid::Ulid>().ok().map(|u| u.0)
}

fn parse_uuid(s: &str) -> Option<u128> {
    let hex = if s.len() == 36 {
        let parts: Vec<&str> = s.split('-').collect();
        if parts.len() != 5
            || parts[0].len() != 8
            || parts[1].len() != 4
            || parts[2].len() != 4
            || parts[3].len() != 4
            || parts[4].len() != 12
        {
            return None;
        }
        parts.join("")
    } else if s.len() == 32 {
        s.to_string()
    } else {
        return None;
    };

    if !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        return None;
    }
    u128::from_str_radix(&hex, 16).ok()
}

fn format_uuid(value: u128) -> String {
    let hex = format!("{:032x}", value);
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_uuid_with_timestamp_is_deterministic() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let id1 = Id::from_uuid_with_timestamp(uuid, 1_700_000_000_000).unwrap();
        let id2 = Id::from_uuid_with_timestamp(uuid, 1_700_000_000_000).unwrap();
        assert_eq!(id1.to_string(), id2.to_string());
        assert!(Id::is_ulid(&id1.to_string()));
    }

    #[test]
    fn from_uuid_with_timestamp_changes_with_timestamp() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let id1 = Id::from_uuid_with_timestamp(uuid, 1_700_000_000_000).unwrap();
        let id2 = Id::from_uuid_with_timestamp(uuid, 1_700_000_000_001).unwrap();
        assert_ne!(id1.to_string(), id2.to_string());
    }

    #[test]
    fn from_uuid_with_timestamp_rejects_non_uuid() {
        assert!(Id::from_uuid_with_timestamp("not-a-uuid", 0).is_err());
        assert!(Id::from_uuid_with_timestamp("01ARZ3NDEKTSV4RRFFQ69G5FAV", 0).is_err());
    }

    #[test]
    fn is_ulid_accepts_ulids_and_rejects_uuids() {
        assert!(Id::is_ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(!Id::is_ulid("550e8400-e29b-41d4-a716-446655440000"));
        assert!(!Id::is_ulid("short"));
    }
}

impl sqlx::Type<sqlx::Postgres> for Id {
    fn type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TEXT")
    }
}

impl sqlx::postgres::PgHasArrayType for Id {
    fn array_type_info() -> sqlx::postgres::PgTypeInfo {
        sqlx::postgres::PgTypeInfo::with_name("TEXT[]")
    }
}

impl<'q> sqlx::Encode<'q, sqlx::Postgres> for Id {
    fn encode_by_ref(
        &self,
        buf: &mut sqlx::postgres::PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <String as sqlx::Encode<sqlx::Postgres>>::encode(self.to_string(), buf)
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Postgres> for Id {
    fn decode(value: sqlx::postgres::PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let s = <String as sqlx::Decode<sqlx::Postgres>>::decode(value)?;
        s.parse::<Id>().map_err(Into::into)
    }
}
