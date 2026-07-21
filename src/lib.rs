// SPDX-License-Identifier: AGPL-3.0-or-later
//! Library for the Sunbeam Kanban backend.
#![recursion_limit = "512"]
#![deny(dead_code)]
#![deny(unused)]
#![deny(unused_mut)]
#![deny(clippy::missing_safety_doc)]
#![deny(clippy::undocumented_unsafe_blocks)]
#![cfg_attr(not(test), deny(clippy::expect_used))]
#![cfg_attr(not(test), deny(clippy::unwrap_used))]
// just keeps syntax consistent
#![deny(clippy::needless_borrow)]

pub mod auth;
pub mod config;
pub mod cpb;
pub mod iam_proto;
pub mod id;
pub mod integrations;
pub mod realtime;
pub mod search_indexing;
pub mod server;
pub mod services;
pub mod system_migrations;

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod codegen_smoke {
    #[test]
    fn types_exist() {
        let _ = std::mem::size_of::<crate::cpb::sunbeam::kanban::v1::Card>();
        let _ = std::mem::size_of::<crate::cpb::sunbeam::kanban::v1::Project>();
        let _ = std::mem::size_of::<crate::cpb::sunbeam::kanban::v1::BoardEventEnvelope>();
    }
}
