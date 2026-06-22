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
// for @siennathesane's sanity and to make it clear the scope of error handling. and because it's
// super fucking subtle and i'll miss it in code reviews sorry not sorry
#![deny(clippy::question_mark_used)]
// just keeps syntax consistent
#![deny(clippy::needless_borrow)]

pub mod auth;
pub mod integrations;
pub mod keto_proto;
pub mod pb;
pub mod realtime;
pub mod server;
pub mod services;

#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
static TEST_RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();

#[cfg(test)]
#[ctor::ctor]
fn init_test_containers() {
    // Start the shared testcontainers stack once per test process. This sets
    // the standard service env vars (DATABASE_URL, NATS_URL, VALKEY_URL, ...)
    // before any test reads them. Keep the runtime alive in a static so the
    // container handles (which are bound to it) can be stopped cleanly at exit.
    let rt = TEST_RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("test runtime"));
    rt.block_on(async {
        let _ = crate::test_support::containers::setup().await;
    });
}

#[cfg(test)]
#[ctor::dtor]
fn teardown_test_containers() {
    // Stop and remove the containers started by `setup()` so the host is not
    // left with dangling containers after the test process exits. The removal
    // runs the container CLI directly, so it does not need a Tokio runtime.
    crate::test_support::containers::teardown();
}

#[cfg(test)]
mod codegen_smoke {
    #[test]
    fn types_exist() {
        let _ = std::mem::size_of::<crate::pb::Card>();
        let _ = std::mem::size_of::<crate::pb::Project>();
        let _ = std::mem::size_of::<crate::pb::BoardEventEnvelope>();
    }
}
