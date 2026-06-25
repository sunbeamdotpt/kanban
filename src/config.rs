// SPDX-License-Identifier: AGPL-3.0-or-later
//! Configuration utilities for the Kanban service.
//!
//! The service uses `clap` (with `env` support) as the single source of truth
//! for environment variables and command-line flags. This module only exports
//! a process-global lock used by tests that mutate environment variables.

#[cfg(test)]
pub static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
