// SPDX-License-Identifier: AGPL-3.0-or-later
//! Service module exports.
//!
//! Each submodule implements one of the kanban gRPC services defined in
//! `proto/sunbeam/kanban/v1`. This file simply re-exports them so the
//! server can register them with Tonic.

pub mod aggregated_boards;
pub mod attachments;
pub mod boards;
pub mod cards;
pub mod github;
pub mod labels;
pub mod milestones;
pub mod projects;
pub mod public_boards;
pub mod search;
pub mod templates;
pub mod visibility;
