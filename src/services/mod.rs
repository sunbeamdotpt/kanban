//! Stage-3 stub service implementations.
//!
//! Every service returns `Unimplemented` until Stage 3 fills them in.
//! The stubs live here so the server can register them and the binary compiles.

pub mod auth;
pub mod attachments;
pub mod boards;
pub mod cards;
pub mod forgejo;
pub mod projects;
pub mod search;
