//! Kanban service library.
#![recursion_limit = "512"]

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
mod codegen_smoke {
    #[test]
    fn types_exist() {
        let _ = std::mem::size_of::<crate::pb::Card>();
        let _ = std::mem::size_of::<crate::pb::Project>();
        let _ = std::mem::size_of::<crate::pb::BoardEventEnvelope>();
    }
}
