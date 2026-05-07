//! Kanban service library.

pub mod auth;
pub mod integrations;
pub mod pb;
pub mod server;
pub mod services;

#[cfg(test)]
mod codegen_smoke {
    #[test]
    fn types_exist() {
        let _ = std::mem::size_of::<crate::pb::Card>();
        let _ = std::mem::size_of::<crate::pb::Project>();
        let _ = std::mem::size_of::<crate::pb::BoardEventEnvelope>();
    }
}
