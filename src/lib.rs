//! Kanban service library.
//! Stage 2a scaffold — the real bootstrap lands in Stage 2c.

pub mod auth;
pub mod pb;

#[cfg(test)]
mod codegen_smoke {
    #[test]
    fn types_exist() {
        // Verify Card type exists
        let _ = std::mem::size_of::<crate::pb::Card>();
        // Verify Project type exists
        let _ = std::mem::size_of::<crate::pb::Project>();
        // Verify BoardEventEnvelope type exists
        let _ = std::mem::size_of::<crate::pb::BoardEventEnvelope>();
    }
}
