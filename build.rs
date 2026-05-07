fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---- kanban v1 proto (tonic-prost client stubs, server=false) ----
    // Paths are relative to the workspace root (where Cargo.toml of the workspace is)
    let kanban_protos = &[
        "../../proto/sunbeam/kanban/v1/auth.proto",
        "../../proto/sunbeam/kanban/v1/attachments.proto",
        "../../proto/sunbeam/kanban/v1/boards.proto",
        "../../proto/sunbeam/kanban/v1/cards.proto",
        "../../proto/sunbeam/kanban/v1/events.proto",
        "../../proto/sunbeam/kanban/v1/forgejo.proto",
        "../../proto/sunbeam/kanban/v1/projects.proto",
        "../../proto/sunbeam/kanban/v1/search.proto",
    ];
    let kanban_includes = &["../../proto"];
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(kanban_protos, kanban_includes)?;

    for p in kanban_protos {
        println!("cargo:rerun-if-changed={p}");
    }

    Ok(())
}
