fn main() -> Result<(), Box<dyn std::error::Error>> {
    let kanban_protos = &[
        "proto/sunbeam/kanban/v1/auth.proto",
        "proto/sunbeam/kanban/v1/attachments.proto",
        "proto/sunbeam/kanban/v1/boards.proto",
        "proto/sunbeam/kanban/v1/cards.proto",
        "proto/sunbeam/kanban/v1/events.proto",
        "proto/sunbeam/kanban/v1/aggregated_boards.proto",
        "proto/sunbeam/kanban/v1/github.proto",
        "proto/sunbeam/kanban/v1/projects.proto",
        "proto/sunbeam/kanban/v1/public_boards.proto",
        "proto/sunbeam/kanban/v1/search.proto",
        "proto/ory/keto/relation_tuples/v1alpha2/read_service.proto",
    ];
    let kanban_includes = &["proto"];

    // ---- tonic-prost: server traits + client stubs ----
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(kanban_protos, kanban_includes)?;

    for p in kanban_protos {
        println!("cargo:rerun-if-changed={p}");
    }

    Ok(())
}
