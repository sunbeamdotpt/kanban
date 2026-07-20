// SPDX-License-Identifier: AGPL-3.0-or-later
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ---- connectrpc-build: sso-gateway IAM clients ----
    connectrpc_build::Config::new()
        .files(&[
            "proto/iam/iam/v1/permission.proto",
            "proto/iam/iam/v1/agent.proto",
            "proto/iam/iam/v1/application.proto",
            "proto/iam/iam/v1/tenant.proto",
            "proto/iam/iam/v1/common.proto",
        ])
        .includes(&["proto/iam"])
        .include_file("_iam.rs")
        .compile()?;

    println!("cargo:rerun-if-changed=proto/iam/iam/v1/permission.proto");
    println!("cargo:rerun-if-changed=proto/iam/iam/v1/agent.proto");
    println!("cargo:rerun-if-changed=proto/iam/iam/v1/application.proto");
    println!("cargo:rerun-if-changed=proto/iam/iam/v1/tenant.proto");
    println!("cargo:rerun-if-changed=proto/iam/iam/v1/common.proto");

    let kanban_protos = &[
        "proto/sunbeam/kanban/v1/attachments.proto",
        "proto/sunbeam/kanban/v1/boards.proto",
        "proto/sunbeam/kanban/v1/cards.proto",
        "proto/sunbeam/kanban/v1/events.proto",
        "proto/sunbeam/kanban/v1/aggregated_boards.proto",
        "proto/sunbeam/kanban/v1/github.proto",
        "proto/sunbeam/kanban/v1/projects.proto",
        "proto/sunbeam/kanban/v1/public_boards.proto",
        "proto/sunbeam/kanban/v1/search.proto",
        "proto/sunbeam/kanban/v1/templates.proto",
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
