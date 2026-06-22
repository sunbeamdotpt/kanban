//! keto-coverage — CI coverage check binary.
//!
//! Parses every `proto/sunbeam/kanban/v1/*.proto` file for RPC declarations
//! and compares the resulting expected method set against the static
//! `MATRIX` exported from `kanban::auth::keto_dispatch`.
//!
//! Exit 0 if the sets are equal; non-zero if there are missing or extra
//! entries.  Intended to run in CI after any proto or matrix change.
//!
//! Usage:
//!   cargo run -p kanban --bin keto-coverage

use std::collections::HashSet;
use std::path::Path;
use std::process;

fn main() {
    let code = run();
    process::exit(code);
}

/// Core coverage check. Returns the process exit code.
fn run() -> i32 {
    let workspace_root = workspace_root();
    let proto_dir = workspace_root.join("proto/sunbeam/kanban/v1");

    let expected: HashSet<String> = scan_proto_methods(&proto_dir)
        .into_iter()
        .filter(|m| !kanban::auth::keto_dispatch::BYPASSED_METHODS.contains(&m.as_str()))
        .collect();
    let actual: HashSet<String> = kanban::auth::keto_dispatch::matrix()
        .iter()
        .map(|e| e.method.to_string())
        .collect();

    let missing: Vec<_> = {
        let mut v: Vec<_> = expected
            .iter()
            .filter(|m| !actual.contains(m.as_str()))
            .collect();
        v.sort();
        v
    };
    let extra: Vec<_> = {
        let mut v: Vec<_> = actual
            .iter()
            .filter(|m| !expected.contains(m.as_str()))
            .collect();
        v.sort();
        v
    };

    let ok = missing.is_empty() && extra.is_empty();

    println!("keto-coverage: proto methods = {}", expected.len());
    println!("keto-coverage: matrix entries = {}", actual.len());

    if missing.is_empty() {
        println!("keto-coverage: no missing entries");
    } else {
        println!("\nMISSING from matrix ({}):", missing.len());
        for m in &missing {
            println!("  - {m}");
        }
    }

    if extra.is_empty() {
        println!("keto-coverage: no extra entries");
    } else {
        println!(
            "\nEXTRA in matrix ({}) (no matching proto RPC):",
            extra.len()
        );
        for m in &extra {
            println!("  + {m}");
        }
    }

    if ok {
        println!(
            "\nketo-coverage: OK — matrix covers all {} RPCs",
            expected.len()
        );
        0
    } else {
        eprintln!(
            "\nketo-coverage: FAIL — {} missing, {} extra",
            missing.len(),
            extra.len()
        );
        1
    }
}

/// Locate the workspace root by walking up from the binary's manifest dir
/// until we find a `sunbeam.workspace.yaml` or `Cargo.toml` at the root.
///
/// At runtime this binary is invoked via `cargo run -p kanban --bin
/// keto-coverage` from the workspace root; `CARGO_MANIFEST_DIR` is set by
/// Cargo to the package directory.  We walk up two levels to reach the
/// workspace root (`apps/kanban` → `apps` → workspace root).
fn workspace_root() -> std::path::PathBuf {
    // CARGO_MANIFEST_DIR is set by Cargo at build time to the package
    // directory. In the monorepo this is apps/kanban; in the split repo it
    // is the repo root. Walk upward until we find a directory that contains
    // sunbeam.workspace.yaml — that is the monorepo root. If not found,
    // fall back to CARGO_MANIFEST_DIR when the proto dir exists there.
    let manifest: std::path::PathBuf = env!("CARGO_MANIFEST_DIR").into();
    let mut candidate = manifest.as_path();

    loop {
        if candidate.join("sunbeam.workspace.yaml").exists() {
            return candidate.to_path_buf();
        }
        // Split-repo fallback: protos live directly under the package root.
        if manifest.join("proto/sunbeam/kanban/v1").exists() {
            return manifest.clone();
        }
        match candidate.parent() {
            Some(p) => candidate = p,
            None => {
                eprintln!(
                    "keto-coverage: could not locate sunbeam.workspace.yaml or \
                     local proto dir; falling back to CARGO_MANIFEST_DIR"
                );
                return manifest;
            }
        }
    }
}

/// Scan all `.proto` files under `dir` and return the set of fully-qualified
/// gRPC method paths in the `sunbeam.kanban.v1` package.
fn scan_proto_methods(dir: &Path) -> HashSet<String> {
    let mut methods = HashSet::new();

    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(err) => {
            eprintln!("keto-coverage: cannot read proto dir {dir:?}: {err}");
            process::exit(2);
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("proto") {
            continue;
        }

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(err) => {
                eprintln!("keto-coverage: cannot read {path:?}: {err}");
                process::exit(2);
            }
        };

        let mut current_service: Option<String> = None;

        for line in content.lines() {
            let trimmed = line.trim();

            if let Some(rest) = trimmed.strip_prefix("service ") {
                let name = rest
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_end_matches('{')
                    .trim()
                    .to_string();
                current_service = Some(name);
                continue;
            }

            if trimmed == "}" {
                if current_service.is_some() {
                    current_service = None;
                }
                continue;
            }

            if let Some(svc) = &current_service
                && let Some(rest) = trimmed.strip_prefix("rpc ")
            {
                let rpc_name = rest.split('(').next().unwrap_or("").trim().to_string();
                if !rpc_name.is_empty() {
                    methods.insert(format!("/sunbeam.kanban.v1.{svc}/{rpc_name}"));
                }
            }
        }
    }

    methods
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_proto_methods_finds_known_rpc() {
        let root = workspace_root();
        let proto_dir = root.join("proto/sunbeam/kanban/v1");
        let methods = scan_proto_methods(&proto_dir);

        assert!(
            methods.contains("/sunbeam.kanban.v1.CardService/CreateCard"),
            "expected CreateCard in scanned methods"
        );
        assert!(
            methods.contains("/sunbeam.kanban.v1.BoardService/GetBoard"),
            "expected GetBoard in scanned methods"
        );
    }

    #[test]
    fn scan_proto_methods_is_non_empty() {
        let root = workspace_root();
        let proto_dir = root.join("proto/sunbeam/kanban/v1");
        let methods = scan_proto_methods(&proto_dir);
        assert!(!methods.is_empty(), "expected at least one proto RPC");
    }

    #[test]
    fn scan_proto_methods_ignores_non_proto_files() {
        let tmp = std::env::temp_dir().join(format!(
            "keto-coverage-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("README.md"), "# not a proto").unwrap();
        let methods = scan_proto_methods(&tmp);
        assert!(methods.is_empty());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn run_exits_zero_when_matrix_covers_protos() {
        // The real run should succeed in a clean checkout.
        assert_eq!(run(), 0);
    }
}
