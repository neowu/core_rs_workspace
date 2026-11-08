use std::path::PathBuf;

use toml_edit::Document;

use super::workspace_dependencies::check;
use crate::lint::diagnostic::Diagnostic;
use crate::lint::workspace::Manifest;
use crate::lint::workspace::Workspace;

fn run(root: &str, member: &str) -> Vec<Diagnostic> {
    let workspace = Workspace {
        root: PathBuf::from("fixture"),
        manifest: Manifest {
            path: PathBuf::from("fixture/Cargo.toml"),
            document: Document::parse(root.to_owned()).expect("valid fixture"),
        },
        members: vec![
            Manifest {
                path: PathBuf::from("fixture/first/Cargo.toml"),
                document: Document::parse(String::new()).expect("valid fixture"),
            },
            Manifest {
                path: PathBuf::from("fixture/member/Cargo.toml"),
                document: Document::parse(member.to_owned()).expect("valid fixture"),
            },
        ],
    };
    check(&workspace)
}

#[test]
fn accepts_inheritance_in_all_dependency_sections() {
    let diagnostics = run(
        r#"
[workspace.dependencies]
runtime = "1"
development = "1"
builder = "1"
platform = "1"
platform_dev = "1"
platform_build = "1"
alias = { package = "actual-package", version = "1" }
"#,
        r#"
[dependencies]
runtime.workspace = true
alias = { workspace = true, features = ["extra"], optional = true }
[dev-dependencies]
development = { workspace = true }
[build-dependencies.builder]
workspace = true
[target.'cfg(target_os = "none")'.dependencies]
platform.workspace = true
[target.'cfg(target_os = "none")'.dev-dependencies]
platform_dev.workspace = true
[target.'cfg(target_os = "none")'.build-dependencies]
platform_build.workspace = true
"#,
    );
    assert!(diagnostics.is_empty(), "inherited declarations should pass");
}

#[test]
fn rejects_direct_versions_and_sources_in_all_sections() {
    let diagnostics = run(
        "",
        r#"[dependencies]
plain = "1"
inline = { version = "1" }
remote = { git = "https://example.com/remote" }
[dev-dependencies]
development = "1"
[build-dependencies.builder]
version = "1"
[target.'cfg(unix)'.dependencies]
platform = "1"
[target.'cfg(unix)'.dev-dependencies]
platform_dev = "1"
[target.'cfg(unix)'.build-dependencies]
platform_build = "1"
"#,
    );
    assert_eq!(diagnostics.len(), 8, "direct registry and Git dependencies should fail");
    assert!(diagnostics.iter().all(|diagnostic| diagnostic.rule == "workspace_dependency_required"));
    let first = diagnostics.first().expect("at least one diagnostic");
    assert_eq!(first.path, PathBuf::from("fixture/member/Cargo.toml"));
    assert_eq!(first.line, 2, "diagnostic should point at the declaration");
    assert!(first.column > 1, "diagnostic should include the value column");
}

#[test]
fn accepts_direct_paths_in_all_dependency_sections() {
    let diagnostics = run(
        "",
        r#"[dependencies]
runtime = { path = "../first" }
alias = { package = "first", path = "../first", optional = true, features = ["extra"] }
[dev-dependencies]
development = { path = "../first", version = "1" }
[build-dependencies.builder]
path = "../first"
[target.'cfg(target_os = "none")'.dependencies]
platform = { path = "../first" }
[target.'cfg(target_os = "none")'.dev-dependencies]
platform_dev = { path = "../first" }
[target.'cfg(target_os = "none")'.build-dependencies]
platform_build = { path = "../first" }
"#,
    );
    assert!(diagnostics.is_empty(), "local path dependencies do not need workspace declarations");
}

#[test]
fn direct_paths_do_not_keep_unused_root_declarations() {
    let diagnostics = run(
        r#"[workspace.dependencies]
first = { path = "first" }
"#,
        r#"[dependencies]
first = { path = "../first" }
"#,
    );
    assert_eq!(diagnostics.len(), 1, "direct paths must not count as workspace inheritance");
    let diagnostic = diagnostics.first().expect("unused root declaration diagnostic");
    assert_eq!(diagnostic.rule, "unused_workspace_dependency");
    assert!(diagnostic.message.contains("`first`"));
}

#[test]
fn tracks_unused_entries_by_dependency_key() {
    let diagnostics = run(
        r#"[workspace.dependencies]
alias = { package = "actual", version = "1" }
actual = "1"
unused = "1"
"#,
        "[dependencies]\nalias.workspace = true\n",
    );
    assert_eq!(diagnostics.len(), 2, "an alias must not count as a use of another root key");
    assert!(diagnostics.iter().all(|diagnostic| diagnostic.rule == "unused_workspace_dependency"));
    assert!(diagnostics.iter().any(|diagnostic| diagnostic.message.contains("`actual`")));
    assert!(diagnostics.iter().any(|diagnostic| diagnostic.message.contains("`unused`")));
}

#[test]
fn checks_root_wildcards_independently_of_member_order() {
    let diagnostics = run(
        r#"[workspace.dependencies]
plain = "*"
inline = { version = "*" }
equivalent = "x"
bounded = "1.*"
local = { path = "../local" }
remote = { git = "https://example.com/remote", version = "*" }
"#,
        "[dependencies]
plain.workspace = true
inline.workspace = true
equivalent.workspace = true
bounded.workspace = true
local.workspace = true
remote.workspace = true
",
    );
    assert_eq!(diagnostics.len(), 3, "only unrestricted registry requirements should fail");
    assert!(diagnostics.iter().all(|diagnostic| diagnostic.rule == "wildcard_workspace_dependency"));
}

#[test]
fn does_not_count_direct_declarations_as_inheritance() {
    let diagnostics = run("[workspace.dependencies]\nshared = \"1\"\n", "[dependencies]\nshared = \"1\"\n");
    assert_eq!(diagnostics.len(), 2, "both the direct declaration and unused root entry should fail");
    assert!(diagnostics.iter().any(|diagnostic| diagnostic.rule == "workspace_dependency_required"));
    assert!(diagnostics.iter().any(|diagnostic| diagnostic.rule == "unused_workspace_dependency"));
}

#[test]
fn reports_missing_workspace_declarations() {
    let diagnostics = run("", "[dependencies]\nmissing.workspace = true\n");
    let diagnostic = diagnostics.first().expect("missing declaration diagnostic");
    assert_eq!(diagnostics.len(), 1, "missing root declaration should fail");
    assert_eq!(diagnostic.rule, "workspace_dependency_required");
    assert!(diagnostic.message.contains("no declaration"));
}
