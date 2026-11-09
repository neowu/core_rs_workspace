use std::collections::BTreeMap;
use std::collections::BTreeSet;

use semver::VersionReq;
use toml_edit::Document;
use toml_edit::Item;
use toml_edit::TableLike;

use super::diagnostic::Diagnostic;
use super::workspace::Manifest;
use super::workspace::Workspace;

const SECTIONS: [&str; 3] = ["dependencies", "dev-dependencies", "build-dependencies"];

pub(super) fn check(workspace: &Workspace) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let root_manifest = &workspace.manifest;
    let root_dependencies: Vec<(&str, &Item)> = root_manifest
        .document
        .get("workspace")
        .and_then(|item| item.get("dependencies"))
        .and_then(Item::as_table_like)
        .map(|table| table.iter().collect())
        .unwrap_or_default();
    let root_packages: BTreeMap<&str, &str> =
        root_dependencies.iter().map(|(name, dependency)| (package_name(name, dependency), *name)).collect();

    // root key -> members inheriting it; package -> direct member declarations
    let mut inherited: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    let mut direct: BTreeMap<&str, Vec<(&Manifest, &Item)>> = BTreeMap::new();
    for member in &workspace.members {
        for (name, dependency) in dependencies(&member.document) {
            if dependency.get("workspace").and_then(Item::as_bool) == Some(true) {
                if root_dependencies.iter().any(|(root_name, _)| *root_name == name) {
                    inherited.entry(name).or_default().insert(&member.package.name);
                } else {
                    diagnostics.push(member.diagnostic(
                        dependency,
                        "missing_workspace_dependency",
                        format!("`{name}` has no declaration in [workspace.dependencies]"),
                    ));
                }
                continue;
            }
            let package = package_name(name, dependency);
            if let Some(root_name) = root_packages.get(package) {
                diagnostics.push(member.diagnostic(
                    dependency,
                    "inherit_workspace_dependency",
                    format!("`{package}` is declared in [workspace.dependencies] as `{root_name}`; inherit it with `workspace = true`"),
                ));
            } else {
                direct.entry(package).or_default().push((member, dependency));
            }
            check_wildcard(member, name, dependency, &mut diagnostics);
        }
    }

    for (package, declarations) in &direct {
        let members: BTreeSet<&str> = declarations.iter().map(|(member, _)| member.package.name.as_str()).collect();
        if members.len() > 1 {
            let members = members.iter().map(|member| format!("`{member}`")).collect::<Vec<_>>().join(", ");
            for (member, dependency) in declarations {
                diagnostics.push(member.diagnostic(
                    dependency,
                    "duplicate_dependency",
                    format!("`{package}` is declared by {members}; move it to [workspace.dependencies] and inherit it with `workspace = true`"),
                ));
            }
        }
    }

    for (name, dependency) in &root_dependencies {
        let members = inherited.get(name).map(|members| members.iter().collect::<Vec<_>>()).unwrap_or_default();
        if let [member] = members.as_slice() {
            diagnostics.push(root_manifest.diagnostic(
                dependency,
                "unshared_workspace_dependency",
                format!(
                    "workspace dependency `{name}` is only inherited by `{member}`; declare it in that member instead"
                ),
            ));
        } else if members.is_empty() {
            diagnostics.push(root_manifest.diagnostic(
                dependency,
                "unused_workspace_dependency",
                format!("workspace dependency `{name}` is not inherited by any member"),
            ));
        }
        check_wildcard(root_manifest, name, dependency, &mut diagnostics);
    }

    diagnostics.sort_by(|left, right| {
        (&left.path, left.span.byte_start, left.rule).cmp(&(&right.path, right.span.byte_start, right.rule))
    });
    diagnostics
}

/// Dependency declarations in every section, including target-specific ones.
fn dependencies(document: &Document<String>) -> Vec<(&str, &Item)> {
    let mut tables: Vec<&dyn TableLike> = vec![document.as_table()];
    if let Some(targets) = document.get("target").and_then(Item::as_table_like) {
        tables.extend(targets.iter().filter_map(|(_, target)| target.as_table_like()));
    }
    tables
        .into_iter()
        .flat_map(|table| SECTIONS.iter().filter_map(|section| table.get(section).and_then(Item::as_table_like)))
        .flat_map(TableLike::iter)
        .collect()
}

fn package_name<'a>(name: &'a str, dependency: &'a Item) -> &'a str {
    dependency.get("package").and_then(Item::as_str).unwrap_or(name)
}

fn check_wildcard(manifest: &Manifest, name: &str, dependency: &Item, diagnostics: &mut Vec<Diagnostic>) {
    if dependency.get("path").is_some() || dependency.get("git").is_some() {
        return;
    }
    let version = dependency.as_str().or_else(|| dependency.get("version").and_then(Item::as_str));
    if version.is_none_or(|version| VersionReq::parse(version).is_ok_and(|req| req == VersionReq::STAR)) {
        diagnostics.push(manifest.diagnostic(
            dependency,
            "wildcard_dependency",
            format!("registry dependency `{name}` must specify a version other than `*`"),
        ));
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::Value;
    use toml_edit::Document;

    use super::super::diagnostic::Diagnostic;
    use super::super::workspace::Manifest;
    use super::super::workspace::Package;
    use super::super::workspace::Workspace;
    use super::check;

    fn manifest(name: &str, path: &str, source: &str) -> Manifest {
        Manifest {
            path: PathBuf::from(path),
            absolute_path: PathBuf::from("/fixture").join(path),
            package: Package {
                id: format!("path+file:///fixture#{name}"),
                name: name.to_owned(),
                manifest_path: PathBuf::from("/fixture").join(path),
                target: Value::Null,
            },
            document: Document::parse(source.to_owned()).expect("valid fixture"),
        }
    }

    fn run(root: &str, members: &[&str]) -> Vec<Diagnostic> {
        let workspace = Workspace {
            root: PathBuf::from("/fixture"),
            manifest: manifest("xtask", "Cargo.toml", root),
            members: members
                .iter()
                .enumerate()
                .map(|(index, source)| manifest(&format!("m{index}"), &format!("m{index}/Cargo.toml"), source))
                .collect(),
        };
        check(&workspace)
    }

    fn rules(diagnostics: &[Diagnostic]) -> Vec<&str> {
        diagnostics.iter().map(|diagnostic| diagnostic.rule).collect()
    }

    #[test]
    fn accepts_shared_root_and_member_own_dependencies() {
        let diagnostics = run(
            r#"
[workspace.dependencies]
shared = "1"
alias = { package = "actual-package", version = "1" }
local = { path = "local" }
"#,
            &[
                r#"
[dependencies]
shared.workspace = true
alias = { workspace = true, features = ["extra"], optional = true }
own = { version = "1", default-features = false }
local.workspace = true
[target.'cfg(unix)'.build-dependencies]
platform = "1"
"#,
                r#"
[dev-dependencies]
shared = { workspace = true }
local.workspace = true
remote = { git = "https://example.com/remote" }
[target.'cfg(unix)'.dependencies]
alias.workspace = true
local.workspace = true
sibling = { path = "../m0" }
"#,
            ],
        );
        assert!(diagnostics.is_empty(), "unexpected diagnostics: {:?}", rules(&diagnostics));
    }

    #[test]
    fn rejects_duplicate_direct_dependencies_across_members() {
        let diagnostics = run(
            "",
            &[
                "[dependencies]\nshared = \"1\"\nlocal = { path = \"../local\" }\n",
                "[dev-dependencies]\nrenamed = { package = \"shared\", version = \"1\" }\n[target.'cfg(unix)'.dependencies]\nlocal = { path = \"../local\" }\n",
            ],
        );
        assert_eq!(
            rules(&diagnostics),
            vec!["duplicate_dependency"; 4],
            "each duplicate declaration should be reported"
        );
        let first = diagnostics.first().expect("duplicate diagnostic");
        assert_eq!(first.path, PathBuf::from("m0/Cargo.toml"));
        assert_eq!((first.span.line_start, first.span.column_start), (2, 10), "diagnostic should point at the value");
        assert!(first.message.contains("`m0`, `m1`"), "message should list members: {}", first.message);
    }

    #[test]
    fn accepts_repeated_declarations_within_one_member() {
        let diagnostics = run(
            "",
            &[r#"
[target.'cfg(target_os = "linux")'.dependencies]
native = { version = "1", features = ["static"] }
[target.'cfg(target_os = "macos")'.dependencies]
native = { version = "1", features = ["dynamic"] }
"#],
        );
        assert!(diagnostics.is_empty(), "one member may declare a dependency per target");
    }

    #[test]
    fn requires_inheriting_root_declared_packages() {
        let diagnostics = run(
            "[workspace.dependencies]\nshared = \"1\"\nalias = { package = \"actual\", version = \"1\" }\n",
            &[
                "[dependencies]\nshared = \"1\"\nactual = \"1\"\n",
                "[dependencies]\nshared.workspace = true\nalias.workspace = true\n",
                "[dependencies]\nshared.workspace = true\nalias.workspace = true\n",
            ],
        );
        assert_eq!(rules(&diagnostics), vec!["inherit_workspace_dependency"; 2]);
        assert!(diagnostics.iter().any(|diagnostic| diagnostic.message.contains("as `alias`")));
    }

    #[test]
    fn reports_unused_and_unshared_root_dependencies() {
        let diagnostics = run(
            "[workspace.dependencies]\nunused = \"1\"\nsingle = \"1\"\nshared = \"1\"\n",
            &[
                "[dependencies]\nsingle.workspace = true\nshared.workspace = true\n[dev-dependencies]\nsingle.workspace = true\n",
                "[dependencies]\nshared.workspace = true\n",
            ],
        );
        assert_eq!(rules(&diagnostics), vec!["unused_workspace_dependency", "unshared_workspace_dependency"]);
        let unshared = diagnostics.get(1).expect("unshared diagnostic");
        assert!(unshared.message.contains("`single`") && unshared.message.contains("`m0`"));
    }

    #[test]
    fn reports_missing_workspace_declarations() {
        let diagnostics = run("", &["[dependencies]\nmissing.workspace = true\n"]);
        assert_eq!(rules(&diagnostics), vec!["missing_workspace_dependency"]);
    }

    #[test]
    fn rejects_wildcard_registry_versions_in_root_and_members() {
        let diagnostics = run(
            r#"[workspace.dependencies]
plain = "*"
inline = { version = "*" }
bounded = "1.*"
local = { path = "local" }
remote = { git = "https://example.com/remote", version = "*" }
"#,
            &[
                r#"[dependencies]
plain.workspace = true
inline.workspace = true
bounded.workspace = true
local.workspace = true
remote.workspace = true
equivalent = "x"
unversioned = { features = ["extra"] }
"#,
                "[dependencies]\nplain.workspace = true\ninline.workspace = true\nbounded.workspace = true\nlocal.workspace = true\nremote.workspace = true\n",
            ],
        );
        assert_eq!(
            rules(&diagnostics),
            vec!["wildcard_dependency"; 4],
            "only unrestricted registry requirements should fail"
        );
    }

    #[test]
    fn renders_cargo_json_messages() {
        let diagnostics = run("", &["[dependencies]\nmissing = { workspace = true,\n  features = [\"x\"] }\n"]);
        let message = diagnostics.first().expect("diagnostic").to_json();
        assert_eq!(message["reason"], "compiler-message");
        assert_eq!(message["package_id"], "path+file:///fixture#m0");
        assert_eq!(message["message"]["code"]["code"], "missing_workspace_dependency");
        let span = &message["message"]["spans"][0];
        assert_eq!(span["file_name"], "m0/Cargo.toml");
        assert_eq!((span["line_start"].as_u64(), span["line_end"].as_u64()), (Some(2), Some(3)));
        assert_eq!(span["text"].as_array().map(Vec::len), Some(2), "every spanned line should be included");
    }
}
