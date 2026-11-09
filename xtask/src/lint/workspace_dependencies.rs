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
