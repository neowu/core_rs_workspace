use std::collections::BTreeSet;

use semver::VersionReq;
use toml_edit::Item;
use toml_edit::TableLike;

use super::diagnostic::Diagnostic;
use super::workspace::Manifest;
use super::workspace::Workspace;

pub(super) fn check(workspace: &Workspace) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let root_dependencies = workspace
        .manifest
        .document
        .get("workspace")
        .and_then(|item| item.get("dependencies"))
        .and_then(Item::as_table_like);
    let mut inherited = BTreeSet::new();
    for member in &workspace.members {
        check_member(member, member.document.as_table(), root_dependencies, &mut inherited, &mut diagnostics);
        if let Some(targets) = member.document.get("target").and_then(Item::as_table_like) {
            for (_, target) in targets.iter() {
                if let Some(table) = target.as_table_like() {
                    check_member(member, table, root_dependencies, &mut inherited, &mut diagnostics);
                }
            }
        }
    }
    if let Some(dependencies) = root_dependencies {
        for (name, dependency) in dependencies.iter() {
            if !inherited.contains(name) {
                diagnostics.push(workspace.manifest.diagnostic(
                    dependency,
                    "unused_workspace_dependency",
                    format!("workspace dependency `{name}` is not inherited by any member"),
                ));
            }
            let version = dependency.as_str().or_else(|| dependency.get("version").and_then(Item::as_str));
            let is_registry = dependency.get("path").is_none() && dependency.get("git").is_none();
            if is_registry
                && version.is_some_and(|version| VersionReq::parse(version).is_ok_and(|req| req == VersionReq::STAR))
            {
                diagnostics.push(workspace.manifest.diagnostic(
                    dependency,
                    "wildcard_workspace_dependency",
                    format!("workspace registry dependency `{name}` must specify a version other than `*`"),
                ));
            }
        }
    }
    diagnostics
}

fn check_member(
    manifest: &Manifest,
    table: &dyn TableLike,
    root_dependencies: Option<&dyn TableLike>,
    inherited: &mut BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for section in ["dependencies", "dev-dependencies", "build-dependencies"] {
        if let Some(dependencies) = table.get(section).and_then(Item::as_table_like) {
            for (name, dependency) in dependencies.iter() {
                if dependency.get("path").and_then(Item::as_str).is_some() {
                    continue;
                }
                if dependency.get("workspace").and_then(Item::as_bool) != Some(true) {
                    diagnostics.push(manifest.diagnostic(
                        dependency,
                        "workspace_dependency_required",
                        format!("`{name}` in [{section}] must use `workspace = true` or a local path"),
                    ));
                } else if !root_dependencies.is_some_and(|root| root.contains_key(name)) {
                    diagnostics.push(manifest.diagnostic(
                        dependency,
                        "workspace_dependency_required",
                        format!("`{name}` has no declaration in [workspace.dependencies]"),
                    ));
                } else {
                    inherited.insert(name.to_owned());
                }
            }
        }
    }
}
