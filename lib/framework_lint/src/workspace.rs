use std::collections::HashSet;
use std::env;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::Value;
use toml_edit::Document;
use toml_edit::Item;

use super::diagnostic::Diagnostic;
use super::diagnostic::Span;
use super::diagnostic::SpanLine;

pub(super) struct Workspace {
    pub(super) root: PathBuf,
    pub(super) manifest: Manifest,
    pub(super) members: Vec<Manifest>,
}

pub(super) struct Manifest {
    /// Relative to the workspace root.
    pub(super) path: PathBuf,
    pub(super) absolute_path: PathBuf,
    /// Package that diagnostics are attributed to in JSON output; the root manifest uses the package running the lint.
    pub(super) package: Package,
    pub(super) document: Document<String>,
}

#[derive(Clone, Deserialize)]
pub(super) struct Package {
    pub(super) id: String,
    pub(super) name: String,
    pub(super) manifest_path: PathBuf,
    #[serde(rename = "targets", deserialize_with = "first_target")]
    pub(super) target: Value,
}

fn first_target<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
    let targets = Vec::<Value>::deserialize(deserializer)?;
    Ok(targets.into_iter().next().unwrap_or_default())
}

impl Manifest {
    fn load(root: &Path, manifest_path: PathBuf, package: Package) -> Result<Self, Box<dyn Error>> {
        let source = fs::read_to_string(&manifest_path)?;
        let document = Document::parse(source).map_err(|error| format!("{}: {error}", manifest_path.display()))?;
        let path = manifest_path.strip_prefix(root).unwrap_or(&manifest_path).to_path_buf();
        Ok(Self { path, absolute_path: manifest_path, package, document })
    }

    pub(super) fn diagnostic(&self, item: &Item, rule: &'static str, message: String) -> Diagnostic {
        let source = self.document.raw();
        let range = item.span().unwrap_or_default();
        let (line_start, column_start) = position(source, range.start);
        let (line_end, column_end) = position(source, range.end);
        let lines = source
            .lines()
            .enumerate()
            .skip(line_start - 1)
            .take(line_end - line_start + 1)
            .map(|(index, text)| SpanLine {
                text: text.to_owned(),
                highlight_start: if index + 1 == line_start { column_start } else { 1 },
                highlight_end: if index + 1 == line_end { column_end } else { text.chars().count() + 1 },
            })
            .collect();
        Diagnostic {
            path: self.path.clone(),
            manifest_path: self.absolute_path.clone(),
            package_id: self.package.id.clone(),
            target: self.package.target.clone(),
            span: Span {
                byte_start: range.start,
                byte_end: range.end,
                line_start,
                line_end,
                column_start,
                column_end,
                lines,
            },
            rule,
            message,
        }
    }
}

/// 1-based line and character column of a byte offset.
fn position(source: &str, offset: usize) -> (usize, usize) {
    let prefix = source.get(..offset).unwrap_or_default();
    let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = prefix.rsplit('\n').next().unwrap_or_default().chars().count() + 1;
    (line, column)
}

#[derive(Deserialize)]
struct Metadata {
    workspace_root: PathBuf,
    workspace_members: HashSet<String>,
    packages: Vec<Package>,
}

pub(super) fn load() -> Result<Workspace, Box<dyn Error>> {
    // `cargo run` sets this at runtime to the lint binary's package, i.e. the wrapper member of the linted workspace.
    let runner_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("cargo lint must be started by `cargo run`")?);
    let runner_manifest = runner_dir.join("Cargo.toml");
    let output =
        super::cargo().args(["metadata", "--no-deps", "--format-version", "1"]).current_dir(&runner_dir).output()?;
    if !output.status.success() {
        return Err(format!("cargo metadata failed:\n{}", String::from_utf8_lossy(&output.stderr)).into());
    }
    let metadata: Metadata = serde_json::from_slice(&output.stdout)?;
    let root = metadata.workspace_root;
    let runner = metadata
        .packages
        .iter()
        .find(|package| package.manifest_path == runner_manifest)
        .ok_or_else(|| format!("{} is not a member of the workspace", runner_manifest.display()))?
        .clone();
    let manifest = Manifest::load(&root, root.join("Cargo.toml"), runner)?;
    let mut members = Vec::new();
    for package in metadata.packages {
        if metadata.workspace_members.contains(&package.id) {
            members.push(Manifest::load(&root, package.manifest_path.clone(), package)?);
        }
    }
    members.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(Workspace { root, manifest, members })
}
