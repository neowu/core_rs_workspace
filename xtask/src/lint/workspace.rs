use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;

use serde::Deserialize;
use toml_edit::Document;
use toml_edit::Item;

use super::diagnostic::Diagnostic;

pub(super) struct Workspace {
    pub(super) root: PathBuf,
    pub(super) manifest: Manifest,
    pub(super) members: Vec<Manifest>,
}

pub(super) struct Manifest {
    pub(super) path: PathBuf,
    pub(super) document: Document<String>,
}

impl Manifest {
    fn load(path: PathBuf) -> Result<Self, Box<dyn Error>> {
        let source = fs::read_to_string(&path)?;
        let document = Document::parse(source).map_err(|error| format!("{}: {error}", path.display()))?;
        Ok(Self { path, document })
    }

    pub(super) fn diagnostic(&self, item: &Item, rule: &'static str, message: String) -> Diagnostic {
        let source = self.document.raw();
        let prefix = item.span().and_then(|span| source.get(..span.start)).unwrap_or_default();
        let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
        let column = prefix.rsplit('\n').next().unwrap_or_default().chars().count() + 1;
        Diagnostic { path: self.path.clone(), line, column, rule, message }
    }
}

#[derive(Deserialize)]
struct Metadata {
    workspace_root: PathBuf,
    workspace_members: HashSet<String>,
    packages: Vec<Package>,
}

#[derive(Deserialize)]
struct Package {
    id: String,
    manifest_path: PathBuf,
}

pub(super) fn load() -> Result<Workspace, Box<dyn Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).parent().ok_or("xtask must be inside the workspace")?;
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(root)
        .output()?;
    if !output.status.success() {
        return Err(format!("cargo metadata failed:\n{}", String::from_utf8_lossy(&output.stderr)).into());
    }
    let metadata: Metadata = serde_json::from_slice(&output.stdout)?;
    let manifest = Manifest::load(metadata.workspace_root.join("Cargo.toml"))?;
    let mut members = Vec::new();
    for package in metadata.packages {
        if metadata.workspace_members.contains(&package.id) {
            members.push(Manifest::load(package.manifest_path)?);
        }
    }
    members.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(Workspace { root: metadata.workspace_root, manifest, members })
}
