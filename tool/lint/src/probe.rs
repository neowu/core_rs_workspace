use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::hash::DefaultHasher;
use std::hash::Hash as _;
use std::hash::Hasher as _;
use std::io;
use std::io::BufRead as _;
use std::io::BufReader;
use std::io::Write as _;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Stdio;

use serde::Deserialize;
use serde_json::Value;
use toml_edit::Document;
use toml_edit::Item;

use super::diagnostic::Diagnostic;
use super::workspace::Manifest;
use super::workspace::Workspace;

mod cycle;
mod field;
mod marker;
mod misplaced;
mod visibility;
mod wildcard;

/// The rule of a marker that excuses nothing, as rustc names an `#[expect]` that is not met.
const UNFULFILLED: &str = "unfulfilled_lint_expectations";

/// The files the probe is built from; a change to any of them starts a new cache.
const PROBE_FILES: [&str; 5] = ["rust-toolchain.toml", "Cargo.toml", "Cargo.lock", "build.rs", "src/main.rs"];

/// What the compiler resolved in one compilation of a crate, as written by the probe.
#[derive(Deserialize)]
struct Unit {
    /// The root file of the crate, which identifies it: a library and its unit tests are the same crate.
    root: String,
    /// Compiled with `cfg(test)`. Items are only recorded without it, uses in both.
    test: bool,
    defs: Vec<Def>,
    uses: Vec<Use>,
}

/// A non-private item, module, struct field, enum variant, or method or constant of a trait. Files are relative to the workspace root, lines 1-based and columns
/// 0-based, pointing at the name.
#[derive(Deserialize)]
struct Def {
    /// Where the name is written, which is how uses refer to the item from any crate.
    key: String,
    file: String,
    line: usize,
    column: usize,
    /// Where the item starts, below its attributes.
    item_line: usize,
    kind: String,
    name: String,
    /// `pub`, or `crate` for any restricted visibility.
    visibility: String,
    /// The module the item is private to, as a path from the crate root.
    module: String,
    /// For a field, variant or trait item, the key of its struct, enum or trait.
    owner: Option<String>,
}

#[derive(Deserialize)]
struct Use {
    to: String,
    /// The module the use is in.
    module: String,
    file: String,
    line: usize,
    column: usize,
    /// In a `use` declaration.
    import: bool,
    /// `call`, `construct`, `pattern`, `write` or `borrow` (mutably) for a place, `through` for a module on the way
    /// to an item, `wildcard` for an enum matched by a catch-all arm, else `use`.
    kind: String,
    /// Where the name of the innermost function, method, type, trait, constant or static the use is in is written.
    from: Option<String>,
    /// The item using itself: inside its own definition, an impl of it, or what a derive wrote for it.
    own: bool,
    /// Outside any body, the item or field whose interface the use is part of: its key, or a fixed `pub` or
    /// `crate` when that item cannot be narrowed.
    interface: Option<String>,
    /// For an item of the same crate, its name.
    name: Option<String>,
    /// For an item of the same crate, the module that owns it.
    to_module: Option<String>,
}

/// The text of the workspace files that items and uses are in.
struct Sources<'a> {
    workspace: &'a Workspace,
    texts: HashMap<String, String>,
}

impl<'a> Sources<'a> {
    fn load(workspace: &'a Workspace, units: &[Unit]) -> Self {
        let mut texts = HashMap::new();
        let defs = units.iter().flat_map(|unit| unit.defs.iter().map(|def| &def.file));
        let uses = units.iter().flat_map(|unit| unit.uses.iter().map(|used| &used.file));
        for file in defs.chain(uses) {
            if !texts.contains_key(file) {
                // a file that is gone is reported by the compiler
                texts.insert(file.clone(), fs::read_to_string(workspace.root.join(file)).unwrap_or_default());
            }
        }
        // a private enum is named from where it is written
        let wildcards = units.iter().flat_map(|unit| unit.uses.iter().filter(|used| used.kind == "wildcard"));
        for file in wildcards.filter_map(|used| position(&used.to)).map(|(file, ..)| file) {
            if !texts.contains_key(file) {
                texts.insert(file.to_owned(), fs::read_to_string(workspace.root.join(file)).unwrap_or_default());
            }
        }
        Self { workspace, texts }
    }

    fn text(&self, file: &str) -> &str {
        self.texts.get(file).map_or("", String::as_str)
    }

    /// The member whose directory holds the file.
    fn member(&self, file: &str) -> Option<&'a Manifest> {
        let members = self.workspace.members.iter();
        let holding = members.filter(|member| member.path.parent().is_some_and(|dir| Path::new(file).starts_with(dir)));
        holding.max_by_key(|member| member.path.as_os_str().len())
    }

    /// The name written at a key; a use outside any item has its file instead.
    fn name_at(&self, key: &str) -> String {
        let Some((file, line, column)) = position(key) else { return key.to_owned() };
        let text = self.text(file).lines().nth(line.saturating_sub(1)).unwrap_or_default();
        text.chars().skip(column).take_while(|char| char.is_alphanumeric() || *char == '_').collect()
    }

    /// Looks for the marker in the comments directly above a line, also above the attributes between them.
    fn marked(&self, file: &str, from_line: usize, to_line: usize, marker: &str) -> bool {
        self.marker_line(file, from_line, to_line, marker).is_some()
    }

    /// The 1-based line of the marker that `marked` finds.
    fn marker_line(&self, file: &str, from_line: usize, to_line: usize, marker: &str) -> Option<usize> {
        let lines: Vec<&str> = self.text(file).lines().take(to_line).collect();
        let is_comment = |line: &str| line.trim_start().starts_with("//");
        let is_attribute = |line: &str| line.trim_start().starts_with("#[");
        let above = lines.iter().enumerate().rev();
        above
            .take_while(|(index, line)| index + 1 >= from_line || is_comment(line) || is_attribute(line))
            .find(|(_, line)| is_comment(line) && line.contains(marker))
            .map(|(index, _)| index + 1)
    }
}

/// Runs the rules that need what the compiler resolved rather than names.
pub(super) fn check(workspace: &Workspace) -> Result<Vec<Diagnostic>, Box<dyn Error>> {
    let units = collect(workspace)?;
    let sources = Sources::load(workspace, &units);
    let mut diagnostics = visibility::check(&units, &sources);
    diagnostics.extend(field::check(&units, &sources));
    diagnostics.extend(cycle::check(&units, &sources));
    diagnostics.extend(wildcard::check(&units, &sources));
    diagnostics.extend(misplaced::check(&units, &sources));
    diagnostics.extend(marker::check(&units, &sources));
    Ok(diagnostics)
}

/// Compiles the workspace with the probe as rustc. Cargo skips crates that have not changed, whose facts stay
/// on disk from the run that compiled them.
fn collect(workspace: &Workspace) -> Result<Vec<Unit>, Box<dyn Error>> {
    // beside this crate in the checkout cargo compiled it from, also as a git dependency
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("../probe");
    let mut hasher = DefaultHasher::new();
    for file in PROBE_FILES {
        let path = source.join(file);
        fs::read(&path).map_err(|error| format!("{}: {error}", path.display()))?.hash(&mut hasher);
    }
    let base = workspace.target_directory.join("probe").join(format!("{:016x}", hasher.finish()));
    let toolchain = Document::parse(fs::read_to_string(source.join("rust-toolchain.toml"))?)?;
    let channel = toolchain.get("toolchain").and_then(|toolchain| toolchain.get("channel"));
    let channel = channel.and_then(Item::as_str).ok_or("the probe's rust-toolchain.toml has no channel")?;

    let built = toolchain_cargo(channel)
        .args(["build", "--release", "--locked", "--quiet", "--manifest-path"])
        .arg(source.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(base.join("build"))
        .status()
        .map_err(|error| format!("rustup: {error}"))?;
    if !built.success() {
        return Err(format!(
            "cannot build the probe; it needs the latest {channel}: `rustup toolchain install {channel} --profile \
             minimal --component rustc-dev --component llvm-tools`, or `rustup update {channel}`"
        )
        .into());
    }

    let facts = base.join("facts");
    let mut child = toolchain_cargo(channel)
        .args(["check", "--workspace", "--all-targets", "--quiet", "--message-format=json", "--target-dir"])
        .arg(base.join("check"))
        .env("RUSTC_WORKSPACE_WRAPPER", base.join("build/release/probe"))
        .env("PROBE_OUT", &facts)
        .env("PROBE_ROOT", &workspace.root)
        .current_dir(&workspace.root)
        .stdout(Stdio::piped())
        .spawn()?;
    let output = child.stdout.take().ok_or("cargo stdout is not captured")?;
    let members: HashSet<&str> = workspace.members.iter().map(|member| member.package.id.as_str()).collect();
    let mut files: Vec<PathBuf> = Vec::new();
    for line in BufReader::new(output).lines() {
        let Ok(message) = serde_json::from_str::<Value>(&line?) else { continue };
        match message.get("reason").and_then(Value::as_str) {
            Some("compiler-message") => {
                let rendered = message.pointer("/message/rendered").and_then(Value::as_str).unwrap_or_default();
                write!(io::stderr(), "{rendered}")?;
            }
            Some("compiler-artifact") => {
                let member = message.get("package_id").and_then(Value::as_str).is_some_and(|id| members.contains(id));
                let build_script = message.pointer("/target/kind/0").and_then(Value::as_str) == Some("custom-build");
                let artifact = message.pointer("/filenames/0").and_then(Value::as_str).map(Path::new);
                if let Some(stem) = artifact.and_then(Path::file_stem).and_then(|stem| stem.to_str())
                    && member
                    && !build_script
                {
                    let file = facts.join(format!("{}.json", stem.strip_prefix("lib").unwrap_or(stem)));
                    if !files.contains(&file) {
                        files.push(file);
                    }
                }
            }
            _ => {}
        }
    }
    if !child.wait()?.success() {
        return Err(format!("the workspace does not compile on {channel}").into());
    }
    let mut units = Vec::new();
    for file in files {
        let text = fs::read_to_string(&file)
            .map_err(|error| format!("{}: {error}; remove {} to compile again", file.display(), base.display()))?;
        units.push(serde_json::from_str(&text).map_err(|error| format!("{}: {error}", file.display()))?);
    }
    Ok(units)
}

/// Cargo of the probe's toolchain, without what the cargo running the lint set for its own.
fn toolchain_cargo(channel: &str) -> Command {
    let mut command = Command::new("rustup");
    command.args(["run", channel, "cargo"]);
    for name in ["RUSTUP_TOOLCHAIN", "CARGO", "RUSTC", "RUSTDOC"] {
        command.env_remove(name);
    }
    command
}

/// Whether the file is under the `src` of a member, where the rules apply.
fn in_src(sources: &Sources<'_>, file: &str) -> bool {
    let src = sources.member(file).and_then(|member| member.path.parent()).map(|dir| dir.join("src"));
    src.is_some_and(|src| Path::new(file).starts_with(src))
}

/// The file, line and column of a key, which is `file:line:column`.
fn position(key: &str) -> Option<(&str, usize, usize)> {
    let mut parts = key.rsplitn(3, ':');
    let column = parts.next()?.parse().ok()?;
    let line = parts.next()?.parse().ok()?;
    Some((parts.next()?, line, column))
}

/// Whether `module` is `of` or inside it, both as paths from the crate root.
fn within(module: &str, of: &str) -> bool {
    of.is_empty() || module.strip_prefix(of).is_some_and(|rest| rest.is_empty() || rest.starts_with("::"))
}

#[cfg(test)]
mod tests {
    use super::within;

    #[test]
    fn places_modules() {
        assert!(within("a::b", "a"));
        assert!(within("a", "a"));
        assert!(within("a", ""));
        assert!(!within("ab", "a"));
        assert!(!within("a", "a::b"));
    }
}
