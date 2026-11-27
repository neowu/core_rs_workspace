use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;

use serde_json::Value;

use super::Def;
use super::Sources;
use super::UNFULFILLED;
use super::Unit;
use super::within;
use crate::diagnostic::Diagnostic;
use crate::workspace::Manifest;

const EXPECT_UNUSED: &str = "lint: expect(unused_pub)";
const EXPECT_EXCESS: &str = "lint: expect(excess_visibility)";

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Level {
    Private,
    Crate,
    Public,
}

struct Item<'a> {
    def: &'a Def,
    /// The root file of its crate.
    root: &'a str,
    member: &'a Manifest,
    declared: Level,
    /// Marked as API for other workspaces, which nothing here uses.
    expect_unused: bool,
    /// Marked as meant to be more visible than its uses need.
    expect_excess: bool,
    /// Its visibility is not to be narrowed: marked, or called by the compiler.
    keep: bool,
    /// A `pub` item of a library that the workspace should use.
    exported: bool,
    /// The widest place it is used from.
    used: Level,
    /// Used by something other than itself and `use` declarations.
    real: bool,
    /// Used so outside tests; an item only tests use is dead code once it is narrowed.
    live: bool,
}

/// `unused_pub` and `excess_visibility` for the items and fields under each member's `src`, from where the
/// compiler found them used.
pub(super) fn check(units: &[Unit], sources: &Sources<'_>) -> Vec<Diagnostic> {
    let mut items: Vec<Item<'_>> = Vec::new();
    let mut index: HashMap<&str, usize> = HashMap::new();
    for unit in units.iter().filter(|unit| !unit.test) {
        for def in &unit.defs {
            let Some(member) = sources.member(&def.file) else { continue };
            let Some(src) = member.path.parent().map(|dir| dir.join("src")) else { continue };
            if index.contains_key(def.key.as_str()) || !Path::new(&def.file).starts_with(&src) {
                continue;
            }
            let declared = if def.visibility == "pub" { Level::Public } else { Level::Crate };
            let marked = |marker| sources.marked(&def.file, def.item_line, def.line, marker);
            let kinds = member.package.target.get("kind").and_then(Value::as_array);
            let proc_macro = kinds.is_some_and(|kinds| kinds.iter().any(|kind| kind == "proc-macro"));
            // the compiler calls the functions at the root of a proc macro crate
            let entry_point = proc_macro && def.kind == "function" && def.module.is_empty();
            let library = Path::new(&unit.root) == src.join("lib.rs");
            index.insert(&def.key, items.len());
            items.push(Item {
                def,
                root: &unit.root,
                member,
                declared,
                expect_unused: marked(EXPECT_UNUSED),
                expect_excess: marked(EXPECT_EXCESS),
                keep: entry_point,
                exported: declared == Level::Public && library && def.kind != "field" && !entry_point,
                used: Level::Private,
                real: false,
                live: false,
            });
        }
    }
    for id in 0..items.len() {
        // API for other workspaces keeps its fields open too
        let api = |item: &Item<'_>| item.declared == Level::Public && item.expect_unused;
        let owner = items[id].def.owner.as_deref().and_then(|owner| index.get(owner));
        let open = items[id].declared == Level::Public && owner.is_some_and(|owner| api(&items[*owner]));
        items[id].keep |= items[id].expect_excess || api(&items[id]) || open;
    }

    // a name in an interface must be as visible as the interface, or rustc's `private_interfaces` fires
    let mut interfaces: Vec<(&str, usize)> = Vec::new();
    for unit in units {
        for used in &unit.uses {
            let Some(&id) = index.get(used.to.as_str()) else { continue };
            let item = &mut items[id];
            let level = if unit.root != item.root {
                Level::Public
            } else if within(&used.module, &item.def.module) {
                Level::Private
            } else {
                Level::Crate
            };
            item.used = item.used.max(level);
            item.real |= !used.import && !used.own;
            item.live |= !used.import && !used.own && !unit.test;
            if let Some(interface) = &used.interface {
                interfaces.push((interface, id));
            }
        }
    }
    let mut required: Vec<Level> =
        items.iter().map(|item| if item.keep { item.declared } else { item.used.min(item.declared) }).collect();
    // interfaces are taken at the visibility they need, not the one they declare, so one run reports a whole chain
    loop {
        let mut changed = false;
        for (interface, id) in &interfaces {
            let level = match *interface {
                "pub" => Level::Public,
                "crate" => Level::Crate,
                // an item that is not recorded is private
                key => index.get(key).map_or(Level::Private, |owner| required[*owner]),
            };
            let level = level.min(items[*id].declared);
            if required[*id] < level {
                required[*id] = level;
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }

    // what each item needs when it is not kept, to tell whether its marker excuses anything
    let mut needed: Vec<Level> = items.iter().map(|item| item.used.min(item.declared)).collect();
    for (interface, id) in &interfaces {
        let level = match *interface {
            "pub" => Level::Public,
            "crate" => Level::Crate,
            key => index.get(key).filter(|owner| *owner != id).map_or(Level::Private, |owner| required[*owner]),
        };
        needed[*id] = needed[*id].max(level.min(items[*id].declared));
    }

    let mut unused = Vec::new();
    let mut excess = Vec::new();
    let mut unfulfilled = Vec::new();
    for (item, needed) in items.iter().zip(needed) {
        let Def { file, kind, name, line, column, .. } = item.def;
        let report = |rule, message| {
            item.member.diagnostic_at(PathBuf::from(file), sources.text(file), name, (*line, *column), rule, message)
        };
        let not_needed = |marker| format!("`// {marker}` on {kind} `{name}` is not needed; remove the comment");
        let is_unused = item.exported && !item.real;
        // an item nothing uses is left to rustc's `dead_code`, which does not see fields
        let is_excess = !is_unused && needed < item.declared && (item.live || kind == "field");
        if item.expect_unused && !is_unused {
            unfulfilled.push(report(UNFULFILLED, not_needed(EXPECT_UNUSED)));
        }
        if item.expect_excess && !is_excess {
            unfulfilled.push(report(UNFULFILLED, not_needed(EXPECT_EXCESS)));
        }
        if is_unused && !item.expect_unused {
            let fix = format!("remove it, or mark it with `// {EXPECT_UNUSED}`");
            unused.push(report("unused_pub", format!("pub {kind} `{name}` is not used in the workspace; {fix}")));
        } else if is_excess && !item.keep {
            let visibility = written(sources.text(file), item.def);
            let fix = match needed {
                Level::Private => format!("is not used outside its module; remove `{visibility}`"),
                _ => "is not used outside its crate; change it to `pub(crate)`".to_owned(),
            };
            let message = format!("`{visibility}` {kind} `{name}` {fix}, or mark it with `// {EXPECT_EXCESS}`");
            excess.push(report("excess_visibility", message));
        }
    }
    for diagnostics in [&mut unused, &mut excess, &mut unfulfilled] {
        diagnostics
            .sort_by(|left, right| (&left.path, left.span.line_start).cmp(&(&right.path, right.span.line_start)));
    }
    unused.extend(excess);
    unused.extend(unfulfilled);
    unused
}

/// The visibility as written, which the probe gives only as `pub` or restricted.
fn written(text: &str, def: &Def) -> String {
    let line = text.lines().nth(def.line - 1).unwrap_or_default();
    let before: String = line.chars().take(def.column).collect();
    let fallback = if def.visibility == "pub" { "pub" } else { "pub(crate)" };
    let Some(start) = before.rfind("pub") else { return fallback.to_owned() };
    let rest = &before[start..];
    match rest.find(')') {
        Some(end) if rest.starts_with("pub(") => rest[..=end].to_owned(),
        _ => "pub".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use serde_json::json;

    use super::check;
    use crate::probe::Sources;
    use crate::probe::Unit;
    use crate::probe::tests::unit;
    use crate::probe::tests::workspace;

    const SOURCE: &str =
        "pub fn one() {}\n// lint: expect(unused_pub), API\n#[inline]\npub fn two() {}\npub(super) fn three() {}";

    /// An item of `m/src/a.rs` in module `a`, named on `line`.
    fn def(line: usize, kind: &str, name: &str, visibility: &str) -> Value {
        json!({
            "key": format!("a:{line}"), "file": "m/src/a.rs", "line": line, "column": 7, "item_line": line,
            "kind": kind, "name": name, "visibility": visibility, "module": "a",
        })
    }

    fn used(line: usize, module: &str) -> Value {
        json!({
            "to": format!("a:{line}"), "module": module, "file": "m/src/b.rs", "line": 1, "column": 0,
            "member": false, "import": false, "own": false,
        })
    }

    fn with(mut fact: Value, field: &str, value: Value) -> Value {
        fact[field] = value;
        fact
    }

    fn messages(units: &[Unit]) -> Vec<String> {
        messages_in(SOURCE, units)
    }

    fn messages_in(source: &str, units: &[Unit]) -> Vec<String> {
        let workspace = workspace();
        let sources = Sources::with(&workspace, &[("m/src/a.rs", source)]);
        let short = |message: String| message.split(", or mark").next().unwrap_or_default().to_owned();
        check(units, &sources).into_iter().map(|diagnostic| short(diagnostic.message)).collect()
    }

    #[test]
    fn narrows_to_where_an_item_is_used() {
        let three = with(def(5, "function", "three", "crate"), "column", json!(14));
        let defs = [def(1, "function", "one", "pub"), def(4, "function", "two", "pub"), three];
        // `two` is marked as API for other workspaces
        let library = || unit("m/src/lib.rs", false, &defs, &[used(1, "a::inner"), used(5, "a")]);
        let three = "`pub(super)` function `three` is not used outside its module; remove `pub(super)`";
        assert_eq!(
            messages(&[library()]),
            ["`pub` function `one` is not used outside its module; remove `pub`", three]
        );
        // another crate needs `pub`, another module `pub(crate)`
        let other = unit("n/src/main.rs", false, &[], &[used(1, "")]);
        assert_eq!(messages(&[library(), other]), [three]);
        let sibling = unit("m/src/lib.rs", true, &[], &[used(1, "b")]);
        assert_eq!(
            messages(&[library(), sibling]),
            ["`pub` function `one` is not used outside its crate; change it to `pub(crate)`", three]
        );
    }

    #[test]
    fn reports_exported_items_nothing_uses() {
        let defs =
            [def(1, "function", "one", "pub"), def(4, "function", "two", "pub"), def(5, "struct", "Three", "pub")];
        let import = with(used(1, ""), "import", json!(true));
        let own = with(used(5, "a"), "own", json!(true));
        let library = unit("m/src/lib.rs", false, &defs, &[import, own]);
        assert_eq!(
            messages(&[library]),
            [
                "pub function `one` is not used in the workspace; remove it",
                "pub struct `Three` is not used in the workspace; remove it",
            ]
        );
        // a binary exports nothing, and what nothing uses there is dead code
        assert!(messages(&[unit("m/src/main.rs", false, &defs[..1], &[])]).is_empty());
    }

    #[test]
    fn keeps_what_an_interface_shows() {
        let defs = [def(1, "function", "one", "pub"), def(5, "struct", "Three", "pub")];
        let signature = with(used(5, "a"), "interface", json!("a:1"));
        let library = unit("m/src/lib.rs", false, &defs, &[used(1, "b"), signature, used(5, "a")]);
        assert_eq!(
            messages(&[library]),
            [
                "`pub` function `one` is not used outside its crate; change it to `pub(crate)`",
                "`pub` struct `Three` is not used outside its crate; change it to `pub(crate)`",
            ]
        );
    }

    #[test]
    fn leaves_items_only_tests_use() {
        let defs = [def(1, "function", "one", "pub")];
        let library = unit("m/src/lib.rs", false, &defs, &[]);
        let tests = unit("m/src/lib.rs", true, &[], &[used(1, "a::tests")]);
        assert!(messages(&[library, tests]).is_empty());
    }

    #[test]
    fn reports_markers_that_are_not_needed() {
        let two = [def(4, "function", "two", "pub")];
        let other = unit("n/src/main.rs", false, &[], &[used(4, "")]);
        assert_eq!(
            messages(&[unit("m/src/lib.rs", false, &two, &[]), other]),
            ["`// lint: expect(unused_pub)` on function `two` is not needed; remove the comment"]
        );
        // nothing is exported from a binary
        assert_eq!(messages(&[unit("m/src/main.rs", false, &two, &[])]).len(), 1);

        let source = "// lint: expect(excess_visibility), set by other workspaces\npub fn one() {}";
        let one = [def(2, "function", "one", "pub")];
        let library = || unit("m/src/lib.rs", false, &one, &[used(2, "b")]);
        assert!(messages_in(source, &[library()]).is_empty());
        let other = unit("n/src/main.rs", false, &[], &[used(2, "")]);
        assert_eq!(
            messages_in(source, &[library(), other]),
            ["`// lint: expect(excess_visibility)` on function `one` is not needed; remove the comment"]
        );
    }
}
