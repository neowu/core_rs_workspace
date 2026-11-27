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

impl Item<'_> {
    /// A variant or an item of a trait, which is as visible as its enum or trait and can only be unused.
    fn part(&self) -> bool {
        matches!(self.def.kind.as_str(), "variant" | "trait method" | "trait constant")
    }

    /// Checked wherever it is used from, since rustc's `dead_code` does not see it.
    fn unseen(&self) -> bool {
        matches!(self.def.kind.as_str(), "field" | "module")
    }

    const fn unused(&self) -> bool {
        self.exported && !self.real
    }
}

/// `unused_pub` and `excess_visibility` for the items, modules and fields under each member's `src`, from where the
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
                exported: declared == Level::Public
                    && library
                    && !matches!(def.kind.as_str(), "field" | "module")
                    && !entry_point,
                used: Level::Private,
                real: false,
                live: false,
            });
        }
    }
    for id in 0..items.len() {
        // API for other workspaces keeps its fields open too, and all of its variants and trait items
        let api = |item: &Item<'_>| item.declared == Level::Public && item.expect_unused;
        let owner = items[id].def.owner.as_deref().and_then(|owner| index.get(owner));
        let open = items[id].declared == Level::Public && owner.is_some_and(|owner| api(&items[*owner]));
        let part = items[id].part();
        items[id].keep |= items[id].expect_excess || api(&items[id]) || open || part;
        // what is unused with its enum or trait is reported there
        let owned = owner.is_none_or(|owner| !items[*owner].expect_unused && items[*owner].exported);
        items[id].exported &= !part || owned;
    }

    // a name in an interface must be as visible as the interface, or rustc's `private_interfaces` fires
    let mut interfaces: Vec<(&str, usize)> = Vec::new();
    for unit in units {
        // a catch-all arm does not name the enum
        for used in unit.uses.iter().filter(|used| used.kind != "wildcard") {
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
        let owner = item.def.owner.as_deref().and_then(|owner| index.get(owner)).map(|owner| &items[*owner]);
        let is_unused = item.unused() && !(item.part() && owner.is_some_and(Item::unused));
        // an item nothing uses is left to rustc's `dead_code`
        let is_excess = !is_unused && !item.part() && needed < item.declared && (item.live || item.unseen());
        if item.expect_unused && !is_unused {
            unfulfilled.push(report(UNFULFILLED, not_needed(EXPECT_UNUSED)));
        }
        if item.expect_excess && !is_excess {
            unfulfilled.push(report(UNFULFILLED, not_needed(EXPECT_EXCESS)));
        }
        if is_unused && !item.expect_unused {
            let fix = format!("remove it, or mark it with `// {EXPECT_UNUSED}`");
            let what = match owner {
                Some(owner) if item.part() => format!("{kind} `{name}` of pub {} `{}`", owner.def.kind, owner.def.name),
                _ => format!("pub {kind} `{name}`"),
            };
            unused.push(report("unused_pub", format!("{what} is not used in the workspace; {fix}")));
        } else if is_excess && !item.keep {
            let visibility = written(sources.text(file), item.def);
            let fix = match needed {
                Level::Private => format!("is not used outside its module; remove `{visibility}`"),
                Level::Crate | Level::Public => "is not used outside its crate; change it to `pub(crate)`".to_owned(),
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
    let after: String = line.chars().skip(def.column).collect();
    let fallback = if def.visibility == "pub" { "pub" } else { "pub(crate)" };
    // a tuple field has no name; it is identified by where it starts, which is its visibility
    let rest = if after.starts_with("pub") {
        after.as_str()
    } else {
        let Some(start) = before.rfind("pub") else { return fallback.to_owned() };
        &before[start..]
    };
    match rest.find(')') {
        Some(end) if rest.starts_with("pub(") => rest[..=end].to_owned(),
        _ => "pub".to_owned(),
    }
}
