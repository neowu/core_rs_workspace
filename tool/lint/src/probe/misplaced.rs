use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

use super::Def;
use super::Sources;
use super::UNFULFILLED;
use super::Unit;
use super::Use;
use super::in_src;
use super::visibility::written;
use super::within;
use crate::diagnostic::Diagnostic;

const EXPECT: &str = "lint: expect(misplaced_item)";

/// `misplaced_item`: a function, constant or static that only one other part of its crate uses, and that needs
/// nothing only its own module can see, can move to its user and be private there. Whether it reads better
/// there is a judgement, so the item is moved or marked.
pub(super) fn check(units: &[Unit], sources: &Sources<'_>) -> Vec<Diagnostic> {
    // by key, an item with the root file of its crate
    let mut defs: HashMap<&str, (&Def, &str)> = HashMap::new();
    for unit in units.iter().filter(|unit| !unit.test) {
        for def in &unit.defs {
            defs.entry(&def.key).or_insert((def, &unit.root));
        }
    }
    let open = |def: &Def| matches!(written(sources.text(&def.file), def).as_str(), "pub" | "pub(crate)");
    // the modules any module of the crate can go through
    let modules = defs.values().filter(|(def, _)| def.kind == "module" && open(def));
    let open_modules: HashSet<(&str, String)> =
        modules.map(|(def, root)| (*root, join(&def.module, &def.name))).collect();

    // by item, the module all of its uses are in or under, and the depth of the shallowest module using it; none
    // when another crate uses it
    let mut users: HashMap<&str, Option<(Vec<&str>, usize)>> = HashMap::new();
    // by item, what its definition uses
    let mut needs: HashMap<&str, Vec<&Use>> = HashMap::new();
    for unit in units {
        // a catch-all arm names nothing
        for used in unit.uses.iter().filter(|used| used.kind != "wildcard") {
            if !unit.test
                && let Some(from) = used.from.as_deref().filter(|from| *from != used.to)
            {
                needs.entry(from).or_default().push(used);
            }
            let Some((_, root)) = defs.get(used.to.as_str()) else { continue };
            // the unit tests of its crate move with the item; a test in another crate cannot reach a private one
            if used.own || (unit.test && unit.root == *root) {
                continue;
            }
            let module = path(&used.module);
            let user = users.entry(&used.to).or_insert_with(|| Some((module.clone(), module.len())));
            if unit.root != *root {
                *user = None;
            } else if let Some((shared, depth)) = user {
                let same = shared.iter().zip(&module).take_while(|(left, right)| left == right).count();
                shared.truncate(same);
                *depth = (*depth).min(module.len());
            }
        }
    }

    let mut items: Vec<&(&Def, &str)> = defs.values().collect();
    items.sort_by_key(|(def, _)| (&def.file, def.line, def.column));
    items.retain(|(def, _)| {
        matches!(def.kind.as_str(), "function" | "constant" | "static") && in_src(sources, &def.file)
    });
    // by item, the module it can move to
    let mut movable: HashMap<&str, String> = HashMap::new();
    for (def, root) in &items {
        // modules that only share a parent are several users, and the parent is not one of them
        let user = users.get(def.key.as_str()).and_then(|user| user.as_ref());
        let Some(user) = user.filter(|(shared, depth)| shared.len() == *depth).map(|(shared, _)| shared.join("::"))
        else {
            continue;
        };
        // used from above is how a module is split into children; used from inside can be private as it is
        if within(&def.module, &user) || within(&user, &def.module) {
            continue;
        }
        let needs = needs.get(def.key.as_str()).map_or(&[][..], Vec::as_slice);
        let reachable = {
            let user = &user;
            needs.iter().all(|used| {
                let Some(to_module) = &used.to_module else { return true };
                let (home, is_open) = match defs.get(used.to.as_str()) {
                    Some((needed, _)) => (needed.module.as_str(), open(needed)),
                    None => (to_module.as_str(), false),
                };
                if within(user, home) {
                    return true;
                }
                // the module the two share sees its child; every module below that must be open
                let (from, to) = (path(user), path(home));
                let shared = from.iter().zip(&to).take_while(|(left, right)| left == right).count();
                is_open && (shared + 1..to.len()).all(|end| open_modules.contains(&(*root, to[..=end].join("::"))))
            })
        };
        if reachable {
            movable.insert(&def.key, user);
        }
    }
    // a constant stays with the constants beside it: one that stays keeps the others of its module
    let value = |def: &Def| matches!(def.kind.as_str(), "constant" | "static");
    let tables: HashSet<(&str, &str)> = items
        .iter()
        .filter(|(def, _)| value(def) && !movable.contains_key(def.key.as_str()))
        .map(|(def, root)| (*root, def.module.as_str()))
        .collect();

    let mut diagnostics = Vec::new();
    for (def, root) in items {
        let Some(member) = sources.member(&def.file) else { continue };
        let kept = value(def) && tables.contains(&(*root, def.module.as_str()));
        let user = movable.get(def.key.as_str()).filter(|_| !kept);
        let marked = sources.marked(&def.file, def.item_line, def.line, EXPECT);
        let (rule, message) = match user {
            Some(user) if !marked => {
                let message = format!(
                    "{} `{}` is used only in `{}` and needs nothing private to `{}`; move it there and it can be \
                     private, or mark it with `// {EXPECT}` and why it stays",
                    def.kind,
                    def.name,
                    join("crate", user),
                    join("crate", &def.module),
                );
                ("misplaced_item", message)
            }
            None if marked => {
                let message = format!("`// {EXPECT}` on {} `{}` is not needed; remove the comment", def.kind, def.name);
                (UNFULFILLED, message)
            }
            _ => continue,
        };
        let position = (def.line, def.column);
        let text = sources.text(&def.file);
        diagnostics.push(member.diagnostic_at(PathBuf::from(&def.file), text, &def.name, position, rule, message));
    }
    diagnostics
}

fn path(module: &str) -> Vec<&str> {
    module.split("::").filter(|part| !part.is_empty()).collect()
}

fn join(module: &str, name: &str) -> String {
    if module.is_empty() { name.to_owned() } else { format!("{module}::{name}") }
}
