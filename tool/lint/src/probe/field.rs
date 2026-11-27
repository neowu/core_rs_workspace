use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::PathBuf;

use super::Def;
use super::Sources;
use super::Unit;
use super::in_src;
use super::within;
use crate::diagnostic::Diagnostic;

const ENFORCE: &str = "lint: enforce(field_write)";

/// Where a field is changed: file, line, column and the name of the field.
type Change<'a> = (&'a str, usize, usize, &'a str);

/// `field_write`, for the structs that ask for it: a function outside the struct's module that changes it in more
/// than one place holds logic that belongs to the struct. A single change is a setter's worth and is left alone.
pub(super) fn check(units: &[Unit], sources: &Sources<'_>) -> Vec<Diagnostic> {
    // by key, an item with the root file of its crate
    let mut defs: HashMap<&str, (&Def, &str)> = HashMap::new();
    for unit in units.iter().filter(|unit| !unit.test) {
        for def in unit.defs.iter().filter(|def| in_src(sources, &def.file)) {
            defs.entry(&def.key).or_insert((def, &unit.root));
        }
    }
    let structs = defs.values().filter(|(def, _)| def.kind == "struct");
    let enforced: HashSet<&str> = structs
        .filter(|(def, _)| sources.marked(&def.file, def.item_line, def.line, ENFORCE))
        .map(|(def, _)| def.key.as_str())
        .collect();

    // by the function the changes are in and the struct they change
    let mut changes: BTreeMap<(&str, &str), BTreeSet<Change<'_>>> = BTreeMap::new();
    for unit in units.iter().filter(|unit| !unit.test) {
        for used in unit.uses.iter().filter(|used| matches!(used.kind.as_str(), "write" | "borrow")) {
            let Some(&(field, root)) = defs.get(used.to.as_str()) else { continue };
            let Some(owner) = field.owner.as_deref().filter(|owner| enforced.contains(owner)) else { continue };
            // its own type, and the module it is private to, change it as they need
            if used.own || (unit.root == root && within(&used.module, &field.module)) || !in_src(sources, &used.file) {
                continue;
            }
            let from = used.from.as_deref().unwrap_or(&used.file);
            changes.entry((from, owner)).or_default().insert((&used.file, used.line, used.column, &field.name));
        }
    }

    let mut diagnostics = Vec::new();
    for ((from, owner), places) in changes {
        let (Some(&(file, line, column, field)), Some((owner, _))) = (places.first(), defs.get(owner)) else {
            continue;
        };
        let Some(member) = sources.member(file) else { continue };
        if places.len() < 2 {
            continue;
        }
        let fields: BTreeSet<&str> = places.iter().map(|place| place.3).collect();
        let fields: Vec<String> = fields.iter().map(|field| format!("`{field}`")).collect();
        let lines: Vec<String> = places.iter().skip(1).map(|place| place.1.to_string()).collect();
        let message = format!(
            "`{}` changes {} of `{owner}` in {} places (also line {}); move that into a method of `{owner}`",
            name_at(sources, from),
            fields.join(", "),
            places.len(),
            lines.join(", "),
            owner = owner.name,
        );
        let position = (line, column);
        diagnostics.push(member.diagnostic_at(
            PathBuf::from(file),
            sources.text(file),
            field,
            position,
            "field_write",
            message,
        ));
    }

    diagnostics
}

/// The name written at a key, which is `file:line:column`; a use outside any item has its file instead.
fn name_at(sources: &Sources<'_>, key: &str) -> String {
    let mut parts = key.rsplitn(3, ':');
    let column = parts.next().and_then(|column| column.parse::<usize>().ok());
    let line = parts.next().and_then(|line| line.parse::<usize>().ok());
    let (Some(column), Some(line), Some(file)) = (column, line, parts.next()) else { return key.to_owned() };
    let text = sources.text(file).lines().nth(line.saturating_sub(1)).unwrap_or_default();
    text.chars().skip(column).take_while(|char| char.is_alphanumeric() || *char == '_').collect()
}
