use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::path::Path;

use super::Sources;
use super::UNFULFILLED;
use super::Unit;
use crate::diagnostic::Diagnostic;
use crate::workspace::Manifest;

const EXPECT: &str = "lint: expect(module_cycle)";

/// `module_cycle` from the items the compiler resolved: every use of an item is a use of the module that owns it,
/// through any re-export, glob import, method call or field access. Tests may use anything and are left out.
pub(super) fn check(units: &[Unit], sources: &Sources<'_>) -> Vec<Diagnostic> {
    let mut files: Vec<&str> = Vec::new();
    let mut seen = HashSet::new();
    // by crate and parent module, the uses between its children
    let mut uses: BTreeMap<(&str, Vec<String>), Uses> = BTreeMap::new();
    // every use, marked or not, to tell whether a marker excuses anything
    let mut edges: BTreeMap<(&str, Vec<String>), Edges<'_>> = BTreeMap::new();
    // by file and line of a marked use, the uses on it
    let mut marked: BTreeMap<(&str, usize), Vec<Marked<'_>>> = BTreeMap::new();
    for unit in units.iter().filter(|unit| !unit.test) {
        for used in &unit.uses {
            let (Some(to_module), Some(name)) = (&used.to_module, &used.name) else { continue };
            // a crate can be compiled more than once
            if !seen.insert((&unit.root, &used.to, &used.file, used.line, &used.module)) {
                continue;
            }
            let is_marked = sources.marked(&used.file, used.line, used.line, EXPECT);
            let from = path(&used.module);
            let to = path(to_module);
            let shared = from.iter().zip(&to).take_while(|(left, right)| left == right).count();
            let parent: Vec<String> = from[..shared].iter().map(|part| (*part).to_owned()).collect();
            let edge = from.get(shared).copied().zip(to.get(shared).copied());
            if is_marked {
                let place = Marked { root: &unit.root, parent: parent.clone(), edge, column: used.column, name };
                marked.entry((&used.file, used.line)).or_default().push(place);
            }
            // a module and the modules inside it are read together
            let Some((user, used_module)) = edge else { continue };
            edges.entry((&unit.root, parent.clone())).or_default().insert((user, used_module));
            if is_marked {
                continue;
            }
            let file = files.iter().position(|file| *file == used.file).unwrap_or_else(|| {
                files.push(&used.file);
                files.len() - 1
            });
            let children = uses.entry((&unit.root, parent)).or_default();
            let key = (user.to_owned(), used_module.to_owned());
            let entry = children.entry(key).or_insert_with(|| Use {
                user: user.to_owned(),
                used: used_module.to_owned(),
                mentions: 0,
                file,
                line: used.line,
                column: used.column,
                start: name.clone(),
                item: name.clone(),
            });
            entry.mentions += 1;
            // reported at the first use in the file
            if file == entry.file && (used.line, used.column) < (entry.line, entry.column) {
                (entry.line, entry.column) = (used.line, used.column);
                entry.start.clone_from(name);
                entry.item.clone_from(name);
            }
        }
    }
    let file = |index: usize| {
        let file = files[index];
        Some((sources.member(file)?, Path::new(file), sources.text(file)))
    };
    let cycles = uses.into_iter().flat_map(|((_, parent), uses)| cycles(&parent, uses));
    let mut diagnostics: Vec<Diagnostic> = cycles.filter_map(|cycle| diagnostic(&cycle, file)).collect();
    diagnostics.sort_by(|left, right| (&left.path, left.span.line_start).cmp(&(&right.path, right.span.line_start)));

    for ((file, line), places) in marked {
        let in_circle = |place: &Marked<'_>| {
            let edges = edges.get(&(place.root, place.parent.clone()));
            place.edge.zip(edges).is_some_and(|((user, used), edges)| reaches(edges, used, user))
        };
        let (Some(first), Some(member)) = (places.first(), sources.member(file)) else { continue };
        if !places.iter().any(in_circle) {
            let message = format!("`// {EXPECT}` is not needed, the use is not part of a circle; remove the comment");
            let position = (line, first.column);
            diagnostics.push(member.diagnostic_at(
                file.into(),
                sources.text(file),
                first.name,
                position,
                UNFULFILLED,
                message,
            ));
        }
    }
    diagnostics
}

/// Uses between siblings, as user and used module.
type Edges<'a> = BTreeSet<(&'a str, &'a str)>;

/// A use under a marker: its crate, the module whose children it is between, and the children, if it is between any.
struct Marked<'a> {
    root: &'a str,
    parent: Vec<String>,
    edge: Option<(&'a str, &'a str)>,
    column: usize,
    name: &'a str,
}

/// Whether `to` can be reached from `from` along the uses between siblings.
fn reaches(edges: &Edges<'_>, from: &str, to: &str) -> bool {
    let mut reached = BTreeSet::from([from]);
    let mut pending = vec![from];
    while let Some(module) = pending.pop() {
        for (_, used) in edges.iter().filter(|(user, _)| *user == module) {
            if *used == to {
                return true;
            }
            if reached.insert(used) {
                pending.push(used);
            }
        }
    }
    false
}

fn path(module: &str) -> Vec<&str> {
    module.split("::").filter(|part| !part.is_empty()).collect()
}

/// One module's use of a sibling: the mentions that make it, the first being where it is reported.
struct Use {
    user: String,
    used: String,
    mentions: usize,
    file: usize,
    line: usize,
    column: usize,
    start: String,
    /// What is used of the sibling, or nothing when the module itself is imported.
    item: String,
}

/// By user and used module.
type Uses = BTreeMap<(String, String), Use>;

struct Cycle {
    /// The module whose children are in the cycle, as a path from the crate.
    parent: String,
    members: Vec<String>,
    /// The shortest circle through the use with the fewest mentions, which comes first and is the cheapest to cut.
    uses: Vec<Use>,
}

/// `file` gives the member, the path from the workspace root and the text of a use's file, by its index.
fn diagnostic<'a>(
    cycle: &Cycle,
    file: impl Fn(usize) -> Option<(&'a Manifest, &'a Path, &'a str)>,
) -> Option<Diagnostic> {
    let uses: Vec<String> = cycle
        .uses
        .iter()
        .filter_map(|step| {
            let Use { user, used, mentions, line, .. } = step;
            let item = if step.item.is_empty() { String::new() } else { format!("`{}`, ", step.item) };
            let path = file(step.file)?.1.display();
            Some(format!("`{user}` uses `{used}` ({item}{mentions} mention(s), {path}:{line})"))
        })
        .collect();
    let first = cycle.uses.first()?;
    let (member, path, text) = file(first.file)?;
    Some(member.diagnostic_at(
        path.to_path_buf(),
        text,
        &first.start,
        (first.line, first.column),
        "module_cycle",
        format!(
            "modules `{}` in `{}` use each other in a circle: {}; cut one of the uses, or mark it with `// {EXPECT}`",
            cycle.members.join("`, `"),
            cycle.parent,
            uses.join(", ")
        ),
    ))
}

/// Each set of children that reach each other, with its shortest circle through its cheapest use.
fn cycles(parent: &[String], mut uses: Uses) -> Vec<Cycle> {
    let reach = |from: &str, uses: &Uses| {
        // by module reached, the module it was reached from, breadth first so the way back is the shortest
        let mut reached: BTreeMap<String, String> = BTreeMap::new();
        let mut pending = vec![from.to_owned()];
        while !pending.is_empty() {
            let mut next = Vec::new();
            for (user, used) in uses.keys().filter(|(user, _)| pending.contains(user)) {
                if !reached.contains_key(used) {
                    reached.insert(used.clone(), user.clone());
                    next.push(used.clone());
                }
            }
            pending = next;
        }
        reached
    };
    let mut cycles = Vec::new();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let names: BTreeSet<String> = uses.keys().map(|(user, _)| user.clone()).collect();
    for name in names {
        let reached = reach(&name, &uses);
        if seen.contains(&name) || !reached.contains_key(&name) {
            continue;
        }
        let members: Vec<String> =
            reached.keys().filter(|other| reach(other, &uses).contains_key(&name)).cloned().collect();
        seen.extend(members.iter().cloned());
        let inside = uses.iter().filter(|((user, used), _)| members.contains(user) && members.contains(used));
        let Some((user, used)) = inside.min_by_key(|(_, tie)| tie.mentions).map(|(key, _)| key.clone()) else {
            continue;
        };
        // from the cheapest use, back from its user to its used module
        let back = reach(&used, &uses);
        let mut circle = vec![(user.clone(), used.clone())];
        let mut at = user;
        while at != used {
            let Some(previous) = back.get(&at) else { break };
            circle.insert(1, (previous.clone(), at.clone()));
            at.clone_from(previous);
        }
        let mut parent_path = vec!["crate".to_owned()];
        parent_path.extend(parent.iter().cloned());
        cycles.push(Cycle {
            parent: parent_path.join("::"),
            members,
            uses: circle.iter().filter_map(|key| uses.remove(key)).collect(),
        });
    }
    cycles
}
