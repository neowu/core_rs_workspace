use std::collections::BTreeMap;
use std::path::PathBuf;

use super::Reexport;
use super::Sources;
use super::UNFULFILLED;
use super::Unit;
use super::in_src;
use super::within;
use crate::diagnostic::Diagnostic;

const EXPECT: &str = "lint: expect(foreign_reexport)";

/// `foreign_reexport`: a module re-exports only what is defined inside it, and no crate re-exports another crate of
/// the workspace, so a name is found where it is owned and the manifests are the true dependency graph. Crates from
/// outside the workspace are free to re-export.
pub(super) fn check(units: &[Unit], sources: &Sources<'_>) -> Vec<Diagnostic> {
    // a crate can be compiled more than once, and one name can stand for several items; the first reported counts
    let mut places: BTreeMap<(&str, usize, usize), (&Reexport, Option<String>)> = BTreeMap::new();
    for unit in units.iter().filter(|unit| !unit.test) {
        for reexport in unit.reexports.iter().filter(|reexport| in_src(sources, &reexport.file)) {
            let place = places.entry((&reexport.file, reexport.line, reexport.column)).or_insert((reexport, None));
            if place.1.is_none() {
                place.1 = problem(reexport);
            }
        }
    }

    let mut diagnostics = Vec::new();
    for ((file, line, column), (reexport, problem)) in places {
        let Some(member) = sources.member(file) else { continue };
        let marked = sources.marked(file, reexport.item_line, line, EXPECT);
        let (rule, message) = match (problem, marked) {
            (Some(problem), false) => ("foreign_reexport", format!("{problem}, or mark it with `// {EXPECT}`")),
            (None, true) => (UNFULFILLED, format!("`// {EXPECT}` is not needed; remove the comment")),
            (Some(_), true) | (None, false) => continue,
        };
        let position = (line, column);
        let name = &reexport.name;
        diagnostics.push(member.diagnostic_at(PathBuf::from(file), sources.text(file), name, position, rule, message));
    }
    diagnostics
}

fn problem(reexport: &Reexport) -> Option<String> {
    let Reexport { module, name, .. } = reexport;
    match (&reexport.to_module, &reexport.to_crate) {
        (Some(to_module), _) if !within(to_module, module) => {
            let owner = if to_module.is_empty() { "crate".to_owned() } else { format!("crate::{to_module}") };
            Some(format!(
                "`crate::{module}` re-exports `{name}` of `{owner}`, which is not inside it; name it where it is \
                 defined"
            ))
        }
        (None, Some(to_crate)) if reexport.workspace => Some(format!(
            "`{name}` of workspace crate `{to_crate}` is re-exported; its users depend on `{to_crate}` and name it \
             there"
        )),
        _ => None,
    }
}
