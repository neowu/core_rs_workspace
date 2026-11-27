use std::collections::BTreeSet;
use std::path::PathBuf;

use super::Sources;
use super::Unit;
use super::in_src;
use crate::diagnostic::Diagnostic;

const EXPECT: &str = "lint: expect(wildcard_match)";

/// `wildcard_match`: a match on an enum of the workspace names its variants, so that adding one makes the compiler
/// point at every match to decide on.
pub(super) fn check(units: &[Unit], sources: &Sources<'_>) -> Vec<Diagnostic> {
    // a crate can be compiled more than once
    let mut arms: BTreeSet<(&str, usize, usize, &str)> = BTreeSet::new();
    for unit in units.iter().filter(|unit| !unit.test) {
        for used in unit.uses.iter().filter(|used| used.kind == "wildcard" && in_src(sources, &used.file)) {
            if !sources.marked(&used.file, used.line, used.line, EXPECT) {
                arms.insert((&used.file, used.line, used.column, &used.to));
            }
        }
    }

    let mut diagnostics = Vec::new();
    for (file, line, column, to) in arms {
        let Some(member) = sources.member(file) else { continue };
        let text = sources.text(file);
        let arm = sources.name_at(&format!("{file}:{line}:{column}"));
        let message = format!(
            "`{arm}` takes every variant added to enum `{}` later; name the variants, or mark it with `// {EXPECT}`",
            sources.name_at(to)
        );
        let position = (line, column);
        diagnostics.push(member.diagnostic_at(PathBuf::from(file), text, &arm, position, "wildcard_match", message));
    }
    diagnostics
}
