use std::collections::HashSet;
use std::path::PathBuf;

use super::Sources;
use super::UNFULFILLED;
use super::Unit;
use super::in_src;
use crate::diagnostic::Diagnostic;

/// What a marker stands above.
#[derive(Clone, Copy, PartialEq, Eq)]
enum On {
    Item,
    Struct,
    Use,
    Arm,
}

/// Every marker there is, by verb and rule.
const MARKERS: [(&str, &str, On); 6] = [
    ("expect", "unused_pub", On::Item),
    ("expect", "excess_visibility", On::Item),
    ("expect", "misplaced_item", On::Item),
    ("expect", "module_cycle", On::Use),
    ("enforce", "field_write", On::Struct),
    ("expect", "wildcard_match", On::Arm),
];

/// A marker comment: its 1-based line, 0-based column, and the text from `lint:` to the closing parenthesis.
struct Marker<'a> {
    file: &'a str,
    line: usize,
    column: usize,
    text: &'a str,
}

/// Every marker comment names a rule that exists and stands above something the rule checks, so
/// none is left behind when a rule or the code under it goes. Whether it excuses anything is for each rule to say.
pub(super) fn check(units: &[Unit], sources: &Sources<'_>) -> Vec<Diagnostic> {
    let mut files: Vec<&str> = sources.texts.keys().map(String::as_str).filter(|file| in_src(sources, file)).collect();
    files.sort_unstable();
    let mut markers: Vec<Marker<'_>> = Vec::new();
    for file in files {
        let lines = sources.text(file).lines().enumerate();
        markers.extend(lines.filter_map(|(index, line)| {
            let (column, text) = parse(line)?;
            Some(Marker { file, line: index + 1, column, text })
        }));
    }
    let with_marker: HashSet<&str> = markers.iter().map(|marker| marker.file).collect();

    // by file, line and marker, the markers that stand above something
    let mut placed: HashSet<(&str, usize, &str)> = HashSet::new();
    for unit in units.iter().filter(|unit| !unit.test) {
        for def in unit.defs.iter().filter(|def| with_marker.contains(def.file.as_str())) {
            for marker in markers.iter().filter(|marker| marker.file == def.file) {
                let on = MARKERS.iter().find(|(verb, rule, _)| marker.text == format!("lint: {verb}({rule})"));
                let fits = on.is_some_and(|(.., on)| *on == On::Item || (*on == On::Struct && def.kind == "struct"));
                if fits && sources.marker_line(&def.file, def.item_line, def.line, marker.text) == Some(marker.line) {
                    placed.insert((marker.file, marker.line, marker.text));
                }
            }
        }
        for used in unit.uses.iter().filter(|used| with_marker.contains(used.file.as_str())) {
            let on = match used.kind.as_str() {
                "wildcard" => On::Arm,
                "through" => continue,
                _ if used.to_module.is_some() => On::Use,
                _ => continue,
            };
            for (verb, rule, _) in MARKERS.iter().filter(|marker| marker.2 == on) {
                let text = format!("lint: {verb}({rule})");
                if let Some(line) = sources.marker_line(&used.file, used.line, used.line, &text)
                    && let Some(marker) = markers.iter().find(|marker| marker.file == used.file && marker.line == line)
                {
                    placed.insert((marker.file, marker.line, marker.text));
                }
            }
        }
    }

    let mut diagnostics = Vec::new();
    for marker in &markers {
        let Marker { file, line, column, text } = *marker;
        let Some(member) = sources.member(file) else { continue };
        let known = MARKERS.iter().find(|(verb, rule, _)| text == format!("lint: {verb}({rule})"));
        let (rule, message) = match known {
            None => {
                let all: Vec<String> = MARKERS.iter().map(|(verb, rule, _)| format!("`{verb}({rule})`")).collect();
                ("unknown_lints", format!("`// {text}` names no rule; there are {}", all.join(", ")))
            }
            Some(_) if placed.contains(&(file, line, text)) => continue,
            Some((.., on)) => {
                let above = match on {
                    On::Item => "an item or field the rule checks",
                    On::Struct => "a struct other modules can use",
                    On::Use => "a use of another module",
                    On::Arm => "a catch-all arm of a match on an enum of the workspace",
                };
                (UNFULFILLED, format!("`// {text}` is not directly above {above}, so it does nothing; remove it"))
            }
        };
        let position = (line, column);
        diagnostics.push(member.diagnostic_at(PathBuf::from(file), sources.text(file), text, position, rule, message));
    }
    diagnostics
}

/// The marker in a comment line: where `lint:` starts, and the text up to the closing parenthesis.
fn parse(line: &str) -> Option<(usize, &str)> {
    if !line.trim_start().starts_with("//") {
        return None;
    }
    let start = line.find("lint: ")?;
    let rest = &line[start..];
    let open = rest.find('(')?;
    let close = rest.find(')')?;
    let is_name = |name: &str| !name.is_empty() && name.chars().all(|char| char.is_ascii_lowercase() || char == '_');
    let valid = open < close && is_name(&rest["lint: ".len()..open]) && is_name(&rest[open + 1..close]);
    valid.then(|| (line[..start].chars().count(), &rest[..=close]))
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn parses_markers() {
        assert_eq!(parse("    // lint: expect(unused_pub), for others"), Some((7, "lint: expect(unused_pub)")));
        assert_eq!(parse("// lint: allow(gone)"), Some((3, "lint: allow(gone)")));
        assert_eq!(parse("const EXPECT: &str = \"lint: expect(unused_pub)\";"), None);
        assert_eq!(parse("// the lint: it checks (some) things"), None);
    }
}
