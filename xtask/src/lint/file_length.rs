use std::error::Error;
use std::fs;
use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;

use proc_macro2::Span;
use syn::Ident;
use syn::Item;
use syn::spanned::Spanned as _;

use super::diagnostic::Diagnostic;
use super::diagnostic::SpanLine;
use super::workspace::Workspace;

const MAX_LINES: usize = 500;

/// Checks `.rs` files under each member's `src`, so build scripts, integration tests, benches and examples are exempt.
pub(super) fn check(workspace: &Workspace) -> Result<Vec<Diagnostic>, Box<dyn Error>> {
    let mut diagnostics = Vec::new();
    for member in &workspace.members {
        let Some(package_dir) = member.absolute_path.parent() else { continue };
        let mut files = Vec::new();
        rust_files(&package_dir.join("src"), &mut files)?;
        for file in files {
            let source = fs::read_to_string(&file)?;
            let Some((line, count)) = overflow(&source) else { continue };
            let byte_start: usize = source.split_inclusive('\n').take(line - 1).map(str::len).sum();
            let text = source.lines().nth(line - 1).unwrap_or_default();
            diagnostics.push(Diagnostic {
                path: file.strip_prefix(&workspace.root).unwrap_or(&file).to_path_buf(),
                manifest_path: member.absolute_path.clone(),
                package_id: member.package.id.clone(),
                target: member.package.target.clone(),
                span: super::diagnostic::Span {
                    byte_start,
                    byte_end: byte_start + text.len(),
                    line_start: line,
                    line_end: line,
                    column_start: 1,
                    column_end: text.chars().count() + 1,
                    lines: vec![SpanLine {
                        text: text.to_owned(),
                        highlight_start: 1,
                        highlight_end: text.chars().count() + 1,
                    }],
                },
                rule: "file_too_long",
                message: format!(
                    "file has {count} lines excluding `#[cfg(test)]` items, more than the limit of {MAX_LINES}"
                ),
            });
        }
    }
    diagnostics.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(diagnostics)
}

fn rust_files(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), Box<dyn Error>> {
    if !dir.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            rust_files(&path, files)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            files.push(path);
        }
    }
    Ok(())
}

/// The first line past the limit and the total line count, both ignoring top-level `#[cfg(test)]` items.
/// Files that fail to parse are skipped; the compiler reports those.
fn overflow(source: &str) -> Option<(usize, usize)> {
    let file = syn::parse_file(source).ok()?;
    let test_lines: Vec<RangeInclusive<usize>> =
        file.items.iter().filter(|item| is_test(item)).map(|item| lines(item.span())).collect();
    let code_lines: Vec<usize> =
        (1..=source.lines().count()).filter(|line| !test_lines.iter().any(|range| range.contains(line))).collect();
    code_lines.get(MAX_LINES).map(|line| (*line, code_lines.len()))
}

fn lines(span: Span) -> RangeInclusive<usize> {
    span.start().line..=span.end().line
}

fn is_test(item: &Item) -> bool {
    let attrs = match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        Item::Verbatim(_) | _ => return false,
    };
    attrs.iter().any(|attr| attr.path().is_ident("cfg") && attr.parse_args::<Ident>().is_ok_and(|arg| arg == "test"))
}

#[cfg(test)]
mod tests {
    use super::MAX_LINES;
    use super::overflow;

    fn code(lines: usize) -> String {
        "const C: u8 = 0;\n".repeat(lines)
    }

    #[test]
    fn accepts_files_at_the_limit() {
        assert_eq!(overflow(&code(MAX_LINES)), None);
    }

    #[test]
    fn reports_first_line_past_the_limit() {
        assert_eq!(overflow(&code(MAX_LINES + 2)), Some((MAX_LINES + 1, MAX_LINES + 2)));
    }

    #[test]
    fn excludes_cfg_test_items() {
        let tests = "#[cfg(test)]\nmod tests {\n    #[test]\n    fn it_works() {}\n}\n";
        let helper = "/// helper\n#[cfg(test)]\nfn helper() {}\n";
        assert_eq!(overflow(&format!("{helper}{}{tests}", code(MAX_LINES))), None);
        assert_eq!(
            overflow(&format!("{helper}{}{tests}", code(MAX_LINES + 1))),
            Some((MAX_LINES + 4, MAX_LINES + 1)),
            "line numbers are in the original file"
        );
    }

    #[test]
    fn counts_other_cfg_items() {
        let source = format!("#[cfg(not(test))]\nfn real() {{}}\n{}", code(MAX_LINES - 1));
        assert_eq!(overflow(&source), Some((MAX_LINES + 1, MAX_LINES + 1)));
    }
}
