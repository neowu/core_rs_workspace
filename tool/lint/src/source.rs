use std::error::Error;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use super::diagnostic::Diagnostic;
use super::diagnostic::Finding;
use super::diagnostic::Span;
use super::diagnostic::SpanLine;
use super::file_length;
use super::test_module;
use super::workspace::Workspace;

/// Checks `.rs` files under each member's `src`, so build scripts, integration tests, benches and examples are exempt.
/// Files that fail to parse are skipped; the compiler reports those.
pub(super) fn check(workspace: &Workspace, max_lines: usize) -> Result<Vec<Diagnostic>, Box<dyn Error>> {
    let mut diagnostics = Vec::new();
    for member in &workspace.members {
        let Some(package_dir) = member.absolute_path.parent() else { continue };
        let mut files = Vec::new();
        rust_files(&package_dir.join("src"), &mut files)?;
        for file in files {
            let source = fs::read_to_string(&file)?;
            let Ok(ast) = syn::parse_file(&source) else { continue };
            let mut findings = test_module::check(&ast);
            findings.extend(file_length::check(&source, &ast, max_lines));
            for finding in findings {
                diagnostics.push(Diagnostic {
                    path: file.strip_prefix(&workspace.root).unwrap_or(&file).to_path_buf(),
                    manifest_path: member.absolute_path.clone(),
                    package_id: member.package.id.clone(),
                    target: member.package.target.clone(),
                    span: span(&source, &finding),
                    rule: finding.rule,
                    message: finding.message,
                });
            }
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

fn span(source: &str, finding: &Finding) -> Span {
    let line = finding.line;
    let text = source.lines().nth(line - 1).unwrap_or_default();
    let (column_start, column_end) = finding.columns.unwrap_or_else(|| (1, text.chars().count() + 1));
    let line_start: usize = source.split_inclusive('\n').take(line - 1).map(str::len).sum();
    let bytes = |column: usize| -> usize { text.chars().take(column - 1).map(char::len_utf8).sum() };
    Span {
        byte_start: line_start + bytes(column_start),
        byte_end: line_start + bytes(column_end),
        line_start: line,
        line_end: line,
        column_start,
        column_end,
        lines: vec![SpanLine { text: text.to_owned(), highlight_start: column_start, highlight_end: column_end }],
    }
}

#[cfg(test)]
mod tests {
    use super::Finding;
    use super::span;

    #[test]
    fn span_of_a_name() {
        let finding = Finding { rule: "rule", message: String::new(), line: 2, columns: Some((4, 6)) };
        let span = span("// é\nfn é1() {}\n", &finding);
        assert_eq!((span.byte_start, span.byte_end), (9, 12));
        assert_eq!((span.line_start, span.column_start, span.column_end), (2, 4, 6));
    }

    #[test]
    fn span_of_a_line() {
        let finding = Finding { rule: "rule", message: String::new(), line: 2, columns: None };
        let span = span("fn a() {}\nfn b() {}\n", &finding);
        assert_eq!((span.byte_start, span.byte_end), (10, 19));
        assert_eq!((span.column_start, span.column_end), (1, 10));
    }
}
