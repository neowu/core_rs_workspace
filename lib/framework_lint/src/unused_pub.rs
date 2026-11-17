use std::collections::HashSet;
use std::error::Error;
use std::fs;

use proc_macro2::Delimiter;
use proc_macro2::Group;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use proc_macro2::TokenTree;
use syn::Attribute;
use syn::File;
use syn::ImplItem;
use syn::Item;
use syn::Visibility;
use syn::spanned::Spanned as _;

use super::diagnostic::Diagnostic;
use super::diagnostic::Span;
use super::diagnostic::SpanLine;
use super::file_length::cfg_test;
use super::file_length::is_test;
use super::file_length::rust_files;
use super::workspace::Workspace;

const ALLOW: &str = "lint: allow(unused_pub)";

struct Function {
    name: String,
    line: usize,
    /// 0-based character column of the name.
    column: usize,
}

/// Reports `pub` functions under each member's `src` whose name is mentioned nowhere else in the workspace.
/// Matching is by name only, so a mention of any same-named item counts as a use.
pub(super) fn check(workspace: &Workspace, exempt_packages: &[String]) -> Result<Vec<Diagnostic>, Box<dyn Error>> {
    let mut used = HashSet::new();
    let mut candidates = Vec::new();
    for member in &workspace.members {
        let Some(package_dir) = member.absolute_path.parent() else { continue };
        let src = package_dir.join("src");
        let exempt = exempt_packages.contains(&member.package.name);
        let mut files = Vec::new();
        rust_files(package_dir, &mut files)?;
        for file in files {
            let source = fs::read_to_string(&file)?;
            let functions = scan(&source, &mut used);
            if !exempt && file.starts_with(&src) && !functions.is_empty() {
                candidates.push((member, file, source, functions));
            }
        }
    }

    let mut diagnostics = Vec::new();
    for (member, file, source, functions) in candidates {
        for function in functions.into_iter().filter(|function| !used.contains(&function.name)) {
            let text = source.lines().nth(function.line - 1).unwrap_or_default();
            let line_offset: usize = source.split_inclusive('\n').take(function.line - 1).map(str::len).sum();
            let byte_start =
                line_offset + text.char_indices().nth(function.column).map_or(text.len(), |(index, _)| index);
            let column_start = function.column + 1;
            let column_end = column_start + function.name.chars().count();
            diagnostics.push(Diagnostic {
                path: file.strip_prefix(&workspace.root).unwrap_or(&file).to_path_buf(),
                manifest_path: member.absolute_path.clone(),
                package_id: member.package.id.clone(),
                target: member.package.target.clone(),
                span: Span {
                    byte_start,
                    byte_end: byte_start + function.name.len(),
                    line_start: function.line,
                    line_end: function.line,
                    column_start,
                    column_end,
                    lines: vec![SpanLine {
                        text: text.to_owned(),
                        highlight_start: column_start,
                        highlight_end: column_end,
                    }],
                },
                rule: "unused_pub",
                message: format!(
                    "pub function `{}` is not used in the workspace; remove it, or mark it with `// {ALLOW}`",
                    function.name
                ),
            });
        }
    }
    diagnostics.sort_by(|left, right| (&left.path, left.span.line_start).cmp(&(&right.path, right.span.line_start)));
    Ok(diagnostics)
}

/// Records the names the source mentions, and returns the `pub` functions it defines.
/// Files that fail to parse are skipped; the compiler reports those.
fn scan(source: &str, used: &mut HashSet<String>) -> Vec<Function> {
    let Ok(tokens) = source.parse::<TokenStream>() else { return Vec::new() };
    collect_uses(tokens.clone(), used);
    let mut functions = Vec::new();
    if let Ok(file) = syn::parse2::<File>(tokens) {
        let lines: Vec<&str> = source.lines().collect();
        collect_functions(&file.items, &lines, &mut functions);
    }
    functions
}

/// Works on raw tokens to see inside macro invocations and `quote!` bodies. String literals count as mentions,
/// for paths such as `#[serde(deserialize_with = "path")]`; doc comments and the name after `fn` do not.
fn collect_uses(tokens: TokenStream, used: &mut HashSet<String>) {
    let mut after_fn = false;
    for token in tokens {
        let is_fn = matches!(&token, TokenTree::Ident(ident) if ident == "fn");
        match token {
            TokenTree::Ident(ident) => {
                if !after_fn {
                    used.insert(ident.to_string());
                }
            }
            TokenTree::Literal(literal) => {
                let text = literal.to_string();
                if text.contains('"') {
                    let words = text.split(|char: char| !char.is_alphanumeric() && char != '_');
                    used.extend(words.filter(|word| !word.is_empty()).map(str::to_owned));
                }
            }
            TokenTree::Group(group) => {
                if !is_doc(&group) {
                    collect_uses(group.stream(), used);
                }
            }
            TokenTree::Punct(_) => {}
        }
        after_fn = is_fn;
    }
}

fn is_doc(group: &Group) -> bool {
    group.delimiter() == Delimiter::Bracket
        && matches!(group.stream().into_iter().next(), Some(TokenTree::Ident(ident)) if ident == "doc")
}

/// Trait impls are skipped since their methods are used through the trait; test items are not public API.
fn collect_functions(items: &[Item], lines: &[&str], functions: &mut Vec<Function>) {
    for item in items.iter().filter(|item| !is_test(item)) {
        match item {
            Item::Fn(function) => {
                let line = function.span().start().line;
                functions.extend(candidate(&function.vis, &function.attrs, &function.sig.ident, line, lines));
            }
            Item::Impl(item) if item.trait_.is_none() => {
                for item in &item.items {
                    if let ImplItem::Fn(function) = item {
                        let line = function.span().start().line;
                        functions.extend(candidate(&function.vis, &function.attrs, &function.sig.ident, line, lines));
                    }
                }
            }
            Item::Mod(module) => {
                if let Some((_, items)) = &module.content {
                    collect_functions(items, lines, functions);
                }
            }
            _ => {}
        }
    }
}

fn candidate(
    visibility: &Visibility,
    attrs: &[Attribute],
    name: &Ident,
    item_line: usize,
    lines: &[&str],
) -> Option<Function> {
    let start = name.span().start();
    let exported = matches!(visibility, Visibility::Public(_))
        && !cfg_test(attrs)
        // proc macros are invoked by the name in the attribute, not the function name
        && !attrs.iter().any(|attr| {
            let path = attr.path();
            path.is_ident("proc_macro") || path.is_ident("proc_macro_derive") || path.is_ident("proc_macro_attribute")
        });
    (exported && !allowed(lines, item_line, start.line)).then(|| Function {
        name: name.to_string(),
        line: start.line,
        column: start.column,
    })
}

/// Looks for the marker in comments from the comment block directly above the item down to the function name,
/// so it can sit above or below the item's doc comments and attributes.
fn allowed(lines: &[&str], item_line: usize, name_line: usize) -> bool {
    let is_comment = |line: &str| line.trim_start().starts_with("//");
    lines
        .iter()
        .enumerate()
        .take(name_line)
        .rev()
        .take_while(|(index, line)| index + 1 >= item_line || is_comment(line))
        .any(|(_, line)| is_comment(line) && line.contains(ALLOW))
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::scan;

    fn unused(sources: &[&str]) -> Vec<String> {
        let mut used = HashSet::new();
        let functions: Vec<_> = sources.iter().flat_map(|source| scan(source, &mut used)).collect();
        functions.into_iter().filter(|function| !used.contains(&function.name)).map(|function| function.name).collect()
    }

    #[test]
    fn reports_unused_functions_and_methods() {
        let source = "pub fn free() {}\nstruct S;\nimpl S {\n    pub fn method(&self) {}\n}\n";
        assert_eq!(unused(&[source]), ["free", "method"]);
    }

    #[test]
    fn accepts_uses_in_other_files() {
        let source = "pub fn free() {}\npub fn in_macro() {}\npub fn in_string() {}\n";
        let uses = "fn main() {\n    lib::free();\n    assert!(lib::in_macro());\n}\n#[serde(with = \"lib::in_string\")]\nstruct S;\n";
        assert_eq!(unused(&[source, uses]), [] as [&str; 0]);
    }

    #[test]
    fn ignores_definitions_and_doc_comments_as_uses() {
        let source = "pub fn free() {}\n";
        let other = "/// Unlike [`free`].\nfn free() {}\ntrait T {\n    fn free();\n}\n";
        assert_eq!(unused(&[source, other]), ["free"]);
    }

    #[test]
    fn skips_items_that_are_not_exported_functions() {
        let source = "pub(crate) fn restricted() {}\nfn private() {}\nimpl T for S {\n    fn method(&self) {}\n}\n\
            #[cfg(test)]\npub fn test_only() {}\n#[proc_macro_derive(Name)]\npub fn derive() {}\n";
        assert_eq!(unused(&[source]), [] as [&str; 0]);
    }

    #[test]
    fn checks_inline_modules() {
        let source =
            "pub mod inner {\n    pub fn nested() {}\n}\n#[cfg(test)]\nmod tests {\n    pub fn helper() {}\n}\n";
        assert_eq!(unused(&[source]), ["nested"]);
    }

    #[test]
    fn accepts_allow_comment() {
        let source = "// lint: allow(unused_pub), used by other workspaces\n/// Docs.\n#[inline]\npub fn above() {}\n\n\
            /// Docs.\n// lint: allow(unused_pub)\npub fn below() {}\n\n// unrelated\npub fn reported() {}\n";
        assert_eq!(unused(&[source]), ["reported"]);
    }
}
