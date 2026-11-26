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
use super::file_length::cfg_test;
use super::file_length::is_test;
use super::file_length::rust_files;
use super::workspace::Workspace;
use crate::unused_pub::macro_rules::Macros;

mod macro_rules;

pub(super) const ALLOW: &str = "lint: allow(unused_pub)";

/// Keywords followed by the name they define.
pub(super) const DEFINITIONS: [&str; 8] = ["fn", "struct", "enum", "union", "trait", "type", "const", "static"];

struct Candidate {
    kind: &'static str,
    name: String,
    line: usize,
    /// 0-based character column of the name.
    column: usize,
}

/// Reports `pub` items under each member's `src` whose name is mentioned nowhere else in the workspace.
/// Matching is by name only, so a mention of any same-named item counts as a use.
pub(super) fn check(workspace: &Workspace, exempt_packages: &[String]) -> Result<Vec<Diagnostic>, Box<dyn Error>> {
    let mut files = Vec::new();
    for member in &workspace.members {
        let Some(package_dir) = member.absolute_path.parent() else { continue };
        let src = package_dir.join("src");
        let exempt = exempt_packages.contains(&member.package.name);
        let mut paths = Vec::new();
        rust_files(package_dir, &mut paths)?;
        for path in paths {
            let source = fs::read_to_string(&path)?;
            files.push((member, !exempt && path.starts_with(&src), path, source));
        }
    }
    let sources: Vec<&str> = files.iter().map(|(.., source)| source.as_str()).collect();
    let (used, candidates) = scan(&sources);

    let mut diagnostics = Vec::new();
    for ((member, checked, file, source), items) in files.iter().zip(candidates) {
        for item in items.into_iter().filter(|item| *checked && !used.contains(&item.name)) {
            diagnostics.push(member.diagnostic_at(
                file.strip_prefix(&workspace.root).unwrap_or(file).to_path_buf(),
                source,
                &item.name,
                (item.line, item.column),
                "unused_pub",
                format!(
                    "pub {} `{}` is not used in the workspace; remove it, or mark it with `// {ALLOW}`",
                    item.kind, item.name
                ),
            ));
        }
    }
    diagnostics.sort_by(|left, right| (&left.path, left.span.line_start).cmp(&(&right.path, right.span.line_start)));
    Ok(diagnostics)
}

struct Uses<'a> {
    macros: &'a Macros,
    used: HashSet<String>,
    /// Positions, in the current file, of macro arguments that name a defined item.
    defined: HashSet<(usize, usize)>,
}

/// Returns the names the sources mention, and the `pub` items each source defines.
/// Files that fail to parse are skipped; the compiler reports those.
fn scan(sources: &[&str]) -> (HashSet<String>, Vec<Vec<Candidate>>) {
    let files: Vec<Option<TokenStream>> = sources.iter().map(|source| source.parse().ok()).collect();
    let mut macros = Macros::new();
    for tokens in files.iter().flatten() {
        macro_rules::collect(tokens.clone(), &mut macros);
    }

    let mut uses = Uses { macros: &macros, used: HashSet::new(), defined: HashSet::new() };
    let mut candidates = Vec::new();
    for (source, tokens) in sources.iter().zip(files) {
        let mut items = Vec::new();
        if let Some(tokens) = tokens {
            uses.defined.clear();
            collect_uses(tokens.clone(), true, &mut uses);
            if let Ok(file) = syn::parse2::<File>(tokens) {
                let lines: Vec<&str> = source.lines().collect();
                collect_candidates(&file.items, &lines, &macros, &mut items);
            }
        }
        candidates.push(items);
    }
    (uses.used, candidates)
}

/// Works on raw tokens to see inside macro invocations and `quote!` bodies. String literals count as mentions,
/// for paths such as `#[serde(deserialize_with = "path")]`; doc comments, the name after a defining keyword,
/// the self type of an `impl`, and macro arguments that name a defined item do not. `items` is whether the tokens can hold items, so `impl` can start one.
fn collect_uses(tokens: TokenStream, items: bool, uses: &mut Uses<'_>) {
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    collect_tokens(&tokens, items, uses);
}

fn collect_tokens(tokens: &[TokenTree], items: bool, uses: &mut Uses<'_>) {
    let mut defines = false;
    let mut index = 0;
    while let Some(token) = tokens.get(index) {
        let previous = index.checked_sub(1).and_then(|index| tokens.get(index));
        if items
            && is_ident(token, "impl")
            && starts_item(previous)
            && let Some(length) = tokens[index..].iter().position(is_brace)
        {
            collect_impl_header(&tokens[index + 1..index + length], uses);
            index += length;
            continue;
        }
        if let (TokenTree::Ident(name), Some(TokenTree::Punct(bang)), Some(TokenTree::Group(group))) =
            (token, tokens.get(index + 1), tokens.get(index + 2))
            && bang.as_char() == '!'
        {
            let bound = macro_rules::bind(uses.macros, &name.to_string(), &group.stream());
            uses.defined.extend(bound.iter().map(|(_, name)| position(name)));
        }
        match token {
            TokenTree::Ident(ident) => {
                let name = ident.to_string();
                let keyword = match name.as_str() {
                    // `*const T` and `'static` are not definitions
                    "const" => !previous.is_some_and(|token| is_punct(token, '*')),
                    "static" => !previous.is_some_and(|token| is_punct(token, '\'')),
                    "mut" => defines,
                    name => DEFINITIONS.contains(&name),
                };
                if !defines && !uses.defined.contains(&position(ident)) {
                    uses.used.insert(name);
                }
                defines = keyword;
            }
            TokenTree::Literal(literal) => {
                let text = literal.to_string();
                if text.contains('"') {
                    let words = text.split(|char: char| !char.is_alphanumeric() && char != '_');
                    uses.used.extend(words.filter(|word| !word.is_empty()).map(str::to_owned));
                }
                defines = false;
            }
            TokenTree::Group(group) => {
                if !is_doc(group) {
                    collect_uses(group.stream(), group.delimiter() == Delimiter::Brace, uses);
                }
                defines = false;
            }
            TokenTree::Punct(_) => defines = false,
        }
        index += 1;
    }
}

/// `header` is the tokens between `impl` and its body. The path of the self type is not a mention, otherwise
/// every type with an `impl` block would count as used; its generics, the trait, and the where clause are.
fn collect_impl_header(header: &[TokenTree], uses: &mut Uses<'_>) {
    // `for<'a>` is a higher-ranked bound, not the trait/type separator
    let separator = (0..header.len()).find(|&index| {
        is_ident(&header[index], "for") && !header.get(index + 1).is_some_and(|token| is_punct(token, '<'))
    });
    let self_type = separator.map_or_else(|| generics_end(header), |index| index + 1);
    let path = header[self_type..]
        .iter()
        .take_while(|token| (matches!(token, TokenTree::Ident(_)) && !is_ident(token, "where")) || is_punct(token, ':'))
        .count();
    collect_tokens(&header[..self_type], false, uses);
    collect_tokens(&header[self_type + path..], false, uses);
}

/// Index after the `<...>` the tokens start with, or 0 without one.
fn generics_end(tokens: &[TokenTree]) -> usize {
    let mut depth = 0;
    for (index, token) in tokens.iter().enumerate() {
        let arrow = index.checked_sub(1).and_then(|index| tokens.get(index)).is_some_and(|token| is_punct(token, '-'));
        if is_punct(token, '<') {
            depth += 1;
        } else if is_punct(token, '>') && !arrow {
            depth -= 1;
        }
        if depth == 0 {
            return if index == 0 { 0 } else { index + 1 };
        }
    }
    tokens.len()
}

/// Whether an `impl` after `previous` starts an item rather than an `impl Trait` type.
fn starts_item(previous: Option<&TokenTree>) -> bool {
    match previous {
        None | Some(TokenTree::Group(_)) => true,
        Some(token) => is_punct(token, ';') || is_ident(token, "unsafe") || is_ident(token, "default"),
    }
}

pub(super) fn position(ident: &Ident) -> (usize, usize) {
    let start = ident.span().start();
    (start.line, start.column)
}

fn is_ident(token: &TokenTree, name: &str) -> bool {
    matches!(token, TokenTree::Ident(ident) if ident == name)
}

pub(super) fn is_punct(token: &TokenTree, char: char) -> bool {
    matches!(token, TokenTree::Punct(punct) if punct.as_char() == char)
}

fn is_brace(token: &TokenTree) -> bool {
    matches!(token, TokenTree::Group(group) if group.delimiter() == Delimiter::Brace)
}

pub(super) fn is_doc(group: &Group) -> bool {
    group.delimiter() == Delimiter::Bracket
        && matches!(group.stream().into_iter().next(), Some(TokenTree::Ident(ident)) if ident == "doc")
}

/// Trait impls are skipped since their methods are used through the trait; test items are not public API.
fn collect_candidates(items: &[Item], lines: &[&str], macros: &Macros, candidates: &mut Vec<Candidate>) {
    for item in items.iter().filter(|item| !is_test(item)) {
        let line = item.span().start().line;
        let (kind, visibility, attrs, name) = match item {
            Item::Fn(item) => ("function", &item.vis, &item.attrs, &item.sig.ident),
            Item::Struct(item) => ("struct", &item.vis, &item.attrs, &item.ident),
            Item::Enum(item) => ("enum", &item.vis, &item.attrs, &item.ident),
            Item::Union(item) => ("union", &item.vis, &item.attrs, &item.ident),
            Item::Trait(item) => ("trait", &item.vis, &item.attrs, &item.ident),
            Item::Type(item) => ("type", &item.vis, &item.attrs, &item.ident),
            Item::Const(item) => ("const", &item.vis, &item.attrs, &item.ident),
            Item::Static(item) => ("static", &item.vis, &item.attrs, &item.ident),
            Item::Impl(item) => {
                if item.trait_.is_none() {
                    for item in &item.items {
                        let line = item.span().start().line;
                        let (kind, visibility, attrs, name) = match item {
                            ImplItem::Fn(item) => ("function", &item.vis, &item.attrs, &item.sig.ident),
                            ImplItem::Const(item) => ("const", &item.vis, &item.attrs, &item.ident),
                            _ => continue,
                        };
                        candidates.extend(candidate(kind, visibility, attrs, name, line, lines));
                    }
                }
                continue;
            }
            Item::Mod(module) => {
                if let Some((_, items)) = &module.content {
                    collect_candidates(items, lines, macros, candidates);
                }
                continue;
            }
            Item::Macro(item) => {
                let name = item.mac.path.segments.last().map_or_default(|segment| segment.ident.to_string());
                for (kind, name) in macro_rules::bind(macros, &name, &item.mac.tokens) {
                    let start = name.span().start();
                    // the marker goes above the name in the argument list
                    if !allowed(lines, start.line, start.line, ALLOW) {
                        candidates.push(Candidate {
                            kind,
                            name: name.to_string(),
                            line: start.line,
                            column: start.column,
                        });
                    }
                }
                continue;
            }
            _ => continue,
        };
        candidates.extend(candidate(kind, visibility, attrs, name, line, lines));
    }
}

fn candidate(
    kind: &'static str,
    visibility: &Visibility,
    attrs: &[Attribute],
    name: &Ident,
    item_line: usize,
    lines: &[&str],
) -> Option<Candidate> {
    let start = name.span().start();
    let exported = matches!(visibility, Visibility::Public(_))
        && !cfg_test(attrs)
        // proc macros are invoked by the name in the attribute, not the function name
        && !attrs.iter().any(|attr| {
            let path = attr.path();
            path.is_ident("proc_macro") || path.is_ident("proc_macro_derive") || path.is_ident("proc_macro_attribute")
        });
    (exported && !allowed(lines, item_line, start.line, ALLOW)).then(|| Candidate {
        kind,
        name: name.to_string(),
        line: start.line,
        column: start.column,
    })
}

/// Looks for the marker in comments from the comment block directly above the item down to the item name,
/// so it can sit above or below the item's doc comments and attributes.
pub(super) fn allowed(lines: &[&str], item_line: usize, name_line: usize, marker: &str) -> bool {
    let is_comment = |line: &str| line.trim_start().starts_with("//");
    lines
        .iter()
        .enumerate()
        .take(name_line)
        .rev()
        .take_while(|(index, line)| index + 1 >= item_line || is_comment(line))
        .any(|(_, line)| is_comment(line) && line.contains(marker))
}

#[cfg(test)]
mod tests {
    use super::scan;

    fn unused(sources: &[&str]) -> Vec<String> {
        let (used, candidates) = scan(sources);
        candidates.into_iter().flatten().filter(|item| !used.contains(&item.name)).map(|item| item.name).collect()
    }

    #[test]
    fn reports_unused_functions_and_methods() {
        let source = "
            pub fn free() {}
            struct S;
            impl S {
                pub const LIMIT: u8 = 0;
                pub fn method(&self) {}
            }
        ";
        assert_eq!(unused(&[source]), ["free", "LIMIT", "method"]);
    }

    #[test]
    fn accepts_uses_in_other_files() {
        let source = "
            pub fn free() {}
            pub fn in_macro() {}
            pub fn in_string() {}
        ";
        let uses = r#"
            fn main() {
                lib::free();
                assert!(lib::in_macro());
            }
            #[serde(with = "lib::in_string")]
            struct S;
        "#;
        assert_eq!(unused(&[source, uses]), [] as [&str; 0]);
    }

    #[test]
    fn ignores_definitions_and_doc_comments_as_uses() {
        let source = "
            pub fn free() {}
        ";
        let other = "
            /// Unlike [`free`].
            fn free() {}
            trait T {
                fn free();
            }
        ";
        assert_eq!(unused(&[source, other]), ["free"]);
    }

    #[test]
    fn skips_items_that_are_not_exported() {
        let source = "
            pub(crate) fn restricted() {}
            fn private() {}
            impl T for S {
                fn method(&self) {}
            }
            #[cfg(test)]
            pub fn test_only() {}
            #[proc_macro_derive(Name)]
            pub fn derive() {}
        ";
        assert_eq!(unused(&[source]), [] as [&str; 0]);
    }

    #[test]
    fn reports_unused_types_and_values() {
        let source = "
            pub struct S {
                pub field: i32,
            }
            pub enum E {}
            pub trait T {}
            pub type A = u8;
            pub const C: u8 = 0;
            pub static mut M: u8 = 0;
            pub struct Used;
            pub struct Pointee;
        ";
        let uses = "
            fn main() {
                let _: &'static Used = &Used;
                let _: *const Pointee;
            }
        ";
        assert_eq!(unused(&[source, uses]), ["S", "E", "T", "A", "C", "M"]);
    }

    #[test]
    fn ignores_impl_self_type_as_use() {
        let source = "
            pub struct Inherent;
            pub struct Generic<T>(T);
            pub struct Implementor;
            pub struct Argument;
            pub trait Implemented {}
            pub trait Bound {}
            pub struct Returned;
        ";
        let impls = "
            impl Inherent {}
            impl<T: Bound> Generic<T> {}
            impl Implemented for lib::Implementor {}
            impl From<Argument> for u8 {}
            fn returned() -> impl Into<Returned> {}
        ";
        assert_eq!(unused(&[source, impls]), ["Inherent", "Generic", "Implementor"]);
    }

    #[test]
    fn reports_unused_items_named_by_macro_arguments() {
        let definition = "
            #[macro_export]
            macro_rules! actions {
                ($namespace:path, [ $( $(#[$attr:meta])* $name:ident),* $(,)? ]) => {
                    $(
                        #[derive(Clone)]
                        $(#[$attr])*
                        pub struct $name;
                    )*
                };
                ([ $($name:ident),* ]) => {
                    $( pub struct $name; )*
                };
            }
            macro_rules! private {
                ($name:ident) => {
                    struct $name;
                };
            }
        ";
        let invocations = "
            actions!(editor, [
                /// Docs.
                Unused,
                Used,
                // lint: allow(unused_pub)
                Allowed,
            ]);
            gpui::actions!([Bare]);
            private!(Private);

            #[cfg(test)]
            mod tests {
                actions!(test, [TestOnly]);
            }
        ";
        let uses = "
            fn main() {
                let _ = Used;
            }
        ";
        assert_eq!(unused(&[invocations, definition, uses]), ["Unused", "Bare"]);
    }

    #[test]
    fn checks_inline_modules() {
        let source = "
            pub mod inner {
                pub fn nested() {}
            }
            #[cfg(test)]
            mod tests {
                pub fn helper() {}
            }
        ";
        assert_eq!(unused(&[source]), ["nested"]);
    }

    #[test]
    fn accepts_allow_comment() {
        let source = "
            // lint: allow(unused_pub), used by other workspaces
            /// Docs.
            #[inline]
            pub fn above() {}

            /// Docs.
            // lint: allow(unused_pub)
            pub fn below() {}

            // unrelated
            pub fn reported() {}
        ";
        assert_eq!(unused(&[source]), ["reported"]);
    }
}
