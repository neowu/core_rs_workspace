use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs;

use proc_macro2::Delimiter;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use proc_macro2::TokenTree;

use super::diagnostic::Diagnostic;
use super::excess_visibility::locate;
use super::file_length::rust_files;
use super::unused_pub::allowed;
use super::unused_pub::is_punct;
use super::workspace::Workspace;

const ALLOW: &str = "lint: allow(module_cycle)";

struct Source {
    text: String,
    krate: usize,
    module: Vec<String>,
}

/// A path written from `crate`, `self` or `super`, or in a `use` from a name in scope, made absolute.
struct Reference {
    file: usize,
    /// The module it is written in.
    from: Vec<String>,
    path: Vec<String>,
    /// Starts at a name in scope, which is a use of this crate only if the name is a child module.
    bare: bool,
    /// The name a `use` brings into `from`, where paths through `from` find it.
    binding: Option<String>,
    line: usize,
    column: usize,
    /// The first token, which a diagnostic points at.
    start: String,
    /// Marked to be left out.
    allowed: bool,
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

/// Reports sibling modules of a crate that use each other in a circle, so there is no order to read them in.
pub(super) fn check(workspace: &Workspace) -> Result<Vec<Diagnostic>, Box<dyn Error>> {
    let mut files = Vec::new();
    let mut sources = Vec::new();
    let mut crates: Vec<(usize, String)> = Vec::new();
    for (package, member) in workspace.members.iter().enumerate() {
        let Some(package_dir) = member.absolute_path.parent() else { continue };
        let src = package_dir.join("src");
        let has_lib = src.join("lib.rs").is_file();
        let mut paths = Vec::new();
        rust_files(&src, &mut paths)?;
        for path in paths {
            let Ok(relative) = path.strip_prefix(&src) else { continue };
            let (krate, module) = locate(relative, has_lib);
            let key = (package, krate);
            let krate = crates.iter().position(|other| *other == key).unwrap_or_else(|| {
                crates.push(key);
                crates.len() - 1
            });
            sources.push(Source { text: fs::read_to_string(&path)?, krate, module });
            files.push((member, path.strip_prefix(&workspace.root).unwrap_or(&path).to_path_buf()));
        }
    }

    let mut diagnostics = Vec::new();
    for cycle in analyze(&sources) {
        let uses: Vec<String> = cycle
            .uses
            .iter()
            .map(|step| {
                let Use { user, used, mentions, line, .. } = step;
                let item = if step.item.is_empty() { String::new() } else { format!("`{}`, ", step.item) };
                let path = files[step.file].1.display();
                format!("`{user}` uses `{used}` ({item}{mentions} mention(s), {path}:{line})")
            })
            .collect();
        let Some(first) = cycle.uses.first() else { continue };
        let (member, path) = &files[first.file];
        diagnostics.push(member.diagnostic_at(
            path.clone(),
            &sources[first.file].text,
            &first.start,
            (first.line, first.column),
            "module_cycle",
            format!(
                "modules `{}` in `{}` use each other in a circle: {}; cut one of the uses, or mark it with `// {ALLOW}`",
                cycle.members.join("`, `"),
                cycle.parent,
                uses.join(", ")
            ),
        ));
    }
    diagnostics.sort_by(|left, right| (&left.path, left.span.line_start).cmp(&(&right.path, right.span.line_start)));
    Ok(diagnostics)
}

/// Files that fail to parse are skipped; the compiler reports those.
fn analyze(sources: &[Source]) -> Vec<Cycle> {
    let mut modules: HashSet<(usize, Vec<String>)> = HashSet::new();
    let mut references = Vec::new();
    for (file, source) in sources.iter().enumerate() {
        for length in 0..=source.module.len() {
            modules.insert((source.krate, source.module[..length].to_vec()));
        }
        let Ok(tokens) = source.text.parse::<TokenStream>() else { continue };
        let tokens: Vec<TokenTree> = tokens.into_iter().collect();
        let lines: Vec<&str> = source.text.lines().collect();
        let mut scanner =
            Scanner { file, lines: &lines, module: source.module.clone(), inline: Vec::new(), references };
        scanner.scan(&tokens);
        modules.extend(scanner.inline.into_iter().map(|module| (source.krate, module)));
        references = scanner.references;
    }
    let krate = |reference: &Reference| sources[reference.file].krate;
    references.retain(|reference| {
        let child = reference.from.iter().chain(reference.path.get(reference.from.len())).cloned().collect();
        !reference.bare || modules.contains(&(krate(reference), child))
    });
    let mut imports: HashMap<(usize, &[String], &str), &[String]> = HashMap::new();
    for reference in &references {
        if let Some(binding) = &reference.binding {
            imports.insert((krate(reference), &reference.from, binding), &reference.path);
        }
    }

    // by crate and parent module, the uses between its children
    let mut uses: BTreeMap<(usize, Vec<String>), Uses> = BTreeMap::new();
    for reference in references.iter().filter(|reference| !reference.allowed) {
        let krate = krate(reference);
        let (to, item) = resolve(krate, &reference.path, &modules, &imports);
        let from = &reference.from;
        let shared = from.iter().zip(&to).take_while(|(left, right)| left == right).count();
        // a module and the modules inside it are read together
        let (Some(user), Some(used)) = (from.get(shared), to.get(shared)) else { continue };
        let children = uses.entry((krate, from[..shared].to_vec())).or_default();
        let entry = children.entry((user.clone(), used.clone())).or_insert_with(|| Use {
            user: user.clone(),
            used: used.clone(),
            mentions: 0,
            file: reference.file,
            line: reference.line,
            column: reference.column,
            start: reference.start.clone(),
            item,
        });
        entry.mentions += 1;
    }
    uses.into_iter().flat_map(|((_, parent), uses)| cycles(&parent, uses)).collect()
}

/// The module that owns what `path` names, and the name: the longest module the path starts with, followed
/// through the `use` declarations that bring the name into that module. Glob imports are not followed.
fn resolve(
    krate: usize,
    path: &[String],
    modules: &HashSet<(usize, Vec<String>)>,
    imports: &HashMap<(usize, &[String], &str), &[String]>,
) -> (Vec<String>, String) {
    let mut path = path.to_vec();
    // a chain of imports that loops does not compile
    for _ in 0..16 {
        let length = (0..=path.len()).rev().find(|length| modules.contains(&(krate, path[..*length].to_vec())));
        let (module, rest) = path.split_at(length.unwrap_or(0));
        let Some(name) = rest.first() else { return (path, String::new()) };
        match imports.get(&(krate, module, name.as_str())) {
            Some(target) if **target != *path => path = target.iter().chain(&rest[1..]).cloned().collect(),
            _ => return (module.to_vec(), name.clone()),
        }
    }
    (path, String::new())
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

struct Scanner<'a> {
    file: usize,
    lines: &'a [&'a str],
    module: Vec<String>,
    /// Modules written inline in the file.
    inline: Vec<Vec<String>>,
    references: Vec<Reference>,
}

impl Scanner<'_> {
    /// Works on raw tokens to see inside macro invocations. `#[cfg(test)]` items are skipped: tests may use
    /// anything.
    fn scan(&mut self, tokens: &[TokenTree]) {
        let mut index = 0;
        while let Some(token) = tokens.get(index) {
            let previous = index.checked_sub(1).and_then(|index| tokens.get(index));
            match token {
                TokenTree::Group(group) if previous.is_some_and(|token| is_punct(token, '#')) => {
                    if is_cfg_test(group) {
                        // to the end of the item: its body, or its `;`
                        let end = tokens[index..].iter().position(|token| is_brace(token) || is_punct(token, ';'));
                        index += end.unwrap_or(tokens.len());
                    }
                }
                TokenTree::Group(group) => {
                    let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                    self.scan(&inner);
                }
                TokenTree::Ident(ident) if ident == "mod" => {
                    if let (Some(TokenTree::Ident(name)), Some(TokenTree::Group(body))) =
                        (tokens.get(index + 1), tokens.get(index + 2))
                        && body.delimiter() == Delimiter::Brace
                    {
                        self.module.push(name.to_string());
                        self.inline.push(self.module.clone());
                        let inner: Vec<TokenTree> = body.stream().into_iter().collect();
                        self.scan(&inner);
                        self.module.pop();
                        index += 2;
                    }
                }
                TokenTree::Ident(ident) if ident == "use" => {
                    let end =
                        tokens[index..].iter().position(|token| is_punct(token, ';')).unwrap_or(tokens.len() - index);
                    self.tree(&tokens[index + 1..index + end], None, None, true);
                    index += end;
                }
                TokenTree::Ident(ident) if ident == "crate" || ident == "self" || ident == "super" => {
                    // `$crate` expands in the crate that invokes the macro, and `a::super` continues a path
                    let continues = index >= 2 && is_separator(tokens, index - 2);
                    let starts = !continues && !previous.is_some_and(|token| is_punct(token, '$'));
                    let mut end = index + 1;
                    while is_separator(tokens, end) && matches!(tokens.get(end + 2), Some(TokenTree::Ident(_))) {
                        end += 3;
                    }
                    if starts && end > index + 1 {
                        self.tree(&tokens[index..end], None, None, false);
                    }
                    index = end - 1;
                }
                _ => {}
            }
            index += 1;
        }
    }

    /// One path of a use tree, after the `base` path its group follows. `start` is the first token of the whole
    /// tree.
    fn tree(&mut self, tokens: &[TokenTree], base: Option<(Vec<String>, bool)>, start: Option<&Ident>, declares: bool) {
        let mut start = start;
        let mut path = base;
        let mut alias = None;
        let mut glob = false;
        let mut index = 0;
        while let Some(token) = tokens.get(index) {
            match token {
                TokenTree::Ident(ident) if ident == "as" => {
                    alias = tokens.get(index + 1).map(ToString::to_string);
                    break;
                }
                TokenTree::Ident(ident) => {
                    start = start.or(Some(ident));
                    let name = ident.to_string();
                    path = Some(match (path, name.as_str()) {
                        (None, "crate") => (Vec::new(), false),
                        (None, "self") => (self.module.clone(), false),
                        (None, "super") => (self.module[..self.module.len().saturating_sub(1)].to_vec(), false),
                        (None, _) => (self.module.iter().cloned().chain([name]).collect(), true),
                        (Some(path), "self") => path,
                        (Some((mut path, bare)), "super") => {
                            path.pop();
                            (path, bare)
                        }
                        (Some((mut path, bare)), _) => {
                            path.push(name);
                            (path, bare)
                        }
                    });
                }
                TokenTree::Group(group) if group.delimiter() == Delimiter::Brace => {
                    let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                    for part in inner.split(|token| is_punct(token, ',')) {
                        self.tree(part, path.clone(), start, declares);
                    }
                    return;
                }
                // `::name` is another crate
                TokenTree::Punct(_) if path.is_none() => return,
                TokenTree::Punct(punct) if punct.as_char() == '*' => glob = true,
                TokenTree::Punct(punct) if punct.as_char() == ':' => {}
                _ => break,
            }
            index += 1;
        }
        let (Some((path, bare)), Some(start)) = (path, start) else { return };
        let position = start.span().start();
        let binding = alias.or_else(|| path.last().cloned()).filter(|_| declares && !glob);
        self.references.push(Reference {
            file: self.file,
            from: self.module.clone(),
            path,
            bare,
            binding,
            line: position.line,
            column: position.column,
            start: start.to_string(),
            allowed: allowed(self.lines, position.line, position.line, ALLOW),
        });
    }
}

/// Whether `::` is at `index`.
fn is_separator(tokens: &[TokenTree], index: usize) -> bool {
    tokens.get(index..index + 2).is_some_and(|tokens| tokens.iter().all(|token| is_punct(token, ':')))
}

fn is_brace(token: &TokenTree) -> bool {
    matches!(token, TokenTree::Group(group) if group.delimiter() == Delimiter::Brace)
}

fn is_cfg_test(attribute: &proc_macro2::Group) -> bool {
    let tokens: Vec<TokenTree> = attribute.stream().into_iter().collect();
    matches!(&*tokens, [TokenTree::Ident(name), TokenTree::Group(arguments)]
        if name == "cfg" && arguments.stream().to_string() == "test")
}

#[cfg(test)]
mod tests {
    use super::Source;
    use super::analyze;

    /// Files of one crate as `(module, text)`; returns `parent: members: user -> used (item), ...` per cycle.
    fn cycles(files: &[(&str, &str)]) -> Vec<String> {
        let sources: Vec<Source> = files
            .iter()
            .map(|(module, text)| Source {
                text: (*text).to_owned(),
                krate: 0,
                module: module.split("::").filter(|part| !part.is_empty()).map(str::to_owned).collect(),
            })
            .collect();
        let cycles = analyze(&sources).into_iter().map(|cycle| {
            let uses: Vec<String> =
                cycle.uses.iter().map(|step| format!("{} -> {} ({})", step.user, step.used, step.item)).collect();
            format!("{}: {}: {}", cycle.parent, cycle.members.join(" "), uses.join(", "))
        });
        cycles.collect()
    }

    #[test]
    fn reports_siblings_that_use_each_other() {
        let a = "
            use crate::b::B;
            use crate::b::Other;
            pub struct A;
        ";
        let b = "
            pub struct B;
            pub struct Other;
            fn f(_: crate::a::A) {}
        ";
        let c = "use crate::a::A;";
        assert_eq!(cycles(&[("a", a), ("b", b), ("c", c)]), ["crate: a b: b -> a (A), a -> b (B)"]);
    }

    #[test]
    fn reports_the_shortest_circle_through_the_cheapest_use() {
        let a = "use crate::b::B; use crate::b::B2; fn f() { crate::c::f(); crate::c::f(); }";
        let b = "use crate::c::C; use crate::c::C2; use crate::a::A; use crate::a::A2;";
        let c = "use crate::a::A; use crate::a::A2; use crate::a::A3;";
        assert_eq!(cycles(&[("a", a), ("b", b), ("c", c)]), ["crate: a b c: a -> b (B), b -> a (A)"]);
    }

    #[test]
    fn accepts_modules_and_the_modules_inside_them() {
        let parent = "
            mod child;
            use self::child::Child;
            use child::Other;
            pub struct Parent;
        ";
        let child = "
            use super::Parent;
            use crate::a::Parent as Renamed;
        ";
        assert_eq!(cycles(&[("a", parent), ("a::child", child)]), [] as [&str; 0]);
    }

    #[test]
    fn reports_cycles_inside_a_module_by_its_children() {
        let x = "use super::y::Y; use crate::a::y::Y;";
        let y = "use super::x::X;";
        let other = "use crate::a::x::X;";
        assert_eq!(cycles(&[("a::x", x), ("a::y", y), ("b", other)]), ["crate::a: x y: y -> x (X), x -> y (Y)"]);
    }

    #[test]
    fn follows_imports_to_the_module_that_owns_the_item() {
        let root = "
            pub use a::A;
            pub use self::b::B as Renamed;
            pub use c::{C, D as E};
        ";
        let a = "use crate::Renamed; pub struct A;";
        let b = "use crate::A; pub struct B;";
        let c = "use crate::E; pub struct C; pub struct D;";
        let files = [("", root), ("a", a), ("b", b), ("c", c)];
        assert_eq!(cycles(&files), ["crate: a b: a -> b (B), b -> a (A)"]);
    }

    #[test]
    fn reads_use_groups_and_inline_modules() {
        let a = "
            use crate::{b::{self, B}, c::*};
            mod inner {
                use super::super::b::B;
            }
        ";
        let b = "
            mod inner {
                fn f() { crate::a::inner::f(); }
            }
        ";
        assert_eq!(cycles(&[("a", a), ("b", b), ("c", "")]), ["crate: a b: b -> a (f), a -> b ()"]);
    }

    #[test]
    fn skips_tests_other_crates_and_macro_crate_paths() {
        let a = "
            use ::b::External;
            use b::Scoped;
            #[cfg(test)]
            use crate::b::B;
            #[cfg(test)]
            mod tests {
                use crate::b::B;
            }
            macro_rules! m {
                () => { $crate::b::f() };
            }
            pub struct A;
        ";
        let b = "use crate::a::A;";
        assert_eq!(cycles(&[("a", a), ("b", b)]), [] as [&str; 0]);
    }

    #[test]
    fn accepts_allow_comment() {
        let a = "
            // lint: allow(module_cycle), a reason
            use crate::b::B;
        ";
        let b = "use crate::a::A;";
        assert_eq!(cycles(&[("a", a), ("b", b)]), [] as [&str; 0]);
    }
}
