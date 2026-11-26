use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::path::Path;

use proc_macro2::TokenStream;
use serde_json::Value;
use syn::File;

use self::items::Floor;
use self::items::Item;
use self::items::Level;
use self::items::Shape;
use self::mentions::Mentions;
use self::mentions::Place;
use super::diagnostic::Diagnostic;
use super::file_length::rust_files;
use super::workspace::Workspace;

mod items;
mod mentions;

const ALLOW: &str = "lint: allow(excess_visibility)";

struct Source {
    text: String,
    /// Index of the workspace member.
    package: usize,
    /// The crate of a file under `src`; other files are crates of their own and are only read for mentions.
    krate: Option<usize>,
    module: Vec<String>,
    /// `pub` in the package is for callers outside the workspace.
    exempt: bool,
    /// No other crate can use the items of a binary.
    binary: bool,
    /// What a proc macro mentions is written into the crates that invoke it.
    proc_macro: bool,
}

struct Finding {
    file: usize,
    item: Item,
    required: Level,
}

/// Reports items and fields under each member's `src` that are more visible than their mentions need.
/// Matching is by name only, so a mention of any same-named item counts as a use.
pub(super) fn check(workspace: &Workspace, exempt_packages: &[String]) -> Result<Vec<Diagnostic>, Box<dyn Error>> {
    let mut files = Vec::new();
    let mut sources = Vec::new();
    let mut crates: Vec<(usize, String)> = Vec::new();
    for (package, member) in workspace.members.iter().enumerate() {
        let Some(package_dir) = member.absolute_path.parent() else { continue };
        let src = package_dir.join("src");
        let has_lib = src.join("lib.rs").is_file();
        let kinds = member.package.target.get("kind").and_then(Value::as_array);
        let proc_macro = kinds.is_some_and(|kinds| kinds.iter().any(|kind| kind == "proc-macro"));
        let mut paths = Vec::new();
        rust_files(package_dir, &mut paths)?;
        for path in paths {
            let (krate, module) = match path.strip_prefix(&src).ok().map(|path| locate(path, has_lib)) {
                Some((krate, module)) => {
                    let key = (package, krate);
                    let id = crates.iter().position(|other| *other == key).unwrap_or_else(|| {
                        crates.push(key);
                        crates.len() - 1
                    });
                    (Some(id), module)
                }
                None => (None, Vec::new()),
            };
            sources.push(Source {
                text: fs::read_to_string(&path)?,
                package,
                krate,
                module,
                exempt: exempt_packages.contains(&member.package.name),
                binary: !has_lib || krate.is_some_and(|id| !crates[id].1.is_empty()),
                proc_macro,
            });
            files.push((member, path));
        }
    }

    let mut diagnostics = Vec::new();
    for Finding { file, item, required } in analyze(&sources, &reach(workspace)) {
        let (member, path) = &files[file];
        let Item { visibility, kind, name, .. } = &item;
        let fix = match required {
            Level::Private => format!("is not used outside its module; remove `{visibility}`"),
            _ => "is not used outside its crate; change it to `pub(crate)`".to_owned(),
        };
        diagnostics.push(member.diagnostic_at(
            path.strip_prefix(&workspace.root).unwrap_or(path).to_path_buf(),
            &sources[file].text,
            name,
            (item.line, item.column),
            "excess_visibility",
            format!("`{visibility}` {kind} `{name}` {fix}, or mark it with `// {ALLOW}`"),
        ));
    }
    diagnostics.sort_by(|left, right| (&left.path, left.span.line_start).cmp(&(&right.path, right.span.line_start)));
    Ok(diagnostics)
}

/// For each member, the members it can use: itself and its dependencies of any kind, followed through.
fn reach(workspace: &Workspace) -> Vec<HashSet<usize>> {
    let members = &workspace.members;
    let direct = |member: usize| {
        let names = members[member].package.dependencies.iter().map(|dependency| &dependency.name);
        names.filter_map(|name| members.iter().position(|member| member.package.name == *name))
    };
    (0..members.len())
        .map(|member| {
            let mut reached = HashSet::from([member]);
            let mut pending = vec![member];
            while let Some(member) = pending.pop() {
                pending.extend(direct(member).filter(|dependency| reached.insert(*dependency)));
            }
            reached
        })
        .collect()
}

/// The crate within its package and the module of a file, by its path under `src`: `src/bin` holds a crate per
/// entry, and `main.rs` beside `lib.rs` is a crate of its own. `#[path]` attributes are not read.
pub(super) fn locate(path: &Path, has_lib: bool) -> (String, Vec<String>) {
    let mut module: Vec<String> =
        path.with_extension("").iter().map(|part| part.to_string_lossy().into_owned()).collect();
    let krate = if module.len() >= 2 && module[0] == "bin" {
        module.drain(..2).collect::<Vec<_>>().join("/")
    } else if has_lib && module == ["main"] {
        "main".to_owned()
    } else {
        String::new()
    };
    let root = module.len() == 1 && (module[0] == "lib" || module[0] == "main");
    if root || module.last().is_some_and(|name| name == "mod") {
        module.pop();
    }
    (krate, module)
}

/// `reach` is, by package, the packages it can use. Files that fail to parse are skipped; the compiler reports those.
fn analyze(sources: &[Source], reach: &[HashSet<usize>]) -> Vec<Finding> {
    let mut mentions = Mentions::default();
    let mut items: Vec<(usize, Item)> = Vec::new();
    let mut floors: Vec<Floor> = Vec::new();
    let mut types: HashSet<String> = HashSet::new();
    for (index, source) in sources.iter().enumerate() {
        let Ok(tokens) = source.text.parse::<TokenStream>() else { continue };
        let fields = if source.krate.is_some()
            && let Ok(file) = syn::parse2::<File>(tokens.clone())
        {
            let lines: Vec<&str> = source.text.lines().collect();
            let collected = items::collect(&file, &lines);
            let offset = items.len();
            floors.extend(collected.floors.into_iter().map(|mut floor| {
                floor.by.iter_mut().for_each(|id| *id += offset);
                floor
            }));
            items.extend(collected.items.into_iter().map(|item| (index, item)));
            types.extend(collected.types);
            collected.fields
        } else {
            HashSet::new()
        };
        mentions::collect(tokens, index, &fields, &mut mentions);
    }

    let mut mentioned = Vec::new();
    let mut kept = Vec::new();
    let mut required = Vec::new();
    for (file, item) in &items {
        let to = &sources[*file];
        let shape = match item.shape {
            Shape::Free => mentions::ANY,
            Shape::Member => mentions::MEMBER,
            Shape::Field => mentions::FIELD,
        };
        let mut places: Vec<Place> = mentions.places(&item.name, shape).collect();
        mentioned.push(!places.is_empty());
        if item.shape == Shape::Field {
            places.extend(item.owner.iter().flat_map(|owner| mentions.updates(owner)));
        }
        places.extend(item.methods.iter().flat_map(|method| mentions.places(method, mentions::MEMBER)));
        let module = [to.module.as_slice(), item.module.as_slice()].concat();
        let level = |place: &Place| {
            let from = &sources[place.file];
            if place.exported || from.proc_macro {
                Some(Level::Public)
            } else if from.krate.is_none() || from.krate != to.krate {
                // a crate that cannot use the item mentions another of its name
                reach[from.package].contains(&to.package).then_some(Level::Public)
            } else if from.module.starts_with(&module) {
                Some(Level::Private)
            } else {
                Some(Level::Crate)
            }
        };
        let keep = item.allowed || (item.declared == Level::Public && (to.exempt || item.api));
        let most = if to.binary { Level::Crate } else { Level::Public };
        let used = places.iter().filter_map(level).max().unwrap_or(Level::Private);
        required.push(if keep { item.declared } else { used.min(item.declared).min(most) });
        kept.push(keep);
    }
    let owners = Owners::new(&items, &types);
    raise_to_interfaces(&items, &floors, &owners, &mut required);
    // a field or method is no more visible than its type, so wider mentions are of other items with its name
    for (id, (_, item)) in items.iter().enumerate() {
        if let Some(owner) = item.owner.as_ref().filter(|_| !kept[id]) {
            required[id] = required[id].min(owners.level(owner, &required));
        }
    }

    let findings = items.into_iter().zip(required).zip(mentioned);
    findings
        .filter(|(((_, item), required), mentioned)| {
            // an item no one mentions is left to `unused_pub` and rustc's `dead_code`, which do not see fields
            *required < item.declared && (*mentioned || item.shape == Shape::Field)
        })
        .map(|(((file, item), required), _)| Finding { file, item, required })
        .collect()
}

/// Items by name, to find the type a field or method belongs to.
struct Owners<'a> {
    by_name: HashMap<&'a str, Vec<usize>>,
    types: &'a HashSet<String>,
}

impl<'a> Owners<'a> {
    fn new(items: &'a [(usize, Item)], types: &'a HashSet<String>) -> Self {
        let mut by_name: HashMap<&str, Vec<usize>> = HashMap::new();
        for (id, (_, item)) in items.iter().enumerate() {
            if item.shape == Shape::Free {
                by_name.entry(&item.name).or_default().push(id);
            }
        }
        Self { by_name, types }
    }

    /// The widest of the items named `owner`. A type no file defines comes from a macro, and is taken as public.
    fn level(&self, owner: &str, required: &[Level]) -> Level {
        let unknown = if self.types.contains(owner) { Level::Private } else { Level::Public };
        let owners = self.by_name.get(owner).into_iter().flatten();
        owners.map(|id| required[*id]).max().unwrap_or(unknown)
    }
}

/// A name in an interface must be as visible as the interface, or rustc's `private_interfaces` fires. Interfaces
/// are taken at the visibility they need, not the one they declare, so one run reports a whole chain.
fn raise_to_interfaces(items: &[(usize, Item)], floors: &[Floor], owners: &Owners<'_>, required: &mut [Level]) {
    loop {
        let mut changed = false;
        for floor in floors {
            let mut level = floor.by.iter().map(|id| required[*id]).min().unwrap_or(Level::Public);
            if let Some(owner) = &floor.owner {
                level = level.min(owners.level(owner, required));
            }
            for id in floor.names.iter().filter_map(|name| owners.by_name.get(name.as_str())).flatten() {
                let level = level.min(items[*id].1.declared);
                if required[*id] < level {
                    required[*id] = level;
                    changed = true;
                }
            }
        }
        if !changed {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::path::Path;

    use super::Level;
    use super::Source;
    use super::analyze;
    use super::locate;

    /// Files as `(crate, module, text)`, where crate 0 is outside any `src` and crates from 10 are binaries;
    /// returns `name: required` per finding.
    fn findings(files: &[(usize, &str, &str)]) -> Vec<String> {
        findings_without(files, &[])
    }

    /// Each crate is a package that can use all others, except the `(user, used)` pairs in `unreachable`.
    fn findings_without(files: &[(usize, &str, &str)], unreachable: &[(usize, usize)]) -> Vec<String> {
        let sources: Vec<Source> = files
            .iter()
            .map(|(krate, module, text)| Source {
                text: (*text).to_owned(),
                package: *krate,
                krate: (*krate != 0).then_some(*krate),
                module: module.split("::").filter(|part| !part.is_empty()).map(str::to_owned).collect(),
                exempt: false,
                binary: *krate >= 10,
                proc_macro: *krate == 20,
            })
            .collect();
        let reach: Vec<HashSet<usize>> =
            (0..=20).map(|user| (0..=20).filter(|used| !unreachable.contains(&(user, *used))).collect()).collect();
        let findings = analyze(&sources, &reach).into_iter().map(|finding| {
            let required = match finding.required {
                Level::Private => "private",
                Level::Crate => "crate",
                Level::Public => "pub",
            };
            format!("{}: {required}", finding.item.name)
        });
        findings.collect()
    }

    #[test]
    fn locates_crates_and_modules() {
        let locate = |path: &str| {
            let (krate, module) = locate(Path::new(path), true);
            format!("{krate}|{}", module.join("::"))
        };
        assert_eq!(locate("lib.rs"), "|");
        assert_eq!(locate("main.rs"), "main|");
        assert_eq!(locate("a/mod.rs"), "|a");
        assert_eq!(locate("a/b.rs"), "|a::b");
        assert_eq!(locate("bin/tool.rs"), "bin/tool|");
        assert_eq!(locate("bin/tool/main.rs"), "bin/tool|");
        assert_eq!(locate("bin/tool/args.rs"), "bin/tool|args");
    }

    #[test]
    fn reports_by_where_items_are_mentioned() {
        let source = "
            pub fn in_module() {}
            pub fn in_child() {}
            pub fn in_crate() {}
            pub fn in_other_crate() {}
            pub fn in_test() {}
            pub(crate) fn restricted() {}
            pub(crate) fn restricted_in_crate() {}
            pub(super) fn parent_only() {}
            fn private() {
                in_module();
                restricted();
                parent_only();
            }
        ";
        let child = "fn f() { super::in_child(); }";
        let sibling = "fn f() { crate::a::in_crate(); crate::a::restricted_in_crate(); }";
        let other = "fn f() { lib::a::in_other_crate(); }";
        let test = "fn f() { lib::a::in_test(); }";
        let files = [(1, "a", source), (1, "a::b", child), (1, "c", sibling), (2, "", other), (0, "", test)];
        assert_eq!(
            findings(&files),
            [
                "in_module: private",
                "in_child: private",
                "in_crate: crate",
                "restricted: private",
                "parent_only: private"
            ]
        );
    }

    #[test]
    fn leaves_unmentioned_items_to_other_lints() {
        let source = "
            pub fn unused() {}
            pub(crate) fn dead() {}
            #[cfg(test)]
            pub(crate) fn test_only() {}
            #[proc_macro]
            pub fn derive() {}
        ";
        assert_eq!(findings(&[(1, "", source)]), [] as [&str; 0]);
    }

    #[test]
    fn reports_methods_by_member_mentions() {
        let source = "
            pub(crate) struct S;
            impl S {
                pub(crate) fn here(&self) {}
                pub(crate) fn elsewhere(&self) {}
                pub(crate) const LIMIT: u8 = 0;
                fn f(&self) {
                    self.here();
                    Self::LIMIT;
                }
            }
        ";
        let other = "
            fn f(s: crate::a::S, here: u8, LIMIT: u8) {
                s.elsewhere();
            }
        ";
        assert_eq!(findings(&[(1, "a", source), (1, "b", other)]), ["here: private", "LIMIT: private"]);
    }

    #[test]
    fn reports_fields_by_field_mentions() {
        let source = "
            pub(crate) struct S {
                pub(crate) unused: u8,
                pub(crate) read: u8,
                pub(crate) literal: u8,
                pub(crate) shorthand: u8,
                pub(crate) pattern: u8,
                pub(crate) local: u8,
                pub(crate) kept: u8,
                pub(crate) called: u8,
                pub(crate) quoted: u8,
                private: u8,
            }
            impl S {
                fn f(&self) -> u8 {
                    self.local + self.kept
                }
            }
        ";
        let other = r#"
            use crate::a::S;
            struct Other {
                kept: u8,
            }
            fn f(s: S, shorthand: u8, local: u8) -> u8 {
                let S { pattern, .. } = S { literal: 1, shorthand };
                let kept = local;
                s.called();
                println!("quoted");
                s.read
            }
        "#;
        assert_eq!(
            findings(&[(1, "a", source), (1, "b", other)]),
            ["unused: private", "local: private", "kept: private", "called: private", "quoted: private"]
        );
    }

    #[test]
    fn keeps_fields_of_structs_updated_elsewhere() {
        let source = "
            pub(crate) struct Updated {
                pub(crate) field: u8,
            }
            pub(crate) struct Matched {
                pub(crate) rest: u8,
            }
        ";
        let other = "
            use crate::a::Matched;
            use crate::a::Updated;
            fn f(base: Updated, matched: Matched) {
                let _ = Updated { ..base };
                let Matched { .. } = matched;
            }
        ";
        assert_eq!(findings(&[(1, "a", source), (1, "b", other)]), ["rest: private"]);
    }

    #[test]
    fn keeps_names_in_wider_interfaces() {
        let source = "
            pub struct Returned;
            pub struct FieldType;
            pub struct Narrowed;
            pub struct Chained;
            pub trait Bound {}
            pub struct Associated;
            pub struct Holder {
                pub field: FieldType,
                pub(crate) narrowed: Narrowed,
            }
            pub fn make<T: Bound>() -> Returned {
                let _ = (Narrowed, Chained, Associated);
                Returned
            }
            pub(crate) fn chain() -> Chained {
                Chained
            }
            impl Iterator for Holder {
                type Item = Associated;
            }
        ";
        let sibling = "fn f() { crate::a::chain(); }";
        let other = "
            fn f(holder: lib::a::Holder) {
                lib::a::make();
                holder.field;
            }
        ";
        assert_eq!(
            findings(&[(1, "a", source), (1, "b", sibling), (2, "", other)]),
            ["Narrowed: private", "Chained: crate", "narrowed: private"]
        );
    }

    #[test]
    fn caps_members_by_their_type_and_items_by_binaries() {
        let source = "
            pub(crate) struct Inner {
                pub field: u8,
            }
            impl Inner {
                pub fn method(&self) {}
            }
            pub struct Outer {
                pub field: u8,
            }
        ";
        let binary = "
            pub struct Model {
                pub field: u8,
            }
        ";
        let sibling = "fn f(inner: crate::a::Inner) { inner.method(); }";
        let other = "fn f(outer: lib::a::Outer, model: other::Model) { outer.field; model.method(); }";
        let files = [(1, "a", source), (1, "b", sibling), (10, "", binary), (2, "", other)];
        assert_eq!(findings(&files), ["field: crate", "method: crate", "Model: crate", "field: crate"]);
    }

    #[test]
    fn ignores_mentions_from_crates_that_cannot_use_the_item() {
        let source = "
            pub fn unrelated() {}
            pub fn generated() {}
            fn f() {
                unrelated();
                generated();
            }
        ";
        let other = "fn f() { unrelated(); }";
        let proc_macro = "fn f() { quote!(::lib::a::generated()); }";
        let files = [(1, "a", source), (2, "", other), (20, "", proc_macro)];
        assert_eq!(findings_without(&files, &[]), [] as [&str; 0]);
        assert_eq!(findings_without(&files, &[(2, 1), (20, 1)]), ["unrelated: private"]);
    }

    #[test]
    fn keeps_traits_whose_methods_are_called() {
        let source = "
            pub trait Called {
                fn call(&self);
            }
            pub trait Named {
                fn name(&self);
            }
            fn f(_: &dyn Called, _: &dyn Named) {}
        ";
        let other = "
            use lib::a::*;
            fn f(value: impl Sized) {
                value.call();
            }
        ";
        assert_eq!(findings(&[(1, "a", source), (2, "", other)]), ["Named: private"]);
    }

    #[test]
    fn keeps_items_mentioned_in_macro_rules() {
        let source = "
            pub fn in_macro() {}
            pub fn outside() {}
            #[macro_export]
            macro_rules! call {
                () => {
                    $crate::a::in_macro()
                };
            }
            fn f() {
                outside();
            }
        ";
        assert_eq!(findings(&[(1, "a", source)]), ["outside: private"]);
    }

    #[test]
    fn accepts_allow_comments() {
        let source = "
            // lint: allow(excess_visibility)
            pub(crate) fn allowed() {}
            // lint: allow(unused_pub)
            pub struct Api {
                pub field: u8,
                pub(crate) internal: u8,
            }
            // lint: allow(unused_pub)
            pub(crate) fn not_api() {}
            fn f(api: Api) {
                allowed();
                not_api();
            }
        ";
        assert_eq!(findings(&[(1, "a", source)]), ["internal: private", "not_api: private"]);
    }
}
