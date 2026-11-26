use std::collections::HashSet;

use proc_macro2::Ident;
use syn::Attribute;
use syn::Fields;
use syn::File;
use syn::ImplItem;
use syn::Item as SynItem;
use syn::TraitItem;
use syn::Type;
use syn::Visibility;
use syn::spanned::Spanned as _;
use syn::visit::Visit;

use super::ALLOW;
use crate::file_length::cfg_test;
use crate::file_length::is_test;
use crate::unused_pub;
use crate::unused_pub::allowed;
use crate::unused_pub::position;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub(super) enum Level {
    Private,
    Crate,
    Public,
}

/// Which mentions of the name count as a use: any, only after `.` or `::`, or only where a field is named.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
    Free,
    Member,
    Field,
}

/// An item or field with a visibility wider than private.
pub(super) struct Item {
    pub(super) kind: &'static str,
    pub(super) shape: Shape,
    pub(super) name: String,
    pub(super) declared: Level,
    /// As written, e.g. `pub(crate)`.
    pub(super) visibility: String,
    /// Inline modules around the item, within its file.
    pub(super) module: Vec<String>,
    pub(super) line: usize,
    /// 0-based character column of the name.
    pub(super) column: usize,
    /// Marked to keep its visibility.
    pub(super) allowed: bool,
    /// Marked as API for other workspaces, which `pub` is for.
    pub(super) api: bool,
    /// The type of a field or method.
    pub(super) owner: Option<String>,
    /// The methods of a trait, whose callers need the trait without naming it.
    pub(super) methods: Vec<String>,
}

/// Names in an interface, which must be at least as visible as the interface itself: the least visible of `by`
/// (indexes of the file's items) and the type named `owner`. With neither, the interface is taken as public.
pub(super) struct Floor {
    pub(super) names: Vec<String>,
    pub(super) by: Vec<usize>,
    pub(super) owner: Option<String>,
}

#[derive(Default)]
pub(super) struct Collected {
    pub(super) items: Vec<Item>,
    pub(super) floors: Vec<Floor>,
    /// Positions of the field names in definitions, which are not mentions.
    pub(super) fields: HashSet<(usize, usize)>,
    /// Types defined with any visibility.
    pub(super) types: Vec<String>,
}

pub(super) fn collect(file: &File, lines: &[&str]) -> Collected {
    let mut collector = Collector { lines, module: Vec::new(), out: Collected::default() };
    collector.items(&file.items);
    collector.out
}

struct Collector<'a> {
    lines: &'a [&'a str],
    module: Vec<String>,
    out: Collected,
}

impl Collector<'_> {
    fn items(&mut self, items: &[SynItem]) {
        for item in items.iter().filter(|item| !is_test(item)) {
            let line = item.span().start().line;
            let mut names = Names::default();
            let id = match item {
                SynItem::Fn(item) => {
                    names.visit_signature(&item.sig);
                    self.push("function", Shape::Free, &item.vis, &item.attrs, &item.sig.ident, line)
                }
                SynItem::Struct(item) => {
                    names.visit_generics(&item.generics);
                    let id = self.type_item("struct", &item.vis, &item.attrs, &item.ident, line);
                    self.fields(&item.fields, id, &item.ident);
                    id
                }
                SynItem::Union(item) => {
                    names.visit_generics(&item.generics);
                    let id = self.type_item("union", &item.vis, &item.attrs, &item.ident, line);
                    self.fields(&Fields::Named(item.fields.clone()), id, &item.ident);
                    id
                }
                SynItem::Enum(item) => {
                    names.visit_item_enum(item);
                    let fields = item.variants.iter().flat_map(|variant| &variant.fields);
                    self.out.fields.extend(fields.filter_map(|field| field.ident.as_ref()).map(position));
                    self.type_item("enum", &item.vis, &item.attrs, &item.ident, line)
                }
                SynItem::Trait(item) => {
                    names.visit_item_trait(item);
                    let id = self.type_item("trait", &item.vis, &item.attrs, &item.ident, line);
                    if let Some(id) = id {
                        let methods = item.items.iter().filter_map(|item| match item {
                            TraitItem::Fn(method) => Some(method.sig.ident.to_string()),
                            _ => None,
                        });
                        self.out.items[id].methods = methods.collect();
                    }
                    id
                }
                SynItem::Type(item) => {
                    names.visit_item_type(item);
                    self.type_item("type", &item.vis, &item.attrs, &item.ident, line)
                }
                SynItem::Const(item) => {
                    names.visit_type(&item.ty);
                    self.push("const", Shape::Free, &item.vis, &item.attrs, &item.ident, line)
                }
                SynItem::Static(item) => {
                    names.visit_type(&item.ty);
                    self.push("static", Shape::Free, &item.vis, &item.attrs, &item.ident, line)
                }
                SynItem::Impl(item) => {
                    self.impl_items(item);
                    continue;
                }
                SynItem::Mod(module) => {
                    if let Some((_, items)) = &module.content {
                        self.module.push(module.ident.to_string());
                        self.items(items);
                        self.module.pop();
                    }
                    continue;
                }
                _ => continue,
            };
            self.floor(names, &[id], None);
        }
    }

    fn impl_items(&mut self, item: &syn::ItemImpl) {
        if item.trait_.is_some() {
            // the associated types of a trait impl are as visible as the trait and the type, which are not resolved
            let mut names = Names::default();
            for item in &item.items {
                if let ImplItem::Type(item) = item {
                    names.visit_type(&item.ty);
                }
            }
            self.floor(names, &[], None);
            return;
        }
        let owner = match &*item.self_ty {
            Type::Path(path) => path.path.segments.last().map(|segment| segment.ident.to_string()),
            _ => None,
        };
        for member in &item.items {
            let line = member.span().start().line;
            let mut names = Names::default();
            names.visit_generics(&item.generics);
            let id = match member {
                ImplItem::Fn(member) => {
                    names.visit_signature(&member.sig);
                    self.push("function", Shape::Member, &member.vis, &member.attrs, &member.sig.ident, line)
                }
                ImplItem::Const(member) => {
                    names.visit_type(&member.ty);
                    self.push("const", Shape::Member, &member.vis, &member.attrs, &member.ident, line)
                }
                _ => continue,
            };
            if let Some(id) = id {
                self.out.items[id].owner.clone_from(&owner);
            }
            self.floor(names, &[id], owner.clone());
        }
    }

    fn fields(&mut self, fields: &Fields, owner_id: Option<usize>, owner: &Ident) {
        for field in fields {
            let mut by = vec![owner_id];
            if let Some(name) = &field.ident {
                self.out.fields.insert(position(name));
                let id = self.push("field", Shape::Field, &field.vis, &field.attrs, name, field.span().start().line);
                if let Some(id) = id {
                    let api = owner_id.is_some_and(|owner| self.out.items[owner].api);
                    self.out.items[id].owner = Some(owner.to_string());
                    self.out.items[id].api |= api;
                }
                by.push(id);
            } else if level(&field.vis) == Level::Private {
                continue;
            }
            let mut names = Names::default();
            names.visit_type(&field.ty);
            self.floor(names, &by, None);
        }
    }

    fn type_item(
        &mut self,
        kind: &'static str,
        visibility: &Visibility,
        attrs: &[Attribute],
        name: &Ident,
        line: usize,
    ) -> Option<usize> {
        self.out.types.push(name.to_string());
        self.push(kind, Shape::Free, visibility, attrs, name, line)
    }

    /// Returns the index of the item, if it is wider than private.
    fn push(
        &mut self,
        kind: &'static str,
        shape: Shape,
        visibility: &Visibility,
        attrs: &[Attribute],
        name: &Ident,
        item_line: usize,
    ) -> Option<usize> {
        let declared = level(visibility);
        // proc macros are exported by their attribute
        let proc_macro = attrs.iter().any(|attr| {
            let path = attr.path();
            path.is_ident("proc_macro") || path.is_ident("proc_macro_derive") || path.is_ident("proc_macro_attribute")
        });
        if declared == Level::Private || cfg_test(attrs) || proc_macro {
            return None;
        }
        let start = name.span().start();
        self.out.items.push(Item {
            kind,
            shape,
            name: name.to_string(),
            declared,
            visibility: written(visibility),
            module: self.module.clone(),
            line: start.line,
            column: start.column,
            allowed: allowed(self.lines, item_line, start.line, ALLOW),
            api: allowed(self.lines, item_line, start.line, unused_pub::ALLOW),
            owner: None,
            methods: Vec::new(),
        });
        Some(self.out.items.len() - 1)
    }

    /// A private item in `by` makes the interface private, which asks nothing of the names.
    fn floor(&mut self, names: Names, by: &[Option<usize>], owner: Option<String>) {
        if let Some(by) = by.iter().copied().collect::<Option<Vec<usize>>>()
            && !names.0.is_empty()
        {
            self.out.floors.push(Floor { names: names.0, by, owner });
        }
    }
}

fn level(visibility: &Visibility) -> Level {
    match visibility {
        Visibility::Public(_) => Level::Public,
        Visibility::Restricted(restricted) if restricted.path.is_ident("self") => Level::Private,
        // `pub(super)` and `pub(in path)` are not told apart from `pub(crate)`
        Visibility::Restricted(_) => Level::Crate,
        Visibility::Inherited => Level::Private,
    }
}

fn written(visibility: &Visibility) -> String {
    let Visibility::Restricted(restricted) = visibility else { return "pub".to_owned() };
    let path: Vec<String> = restricted.path.segments.iter().map(|segment| segment.ident.to_string()).collect();
    format!("pub({}{})", if restricted.in_token.is_some() { "in " } else { "" }, path.join("::"))
}

/// The identifiers of a signature or type; bodies, patterns and attributes are not part of an interface.
#[derive(Default)]
struct Names(Vec<String>);

impl<'ast> Visit<'ast> for Names {
    fn visit_ident(&mut self, ident: &'ast Ident) {
        self.0.push(ident.to_string());
    }

    fn visit_attribute(&mut self, _: &'ast Attribute) {}

    fn visit_block(&mut self, _: &'ast syn::Block) {}

    fn visit_expr(&mut self, _: &'ast syn::Expr) {}

    fn visit_pat(&mut self, _: &'ast syn::Pat) {}
}
