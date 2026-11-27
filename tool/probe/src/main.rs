//! A rustc driver that compiles a crate as rustc does and also writes what the compiler resolved: the items the
//! crate defines and every use of an item of the workspace. It only extracts facts; rules live in `lint`.
#![feature(rustc_private)]

extern crate rustc_driver;
extern crate rustc_hir;
extern crate rustc_interface;
extern crate rustc_middle;
extern crate rustc_span;

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::mem;
use std::path::Path;
use std::path::PathBuf;
use std::process::ExitCode;

use rustc_driver::Callbacks;
use rustc_driver::Compilation;
use rustc_hir as hir;
use rustc_hir::HirId;
use rustc_hir::def::DefKind;
use rustc_hir::def::Res;
use rustc_hir::def_id::CRATE_DEF_ID;
use rustc_hir::def_id::DefId;
use rustc_hir::def_id::LOCAL_CRATE;
use rustc_hir::def_id::LocalDefId;
use rustc_hir::intravisit;
use rustc_hir::intravisit::Visitor;
use rustc_interface::interface::Compiler;
use rustc_middle::hir::nested_filter;
use rustc_middle::ty;
use rustc_middle::ty::TyCtxt;
use rustc_middle::ty::TypeckResults;
use rustc_span::ExpnKind;
use rustc_span::FileName;
use rustc_span::MacroKind;
use rustc_span::Span;

fn main() -> ExitCode {
    let mut args: Vec<String> = env::args().collect();
    // as `RUSTC_WORKSPACE_WRAPPER`, cargo passes the path of rustc first
    if args.get(1).is_some_and(|arg| Path::new(arg).file_stem().is_some_and(|stem| stem == "rustc")) {
        args.remove(1);
    }
    if !args.iter().any(|arg| arg.starts_with("--sysroot")) {
        args.extend(["--sysroot".to_owned(), env!("PROBE_SYSROOT").to_owned()]);
    }
    // the workspace is linted on its own toolchain; warnings new to this one are not its concern
    if !args.iter().any(|arg| arg.starts_with("--cap-lints")) {
        args.push("--cap-lints=allow".to_owned());
    }
    rustc_driver::catch_with_exit_code(|| rustc_driver::compiler_entrypoint(&args, &mut Probe))
}

struct Probe;

impl Callbacks for Probe {
    fn after_analysis<'tcx>(&mut self, _: &Compiler, tcx: TyCtxt<'tcx>) -> Compilation {
        if let (Some(out), Some(root)) = (env::var_os("PROBE_OUT"), env::var_os("PROBE_ROOT")) {
            write_facts(tcx, Path::new(&out), PathBuf::from(root));
        }
        Compilation::Continue
    }
}

/// One file per compilation, named as cargo names the artifact, so the reader can tell current files from stale.
fn write_facts(tcx: TyCtxt<'_>, out: &Path, root: PathBuf) {
    let name = tcx.crate_name(LOCAL_CRATE);
    if name.as_str().starts_with("build_script_") {
        return;
    }
    let mut collector = Collector {
        tcx,
        root,
        cwd: env::current_dir().expect("current directory"),
        test: tcx.sess.is_test_crate(),
        keys: HashMap::new(),
        types: Vec::new(),
        typeck: None,
        context: Context::default(),
        defs: Vec::new(),
        uses: BTreeMap::new(),
    };
    let Some((crate_root, ..)) = collector.position(tcx.def_span(CRATE_DEF_ID)) else { return };
    for item in tcx.hir_free_items() {
        let def_id = item.owner_id.to_def_id();
        if matches!(tcx.def_kind(def_id), DefKind::Struct | DefKind::Enum | DefKind::Union)
            && let Some(position) = tcx.def_ident_span(def_id).and_then(|span| collector.position(span))
        {
            collector.types.push((position, def_id));
        }
    }
    collector.types.sort_by(|left, right| left.0.cmp(&right.0));
    tcx.hir_walk_toplevel_module(&mut collector);

    let mut json = format!("{{\"root\":{},\"test\":{},\"defs\":[", quote(&crate_root), collector.test);
    json.push_str(&collector.defs.join(","));
    json.push_str("],\"uses\":[");
    let uses: Vec<String> = collector.uses.iter().map(|(place, detail)| place.json(detail)).collect();
    json.push_str(&uses.join(","));
    json.push_str("]}\n");
    fs::create_dir_all(out).expect("create facts directory");
    let file = out.join(format!("{name}{}.json", tcx.sess.opts.cg.extra_filename));
    fs::write(&file, json).unwrap_or_else(|error| panic!("write {}: {error}", file.display()));
}

/// What surrounds the code being visited; saved and reset at each item, since items nest inside bodies.
#[derive(Default, Clone)]
struct Context {
    in_body: bool,
    in_use: bool,
    /// Directly inside a struct, where each field is an interface of its own.
    in_struct: Option<String>,
    in_trait_impl: bool,
    /// The self type of the enclosing impl.
    self_type: Option<String>,
    /// What the code outside bodies is the interface of: the key of an item or field, or a fixed `pub` or `crate`.
    interface: Option<String>,
    /// Enclosing items and the self type of enclosing impls; a use of one of them is the item using itself.
    own: Vec<String>,
}

/// A use, without the column, so a path whose segments resolve to the same item counts once.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct Place {
    to: String,
    module: String,
    file: String,
    line: usize,
    member: bool,
    import: bool,
    own: bool,
    interface: Option<String>,
}

struct Detail {
    column: usize,
    /// For an item of this crate: its name and the module that owns it.
    target: Option<(String, String)>,
}

impl Place {
    fn json(&self, detail: &Detail) -> String {
        let mut json = format!(
            "{{\"to\":{},\"module\":{},\"file\":{},\"line\":{},\"column\":{},\"member\":{},\"import\":{},\"own\":{}",
            quote(&self.to),
            quote(&self.module),
            quote(&self.file),
            self.line,
            detail.column,
            self.member,
            self.import,
            self.own
        );
        if let Some(interface) = &self.interface {
            write!(json, ",\"interface\":{}", quote(interface)).expect("write to string");
        }
        if let Some((name, module)) = &detail.target {
            write!(json, ",\"name\":{},\"to_module\":{}", quote(name), quote(module)).expect("write to string");
        }
        json.push('}');
        json
    }
}

fn quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for char in text.chars() {
        match char {
            '"' | '\\' => {
                quoted.push('\\');
                quoted.push(char);
            }
            char if char.is_control() => write!(quoted, "\\u{:04x}", u32::from(char)).expect("write to string"),
            char => quoted.push(char),
        }
    }
    quoted.push('"');
    quoted
}

struct Collector<'tcx> {
    tcx: TyCtxt<'tcx>,
    /// The workspace root; items and uses outside it are not recorded.
    root: PathBuf,
    cwd: PathBuf,
    test: bool,
    keys: HashMap<DefId, Option<String>>,
    /// The types of the crate by where their names are written, to find the type a derive is on.
    types: Vec<((String, usize, usize), DefId)>,
    typeck: Option<&'tcx TypeckResults<'tcx>>,
    context: Context,
    defs: Vec<String>,
    uses: BTreeMap<Place, Detail>,
}

impl<'tcx> Collector<'tcx> {
    /// File relative to the workspace root, 1-based line and 0-based character column of where `span` starts.
    fn position(&self, span: Span) -> Option<(String, usize, usize)> {
        let location = self.tcx.sess.source_map().lookup_char_pos(span.lo());
        let FileName::Real(name) = &location.file.name else { return None };
        let path = name.local_path()?;
        let path = if path.is_absolute() { path.to_path_buf() } else { self.cwd.join(path) };
        let path = path.strip_prefix(&self.root).ok()?;
        Some((path.to_str()?.to_owned(), location.line, location.col.0))
    }

    /// An item is identified by where its name is written, which is the same in every crate that sees it.
    fn key(&mut self, def_id: DefId) -> Option<String> {
        if let Some(key) = self.keys.get(&def_id) {
            return key.clone();
        }
        let key = self.tcx.def_ident_span(def_id).and_then(|span| self.position(span));
        let key = key.map(|(file, line, column)| format!("{file}:{line}:{column}"));
        self.keys.insert(def_id, key.clone());
        key
    }

    fn module(&self, module: DefId) -> String {
        let path = self.tcx.def_path(module);
        let names: Vec<String> = path.data.iter().map(|part| part.data.to_string()).collect();
        names.join("::")
    }

    fn visibility(&self, def_id: LocalDefId) -> &'static str {
        match self.tcx.visibility(def_id) {
            ty::Visibility::Public => "pub",
            ty::Visibility::Restricted(module) => {
                if module.to_def_id() == self.tcx.parent_module_from_def_id(def_id).to_def_id() {
                    "private"
                } else {
                    "crate"
                }
            }
        }
    }

    /// Records an item or field. Returns its key, and what its interface is the interface of: the key when the
    /// item is recorded, else its visibility, which then cannot be narrowed.
    fn define(
        &mut self,
        def_id: LocalDefId,
        kind: &str,
        span: Span,
        owner: Option<&str>,
    ) -> (Option<String>, Option<String>) {
        let visibility = self.visibility(def_id);
        let fixed = (visibility != "private").then(|| visibility.to_owned());
        let Some(ident) = self.tcx.def_ident_span(def_id) else { return (None, fixed) };
        let Some(key) = self.key(def_id.to_def_id()) else { return (None, fixed) };
        // a name written by a macro has no place to report
        if visibility == "private" || ident.from_expansion() {
            return (Some(key), fixed);
        }
        // tests are compiled again with the crate, which is where its items are recorded
        if self.test {
            return (Some(key.clone()), Some(key));
        }
        let Some((file, line, column)) = self.position(ident) else { return (Some(key), fixed) };
        // where the item starts, below its attributes, which the compiler keeps no place for
        let item_line = self.position(span).filter(|(start, ..)| *start == file).map_or(line, |(_, line, _)| line);
        let module = self.module(self.tcx.parent_module_from_def_id(def_id).to_def_id());
        let mut json = format!(
            "{{\"key\":{},\"file\":{},\"line\":{line},\"column\":{column},\"item_line\":{item_line},\"kind\":{},\"name\":{},\"visibility\":{},\"module\":{}",
            quote(&key),
            quote(&file),
            quote(kind),
            quote(self.tcx.item_name(def_id.to_def_id()).as_str()),
            quote(visibility),
            quote(&module),
        );
        if let Some(owner) = owner {
            write!(json, ",\"owner\":{}", quote(owner)).expect("write to string");
        }
        json.push('}');
        self.defs.push(json);
        (Some(key.clone()), Some(key))
    }

    fn record(&mut self, def_id: DefId, span: Span, hir_id: HirId, member: bool) {
        let tcx = self.tcx;
        let mut def_id = def_id;
        // a constructor or a variant is a use of its type
        while matches!(tcx.def_kind(def_id), DefKind::Ctor(..) | DefKind::Variant) {
            def_id = tcx.parent(def_id);
        }
        let kind = tcx.def_kind(def_id);
        match kind {
            DefKind::AssocFn | DefKind::AssocConst | DefKind::AssocTy => {
                // a trait must be visible wherever its items are used
                let parent = tcx.parent(def_id);
                if tcx.def_kind(parent) == DefKind::Trait {
                    self.record(parent, span, hir_id, true);
                }
            }
            DefKind::Fn
            | DefKind::Struct
            | DefKind::Enum
            | DefKind::Union
            | DefKind::Trait
            | DefKind::TyAlias
            | DefKind::Const { .. }
            | DefKind::Static { .. }
            | DefKind::Field
            | DefKind::Mod => {}
            _ => return,
        }
        let Some(to) = self.key(def_id) else { return };
        let Some((file, line, column)) = self.position(span.source_callsite()) else { return };
        let module = self.module(tcx.parent_module(hir_id).to_def_id());
        let target = def_id.as_local().map(|local| {
            let owner = if kind == DefKind::Mod { def_id } else { tcx.parent_module_from_def_id(local).to_def_id() };
            (tcx.item_name(def_id).to_string(), self.module(owner))
        });
        let place = Place {
            own: self.context.own.contains(&to),
            to,
            module,
            file,
            line,
            member,
            import: self.context.in_use,
            interface: if self.context.in_body { None } else { self.context.interface.clone() },
        };
        self.uses.entry(place).or_insert(Detail { column, target });
    }

    /// The type a derive is written on: the first one named after the derive in its file.
    fn derived(&mut self, span: Span) -> Option<String> {
        let expansion = span.ctxt().outer_expn_data();
        if !matches!(expansion.kind, ExpnKind::Macro(MacroKind::Derive, _)) {
            return None;
        }
        let derive = self.position(expansion.call_site)?;
        let index = self.types.partition_point(|(position, _)| *position < derive);
        let (position, def_id) = self.types.get(index)?;
        if position.0 == derive.0 { self.key(*def_id) } else { None }
    }

    /// Visits an item with a context of its own, keeping what encloses it.
    fn nested(&mut self, visit: impl FnOnce(&mut Self)) {
        let typeck = self.typeck.take();
        let context = self.context.clone();
        self.context.in_body = false;
        self.context.in_use = false;
        self.context.in_struct = None;
        visit(self);
        self.context = context;
        self.typeck = typeck;
    }
}

impl<'tcx> Visitor<'tcx> for Collector<'tcx> {
    type NestedFilter = nested_filter::All;

    fn maybe_tcx(&mut self) -> Self::MaybeTyCtxt {
        self.tcx
    }

    fn visit_nested_body(&mut self, id: hir::BodyId) {
        let typeck = self.typeck.replace(self.tcx.typeck_body(id));
        let in_body = mem::replace(&mut self.context.in_body, true);
        intravisit::walk_body(self, self.tcx.hir_body(id));
        self.context.in_body = in_body;
        self.typeck = typeck;
    }

    fn visit_item(&mut self, item: &'tcx hir::Item<'tcx>) {
        let def_id = item.owner_id.def_id;
        let kind = self.tcx.def_kind(def_id);
        let label = match kind {
            DefKind::Fn => Some("function"),
            DefKind::Struct => Some("struct"),
            DefKind::Enum => Some("enum"),
            DefKind::Union => Some("union"),
            DefKind::Trait => Some("trait"),
            DefKind::TyAlias => Some("type alias"),
            DefKind::Const { .. } => Some("constant"),
            DefKind::Static { .. } => Some("static"),
            _ => None,
        };
        self.nested(|this| {
            this.context.in_use = matches!(item.kind, hir::ItemKind::Use(..));
            // what a derive writes for a type is the type using itself
            let derived = this.derived(item.span);
            this.context.own.extend(derived);
            if let DefKind::Impl { of_trait } = kind {
                this.context.in_trait_impl = of_trait;
                this.context.interface = None;
                let self_type = this.tcx.type_of(def_id).instantiate_identity().skip_normalization().ty_adt_def();
                this.context.self_type = self_type.and_then(|adt| this.key(adt.did()));
                this.context.own.extend(this.context.self_type.clone());
            } else if let Some(label) = label {
                let (key, interface) = this.define(def_id, label, item.span, None);
                this.context.interface = interface;
                if kind == DefKind::Struct {
                    this.context.in_struct.clone_from(&key);
                }
                this.context.own.extend(key);
            } else if kind != DefKind::Mod {
                this.context.interface = None;
            }
            intravisit::walk_item(this, item);
        });
    }

    fn visit_impl_item(&mut self, item: &'tcx hir::ImplItem<'tcx>) {
        let def_id = item.owner_id.def_id;
        let kind = self.tcx.def_kind(def_id);
        self.nested(|this| {
            if this.context.in_trait_impl {
                // signatures repeat the trait's; an associated type is as visible as the trait and the self type
                let exposed = this.context.self_type.clone().unwrap_or_else(|| "pub".to_owned());
                this.context.interface = (kind == DefKind::AssocTy).then_some(exposed);
            } else {
                let label = match kind {
                    DefKind::AssocFn => "method",
                    DefKind::AssocConst => "associated constant",
                    _ => "associated type",
                };
                let (key, interface) = this.define(def_id, label, item.span, None);
                this.context.interface = interface;
                this.context.own.extend(key);
            }
            intravisit::walk_impl_item(this, item);
        });
    }

    fn visit_field_def(&mut self, field: &'tcx hir::FieldDef<'tcx>) {
        let interface = self.context.interface.clone();
        if let Some(owner) = self.context.in_struct.clone() {
            if field.is_positional() {
                let visibility = self.visibility(field.def_id);
                self.context.interface = (visibility != "private").then(|| visibility.to_owned());
            } else {
                self.context.interface = self.define(field.def_id, "field", field.span, Some(&owner)).1;
            }
        }
        intravisit::walk_field_def(self, field);
        self.context.interface = interface;
    }

    fn visit_path(&mut self, path: &hir::Path<'tcx>, id: HirId) {
        let last = path.segments.len().saturating_sub(1);
        for (index, segment) in path.segments.iter().enumerate() {
            // a module on the way to an item is not what is used
            if let Res::Def(kind, def_id) = segment.res
                && (index == last || kind != DefKind::Mod)
            {
                self.record(def_id, segment.ident.span, id, false);
            }
        }
        if let Res::Def(_, def_id) = path.res {
            self.record(def_id, path.span, id, false);
        }
        intravisit::walk_path(self, path);
    }

    fn visit_qpath(&mut self, qpath: &'tcx hir::QPath<'tcx>, id: HirId, _: Span) {
        if let hir::QPath::TypeRelative(_, segment) = qpath
            && let Some(typeck) = self.typeck
            && let Res::Def(_, def_id) = typeck.qpath_res(qpath, id)
        {
            self.record(def_id, segment.ident.span, id, false);
        }
        intravisit::walk_qpath(self, qpath, id);
    }

    fn visit_expr(&mut self, expr: &'tcx hir::Expr<'tcx>) {
        if let Some(typeck) = self.typeck {
            match expr.kind {
                hir::ExprKind::MethodCall(segment, ..) => {
                    if let Some(def_id) = typeck.type_dependent_def_id(expr.hir_id) {
                        self.record(def_id, segment.ident.span, expr.hir_id, true);
                    }
                }
                hir::ExprKind::Field(base, ident) => {
                    if let Some(adt) = typeck.expr_ty_adjusted(base).ty_adt_def()
                        && !adt.is_enum()
                        && let Some(index) = typeck.opt_field_index(expr.hir_id)
                    {
                        self.record(adt.non_enum_variant().fields[index].did, ident.span, expr.hir_id, true);
                    }
                }
                hir::ExprKind::Struct(qpath, fields, tail) => {
                    if let Some(adt) = typeck.expr_ty(expr).ty_adt_def() {
                        let variant = adt.variant_of_res(typeck.qpath_res(qpath, expr.hir_id));
                        for field in fields {
                            if let Some(index) = typeck.opt_field_index(field.hir_id) {
                                self.record(variant.fields[index].did, field.ident.span, expr.hir_id, true);
                            }
                        }
                        // `..base` reads every field that is not written
                        if matches!(tail, hir::StructTailExpr::Base(_)) {
                            for field in &variant.fields {
                                self.record(field.did, expr.span, expr.hir_id, true);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        intravisit::walk_expr(self, expr);
    }

    fn visit_pat(&mut self, pat: &'tcx hir::Pat<'tcx>) {
        if let Some(typeck) = self.typeck
            && let hir::PatKind::Struct(qpath, fields, _) = pat.kind
            && let Some(adt) = typeck.pat_ty(pat).ty_adt_def()
        {
            let variant = adt.variant_of_res(typeck.qpath_res(&qpath, pat.hir_id));
            for field in fields {
                if let Some(index) = typeck.opt_field_index(field.hir_id) {
                    self.record(variant.fields[index].did, field.ident.span, pat.hir_id, true);
                }
            }
        }
        intravisit::walk_pat(self, pat);
    }
}
