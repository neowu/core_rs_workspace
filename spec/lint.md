# Workspace lint

```sh
cargo lint
cargo +nightly fmt --all -- --check
```

Every rule is a warning, the workspace's own and the Clippy and rustc lints `Cargo.toml` turns on, and
no warning is left: each is fixed, or excused where it stands with `#[expect(.., reason = "..")]` or the
rule's marker comment. Only what does not compile is an error. The repository Cargo configuration denies
warnings in every build (`[build] warnings = "deny"`), and `cargo lint` fails on any.

## Packaging

The checks live in the `lint` library (`tool/lint`) so other workspaces can run them the
same way this one does. Each workspace keeps a thin `xtask` binary member that calls
`lint::run(&LintConfig { .. })`, plus the Cargo alias
`lint = "run --quiet -p xtask -- lint"`. Customization is passed in code through
`LintConfig` (`max_file_lines`, default 500; `api_tests`, default none), not a config file. Other
workspaces depend on it via git, like the other framework crates.

The workspace is located at runtime: the library reads `CARGO_MANIFEST_DIR` and
`CARGO`, which `cargo run` sets for the wrapper binary, so it checks the workspace
that contains the wrapper rather than wherever the library was compiled from. It
must therefore be started through `cargo run`.

`cargo lint` runs repository checks through the `xtask` workspace member, then
Clippy with `--workspace --all-targets -- -D warnings`. Either phase failing makes
the command fail. `cargo lint --custom-only` runs only the repository checks.
Repository checks are printed as warnings, and any of them fails the command, so a caller
that only reads the exit status cannot miss one.
CI uses the same command as local development.
Unit tests live in the same file as the code they test, in a `#[cfg(test)] mod tests`.
The probe rules are tested end to end instead, by `test/lint_test`: it runs `cargo lint --custom-only`
on a small workspace of real sources in its `assets/` and compares the diagnostics, so facts are never
written by hand.

`--message-format=json` emits the same JSON line stream as
`cargo clippy --message-format=json` on stdout, so IDEs and tools that read Clippy
output can read repository checks too. Repository diagnostics are
`compiler-message` entries with the rule as the diagnostic code and the manifest,
or source file, relative to the workspace root, as the span file. Diagnostics on the root
manifest are attributed to the wrapper (`xtask`) package, since a virtual manifest has no
package. Clippy's own `build-finished` message is replaced by one final
`build-finished` whose `success` covers both phases. The default
`--message-format=human` prints rustc-style diagnostics to stderr.

## Dependency layout

`[workspace.dependencies]` holds only dependencies shared by two or more members.
A dependency used by a single member is declared in that member's manifest with
its own version, source, and features. Once a dependency is declared in the root,
every member inherits it with `workspace = true`, adding member-specific features,
optionality, and default-feature settings as needed. This applies to normal,
development, build, and target-specific dependencies, and to registry, Git, and
local path dependencies alike.

| Rule | Requirement |
| --- | --- |
| `missing_workspace_dependency` | A `workspace = true` dependency has a root declaration with the same key. |
| `inherit_workspace_dependency` | A member does not declare directly a package that the root declares. |
| `duplicate_dependency` | A package declared directly by more than one member moves to the root. |
| `unused_workspace_dependency` | Each root declaration is inherited by at least one member. |
| `unshared_workspace_dependency` | Each root declaration is inherited by at least two members; otherwise it belongs in that member. |
| `wildcard_dependency` | Registry dependencies in the root and members specify a version other than `*` (or an equivalent such as `x`, or none). Path and Git dependencies are exempt. |

Path dependencies on workspace members are exempt from the `unused_workspace_dependency`
and `unshared_workspace_dependency` rules, so the root can list every workspace crate
(such as `lint`, used only by `xtask`) and members always inherit them.

A package overridden in any root `[patch.<source>]` table counts the patch as a root use,
so it is exempt from `unshared_workspace_dependency` and may stay in the root with a
single inheriting member, keeping the declaration next to its patch.

Packages are identified by their `package` name, so renamed dependencies count as
the same package. Root use is counted by root key and distinct member; a member
declaring the same dependency in several sections or targets counts once, and
optional or inactive platform-specific declarations count as uses. These checks
read manifest references; the Rust `unused_crate_dependencies` lint separately
checks usage in source code. Diagnostics identify the manifest, line, column, and
rule; violations produce a nonzero exit status.

## File length

| Rule | Requirement |
| --- | --- |
| `file_too_long` | A Rust source file has at most `max_file_lines` (default 500) lines, not counting top-level `#[cfg(test)]` items. |

Files are parsed with `syn` so test items are excluded by their attributes rather
than by layout. Only `.rs` files under each member's `src` are checked; build
scripts, `tests/`, benches, and examples are exempt. A test module in a separate
file (`#[cfg(test)] mod x;`) is not recognized as test code, which is why unit tests
stay inline. Files that do not parse are skipped and left to the compiler. The
diagnostic points at the first line past the limit.

## Probe

`unused_pub`, `excess_visibility`, `field_write`, `wildcard_match`, `misplaced_item` and `module_cycle` read what the compiler resolved, not
names, so two items of the same name are never confused. They run in two phases:

1. **Facts.** `tool/probe` is a rustc driver, built with the latest nightly (its
   `rust-toolchain.toml` names the unpinned `nightly` channel), which that toolchain's `cargo check --workspace --all-targets`
   runs in place of rustc for workspace members. For each compilation it writes one JSON
   file: the non-private items, modules, struct fields, enum variants and trait items the
   crate defines, and every use of a workspace item, with the module and the function or
   type it is in, and how it is used: called, constructed, matched in a pattern, written,
   mutably borrowed, or only named. A catch-all arm of a match on an enum is recorded as a
   use of the enum. The probe holds no rule.
2. **Rules.** `lint`, on stable, reads the files and reports.

The probe is not a workspace member and nothing depends on it, so stable builds, Clippy
and rust-analyzer never compile it. `lint` finds its source beside its own, also as a git
dependency, and builds it on first use. The toolchain is not installed automatically; the
error names the `rustup` command, which CI runs as a step. The nightly is not pinned: the
latest one is assumed everywhere, and when it breaks the probe, the probe is fixed, since
only linting is affected. Workspaces using `lint` from git update it when that happens. Warnings are capped while
probing, since the workspace is linted on its own toolchain.

Everything is kept under `target/probe/<hash of the probe source>/`: the probe binary,
a check cache of its own so stable artifacts are untouched, and the facts. Cargo skips
crates that have not changed and their facts stay on disk, so a run after a small change
is fast. Only facts of artifacts cargo reports for the run are read; stale files are
ignored.

How the facts are read:

- An item is identified by the file, line and column of its name, which is the same from
  every crate that sees it. A crate is identified by its root file, so a library and its
  unit tests are one crate, and its integration tests, examples and binaries are others.
- Items are taken from compilations without `cfg(test)`, so test-only items are not
  checked; uses are taken from all compilations.
- A use is a resolved path, method call, field access, or field named in a struct literal
  or pattern, also inside macro expansions. `Type { ..base }` uses every field, and so
  does calling or matching the constructor of a tuple struct. A constructor or variant is a
  use of its type, and a use of a trait's item is a use of the trait.
- A module is used by every path that goes through it. Such a use counts for where the
  module must be visible, not as a use of the module by `module_cycle`, since the module
  may only re-export the item.
- A use of an item inside its own definition, inside an impl of it, or in what a derive
  wrote for it, is the item using itself. A field is its type's own. `use` declarations are imports. Neither makes
  an item used, but both count for where it must be visible.

Checked: functions, inherent methods and associated consts, structs, enums, unions,
traits, type aliases, consts, statics, modules, and named and tuple struct fields under a
member's `src`, including items a `macro_rules!` invocation names by argument, reported at
the invocation. Enum variants and the methods and constants of traits have no visibility of
their own and are checked only for being used.

Not checked: fields of enum variants, associated types of traits, trait impls, `use`
declarations, `#[cfg(test)]` items, proc macro entry points, and items whose name a macro
writes.

Limits: only the configuration compiled is seen, so an item used only under another
target or feature looks unused, and a path in a `macro_rules!` body is seen only where
the macro is invoked in the workspace. The path of a macro invocation (`a::b::submit!`) is
resolved before the facts are taken, so a module only such paths go through looks unused.
All are handled with the exceptions below.

Exceptions are comments, since stable Rust rejects lint names of unknown tools in
`#[expect]`: `// lint: expect(<rule>)` (a reason may follow) in the comment block directly
above an item or field, above its attributes, or between them and its name. An item a
macro writes around a name given by argument starts inside the macro, so its marker goes
directly above the line of the name. Like
`#[expect]`, a marker must excuse something: one on an item the rule would not report is
itself reported, as `unfulfilled_lint_expectations`, so exceptions do not outlive their
reason. There is no exception wider than an item.

Every comment of the form `// lint: <verb>(<rule>)` is checked, so none is left behind
when a rule or the code under it goes. The markers are `expect(unused_pub)`,
`expect(excess_visibility)`, `expect(misplaced_item)`, `expect(module_cycle)`,
`expect(wildcard_match)` and
`enforce(field_write)`; any other
verb or rule is reported as `unknown_lints`. One that is not directly above something its
rule checks, such as a private item, is reported as `unfulfilled_lint_expectations`.

## Unused pub items

| Rule | Requirement |
| --- | --- |
| `unused_pub` | A `pub` item of a library is used somewhere in the workspace by something other than itself and `use` declarations. |

rustc's `dead_code` covers everything not exported from a crate (`pub(crate)`, private
items, and `pub` items in binaries), and `unreachable_pub` forces unexported items to be
`pub(crate)`. This rule covers the rest: items a library exports that no one uses.
The workspace is treated as the universe, so a library's public API must be used by code
that is not a test: an item only tests use is dead, and is reported as only used by tests.
That is the default because it is right for an application workspace. A type that only has
impls, or that is only re-exported, is unused. Fields and modules are not checked.

Tests count as callers in two cases:

- `LintConfig::api_tests` names member directories, relative to the workspace root, whose
  tests stand for the callers of a public API. A framework workspace names its e2e tests
  there, so its API is held by them instead of a marker on every item; a unit test still
  holds nothing. A directory no member is under is an error.
- A library that other members have only as a dev-dependency is there for tests, so tests
  are the callers of its items.

A variant of a `pub` enum must be named, and a method or constant of a `pub` trait called
or read, somewhere outside what a derive wrote; implementing a trait method is not using
it. rustc's `dead_code` covers both for enums and traits a crate does not export. They are
reported once, on the enum or trait, when that is unused itself.

Exception: `// lint: expect(unused_pub)`, for API meant for callers outside the workspace.
It is not needed, and reported, once something in the workspace uses the item. On an enum
or trait it covers its variants and items.

## Excess visibility

| Rule | Requirement |
| --- | --- |
| `excess_visibility` | An item, module or field is no more visible than its uses need: `pub` only if another crate uses it, `pub(crate)` only if a module outside the defining module and its children does. |

The goal is the smallest open surface: whatever stays `pub` or `pub(crate)` is what other
code really depends on. `unused_pub` asks whether an exported item is used at all; this
rule asks from where, and covers binaries, `pub(crate)` and fields as well. rustc has no
equivalent: `unreachable_pub` only catches `pub` that the crate does not export.

- The defining module is the one the item is private to: for a method, the module of its
  impl. `pub(super)` and `pub(in path)` are treated as `pub(crate)`.
- A name in an interface (signature, field type, bound, alias, enum variant, trait
  definition) must stay as visible as that interface needs to be, matching rustc's
  `private_interfaces`. This is solved to a fixed point, so one run reports a whole chain.
  An associated type in a trait impl is as visible as the impl's self type.
- Advice never asks for what rustc rejects. Only an interface that is itself reported is
  taken at the visibility it needs; one that is unused, only used by tests or marked stays
  as declared, and so does what it names, until it is removed or narrowed.
- An item that would no longer be `pub` is reported with the lines of its `pub use`
  declarations in its crate, which rustc rejects unless they are narrowed with it.
- An item nothing uses is left to `unused_pub` and rustc's `dead_code`, and so is one only
  tests use, since narrowing it makes it dead code. Fields and modules are reported either
  way, since neither rule sees them.
- A module needs to be as visible as the paths written through it. One that its parent
  only re-exports from is private, which makes the parent the single way in.

Exceptions:

- `// lint: expect(excess_visibility)` on an item or a field, for one that is used in the
  workspace but meant to be more visible than those uses need.
- `pub` items marked `// lint: expect(unused_pub)`, together with their `pub` fields, are
  API for other workspaces and keep `pub`.

## Field writes

| Rule | Requirement |
| --- | --- |
| `field_write` | A function outside the module of a struct marked `// lint: enforce(field_write)` changes it in at most one place. |

Most structs are data: they are filled in and passed on, and their fields are open on
purpose. The rule is therefore off unless a struct asks for it, with the marker in the
comment block above it, for a struct that keeps state its fields must agree on.

For such a struct, one change from outside is what a setter would do, and a setter adds
nothing, so it is allowed. A function that changes two fields, or one field twice, holds
logic about the struct's state; it is reported at its first change, to be moved into a
method of the struct.

- A change is an assignment or a mutable borrow of a field or of a place inside it,
  including the borrow a `&mut self` method call takes. What is behind a reference the
  field holds is not the field.
- Changes are counted per function and struct, by the line they are on. Code in an impl of
  the struct, in any module, and code in the module the fields are private to is not
  counted. Tests may change anything.

Limits: a field changed through a `ref mut` or `&mut` pattern binding, or through interior
mutability, is not seen.

## Wildcard matches

| Rule | Requirement |
| --- | --- |
| `wildcard_match` | A `match` on an enum defined in the workspace has no catch-all arm. |

In one workspace, adding a variant should make the compiler point at every match that has
to decide about it. A catch-all arm takes the new variant silently, so the variants are
named instead. Enums from outside the workspace are not checked: their variants are not
ours to follow.

- A catch-all arm is `_` or a bare binding without a guard, also behind `&` or as one side
  of `|`.
- Only the type matched on is checked, through references: an enum inside a tuple, an
  `Option` or another pattern is not, and neither are `if let`, `let else` and `matches!`,
  which say they care about one shape.
- Arms written by a macro, and tests, are not checked.

Exception: `// lint: expect(wildcard_match)` directly above the arm, for a match that is
about a few variants of a large enum.

## Misplaced items

| Rule | Requirement |
| --- | --- |
| `misplaced_item` | A function, constant or static is not used by only one other part of its crate while needing nothing private to its own module. |

Such an item can move to the module that uses it and be private there: one name less for
the rest of the crate, nothing else widened, and no module cycle created, since its user
already depended on its module and its module never used it. Whether it also reads better
there is a judgement (it may be grouped by topic on purpose), so each finding is decided
once: the item is moved, or marked with `// lint: expect(misplaced_item)` and the reason it
stays. The marker is reported when the rule would no longer report the item.

An item is reported when all of these hold:

- Every use of it, outside the unit tests of its crate, is in one module of its crate or under it, and that
  module is neither above its own module (a module split into children) nor inside it
  (`excess_visibility` then applies).
- Everything its signature and body use is visible from that module as declared: items of
  other crates, items in modules around the user, and `pub` or `pub(crate)` items reached
  through `pub` or `pub(crate)` modules. An item that needs something private belongs
  where it is.

Not checked: methods and types, which bring their impls along, and items used by another
crate, its tests included, which could not reach a private item.

## Module cycles

| Rule | Requirement |
| --- | --- |
| `module_cycle` | Sibling modules of a crate do not use each other in a circle. |

Modules that use each other have no order to be read in and cannot be changed apart, so the
uses between the children of every module, and of the crate root, form a graph without
circles. Crates need no check, since Cargo keeps them in order. A module and the modules
inside it are read together: uses between a module and its own descendants do not count,
and a use of anything inside a sibling is a use of that sibling.

Every use of an item is a use of the module that owns it, through any re-export or glob
import, and method calls and field accesses count as well as paths. Uses in compilations
with `cfg(test)` are skipped; tests may use anything.

One diagnostic is reported per set of modules that reach each other. It names the members
and shows the shortest circle through the use with the fewest mentions, which is the
cheapest to cut and where the diagnostic points; other circles in the set appear once that
one is cut.

Exception: `// lint: expect(module_cycle)` directly above a use leaves it out of the graph.
It covers the uses on that line, and is not needed unless one of them is part of a circle.
