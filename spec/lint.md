# Workspace lint

```sh
cargo lint
cargo +nightly fmt --all -- --check
```

The repository Cargo configuration also treats build warnings as errors.

## Packaging

The checks live in the `framework_lint` library so other workspaces can run them the
same way this one does. Each workspace keeps a thin `xtask` binary member that calls
`framework_lint::run(&LintConfig { .. })`, plus the Cargo alias
`lint = "run --quiet -p xtask -- lint"`. Customization is passed in code through
`LintConfig` (`max_file_lines`, default 500; `unused_pub_exempt_packages`, default
none), not a config file. Other
workspaces depend on it via git, like the other framework crates.

The workspace is located at runtime: the library reads `CARGO_MANIFEST_DIR` and
`CARGO`, which `cargo run` sets for the wrapper binary, so it checks the workspace
that contains the wrapper rather than wherever the library was compiled from. It
must therefore be started through `cargo run`.

`cargo lint` runs repository checks through the `xtask` workspace member, then
Clippy with `--workspace --all-targets -- -D warnings`. Either phase failing makes
the command fail. `cargo lint --custom-only` runs only the repository checks.
CI uses the same command as local development.
Unit tests live in the same file as the code they test, in a `#[cfg(test)] mod tests`.

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
(such as `framework_lint`, used only by `xtask`) and members always inherit them.

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

## Unused pub items

| Rule | Requirement |
| --- | --- |
| `unused_pub` | A `pub` function, inherent method or associated const, struct, enum, union, trait, type alias, const, or static under a member's `src` is mentioned somewhere else in the workspace. |

rustc's `dead_code` covers everything not exported from a crate (`pub(crate)`, private
items, and `pub` items in binaries), and `unreachable_pub` forces unexported items to be
`pub(crate)`. This rule covers the rest: items a library exports that no one uses.
The workspace is treated as the universe, so a library's public API must be exercised by
at least one other crate, test, benchmark, or unit test.

Matching is by name on raw tokens, not resolved references, which keeps the check fast
and dependency free, and biased against false positives: any mention of the name in any
`.rs` file of any member counts as a use, including inside macro invocations, `quote!`
bodies, and string literals (e.g. `#[serde(deserialize_with = "path")]`). Doc comments,
other definitions of the same name (the name after `fn`, `struct`, `const`, ...), and
the self type of an `impl` block (`impl Type`, `impl Trait for Type`) do not count, so a
type that only has impls is still unused. The cost is false negatives: an unused item
sharing its name with any used item is not reported, and neither is one mentioned only
by a `use`/`pub use` or by its own impl body (`Type {}` rather than `Self {}`).

Items named by `macro_rules!` arguments are checked without expansion. A macro whose
body holds `pub <keyword> $name` (e.g. `pub struct $name;`) defines an item per
identifier bound to `$name`, so each invocation's arguments are matched against the
macro's patterns and the bound identifiers become candidates, reported at the invocation.
That occurrence is the definition, not a mention. Macros are identified by name only,
across the workspace; matching approximates rustc (fragments parsed with `syn`, greedy
repetitions), and an invocation no arm matches is skipped.

Not checked: `pub(crate)` and private items, fields and enum variants, trait methods and
trait impls, `#[cfg(test)]` items, proc macro entry points, modules, and items generated
any other way: with a name fixed in a macro body, through a macro that calls another
macro, or by proc macros and derives.

Exceptions, for API meant for callers outside the workspace:

- per item, a `// lint: allow(unused_pub)` comment (a reason may follow) in the
  comment block directly above the item, or between its doc comments, attributes, and
  name. For an item named by a macro argument, the comment goes directly above that
  name in the argument list. A comment is used because stable Rust rejects attributes and lint names of
  unknown tools.
- per package, `LintConfig::unused_pub_exempt_packages`, for libraries consumed only by
  other workspaces. Exempt packages still count as users of other packages.
