# Workspace lint

```sh
cargo lint
cargo +nightly fmt --all -- --check
```

The repository Cargo configuration also treats build warnings as errors.

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
manifest are attributed to the `xtask` package, since a virtual manifest has no
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
| `file_too_long` | A Rust source file has at most 500 lines, not counting top-level `#[cfg(test)]` items. |

Files are parsed with `syn` so test items are excluded by their attributes rather
than by layout. Only `.rs` files under each member's `src` are checked; build
scripts, `tests/`, benches, and examples are exempt. A test module in a separate
file (`#[cfg(test)] mod x;`) is not recognized as test code, which is why unit tests
stay inline. Files that do not parse are skipped and left to the compiler. The
diagnostic points at the first line past the limit.
