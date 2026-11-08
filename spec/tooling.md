# Workspace lint policy

Workspace Clippy configuration selects individual rules for correctness,
performance, and readable code. Nursery and restriction groups are not enabled
wholesale. Workspace members opt in through `lints.workspace = true`.

Keep rules that catch mistakes or simplify code without imposing unnecessary
abstractions, API bounds, or cosmetic rewrites. A rule with no current diagnostics
remains a candidate for review when it first fires; a clean run alone does not
establish that its suggestions suit the project.

Follow the [code style](code_style.md), consume collections directly when they
are no longer needed, and keep the first rustdoc paragraph a short summary.

The following nursery rules are intentionally omitted:

| Rule | Decision |
| --- | --- |
| `use_self` | Prefer explicit concrete type names for searchability and local readability, as defined in the code style. |
| `redundant_pub_crate` | Keep explicit `pub(crate)` on internal items, including inside private modules. Replacing it with `pub` conflicts with `unreachable_pub` and obscures the intended API boundary. |
| `option_if_let_else` | Allow readable `match` and `if let` branches. Do not require multi-line branches to become `map_or_else` closures. |
| `single_option_map` | Allow helpers to accept and return `Option` when optionality is part of their purpose, such as recursive error sources. |
| `future_not_send` | Allow generic futures whose `Send` implementation depends on their arguments. Enforce `Send` at task boundaries and in trait contracts that require it, without adding bounds solely to satisfy this lint. |
| `significant_drop_tightening` | Use explicit scopes for locks. Do not require redundant drops immediately before scope exit or rewrites that obscure the guard's borrowed data. Keep `significant_drop_in_scrutinee` to catch guards held across branch bodies. |

Validate all workspace targets, including tests, examples, and benchmarks:

```sh
cargo lint
cargo +nightly fmt --all -- --check
```

The repository Cargo configuration also treats build warnings as errors.

`cargo lint` runs repository checks through the `xtask` workspace member, then
Clippy with `--workspace --all-targets -- -D warnings`. Either phase failing makes
the command fail. `cargo lint --custom-only` runs only the repository checks.
CI uses the same command as local development.
Custom rule unit tests live beside their rule in sibling `_test.rs` modules.

All workspace members, including tests, benchmarks, and tooling, must inherit
registry and Git dependencies using `workspace = true`. Local `path` dependencies
may be declared directly in members. This applies to normal, development, build,
and target-specific dependencies. Centralized versions and sources belong in
`[workspace.dependencies]`; member-specific features, optionality, and
default-feature settings remain supported.

Repository checks enforce three dependency rules:

| Rule | Requirement |
| --- | --- |
| `workspace_dependency_required` | Each member dependency inherits an existing root declaration or declares a local path. |
| `unused_workspace_dependency` | Each root dependency declaration is inherited by at least one member. |
| `wildcard_workspace_dependency` | Registry dependencies in the root must not use an unrestricted `*` version requirement. Path and Git dependencies are exempt. |

Inheritance is checked by dependency key, including renamed dependencies. A
direct path dependency does not keep an otherwise unused root declaration.
Optional and platform-specific declarations count as uses even when inactive.
This checks manifest references; the Rust `unused_crate_dependencies` lint
separately checks usage in source code. Root wildcard checking is independent of
Clippy's first-package workspace limitation. Diagnostics identify the manifest,
line, column, and rule; violations produce a nonzero exit status.
