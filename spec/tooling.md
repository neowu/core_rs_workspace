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
cargo clippy --workspace --all-targets -- -D warnings
cargo +nightly fmt --all -- --check
```

The repository Cargo configuration also treats build warnings as errors.
