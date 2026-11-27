# Design harness: lints for code structure

Research and decisions from 2026-11-26. The goal is a set of mechanical checks that push
code toward a small open surface and a readable module structure, so that people and
agents get the same design feedback from `cargo lint` that they get for style from Clippy.

Rule behavior is specified in `spec/lint.md`; this memo records why the rules exist, what
was considered, and what is left.

## Goal

Minimize the open area. Whatever stays `pub` or `pub(crate)` should be what other code
really depends on. If moving a function lets a field go from `pub` to private, the result
is less to learn and less that can change underneath callers.

Two properties are checked:

- **Visibility is no wider than its uses.** Mechanical: a narrower keyword either compiles
  or it does not.
- **Modules have an order.** Sibling modules that use each other in a circle cannot be read
  or changed apart.

A third, encapsulation that needs code to move, is a judgement and is reported rather than
enforced (see Tier 2).

## Source of the idea: deslop-please

<https://github.com/deterministic-machines/deslop-please> (v0.1.6 when read, MIT). An
opinionated Rust linter after *A Philosophy of Software Design*: deep modules, narrow
interfaces.

| Lint | Scope | Flags |
| --- | --- | --- |
| `module-cycle` | Sibling modules, one crate | Modules that use each other in a circle. |
| `tangle` | Types and free functions | Items that know each other in a circle, across modules or more than 7 of them. |
| `field-leak` | Per struct | Code outside a struct's methods that writes, borrows, indexes or calls into its fields, also through getters and setters. Plain reads pass. |
| `shallow-module` | Crate, or module with children | More than 7 functions used from outside, and at least as many shown as hidden. |
| `wide-directory`, `long-file` | Files | More than 15 entries; more than 2,000 lines. |

How it works, and why it was not adopted as is:

- It reads resolved references from `rust-analyzer scip`, which builds build scripts and
  proc macros. That is what makes `field-leak` and `tangle` possible, and it is a heavy
  dependency for `framework_lint`, which is fast and dependency free.
- Its unit is the module tree inside a crate. Crate dependencies are not checked.
- Thresholds (7, 15, 2,000) are constants. Suppression is a baseline allow file, not inline.
- `shallow-module` counts functions used from outside as cost. A framework crate's purpose
  is a wide API, so it would be noisy there.

Taken from it: `module-cycle`, and the framing of the rest as prompts for a redesign
rather than mechanical fixes. Its `AGENTS.md` advice is worth keeping: fix from first
principles, not by moving code until the lint is quiet.

## What is implemented

Both rules live in `framework_lint` and run in `cargo lint`. Both match by name on raw
tokens, like `unused_pub`: no type resolution, biased against false positives.

### `excess_visibility` (Tier 1)

An item, method or named field is flagged when it is more visible than its mentions need:
`pub` with no mention from another crate, or `pub`/`pub(crate)` with every mention inside
the defining module and its children.

Design decisions:

- **Where a mention is, not whether there is one.** This is `unused_pub` extended from
  "used at all" to "used from where". rustc has no equivalent; `unreachable_pub` only
  covers `pub` the crate does not export.
- **Rust's privacy boundary is the module**, so "can be private" means all mentions are in
  the defining module's subtree.
- **Interfaces are kept consistent.** A name in a signature or field type stays as visible
  as that interface needs to be, solved to a fixed point, so a suggestion never trips
  rustc's `private_interfaces` and one run reports a whole chain.
- **Collisions are cut down with sound rules only**: a field or method is no more visible
  than its type; nothing in a binary needs more than `pub(crate)`; a mention in a package
  that does not depend on the item's package is ignored; `.name(...)` and words in strings
  are not field mentions.
- **Unused items are not its job.** An item nobody mentions is left to `unused_pub` and
  `dead_code`. When narrowing an item makes `dead_code` fire, its only mentions were
  same-named items: it is unused in the workspace, to be removed or marked as external API.
- **External API reuses the existing knobs**: `unused_pub_exempt_packages` and
  `// lint: allow(unused_pub)` mean "`pub` is for callers outside the workspace".

Accuracy, as measured when introduced: every suggestion applied compiled, so no false
positive was observed. False negatives remain and are not measured: same-named mentions
inside the crate or in a dependent crate still count, so common names (`id`, `new`) are
rarely flagged, and tuple fields, enum variants and macro-generated items are not checked.
A false positive is cheap, since the compiler rejects the change at once.

### `module_cycle`

Sibling modules of a crate must not use each other in a circle. A module and its
descendants count as one unit; crates are skipped because Cargo orders them.

Design decisions:

- **Uses are paths from `crate`, `self` or `super`**, read from tokens. Paths from a name
  in scope are only read in `use` declarations, since any such name was imported by a path
  that is already counted.
- **Re-exports are followed** to the module that owns the item. Without this, a use through
  a parent's `pub use child::Item` looks like a use of the parent and hides the real edge.
  Glob re-exports are not followed.
- **Tests are skipped.** Test code may use anything.
- **One diagnostic per tangle**, showing the shortest circle through the use with the
  fewest mentions: the cheapest place to cut. Cutting it may reveal the next circle.
- **Suppression is per use**, not per module: `// lint: allow(module_cycle)` above a path
  removes that edge, which records where a cycle was accepted.

## Not implemented

### Tier 2: almost-encapsulated report

Once Tier 1 is clean, everything still non-private is the real open area. For each such
field or item, list who uses it from outside, fewest users first:
"`RunQueue::items` is `pub(crate)` for 2 sites in `sched::policy`". The top of the list is
the "move one function and this goes private" case.

This should be a report, not a CI error: whether the function belongs in the other module
is a judgement. It is a good prompt for an agent. It can start name-based; common field
names will be blind spots.

### Other candidates from deslop-please

- `tangle` and `field-leak` need resolved references (method calls on fields, getters,
  forwarders). Not worth approximating with tokens.
- `shallow-module` is a poor fit for framework crates.

## Options for exactness

Name matching cannot be exact. The options when that matters:

| Option | Exact? | Toolchain | Cost |
| --- | --- | --- | --- |
| Token matching (current) | No: safe when it speaks, silent on collisions | Stable | Fast, no dependency |
| Compiler as oracle | Yes, for the configurations compiled | Stable | Slow, rewrites source |
| rust-analyzer SCIP | Nearly: one target and feature set per index, inference is not rustc's | Stable, needs rust-analyzer | Index build, plus a model on top of references |
| rustc lint (dylint, `rustc_private`) | Yes | Pinned nightly | Most code; breaks when the pin moves |

Notes:

- **Compiler as oracle.** An on-demand `cargo xtask tighten`: narrow every non-private item
  and field one level, run `cargo check --workspace --all-targets`, revert what the privacy
  errors (E0603, E0616, E0624) name, repeat until it compiles. Exact by construction and
  no new dependency. Its error output lists every outside use of what could not be
  narrowed, which is the Tier 2 data. Too slow and invasive for CI, so it complements the
  lint. Packages in `unused_pub_exempt_packages` are skipped.
- **Nightly is only needed for a rustc lint.** Resolved types and effective visibility are
  exposed through rustc's internal crates, which only nightly offers. Code still builds and
  ships on stable; only the lint step uses the pinned nightly, as `cargo +nightly fmt`
  already does. The internal API has no stability guarantee.
- **SCIP is the route to the structural lints** (`tangle`, `field-leak`), which need the
  full reference graph and not a yes or no on visibility.

## Probe (added 2026-11-27)

The compiler route replaced the token rules; behavior is in `spec/lint.md`. Sections above
describe the token rules as first built and are kept for the reasoning.

- `tool/probe`: a rustc driver on a pinned nightly, outside the workspace. It writes facts
  (definitions and resolved uses) per compilation and holds no rule. Hand-written rather
  than Dylint: nothing here is a rustc lint, and it keeps `cargo lint` free of installed
  tools.
- `tool/lint` (was `lib/framework_lint`): `unused_pub`, `excess_visibility` and
  `module_cycle` over the facts, in `cargo lint` by default. The token versions are removed.

First run on this workspace: 52 `excess_visibility` and 2 `unused_pub` findings that the
token rules missed, no module cycle. Applying the suggestions compiled with warnings
denied. A cold run takes about 10 s, a cached one under 1 s.

Exceptions are `// lint: expect(<rule>)` comments, reported when they excuse nothing; the
per-package exemption is removed. An attribute form needs `register_tool`, which is
unstable.

Cost accepted: `cargo lint` needs the pinned nightly with `rustc-dev`, locally and in CI,
and the probe must be fixed when the pin moves.

Not done:

- The probe records where an item is used, not how. Read, write and borrow kinds (from
  MIR) are the next facts to add for the Tier 2 report.
- Tuple fields, enum variants and trait items are still not checked.

## Recommendation

1. Build the Tier 2 report on the probe's facts, adding use kinds.
2. Consider SCIP only if the probe proves too costly to keep up with nightly.
