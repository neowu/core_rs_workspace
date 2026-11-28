# Overview

Rust framework for server-side applications.

Library crates: `lib/`
Application crates: `app/`
Tooling crates: `tool/` (`tool/lint` runs `cargo lint`; `tool/probe` is its nightly rustc driver, outside the workspace)
e2e test: `test/`, spec: `@spec/test/spec.md`

# code style

- Keep comments minimal and focused on intent or non-obvious behaviour
- Prefer straightforward code over unnecessary encapsulation and abstraction
- Format Rust code with `cargo +nightly fmt --all`
- Leave no warning from `cargo lint`: fix each, or where the rule does not fit, excuse it with `#[expect(.., reason = "..")]` or the marker comment it names, and the reason

# spec

- update `/spec` to reflect what code and design changed
- for spec doc, only keep key design decisions / behavior / requirements, always goes to code for impl details
