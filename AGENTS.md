# Overview

Rust framework for server-side applications.

Library crates: `lib/`
Application crates: `app/`
e2e test: `test/`, spec: `@spec/test/spec.md`

# code style

- Keep comments minimal and focused on intent or non-obvious behaviour
- Prefer straightforward code over unnecessary encapsulation and abstraction
- Format Rust code with `cargo +nightly fmt --all`
- Fix all warnings reported by `cargo clippy --workspace --all-targets`

# spec

- update `/spec` to reflect what code and design changed
- for spec doc, only keep key design decisions / behavior / requirements, always goes to code for impl details
