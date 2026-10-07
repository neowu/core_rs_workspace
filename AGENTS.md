# Overview

Rust framework for server-side applications.

Library crates: `lib/`
Application crates: `app/`
e2e test: `test/`, spec: `@spec/test/spec.md`

# code style

- only keep minimal comments
- use `cargo +nightly fmt` to format code
- no over encapsulation and abstraction, make code easier to understand and review

# spec

- update `/spec` to reflect what code and design changed
- for spec doc, only keep key design decisions / behaviour / requirements, always goes to code for impl details
