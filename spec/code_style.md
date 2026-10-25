# Code style

- Keep comments minimal and focused on intent or non-obvious behaviour.
- Prefer straightforward code over unnecessary encapsulation and abstraction.
- Format Rust code with `cargo +nightly fmt --all`.
- Use explicit type names for concrete types, including signatures, constructors,
  struct fields and patterns, enum variants, and associated function or constant
  paths. This keeps uses searchable by type name and readable without the
  surrounding implementation. Include generic arguments where required.
- Keep `Self` in trait definitions and provided trait methods, where it denotes
  the implementing type, and in associated type projections such as
  `Self::Target` and `Self::Output`. Use explicit names inside concrete trait
  implementations wherever these exceptions do not apply. Generated code follows
  the same convention.
