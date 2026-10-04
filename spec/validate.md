# Validate

`#[derive(Validate)]` (`lib/framework_macro/src/validate.rs`) generates `framework::validate::Validator` for a
struct with named fields. Checks are generated code, fail fast, and format the error message only on failure.

## Attributes

| attribute | field type | check |
|---|---|---|
| `#[range(min = a, max = b)]` | numeric, `Copy` | `a <= value <= b` |
| `#[length(min = a, max = b)]` | `String` / `&str`: Unicode scalar values (`chars().count()`); others: `len()` | `a <= length <= b` |
| `#[not_blank]` | `String` / `&str` | rejects empty or all Unicode whitespace (`char::is_whitespace`) |
| `#[validate]` | any `T: Validator` | delegates to `Validator::validate` |

- Bounds are inclusive, at least one of `min` / `max` is required, `min <= max`, values are integer literals
  (negative allowed for `range` only). Constants and other expressions are rejected, not ignored.
- Unknown keys, duplicate keys, a repeated attribute on one field, and arguments on `not_blank` / `validate` are
  compile errors. The rule: a constraint the user wrote either generates a check or fails compilation.
- `Option<T>` field: checks apply to the inner value, `None` passes.
- Types are matched by last path segment (`Option`, `String`, `str`), so qualified paths behave the same as plain
  ones; type aliases are not resolved. `Box<str>` / `Cow<str>` fall to `len()` (bytes).

## Behaviour

- Returns the first error. Order: fields in declaration order; per field `range`, `length`, `not_blank`, `validate`,
  regardless of attribute order; `min` before `max`.
- Errors are `validation_error!` (`VALIDATION_ERROR`, severity warn), message
  `{field} [length ]must not be less|greater than {bound}, value={actual}` or `{field} must not be blank`.
  A nested error carries only the child's field name, no parent path.
- `Validator` is implemented for `Option<T>` and `Vec<T>`, so `#[validate]` emits one fully qualified call
  `framework::validate::Validator::validate(&self.field)` for any nesting (`Vec<Option<Child>>`). The qualified call
  needs no trait import at the derive site and is not shadowed by an inherent `validate` method on the child.
- Generated impl keeps the struct generics and where clause; bounds required by the checks (e.g. `T: Validator` for
  `#[validate] items: Vec<T>`) are declared by the user. Std types in the signature are fully qualified, so a caller
  `type Result<T>` alias does not break it.

## Tests

- `lib/framework_macro`: generated token snapshot and compile errors of invalid attributes.
- `test/validator_test`: runtime behaviour through the real derive.
