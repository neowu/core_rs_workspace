# `load_config!` / `EnvString` design

Code: [`lib/framework/src/config.rs`](../lib/framework/src/config.rs) · sibling:
[`lib/framework/src/asset.rs`](../lib/framework/src/asset.rs) · consumers:
[`log_processor_rs/src/main.rs`](../app/log_processor_rs/src/main.rs),
[`log_collector/src/main.rs`](../app/log_collector/src/main.rs),
[`demo/src/lib.rs`](../app/demo/src/lib.rs)

Startup config is JSON deserialized into an app-owned struct. Two mechanisms overlay environment
variables on top of that JSON, at different granularities, and they are deliberately separate:

| layer | granularity | supplies | typical source |
|---|---|---|---|
| `load_config!(path, env = "NAME")` | the whole document | config *shape* per deployment revision | Cloud Run env var / revision yaml |
| `EnvString` (`"env:NAME"`) | one string field | *secrets* that must not be in git or the image | Secret Manager → env |

They compose: a document that arrived from `NAME` may still contain `env:` references, and those
resolve normally. The split exists because the two have opposite review properties — structure
should be diffable in git, secrets must never be.

## Resolution order

```rust
#[cfg(debug_assertions)]
load_dev_env(manifest_dir);

let json = if let Some(json) = env_name.and_then(load_from_env) {
    json
} else {
    let config_path = resolve_config_path(path, manifest_dir);
    read_to_string(&config_path).unwrap_or_else(|err| panic!(".."))
};
```

1. `.env` next to `CARGO_MANIFEST_DIR`, **debug builds only** — loaded first, not last (below).
2. the env var named by `env`, when set and not blank.
3. `path` resolved next to the current exe (`exe_path.with_file_name(path)`).
4. `path` joined onto `CARGO_MANIFEST_DIR`, **debug builds only**.

**Env is checked before the filesystem, not as a fallback for a missing file.** If it were a
fallback, a stale `assets/conf.json` baked into the image would silently win whenever the env var
was misspelled or absent — the failure mode would be "quietly running last month's config" instead
of "obviously misconfigured". Precedence must not depend on what happens to exist on disk.

**A blank value counts as unset.** Terraform and Cloud Run revision yaml both produce `NAME=""`
readily; the alternative is a `serde_json` EOF error, which says nothing useful about the cause.

**The manifest branch is `cfg(debug_assertions)`.** Release images copy assets next to the binary —
`COPY --from=builder /usr/src/app/log_processor_rs/assets /opt/app/assets` in
[`Dockerfile`](../app/log_processor_rs/Dockerfile) — so step 3 is the only file path that ever runs
in production. A release binary never reads a source-tree path that does not exist in the image.

## Why a macro

`env!("CARGO_MANIFEST_DIR")` has to expand in the **calling** crate. Expanded inside `framework` it
would yield `lib/framework`, and every app's dev-mode config lookup would point at the framework
crate. That is the entire reason `load_config!` is a macro wrapping `__load_config` rather than a
plain function.

## Call syntax: path first, `env = "NAME"` keyword second

```rust
let config: AppConfig = load_config!("assets/conf.json");
let config: AppConfig = load_config!("assets/conf.json", env = "CONFIG");
```

```rust
macro_rules! load_config {
    ($path:expr) => {{ $crate::config::__load_config(None, $path, env!("CARGO_MANIFEST_DIR")) }};
    ($path:expr, env = $env:expr) => {{ $crate::config::__load_config(Some($env), $path, env!("CARGO_MANIFEST_DIR")) }};
}
```

Both arguments are string literals, so the compiler is the only thing that can catch a transposed
call. The `env` token makes the wrong order unparseable:

```
error: no rules expected `"assets/conf.json"`
  |
  | load_config!(env = "CONFIG", "assets/conf.json")
  |                              ^^^^^^^^^^^^^^^^^^ no rules expected this token in macro call
note: while trying to match `env`
```

Path stays in position 1 in both arms for two reasons: adding an override to an existing call site
is purely additive, and argument order no longer implies precedence — leaving `__load_config` free
to check env first.

### Rejected alternatives — do not re-litigate these

| approach | why not |
|---|---|
| two positional literals, `load_config!("CONFIG", "assets/conf.json")` | transposition compiles and fails at runtime, in the deployed environment — the one place you cannot iterate |
| fixed convention name, always read `CONFIG` | needs no macro change, but binds every app to one name and can pick up an unrelated variable; an explicit name is greppable from the call site |
| `&Path` / `PathBuf` parameter | `Path` is an unsized `OsStr` wrapper that validates nothing — `Path::new("assets/conf.json")` proves nothing the literal doesn't — and it makes the real precondition (below) *easier* to violate |
| newtypes, `load_config!(AssetPath(".."), EnvName("..")) ` | same guarantee as the keyword arm, with ceremony at every call site and two more `pub` types |
| a second macro, `load_config_from_env!` | does not address ordering at all, and splits the docs |

## The path is a relative fragment, not a path

`path` is resolved against **two different bases** (steps 3 and 4). That makes "relative" a
precondition, not a preference. An absolute path breaks both bases in different ways:
`PathBuf::join` silently discards the base, so step 4 would "work" and step 3 would produce
something incoherent. `PathBuf` in the signature would invite exactly that. The precondition is
currently unchecked — see *Known gaps*.

`path` and `manifest_dir` are `&str`, not `&'static str`. The macro only ever feeds them literals,
so the `'static` bound constrained nothing reachable while blocking tests from passing a temp dir.
This also matches `asset::__resolve`.

## `.env` is loaded first, in debug builds only

It used to load lazily, inside the source-folder branch. Hoisting it to the top of `__load_config`
buys two things:

- `env:` references resolve the same way regardless of *which* branch supplied the document;
- `.env` can define the config env var itself, so **the deployed code path is exercisable locally**.
  Otherwise step 2 would only ever run in production.

In release builds the whole function is `cfg`'d out — no cost, and no chance of reading a stray
`.env` from a working directory.

The parser is deliberately minimal: line-based, `#` comments and blank lines skipped, first `=`
splits, both sides trimmed, and a line without `=` panics. No quoting, escapes, `export` prefix or
multi-line values.

`env::set_var` is `unsafe` in edition 2024. It is sound here only because config loading happens on
the startup thread before any other thread exists to read the environment. That is an invariant of
"call this once at startup", enforced by convention rather than by the type system.

## `EnvString`

```rust
let raw = String::deserialize(deserializer)?;
let resolved = if let Some(key) = raw.strip_prefix("env:") {
    env::var(key).map_err(|err| de::Error::custom(format!("failed to load from env, env={key}, err={err}")))?
} else {
    raw
};
```

- **The JSON value stays a plain string.** Not a tagged enum, not `{ "env": "NAME" }`. The document
  shape is identical whether a value is literal or environment-sourced, so a field can move from
  one to the other without touching the struct — which is what makes the same `conf.json` usable in
  dev with inline values and in production with `env:` references.
- **Resolution happens at deserialize time**, so a missing variable is a startup failure naming the
  variable (`failed to load from env, env=SLACK_TOKEN, ..`) rather than an empty credential
  discovered on first use.
- **In-band tagging costs one thing**: a literal that genuinely starts with `env:` cannot be
  expressed. No such value exists in these configs, and the escape hatch — typing the field as
  `String` — is always available.
- `Deref<Target = String>`, `Display` and `From<EnvString> for String` cover use; `Debug` forwards
  to `Display`, which means it prints the **resolved** value. See *Known gaps*.

A field is `EnvString` only if it should accept `env:`. Typing a field `String` while its JSON value
is `"env:FOO"` is not an error anywhere — the app just gets the literal string `env:FOO`.

## Failure model: panic, not `Result`

Every failure here is fatal by construction: the function runs once, before the app exists, and no
caller could do anything with an error but exit. Panicking keeps the signature `-> T`, so call sites
read `let config: AppConfig = load_config!("assets/conf.json");` with no `?` in `main`'s prologue and
no `Result` threading through setup. Messages carry the resolved config path *and* the exe path, so a
bad image layout is diagnosable from container logs alone:

```
config not found, path=/opt/app/assets/conf.json, exe=/opt/app/log_processor_rs
```

## Invariants

- **Called once, at startup, before any other thread runs.** Required for `env::set_var` soundness.
- **Env beats disk**, and a blank env var is indistinguishable from an unset one.
- **Release builds read only next to the exe.** `.env` and the manifest fallback are debug-only.
- **Deployment must place `assets/` beside the binary**, per each app's `Dockerfile`.
- **The config JSON is echoed to stdout** by `console!` before parsing, deliberately — it is the
  only record of what a revision actually started with.

## Tests

`lib/framework/src/config.rs` covers each branch and the two behaviours that are easy to regress:

| test | pins |
|---|---|
| `load_config_from_source_folder` | debug manifest branch |
| `load_config_from_env` | env wins over an existing file on disk |
| `load_config_with_blank_env` | blank is treated as unset, falls through to the file |
| `load_config_with_unset_env` | unset falls through to the file |
| `load_config_with_missing_file` | panics with `config not found` |
| `load_config_from_env_resolves_dev_env` | `.env` is loaded *before* the env var is read — fails if `load_dev_env` moves back into the file branch |

Tests pass a per-test temp dir as `manifest_dir`, which is why the `'static` bound had to go.

## Known gaps

1. **Secrets can reach stdout.** `console!("config:\n{json}")` prints the whole document. With
   `env:` references the secrets stay out of it, but a document supplied entirely through the env
   var with inline credentials lands in the log pipeline. Convention only.
2. **`EnvString`'s `Debug` prints the resolved value**, and every `AppConfig` derives `Debug`. Any
   `{config:?}` — none exists today — would dump credentials.
3. **The relative-path precondition is unchecked.** Since `path` is always a literal, a
   `const { assert!(..) }` in the macro could reject an absolute path at compile time for free.
4. **`asset_path!` duplicates the resolution** with the same precondition and the opposite argument
   order (`__resolve(manifest_dir, path)` vs `resolve_config_path(path, manifest_dir)`). They should
   share one function.
5. **No test for release-build behaviour.** `cargo test` is a debug build, so the two `cfg`'d-out
   branches are never exercised as they ship.
