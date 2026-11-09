## Overview

As AI agents take on more software development, Rust's type system and lint rules, combined with a structured framework, provide a strong foundation for reliable, agent-driven development.

This framework is designed for building server-side applications with strong observability, straightforward troubleshooting, and minimal runtime overhead.

Even if you do not adopt the framework directly, you can use its implementation as a reference for AI agents building applications with these goals.

[![Build](https://github.com/neowu/core_rs_workspace/actions/workflows/build.yml/badge.svg)](https://github.com/neowu/core_rs_workspace/actions/workflows/build.yml)
[![CodeQL](https://github.com/neowu/core_rs_workspace/actions/workflows/codeql.yml/badge.svg)](https://github.com/neowu/core_rs_workspace/actions/workflows/codeql.yml)

## Environment Setup

to compile framework_kafka

On macOS, install `librdkafka` and `pkgconf` using Homebrew:

```sh
brew install pkgconf librdkafka
```

`openssl@3` (a librdkafka dependency) is keg-only, so `.cargo/config.toml` sets `PKG_CONFIG_PATH` to `/opt/homebrew/opt/openssl@3/lib/pkgconfig`.

On Linux, install `cmake`, `libcurl4-openssl-dev`

## Lint

`framework_lint` runs workspace checks (dependency layout, file length) followed by `cargo clippy --workspace --all-targets -- -D warnings`. Rules are described in [spec/lint.md](spec/lint.md).

To use it in another workspace, add a thin `xtask` member that calls the library.

`Cargo.toml` (workspace root):

```toml
[workspace]
members = [..., "xtask"]
```

`xtask/Cargo.toml`, declaring the dependency here since only `xtask` uses it (a root declaration would fail `unshared_workspace_dependency`):

```toml
[package]
name = "xtask"
edition = "2024"
publish = false

[dependencies]
framework_lint = { git = "https://github.com/neowu/core_rs_workspace.git" }
```

`xtask/src/main.rs`, where customization is passed in code through `LintConfig`:

```rust
use std::error::Error;
use std::process::ExitCode;

use framework_lint::LintConfig;

fn main() -> Result<ExitCode, Box<dyn Error>> {
    framework_lint::run(&LintConfig { max_file_lines: 500 })
}
```

`.cargo/config.toml`:

```toml
[alias]
lint = "run --quiet -p xtask -- lint"
```

Then run:

```sh
cargo lint                         # workspace checks + clippy
cargo lint --custom-only           # workspace checks only
cargo lint --message-format=json   # clippy-compatible JSON for IDEs
```

The lint must be started through `cargo run` (the alias does this), since it locates the workspace from the `xtask` member's runtime environment.
