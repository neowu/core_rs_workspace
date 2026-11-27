#![allow(clippy::tests_outside_test_module)]

use std::env;
use std::path::Path;
use std::process::Command;

use serde_json::Value;

/// Runs `cargo lint --custom-only` on the workspace in assets/ and compares `rule file:line:column message`, sorted.
#[test]
fn reports_what_the_compiler_resolved() {
    let assets = Path::new(env!("CARGO_MANIFEST_DIR")).join("assets");
    let output = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .args(["run", "--quiet", "-p", "xtask", "--", "lint", "--custom-only", "--message-format=json"])
        .env("CARGO_TARGET_DIR", Path::new(env!("CARGO_TARGET_TMPDIR")).join("lint_test"))
        .current_dir(assets)
        .output()
        .unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains(r#""reason":"build-finished""#), "{}", String::from_utf8_lossy(&output.stderr));
    let mut diagnostics: Vec<String> = stdout
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .filter(|message| message["reason"] == "compiler-message")
        .map(|message| {
            let message = &message["message"];
            let span = &message["spans"][0];
            let text = message["message"].as_str().unwrap();
            let text = text.split(", or mark").next().unwrap();
            format!(
                "{} {}:{}:{} {text}",
                message["code"]["code"].as_str().unwrap(),
                span["file_name"].as_str().unwrap(),
                span["line_start"],
                span["column_start"]
            )
        })
        .collect();
    diagnostics.sort();

    assert_eq!(
        diagnostics,
        [
            "excess_visibility visibility/src/a.rs:11:8 `pub` function `four` is not used outside its crate; change it to `pub(crate)`",
            "excess_visibility visibility/src/a.rs:15:12 `pub` struct `Shown` is not used outside its crate; change it to `pub(crate)`",
            "excess_visibility visibility/src/a.rs:2:8 `pub` function `one` is not used outside its module; remove `pub`",
            "excess_visibility visibility/src/a.rs:8:15 `pub(super)` function `three` is not used outside its module; remove `pub(super)`",
            "module_cycle cycle/src/pair/b.rs:2:15 modules `a`, `b` in `crate::pair` use each other in a circle: `b` uses `a` (`item`, 1 mention(s), cycle/src/pair/b.rs:2), `a` uses `b` (`item`, 2 mention(s), cycle/src/pair/a.rs:2); cut one of the uses",
            "module_cycle cycle/src/triangle/a.rs:2:15 modules `a`, `b`, `c` in `crate::triangle` use each other in a circle: `a` uses `b` (`item`, 1 mention(s), cycle/src/triangle/a.rs:2), `b` uses `a` (`item`, 2 mention(s), cycle/src/triangle/b.rs:4); cut one of the uses",
            "unfulfilled_lint_expectations cycle/src/unneeded/a.rs:3:15 `// lint: expect(module_cycle)` is not needed, the use is not part of a circle; remove the comment",
            "unfulfilled_lint_expectations visibility/src/a.rs:29:8 `// lint: expect(unused_pub)` on function `eight` is not needed; remove the comment",
            "unfulfilled_lint_expectations visibility/src/a.rs:32:8 `// lint: expect(excess_visibility)` on function `nine` is not needed; remove the comment",
            "unused_pub visibility/src/a.rs:23:8 pub function `six` is not used in the workspace; remove it",
        ]
    );
}
