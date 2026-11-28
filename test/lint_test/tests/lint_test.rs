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
            "excess_visibility visibility/src/parts.rs:2:9 `pub` module `inner` is not used outside its module; remove `pub`",
            "excess_visibility visibility/src/parts/inner.rs:12:26 `pub` field `1` is not used outside its module; remove `pub`",
            "excess_visibility visibility/src/parts/inner.rs:28:12 `pub` struct `Local` is not used outside its crate; change it to `pub(crate)`, also in its `pub use` at visibility/src/parts.rs:7",
            "field_write app/src/main.rs:45:13 `add` changes `count`, `total` of `Counter` in 2 places (also line 46); move that into a method of `Counter`",
            "field_write app/src/main.rs:51:13 `append` changes `name` of `Counter` in 2 places (also line 52); move that into a method of `Counter`",
            "misplaced_item cycle/src/marked/a.rs:1:15 function `item` is used only in `crate::marked::b` and needs nothing private to `crate::marked::a`; move it there and it can be private",
            "misplaced_item cycle/src/marked/b.rs:1:15 function `item` is used only in `crate::marked::a` and needs nothing private to `crate::marked::b`; move it there and it can be private",
            "misplaced_item cycle/src/nested/x.rs:2:15 function `item` is used only in `crate::nested::y` and needs nothing private to `crate::nested::x`; move it there and it can be private",
            "misplaced_item cycle/src/pair/a.rs:1:15 function `item` is used only in `crate::pair::b` and needs nothing private to `crate::pair::a`; move it there and it can be private",
            "misplaced_item cycle/src/pair/b.rs:1:15 function `item` is used only in `crate::pair::a` and needs nothing private to `crate::pair::b`; move it there and it can be private",
            "misplaced_item cycle/src/tested/b.rs:1:15 function `item` is used only in `crate::tested::a` and needs nothing private to `crate::tested::b`; move it there and it can be private",
            "misplaced_item cycle/src/unneeded/b.rs:1:15 function `item` is used only in `crate::unneeded::a` and needs nothing private to `crate::unneeded::b`; move it there and it can be private",
            "misplaced_item visibility/src/a.rs:11:8 function `four` is used only in `crate::b` and needs nothing private to `crate::a`; move it there and it can be private",
            "misplaced_item visibility/src/a.rs:35:8 function `ten` is used only in `crate::b` and needs nothing private to `crate::a`; move it there and it can be private",
            "misplaced_item visibility/src/place/helper.rs:2:15 function `lone` is used only in `crate::place::user` and needs nothing private to `crate::place::helper`; move it there and it can be private",
            "module_cycle cycle/src/pair/b.rs:2:15 modules `a`, `b` in `crate::pair` use each other in a circle: `b` uses `a` (`item`, 1 mention(s), cycle/src/pair/b.rs:2), `a` uses `b` (`item`, 2 mention(s), cycle/src/pair/a.rs:2); cut one of the uses",
            "module_cycle cycle/src/triangle/a.rs:2:15 modules `a`, `b`, `c` in `crate::triangle` use each other in a circle: `a` uses `b` (`item`, 1 mention(s), cycle/src/triangle/a.rs:2), `b` uses `a` (`item`, 2 mention(s), cycle/src/triangle/b.rs:4); cut one of the uses",
            "unfulfilled_lint_expectations app/src/main.rs:82:12 `// lint: expect(wildcard_match)` is not directly above a catch-all arm of a match on an enum of the workspace, so it does nothing; remove it",
            "unfulfilled_lint_expectations cycle/src/unneeded/a.rs:3:15 `// lint: expect(module_cycle)` is not needed, the use is not part of a circle; remove the comment",
            "unfulfilled_lint_expectations visibility/src/a.rs:29:8 `// lint: expect(unused_pub)` on function `eight` is not needed; remove the comment",
            "unfulfilled_lint_expectations visibility/src/a.rs:32:8 `// lint: expect(excess_visibility)` on function `nine` is not needed; remove the comment",
            "unfulfilled_lint_expectations visibility/src/parts/inner.rs:8:5 `// lint: expect(unused_pub)` on variant `Named` is not needed; remove the comment",
            "unfulfilled_lint_expectations visibility/src/place/helper.rs:24:15 `// lint: expect(misplaced_item)` on function `excused` is not needed; remove the comment",
            "unfulfilled_lint_expectations visibility/src/state.rs:26:4 `// lint: enforce(field_write)` is not directly above a struct other modules can use, so it does nothing; remove it",
            "unfulfilled_lint_expectations visibility/src/state.rs:33:4 `// lint: expect(excess_visibility)` is not directly above an item or field the rule checks, so it does nothing; remove it",
            "unknown_lints visibility/src/state.rs:31:4 `// lint: allow(unused_pub)` names no rule; there are `expect(unused_pub)`, `expect(excess_visibility)`, `expect(misplaced_item)`, `expect(module_cycle)`, `enforce(field_write)`, `expect(wildcard_match)`",
            "unknown_lints visibility/src/state.rs:32:4 `// lint: expect(unused_code)` names no rule; there are `expect(unused_pub)`, `expect(excess_visibility)`, `expect(misplaced_item)`, `expect(module_cycle)`, `enforce(field_write)`, `expect(wildcard_match)`",
            "unused_pub support/src/lib.rs:4:8 pub function `spare` is not used in the workspace; remove it",
            "unused_pub visibility/src/a.rs:23:8 pub function `six` is not used in the workspace; remove it",
            "unused_pub visibility/src/a.rs:26:8 pub function `seven` is only used by tests; remove it",
            "unused_pub visibility/src/a.rs:41:8 pub function `twelve` is not used in the workspace; remove it",
            "unused_pub visibility/src/parts/inner.rs:24:8 trait method `skip` of pub trait `Source` is not used in the workspace; remove it",
            "unused_pub visibility/src/parts/inner.rs:4:5 variant `Unused` of pub enum `Kind` is not used in the workspace; remove it",
            "wildcard_match app/src/main.rs:59:9 `_` takes every variant added to enum `Kind` later; name the variants",
            "wildcard_match app/src/main.rs:67:9 `rest` takes every variant added to enum `Kind` later; name the variants",
        ]
    );
}
