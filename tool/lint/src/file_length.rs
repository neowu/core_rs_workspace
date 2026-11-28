use std::ops::RangeInclusive;

use proc_macro2::Span;
use syn::File;
use syn::Item;
use syn::spanned::Spanned as _;

use super::diagnostic::Finding;
use super::test_module::cfg_test;

pub(super) fn check(source: &str, file: &File, max_lines: usize) -> Option<Finding> {
    let (line, count) = overflow(source, file, max_lines)?;
    Some(Finding {
        rule: "file_too_long",
        message: format!("file has {count} lines excluding `#[cfg(test)]` items, more than the limit of {max_lines}"),
        line,
        columns: None,
    })
}

/// The first line past the limit and the total line count, both ignoring top-level `#[cfg(test)]` items.
fn overflow(source: &str, file: &File, max_lines: usize) -> Option<(usize, usize)> {
    // leaving test items out only lowers the count
    if source.lines().count() <= max_lines {
        return None;
    }
    let test_lines: Vec<RangeInclusive<usize>> =
        file.items.iter().filter(|item| is_test(item)).map(|item| lines(item.span())).collect();
    let code_lines: Vec<usize> =
        (1..=source.lines().count()).filter(|line| !test_lines.iter().any(|range| range.contains(line))).collect();
    code_lines.get(max_lines).map(|line| (*line, code_lines.len()))
}

fn lines(span: Span) -> RangeInclusive<usize> {
    span.start().line..=span.end().line
}

fn is_test(item: &Item) -> bool {
    let attrs = match item {
        Item::Const(item) => &item.attrs,
        Item::Enum(item) => &item.attrs,
        Item::ExternCrate(item) => &item.attrs,
        Item::Fn(item) => &item.attrs,
        Item::ForeignMod(item) => &item.attrs,
        Item::Impl(item) => &item.attrs,
        Item::Macro(item) => &item.attrs,
        Item::Mod(item) => &item.attrs,
        Item::Static(item) => &item.attrs,
        Item::Struct(item) => &item.attrs,
        Item::Trait(item) => &item.attrs,
        Item::TraitAlias(item) => &item.attrs,
        Item::Type(item) => &item.attrs,
        Item::Union(item) => &item.attrs,
        Item::Use(item) => &item.attrs,
        Item::Verbatim(_) | _ => return false,
    };
    cfg_test(attrs)
}

#[cfg(test)]
mod tests {
    const MAX_LINES: usize = 20;

    fn overflow(source: &str, max_lines: usize) -> Option<(usize, usize)> {
        super::overflow(source, &syn::parse_file(source).unwrap(), max_lines)
    }

    fn code(lines: usize) -> String {
        "const C: u8 = 0;\n".repeat(lines)
    }

    #[test]
    fn accepts_files_at_the_limit() {
        assert_eq!(overflow(&code(MAX_LINES), MAX_LINES), None);
    }

    #[test]
    fn reports_first_line_past_the_limit() {
        assert_eq!(overflow(&code(MAX_LINES + 2), MAX_LINES), Some((MAX_LINES + 1, MAX_LINES + 2)));
    }

    #[test]
    fn excludes_cfg_test_items() {
        let tests = "#[cfg(test)]\nmod tests {\n    #[test]\n    fn it_works() {}\n}\n";
        let helper = "/// helper\n#[cfg(test)]\nfn helper() {}\n";
        assert_eq!(overflow(&format!("{helper}{}{tests}", code(MAX_LINES)), MAX_LINES), None);
        assert_eq!(
            overflow(&format!("{helper}{}{tests}", code(MAX_LINES + 1)), MAX_LINES),
            Some((MAX_LINES + 4, MAX_LINES + 1)),
            "line numbers are in the original file"
        );
    }

    #[test]
    fn counts_other_cfg_items() {
        let source = format!("#[cfg(not(test))]\nfn real() {{}}\n{}", code(MAX_LINES - 1));
        assert_eq!(overflow(&source, MAX_LINES), Some((MAX_LINES + 1, MAX_LINES + 1)));
    }
}
