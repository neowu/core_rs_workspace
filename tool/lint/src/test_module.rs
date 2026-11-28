use syn::Attribute;
use syn::File;
use syn::Ident;
use syn::Item;

use super::diagnostic::Finding;

/// Test functions that are not inside a `#[cfg(test)]` module. Read as written, so a test a macro writes is not seen.
pub(super) fn check(file: &File) -> Vec<Finding> {
    let mut findings = Vec::new();
    visit(&file.items, &mut findings);
    findings
}

fn visit(items: &[Item], findings: &mut Vec<Finding>) {
    for item in items {
        match item {
            Item::Fn(function) if function.attrs.iter().any(is_test) => {
                let name = &function.sig.ident;
                let start = name.span().start();
                let end = name.span().end();
                findings.push(Finding {
                    rule: "tests_outside_test_module",
                    message: format!("test `{name}` is outside a `#[cfg(test)]` module; move it into one"),
                    line: start.line,
                    columns: Some((start.column + 1, end.column + 1)),
                });
            }
            Item::Mod(module) if !cfg_test(&module.attrs) => {
                if let Some((_, items)) = &module.content {
                    visit(items, findings);
                }
            }
            _ => {}
        }
    }
}

/// `#[test]` and what stands for it, like `#[tokio::test]` and `#[integration_test]`.
fn is_test(attr: &Attribute) -> bool {
    attr.path().segments.last().is_some_and(|segment| segment.ident == "test" || segment.ident == "integration_test")
}

pub(super) fn cfg_test(attrs: &[Attribute]) -> bool {
    attrs.iter().any(|attr| attr.path().is_ident("cfg") && attr.parse_args::<Ident>().is_ok_and(|arg| arg == "test"))
}

#[cfg(test)]
mod tests {
    use super::check;

    /// `line:column-column name` of the tests reported.
    fn found(source: &str) -> Vec<String> {
        let file = syn::parse_file(source).unwrap();
        check(&file)
            .into_iter()
            .map(|finding| {
                let (start, end) = finding.columns.unwrap();
                let name = finding.message.split('`').nth(1).unwrap().to_owned();
                format!("{}:{start}-{end} {name}", finding.line)
            })
            .collect()
    }

    #[test]
    fn accepts_tests_in_a_test_module() {
        let source = "fn real() {}\n#[cfg(test)]\nmod tests {\n    #[test]\n    fn works() {}\n    mod inner {\n        #[tokio::test]\n        async fn works() {}\n    }\n}\n";
        assert_eq!(found(source), Vec::<String>::new());
    }

    #[test]
    fn reports_tests_outside() {
        let source = "#[test]\nfn top() {}\nmod plain {\n    #[tokio::test(flavor = \"multi_thread\")]\n    async fn nested() {}\n    #[integration_test]\n    async fn e2e() {}\n}\n";
        assert_eq!(found(source), ["2:4-7 top", "5:14-20 nested", "7:14-17 e2e"]);
    }

    #[test]
    fn reports_a_test_that_is_only_cfg_test_itself() {
        assert_eq!(found("#[cfg(test)]\n#[test]\nfn alone() {}\n"), ["3:4-9 alone"]);
    }
}
