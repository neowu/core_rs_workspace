use std::collections::HashMap;

use proc_macro2::Delimiter;
use proc_macro2::Group;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use proc_macro2::TokenTree;
use syn::ext::IdentExt as _;
use syn::parse::ParseStream;
use syn::parse::Parser as _;
use syn::parse::discouraged::Speculative as _;

use super::DEFINITIONS;

/// `macro_rules!` macros, by name, that define `pub` items named by their arguments.
pub(super) type Macros = HashMap<String, Vec<Arm>>;

pub(super) struct Arm {
    matcher: Vec<Pattern>,
    /// Metavariables naming a `pub` item in the body, with the item kind.
    defines: HashMap<String, &'static str>,
}

enum Pattern {
    Token(String),
    Group(Delimiter, Vec<Self>),
    Variable { name: String, fragment: String },
    Repeat { patterns: Vec<Self>, separator: Option<String>, operator: char },
}

pub(super) fn collect(tokens: TokenStream, macros: &mut Macros) {
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    for (index, token) in tokens.iter().enumerate() {
        let TokenTree::Group(group) = token else { continue };
        if let [.., TokenTree::Ident(keyword), TokenTree::Punct(_), TokenTree::Ident(name)] = &tokens[..index]
            && keyword == "macro_rules"
        {
            let arms = arms(group.stream());
            if arms.iter().any(|arm| !arm.defines.is_empty()) {
                macros.entry(name.to_string()).or_default().extend(arms);
            }
        } else {
            collect(group.stream(), macros);
        }
    }
}

/// Matches the arguments of a `name!` invocation against the macro's arms, in order as rustc does, and returns
/// the identifiers that become `pub` items, with the item kind. Nothing is expanded.
pub(super) fn bind(macros: &Macros, name: &str, tokens: &TokenStream) -> Vec<(&'static str, Ident)> {
    for arm in macros.get(name).into_iter().flatten() {
        let mut bound = Vec::new();
        let matched = (|input: ParseStream<'_>| matches(&arm.matcher, input, &mut bound)).parse2(tokens.clone());
        if matched.is_ok() {
            return bound.into_iter().filter_map(|(name, ident)| Some((*arm.defines.get(&name)?, ident))).collect();
        }
    }
    Vec::new()
}

/// The body is `(matcher) => { transcriber };` repeated.
fn arms(body: TokenStream) -> Vec<Arm> {
    let groups: Vec<Group> = body
        .into_iter()
        .filter_map(|token| match token {
            TokenTree::Group(group) => Some(group),
            _ => None,
        })
        .collect();
    let (arms, _) = groups.as_chunks::<2>();
    arms.iter()
        .map(|[matcher, transcriber]| {
            let mut defines = HashMap::new();
            collect_defines(transcriber.stream(), &mut defines);
            Arm { matcher: patterns(matcher.stream()), defines }
        })
        .collect()
}

/// Finds `pub <keyword> $name` in a transcriber; `pub(crate)` and `$vis` items are not exported by the macro.
fn collect_defines(tokens: TokenStream, defines: &mut HashMap<String, &'static str>) {
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    let mut public = false;
    for (index, token) in tokens.iter().enumerate() {
        match token {
            TokenTree::Ident(ident) if ident == "pub" => {
                public = !matches!(tokens.get(index + 1), Some(TokenTree::Group(_)));
            }
            // modifiers such as `const` and `async` sit between `pub` and the keyword
            TokenTree::Ident(ident) => {
                if public
                    && let Some(keyword) = DEFINITIONS.iter().find(|keyword| ident == **keyword)
                    && let Some([TokenTree::Punct(dollar), TokenTree::Ident(name)]) = tokens.get(index + 1..index + 3)
                    && dollar.as_char() == '$'
                {
                    defines.insert(name.to_string(), if *keyword == "fn" { "function" } else { keyword });
                }
            }
            TokenTree::Group(group) => {
                collect_defines(group.stream(), defines);
                public = false;
            }
            TokenTree::Punct(_) | TokenTree::Literal(_) => public = false,
        }
    }
}

fn patterns(tokens: TokenStream) -> Vec<Pattern> {
    let mut patterns = Vec::new();
    let mut tokens = tokens.into_iter();
    while let Some(token) = tokens.next() {
        let pattern = match token {
            TokenTree::Punct(punct) if punct.as_char() == '$' => match tokens.next() {
                // `$name:fragment`
                Some(TokenTree::Ident(name)) => {
                    tokens.next();
                    let fragment = tokens.next().map_or_default(|token| token.to_string());
                    Pattern::Variable { name: name.to_string(), fragment }
                }
                // `$(...) separator operator`
                Some(TokenTree::Group(group)) => {
                    let mut separator = None;
                    let mut operator = '*';
                    for token in tokens.by_ref() {
                        if let TokenTree::Punct(punct) = &token
                            && matches!(punct.as_char(), '*' | '+' | '?')
                        {
                            operator = punct.as_char();
                            break;
                        }
                        separator = Some(token.to_string());
                    }
                    Pattern::Repeat { patterns: self::patterns(group.stream()), separator, operator }
                }
                _ => continue,
            },
            TokenTree::Group(group) => Pattern::Group(group.delimiter(), self::patterns(group.stream())),
            token => Pattern::Token(token.to_string()),
        };
        patterns.push(pattern);
    }
    patterns
}

/// Fragments are parsed with `syn`, and repetitions are greedy without backtracking, which approximates rustc
/// closely enough to tell arms apart. An unsupported fragment fails the arm.
fn matches(patterns: &[Pattern], input: ParseStream<'_>, bound: &mut Vec<(String, Ident)>) -> syn::Result<()> {
    for pattern in patterns {
        match pattern {
            Pattern::Token(expected) => token(input, expected)?,
            Pattern::Group(delimiter, patterns) => match input.parse()? {
                TokenTree::Group(group) if group.delimiter() == *delimiter => {
                    (|input: ParseStream<'_>| matches(patterns, input, bound)).parse2(group.stream())?;
                }
                _ => return Err(input.error("expected group")),
            },
            Pattern::Variable { name, fragment } => match fragment.as_str() {
                "ident" => bound.push((name.clone(), input.call(Ident::parse_any)?)),
                "path" => drop(input.parse::<syn::Path>()?),
                "meta" => drop(input.parse::<syn::Meta>()?),
                "ty" => drop(input.parse::<syn::Type>()?),
                "expr" | "expr_2021" => drop(input.parse::<syn::Expr>()?),
                "tt" => drop(input.parse::<TokenTree>()?),
                "literal" => drop(input.parse::<syn::Lit>()?),
                "lifetime" => drop(input.parse::<syn::Lifetime>()?),
                "vis" => drop(input.parse::<syn::Visibility>()?),
                "block" => drop(input.parse::<syn::Block>()?),
                "item" => drop(input.parse::<syn::Item>()?),
                "stmt" => drop(input.parse::<syn::Stmt>()?),
                "pat" | "pat_param" => drop(syn::Pat::parse_multi_with_leading_vert(input)?),
                _ => return Err(input.error("unsupported fragment")),
            },
            Pattern::Repeat { patterns, separator, operator } => {
                let mut count = 0;
                loop {
                    let fork = input.fork();
                    let mut attempt = Vec::new();
                    let separated =
                        count == 0 || separator.as_ref().is_none_or(|separator| token(&fork, separator).is_ok());
                    // an iteration that consumes nothing would repeat forever
                    if !separated || matches(patterns, &fork, &mut attempt).is_err() || fork.cursor() == input.cursor()
                    {
                        break;
                    }
                    input.advance_to(&fork);
                    bound.extend(attempt);
                    count += 1;
                    if *operator == '?' {
                        break;
                    }
                }
                if *operator == '+' && count == 0 {
                    return Err(input.error("expected repetition"));
                }
            }
        }
    }
    Ok(())
}

fn token(input: ParseStream<'_>, expected: &str) -> syn::Result<()> {
    if input.parse::<TokenTree>()?.to_string() == expected { Ok(()) } else { Err(input.error("unexpected token")) }
}
