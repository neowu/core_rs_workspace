use std::collections::HashMap;
use std::collections::HashSet;

use proc_macro2::Delimiter;
use proc_macro2::Spacing;
use proc_macro2::TokenStream;
use proc_macro2::TokenTree;

use crate::unused_pub::DEFINITIONS;
use crate::unused_pub::is_doc;
use crate::unused_pub::is_punct;
use crate::unused_pub::position;

/// Any mention of a name.
pub(super) const ANY: u8 = 1;
/// After `.` or `::`, as methods and associated consts are named.
pub(super) const MEMBER: u8 = 2;
/// After `.` without a call, or in a struct literal or pattern, as fields are named.
pub(super) const FIELD: u8 = 4;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct Place {
    pub(super) file: usize,
    /// In a `macro_rules!` body, which expands wherever the macro is invoked.
    pub(super) exported: bool,
}

#[derive(Default)]
pub(super) struct Mentions {
    names: HashMap<String, HashMap<Place, u8>>,
    /// By type name, where `Type { ..base }` is written, which needs every field of the type.
    updates: HashMap<String, HashSet<Place>>,
}

impl Mentions {
    pub(super) fn places(&self, name: &str, shape: u8) -> impl Iterator<Item = Place> {
        let places = self.names.get(name).into_iter().flatten();
        places.filter(move |(_, shapes)| **shapes & shape != 0).map(|(place, _)| *place)
    }

    pub(super) fn updates(&self, owner: &str) -> impl Iterator<Item = Place> {
        self.updates.get(owner).into_iter().flatten().copied()
    }

    fn add(&mut self, name: String, place: Place, shapes: u8) {
        *self.names.entry(name).or_default().entry(place).or_default() |= shapes;
    }
}

/// Works on raw tokens to see inside macro invocations and `quote!` bodies. Doc comments, the name after a
/// defining keyword and the field names at `fields` are not mentions; words in string literals are, except of fields,
/// for paths such as `#[serde(deserialize_with = "path")]`.
pub(super) fn collect(tokens: TokenStream, file: usize, fields: &HashSet<(usize, usize)>, mentions: &mut Mentions) {
    let tokens: Vec<TokenTree> = tokens.into_iter().collect();
    Scanner { file, fields, mentions }.scan(&tokens, false, false);
}

struct Scanner<'a> {
    file: usize,
    fields: &'a HashSet<(usize, usize)>,
    mentions: &'a mut Mentions,
}

impl Scanner<'_> {
    fn scan(&mut self, tokens: &[TokenTree], brace: bool, exported: bool) {
        let place = Place { file: self.file, exported };
        let mut defines = false;
        let mut macro_rules = false;
        for (index, token) in tokens.iter().enumerate() {
            let previous = index.checked_sub(1).and_then(|index| tokens.get(index));
            match token {
                TokenTree::Ident(ident) => {
                    let name = ident.to_string();
                    let keyword = match name.as_str() {
                        // `*const T` and `'static` are not definitions
                        "const" => !previous.is_some_and(|token| is_punct(token, '*')),
                        "static" => !previous.is_some_and(|token| is_punct(token, '\'')),
                        "mut" => defines,
                        name => DEFINITIONS.contains(&name),
                    };
                    macro_rules |= name == "macro_rules";
                    if !defines && !self.fields.contains(&position(ident)) {
                        let dot = previous.is_some_and(|token| is_punct(token, '.'));
                        let path = index >= 2 && tokens[index - 2..index].iter().all(|token| is_punct(token, ':'));
                        let mut shapes = ANY;
                        if dot || path {
                            shapes |= MEMBER;
                        }
                        // `.name(...)` calls a method
                        let call = matches!(tokens.get(index + 1), Some(TokenTree::Group(group)) if group.delimiter() == Delimiter::Parenthesis);
                        if (dot && !call) || (brace && names_field(tokens, index)) {
                            shapes |= FIELD;
                        }
                        self.mentions.add(name, place, shapes);
                    }
                    defines = keyword;
                }
                TokenTree::Literal(literal) => {
                    let text = literal.to_string();
                    if text.contains('"') {
                        for word in text.split(|char: char| !char.is_alphanumeric() && char != '_') {
                            if !word.is_empty() {
                                self.mentions.add(word.to_owned(), place, ANY | MEMBER);
                            }
                        }
                    }
                    defines = false;
                }
                TokenTree::Group(group) => {
                    if !is_doc(group) {
                        let inner: Vec<TokenTree> = group.stream().into_iter().collect();
                        let brace = group.delimiter() == Delimiter::Brace;
                        if brace
                            && let Some(TokenTree::Ident(name)) = previous
                            && updates(&inner)
                        {
                            self.mentions.updates.entry(name.to_string()).or_default().insert(place);
                        }
                        self.scan(&inner, brace, exported || macro_rules);
                    }
                    macro_rules = false;
                    defines = false;
                }
                TokenTree::Punct(_) => defines = false,
            }
        }
    }
}

/// Whether the identifier in a brace group stands where a struct literal or pattern names a field:
/// `name: value`, or the shorthand `name`. A block that is only `name` looks the same and counts too.
fn names_field(tokens: &[TokenTree], index: usize) -> bool {
    let before = match index.checked_sub(1).and_then(|index| tokens.get(index)) {
        None => true,
        Some(TokenTree::Ident(ident)) => ident == "ref" || ident == "mut",
        Some(token) => is_punct(token, ','),
    };
    let after = match tokens.get(index + 1) {
        None => true,
        Some(token) if is_punct(token, ':') => !tokens.get(index + 2).is_some_and(|token| is_punct(token, ':')),
        Some(token) => is_punct(token, ','),
    };
    before && after
}

/// Whether the tokens of a brace group hold `..base`, unlike the rest pattern `..` that ends a group.
fn updates(tokens: &[TokenTree]) -> bool {
    (0..tokens.len()).any(|index| {
        let dots = matches!(
            (&tokens[index], tokens.get(index + 1)),
            (TokenTree::Punct(first), Some(TokenTree::Punct(second)))
                if first.as_char() == '.' && first.spacing() == Spacing::Joint && second.as_char() == '.'
        );
        let starts = index.checked_sub(1).is_none_or(|index| is_punct(&tokens[index], ','));
        dots && starts && tokens.get(index + 2).is_some_and(|token| !is_punct(token, '='))
    })
}
