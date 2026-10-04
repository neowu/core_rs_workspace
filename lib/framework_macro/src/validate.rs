use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Attribute;
use syn::Data;
use syn::DataStruct;
use syn::DeriveInput;
use syn::Error;
use syn::Expr;
use syn::ExprLit;
use syn::ExprUnary;
use syn::Field;
use syn::Fields;
use syn::GenericArgument;
use syn::Ident;
use syn::Lit;
use syn::LitInt;
use syn::MetaNameValue;
use syn::PathArguments;
use syn::Result;
use syn::Token;
use syn::Type;
use syn::TypePath;
use syn::UnOp;
use syn::parse2;
use syn::punctuated::Punctuated;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let ast: DeriveInput = parse2(tokens)?;
    let Data::Struct(DataStruct { fields: Fields::Named(fields), .. }) = &ast.data else {
        return Err(Error::new_spanned(&ast.ident, "derive target must be struct with named fields"));
    };

    let mut body = vec![];
    for field in &fields.named {
        body.extend(build_field_validators(field)?);
    }

    let struct_name = &ast.ident;
    let (impl_generics, ty_generics, where_clause) = ast.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics framework::validate::Validator for #struct_name #ty_generics #where_clause {
            fn validate(&self) -> ::core::result::Result<(), framework::exception::Exception> {
                #(#body)*
                Ok(())
            }
        }
    })
}

#[derive(Default)]
struct FieldAttrs<'a> {
    range: Option<&'a Attribute>,
    length: Option<&'a Attribute>,
    not_blank: Option<&'a Attribute>,
    validate: Option<&'a Attribute>,
}

fn parse_field_attrs(field: &Field) -> Result<FieldAttrs<'_>> {
    let mut attrs = FieldAttrs::default();
    for attr in &field.attrs {
        let path = attr.path();
        let slot = if path.is_ident("range") {
            &mut attrs.range
        } else if path.is_ident("length") {
            &mut attrs.length
        } else if path.is_ident("not_blank") {
            &mut attrs.not_blank
        } else if path.is_ident("validate") {
            &mut attrs.validate
        } else {
            continue;
        };
        if slot.is_some() {
            return Err(Error::new_spanned(attr, "duplicate validation attribute"));
        }
        *slot = Some(attr);
    }
    Ok(attrs)
}

fn build_field_validators(field: &Field) -> Result<Vec<TokenStream>> {
    let attrs = parse_field_attrs(field)?;
    let mut body = vec![];

    if let Some(attr) = attrs.range {
        body.push(build_range_validator(field, &parse_bounds(attr, false)?));
    }

    if let Some(attr) = attrs.length {
        body.push(build_length_validator(field, &parse_bounds(attr, true)?));
    }

    if let Some(attr) = attrs.not_blank {
        attr.meta.require_path_only()?;
        body.push(build_not_blank_validator(field));
    }

    if let Some(attr) = attrs.validate {
        attr.meta.require_path_only()?;
        let field_ident = &field.ident;
        body.push(quote!(framework::validate::Validator::validate(&self.#field_ident)?;));
    }

    Ok(body)
}

struct Bounds {
    min: Option<LitInt>,
    max: Option<LitInt>,
}

fn parse_bounds(attr: &Attribute, non_negative: bool) -> Result<Bounds> {
    let mut bounds = Bounds { min: None, max: None };
    for meta in attr.parse_args_with(Punctuated::<MetaNameValue, Token![,]>::parse_terminated)? {
        let slot = if meta.path.is_ident("min") {
            &mut bounds.min
        } else if meta.path.is_ident("max") {
            &mut bounds.max
        } else {
            return Err(Error::new_spanned(&meta.path, "unknown key, expected `min` or `max`"));
        };
        if slot.is_some() {
            return Err(Error::new_spanned(&meta.path, "duplicate key"));
        }
        let value = int_literal(&meta.value)?;
        if non_negative && value.base10_parse::<i128>()? < 0 {
            return Err(Error::new_spanned(&value, "value must not be negative"));
        }
        *slot = Some(value);
    }

    match (&bounds.min, &bounds.max) {
        (None, None) => return Err(Error::new_spanned(attr, "requires `min` or `max`")),
        (Some(min), Some(max)) if min.base10_parse::<i128>()? > max.base10_parse::<i128>()? => {
            return Err(Error::new_spanned(attr, "`min` must not be greater than `max`"));
        }
        _ => {}
    }
    Ok(bounds)
}

// syn parses `-1` as a literal only when it is the last token of the stream, e.g. not in `min = -1, max = 1`
fn int_literal(expr: &Expr) -> Result<LitInt> {
    if let Expr::Lit(ExprLit { lit: Lit::Int(value), .. }) = expr {
        return Ok(value.clone());
    }
    if let Expr::Unary(ExprUnary { op: UnOp::Neg(_), expr: operand, .. }) = expr
        && let Expr::Lit(ExprLit { lit: Lit::Int(value), .. }) = operand.as_ref()
    {
        return Ok(LitInt::new(&format!("-{value}"), value.span()));
    }
    Err(Error::new_spanned(expr, "value must be an integer literal"))
}

// value: local bound to the checked value, compared against the bounds and printed on failure
fn build_bound_checks(field: &Field, label: &str, value: &str, bounds: &Bounds) -> Vec<TokenStream> {
    let field_ident = field.ident.as_ref().expect("field must be named");
    let value_ident = Ident::new(value, Span::call_site());
    let mut checks = vec![];
    if let Some(min) = &bounds.min {
        let message = format!("{field_ident} {label}must not be less than {}, value={{{value}}}", min.base10_digits());
        checks.push(quote!(
            if #value_ident < #min {
                return Err(framework::validation_error!(format!(#message)));
            }
        ));
    }
    if let Some(max) = &bounds.max {
        let message =
            format!("{field_ident} {label}must not be greater than {}, value={{{value}}}", max.base10_digits());
        checks.push(quote!(
            if #value_ident > #max {
                return Err(framework::validation_error!(format!(#message)));
            }
        ));
    }
    checks
}

fn build_range_validator(field: &Field, bounds: &Bounds) -> TokenStream {
    let field_ident = &field.ident;
    let checks = build_bound_checks(field, "", "value", bounds);
    if option_inner_type(&field.ty).is_some() {
        quote!(
            if let Some(value) = self.#field_ident {
                #(#checks)*
            }
        )
    } else {
        quote!({
            let value = self.#field_ident;
            #(#checks)*
        })
    }
}

fn build_length_validator(field: &Field, bounds: &Bounds) -> TokenStream {
    let field_ident = &field.ident;
    let inner_type = option_inner_type(&field.ty);
    // str::len() returns byte length, which is not char count for non-ascii utf-8
    let length = if is_string_type(inner_type.unwrap_or(&field.ty)) { quote!(chars().count()) } else { quote!(len()) };
    let checks = build_bound_checks(field, "length ", "length", bounds);
    if inner_type.is_some() {
        quote!(
            if let Some(ref value) = self.#field_ident {
                let length = value.#length;
                #(#checks)*
            }
        )
    } else {
        quote!({
            let length = self.#field_ident.#length;
            #(#checks)*
        })
    }
}

fn build_not_blank_validator(field: &Field) -> TokenStream {
    let field_ident = field.ident.as_ref().expect("field must be named");
    let message = format!("{field_ident} must not be blank");
    if option_inner_type(&field.ty).is_some() {
        quote!(
            if let Some(ref value) = self.#field_ident && value.chars().all(char::is_whitespace) {
                return Err(framework::validation_error!(#message));
            }
        )
    } else {
        quote!(
            if self.#field_ident.chars().all(char::is_whitespace) {
                return Err(framework::validation_error!(#message));
            }
        )
    }
}

// matches by last path segment, so `Option<T>` and `std::option::Option<T>` are the same, type aliases are not resolved
fn option_inner_type(ty: &Type) -> Option<&Type> {
    let Type::Path(TypePath { qself: None, path, .. }) = ty else {
        return None;
    };
    let segment = path.segments.last()?;
    if segment.ident != "Option" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let Some(GenericArgument::Type(inner_type)) = args.args.first() else {
        return None;
    };
    Some(inner_type)
}

// String or &str
fn is_string_type(ty: &Type) -> bool {
    if let Type::Reference(reference) = ty {
        return is_string_type(&reference.elem);
    }
    let Type::Path(TypePath { qself: None, path, .. }) = ty else {
        return false;
    };
    path.segments.last().is_some_and(|segment| segment.ident == "String" || segment.ident == "str")
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::Result;

    use super::build;

    #[test]
    fn validate_impl() -> Result<()> {
        let source = quote! {
            #[derive(Validate)]
            struct TestBean {
                #[range(min = 2, max = 100)]
                col1: i32,
                #[length(min = 1, max = 10)]
                col2: Vec<String>,
                #[length(min = 1)]
                col3: Option<Vec<String>>,
                #[not_blank]
                col4: String,
                #[length(min = 1, max = 10)]
                col5: std::string::String,
                #[length(max = 10)]
                col6: std::option::Option<String>,
                #[range(min = -5, max = 5)]
                col7: Option<i64>,
                #[validate]
                child: Child,
                #[validate]
                optional_children: Option<Vec<Child>>,
            }
        };

        let output = build(source)?;

        assert_eq!(output.to_string(), quote! {
            impl framework::validate::Validator for TestBean {
                fn validate(&self) -> ::core::result::Result<(), framework::exception::Exception> {
                    {
                        let value = self.col1;
                        if value < 2 {
                            return Err(framework::validation_error!(format!("col1 must not be less than 2, value={value}")));
                        }
                        if value > 100 {
                            return Err(framework::validation_error!(format!("col1 must not be greater than 100, value={value}")));
                        }
                    }

                    {
                        let length = self.col2.len();
                        if length < 1 {
                            return Err(framework::validation_error!(format!("col2 length must not be less than 1, value={length}")));
                        }
                        if length > 10 {
                            return Err(framework::validation_error!(format!("col2 length must not be greater than 10, value={length}")));
                        }
                    }

                    if let Some(ref value) = self.col3 {
                        let length = value.len();
                        if length < 1 {
                            return Err(framework::validation_error!(format!("col3 length must not be less than 1, value={length}")));
                        }
                    }

                    if self.col4.chars().all(char::is_whitespace) {
                        return Err(framework::validation_error!("col4 must not be blank"));
                    }

                    {
                        let length = self.col5.chars().count();
                        if length < 1 {
                            return Err(framework::validation_error!(format!("col5 length must not be less than 1, value={length}")));
                        }
                        if length > 10 {
                            return Err(framework::validation_error!(format!("col5 length must not be greater than 10, value={length}")));
                        }
                    }

                    if let Some(ref value) = self.col6 {
                        let length = value.chars().count();
                        if length > 10 {
                            return Err(framework::validation_error!(format!("col6 length must not be greater than 10, value={length}")));
                        }
                    }

                    if let Some(value) = self.col7 {
                        if value < -5 {
                            return Err(framework::validation_error!(format!("col7 must not be less than -5, value={value}")));
                        }
                        if value > 5 {
                            return Err(framework::validation_error!(format!("col7 must not be greater than 5, value={value}")));
                        }
                    }

                    framework::validate::Validator::validate(&self.child)?;

                    framework::validate::Validator::validate(&self.optional_children)?;

                    Ok(())
                }
            }
        }
        .to_string());

        Ok(())
    }

    #[test]
    fn validate_impl_with_generics() -> Result<()> {
        let source = quote! {
            struct Page<'a, T: Clone> where T: Send {
                #[length(max = 10)]
                name: &'a str,
                #[validate]
                items: Vec<T>,
            }
        };

        let output = build(source)?.to_string();

        assert!(output.starts_with(
            &quote!(impl<'a, T: Clone> framework::validate::Validator for Page<'a, T> where T: Send).to_string()
        ));
        assert!(output.contains(&quote!(let length = self.name.chars().count();).to_string()));
        Ok(())
    }

    #[test]
    fn invalid_attributes() {
        let cases = [
            (quote!(#[range(min = MIN)] a: i32), "value must be an integer literal"),
            (quote!(#[range(mni = 10)] a: i32), "unknown key, expected `min` or `max`"),
            (quote!(#[range(min = 1, min = 2)] a: i32), "duplicate key"),
            (quote!(#[range(min = 0)] #[range(max = 10)] a: i32), "duplicate validation attribute"),
            (quote!(#[range()] a: i32), "requires `min` or `max`"),
            (quote!(#[range] a: i32), "expected attribute arguments in parentheses"),
            (quote!(#[range(min = 10, max = 1)] a: i32), "`min` must not be greater than `max`"),
            (quote!(#[length(min = -1)] a: String), "value must not be negative"),
            (quote!(#[range(min = 1.5)] a: f64), "value must be an integer literal"),
            (quote!(#[range(min = -MIN, max = 1)] a: i32), "value must be an integer literal"),
            (quote!(#[not_blank(true)] a: String), "unexpected token in attribute"),
        ];
        for (field, expected) in cases {
            let error = build(quote!(struct Bean { #field })).expect_err("must fail");
            assert!(error.to_string().starts_with(expected), "field={field}, error={error}");
        }
    }
}
