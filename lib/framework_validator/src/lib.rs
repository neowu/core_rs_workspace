use proc_macro2::TokenStream;
use quote::quote;
use syn::Data::Struct;
use syn::DataStruct;
use syn::DeriveInput;
use syn::Fields::Named;
use syn::FieldsNamed;
use syn::Meta;
use syn::Token;
use syn::parse2;
use syn::punctuated::Punctuated;

use crate::field::FieldDefinition;

mod field;

/**
`#[derive(Validate)]` supports following field validations:
```
#[validate(range(max = 10, min = 1))]   // for Numeric
#[validate(length(max = 10, min = 1))]   // for String, Collections
#[validate(nested)]                     // for nested struct
#[validate(not_blank)]                  // for String
```
*/
#[proc_macro_derive(Validate, attributes(validate))]
pub fn validate(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    build_impl(item.into()).into()
}

fn build_impl(item: TokenStream) -> TokenStream {
    let ast: DeriveInput = parse2(item).unwrap();
    let name = ast.ident;

    let fields = match ast.data {
        Struct(DataStruct {
            fields: Named(FieldsNamed { ref named, .. }),
            ..
        }) => named,
        _ => unimplemented!("only implemented for structs"),
    };
    let fields = field::parse(fields);

    let body = build_body(fields);

    quote! {
        impl framework::validate::Validator for #name {
            fn validate(&self) -> Result<(), framework::exception::Exception> {
                #(#body)*
                Ok(())
            }
        }
    }
}

fn build_body(fields: Vec<FieldDefinition>) -> Vec<TokenStream> {
    let mut impls = vec![];

    for field in fields {
        if let Some(attr) = field.attr("validate") {
            let nested = attr
                .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
                .expect("validate attr should be splitted by comma");

            for meta in nested {
                match meta {
                    Meta::List(list) => {
                        if list.path.is_ident("range") {
                            impls.append(&mut build_range_validator(&field, &list));
                        }
                        if list.path.is_ident("length") {
                            impls.append(&mut build_length_validator(&field, &list));
                        }
                    }
                    Meta::Path(path) => {
                        if path.is_ident("nested") {
                            impls.append(&mut build_nested_validator(&field));
                        }
                        if path.is_ident("not_blank") {
                            impls.append(&mut build_not_blank_validator(&field));
                        }
                    }
                    _ => (),
                }
            }
        }
    }
    impls
}

fn build_not_blank_validator(field: &FieldDefinition) -> Vec<TokenStream> {
    let field_ident = field.ident;
    let field_name = &field.name;

    let mut impls = vec![];

    if field.is_optional {
        impls.push(quote!(
            if let Some(ref value) = self.#field_ident && value.chars().all(char::is_whitespace) {
                return Err(framework::validation_error!(message = format!("{} must not be blank", #field_name)));
            }
        ));
    } else {
        impls.push(quote!(
            if self.#field_ident.chars().all(char::is_whitespace) {
                return Err(framework::validation_error!(message = format!("{} must not be blank", #field_name)));
            }
        ));
    }

    impls
}

fn build_nested_validator(field: &FieldDefinition) -> Vec<TokenStream> {
    let field_ident = field.ident;
    let mut impls = vec![];

    if field.is_optional {
        impls.push(quote!(
            if let Some(ref value) = self.#field_ident {
                value.validate()?;
            }
        ));
    } else {
        impls.push(quote!(
            self.#field_ident.validate()?;
        ));
    }

    impls
}

fn build_length_validator(field: &FieldDefinition, list: &syn::MetaList) -> Vec<TokenStream> {
    let field_ident = field.ident;
    let field_name = &field.name;

    let nested = list
        .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .expect("length attr should be splitted by comma");

    let mut impls = vec![];

    for meta in nested {
        if let Meta::NameValue(name_value) = meta {
            let value = name_value.value;
            if name_value.path.is_ident("max") {
                if field.is_optional {
                    impls.push(quote!(
                        if let Some(ref value) = self.#field_ident && value.len() > #value {
                            let value = value.len();
                            return Err(framework::validation_error!(message = format!("{} length must not be greater than {}, value={value}", #field_name, #value)))
                        }
                    ))
                } else {
                    impls.push(quote!(
                        let value = self.#field_ident.len();
                        if value > #value {
                            return Err(framework::validation_error!(message = format!("{} length must not be greater than {}, value={value}", #field_name, #value)))
                        }
                    ))
                }
            } else if name_value.path.is_ident("min") {
                if field.is_optional {
                    impls.push(quote!(
                        if let Some(ref value) = self.#field_ident && value.len() < #value {
                            let value = value.len();
                            return Err(framework::validation_error!(message = format!("{} length must not be less than {}, value={value}", #field_name, #value)))
                        }
                    ))
                } else {
                    impls.push(quote!(
                        let value = self.#field_ident.len();
                        if value < #value {
                            return Err(framework::validation_error!(message = format!("{} length must not be less than {}, value={value}", #field_name, #value)))
                        }
                    ))
                }
            }
        }
    }

    impls
}

fn build_range_validator(field: &FieldDefinition, list: &syn::MetaList) -> Vec<TokenStream> {
    let field_ident = field.ident;
    let field_name = &field.name;

    let nested = list
        .parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)
        .expect("range attr should be splitted by comma");

    let mut impls = vec![];

    for meta in nested {
        if let Meta::NameValue(name_value) = meta {
            let value = name_value.value;
            if name_value.path.is_ident("max") {
                if field.is_optional {
                    impls.push(quote!(
                        if let Some(value) = self.#field_ident && value > #value {
                            return Err(framework::validation_error!(message = format!("{} must not be greater than {}, value={value}", #field_name, #value)))
                        }
                    ))
                } else {
                    impls.push(quote!(
                        let value = self.#field_ident;
                        if value > #value {
                            return Err(framework::validation_error!(message = format!("{} must not be greater than {}, value={value}", #field_name, #value)))
                        }
                    ))
                }
            } else if name_value.path.is_ident("min") {
                if field.is_optional {
                    impls.push(quote!(
                        if let Some(value) = self.#field_ident && value < #value {
                            return Err(framework::validation_error!(message = format!("{} must not be less than {}, value={value}", #field_name, #value)))
                        }
                    ))
                } else {
                    impls.push(quote!(
                        let value = self.#field_ident;
                        if value < #value {
                            return Err(framework::validation_error!(message = format!("{} must not be less than {}, value={value}", #field_name, #value)))
                        }
                    ))
                }
            }
        }
    }

    impls
}
