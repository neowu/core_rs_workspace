use proc_macro2::TokenStream;
use quote::quote;
use syn::Lit;
use syn::Result;

use crate::model;
use crate::model::AttributeModel;
use crate::model::FieldModel;

pub(crate) fn validate_impl(tokens: TokenStream) -> Result<TokenStream> {
    let model = model::parse_struct(tokens)?;
    let name = &model.ident;

    let mut body = vec![];
    for field in &model.fields {
        body.extend(build_field_validators(field)?);
    }

    Ok(quote! {
        impl framework::validate::Validator for #name {
            fn validate(&self) -> Result<(), framework::exception::Exception> {
                #(#body)*
                Ok(())
            }
        }
    })
}

fn build_field_validators(field: &FieldModel) -> Result<Vec<TokenStream>> {
    let mut impls = vec![];

    if let Some(attr) = field.attrs.get_optional("range") {
        impls.extend(build_range_validator(field, attr)?);
    }

    if let Some(attr) = field.attrs.get_optional("length") {
        impls.extend(build_length_validator(field, attr)?);
    }

    if field.attrs.get_optional("not_blank").is_some() {
        impls.extend(build_not_blank_validator(field));
    }

    if field.attrs.get_optional("validate").is_some() {
        impls.extend(build_nested_validator(field));
    }

    Ok(impls)
}

fn build_range_validator(field: &FieldModel, attr: &AttributeModel) -> Result<Vec<TokenStream>> {
    let field_ident = &field.ident;
    let field_name = field.ident.to_string();
    let optional = field.is_optional();
    let mut impls = vec![];

    if let Ok(Lit::Int(max)) = attr.meta_value("max") {
        if optional {
            impls.push(quote!(
                if let Some(value) = self.#field_ident && value > #max {
                    return Err(framework::validation_error!(message = format!("{} must not be greater than {}, value={value}", #field_name, #max)));
                }
            ));
        } else {
            impls.push(quote!(
                let value = self.#field_ident;
                if value > #max {
                    return Err(framework::validation_error!(message = format!("{} must not be greater than {}, value={value}", #field_name, #max)));
                }
            ));
        }
    }

    if let Ok(Lit::Int(min)) = attr.meta_value("min") {
        if optional {
            impls.push(quote!(
                if let Some(value) = self.#field_ident && value < #min {
                    return Err(framework::validation_error!(message = format!("{} must not be less than {}, value={value}", #field_name, #min)));
                }
            ));
        } else {
            impls.push(quote!(
                let value = self.#field_ident;
                if value < #min {
                    return Err(framework::validation_error!(message = format!("{} must not be less than {}, value={value}", #field_name, #min)));
                }
            ));
        }
    }

    Ok(impls)
}

fn build_length_validator(field: &FieldModel, attr: &AttributeModel) -> Result<Vec<TokenStream>> {
    let field_ident = &field.ident;
    let field_name = field.ident.to_string();
    let optional = field.is_optional();
    let mut impls = vec![];

    if let Ok(Lit::Int(max)) = attr.meta_value("max") {
        if optional {
            impls.push(quote!(
                if let Some(ref value) = self.#field_ident && value.len() > #max {
                    let value = value.len();
                    return Err(framework::validation_error!(message = format!("{} length must not be greater than {}, value={value}", #field_name, #max)));
                }
            ));
        } else {
            impls.push(quote!(
                let value = self.#field_ident.len();
                if value > #max {
                    return Err(framework::validation_error!(message = format!("{} length must not be greater than {}, value={value}", #field_name, #max)));
                }
            ));
        }
    }

    if let Ok(Lit::Int(min)) = attr.meta_value("min") {
        if optional {
            impls.push(quote!(
                if let Some(ref value) = self.#field_ident && value.len() < #min {
                    let value = value.len();
                    return Err(framework::validation_error!(message = format!("{} length must not be less than {}, value={value}", #field_name, #min)));
                }
            ));
        } else {
            impls.push(quote!(
                let value = self.#field_ident.len();
                if value < #min {
                    return Err(framework::validation_error!(message = format!("{} length must not be less than {}, value={value}", #field_name, #min)));
                }
            ));
        }
    }

    Ok(impls)
}

fn build_not_blank_validator(field: &FieldModel) -> Vec<TokenStream> {
    let field_ident = &field.ident;
    let field_name = field.ident.to_string();
    let mut impls = vec![];

    if field.is_optional() {
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

fn build_nested_validator(field: &FieldModel) -> Vec<TokenStream> {
    let field_ident = &field.ident;
    let mut impls = vec![];

    if field.is_optional() {
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

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::Result;

    use super::validate_impl;

    #[test]
    fn test_validate_impl() -> Result<()> {
        let source = quote! {
            #[derive(Validate)]
            struct TestBean {
                #[range(min = 2, max = 100)]
                col1: i32,
                #[length(min = 1, max = 10)]
                col2: Vec<String>,
                #[not_blank]
                col3: String,
                #[validate]
                col4: Child,
                #[length(min = 1)]
                col5: Option<Vec<String>>,
            }
        };

        let output = validate_impl(source)?;

        assert_eq!(output.to_string(), quote! {
            impl framework::validate::Validator for TestBean {
                fn validate(&self) -> Result<(), framework::exception::Exception> {
                    let value = self.col1;
                    if value > 100 {
                        return Err(framework::validation_error!(message = format!("{} must not be greater than {}, value={value}", "col1", 100)));
                    }
                    let value = self.col1;
                    if value < 2 {
                        return Err(framework::validation_error!(message = format!("{} must not be less than {}, value={value}", "col1", 2)));
                    }

                    let value = self.col2.len();
                    if value > 10 {
                        return Err(framework::validation_error!(message=format!("{} length must not be greater than {}, value={value}", "col2", 10)));
                    }
                    let value = self.col2.len();
                    if value < 1 {
                        return Err(framework::validation_error!(message=format!("{} length must not be less than {}, value={value}", "col2", 1)));
                    }

                    if self.col3.chars().all(char::is_whitespace) {
                        return Err(framework::validation_error!(message=format!("{} must not be blank", "col3")));
                    }

                    self.col4.validate()?;

                    if let Some(ref value) = self.col5 && value.len() < 1 {
                        let value = value.len();
                        return Err(framework::validation_error!(message = format!("{} length must not be less than {}, value={value}", "col5", 1)));
                    }

                    Ok(())
                }
            }
       }.to_string());

        Ok(())
    }
}
