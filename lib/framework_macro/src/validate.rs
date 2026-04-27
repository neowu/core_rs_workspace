use proc_macro2::TokenStream;
use quote::quote;
use syn::Result;

use crate::model;
use crate::model::AttributeModel;
use crate::model::FieldModel;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let model = model::parse_struct(tokens)?;
    let struct_name = &model.ident;

    let mut body = vec![];
    for field in &model.fields {
        body.extend(build_field_validators(field)?);
    }

    Ok(quote! {
        impl framework::validate::Validator for #struct_name {
            fn validate(&self) -> Result<(), framework::exception::Exception> {
                #(#body)*
                Ok(())
            }
        }
    })
}

fn build_field_validators(field: &FieldModel) -> Result<Vec<TokenStream>> {
    let mut body = vec![];

    if let Some(attr) = field.optional_attr("range") {
        body.extend(build_range_validator(field, attr)?);
    }

    if let Some(attr) = field.optional_attr("length") {
        body.extend(build_length_validator(field, attr)?);
    }

    if field.optional_attr("not_blank").is_some() {
        body.push(build_not_blank_validator(field));
    }

    if field.optional_attr("validate").is_some() {
        body.push(build_nested_validator(field));
    }

    Ok(body)
}

fn build_range_validator(field: &FieldModel, attr: &AttributeModel) -> Result<Vec<TokenStream>> {
    let field_ident = &field.ident;
    let mut body = vec![];

    if let Some(max) = attr.optional_int_meta_value("max")? {
        let message = format!("{field_ident} must not be greater than {max}, value={{value}}");
        if field.is_optional_type() {
            body.push(quote!(
                if let Some(value) = self.#field_ident && value > #max {
                    return Err(framework::validation_error!(message = format!(#message)));
                }
            ));
        } else {
            body.push(quote!(
                let value = self.#field_ident;
                if value > #max {
                    return Err(framework::validation_error!(message = format!(#message)));
                }
            ));
        }
    }

    if let Some(min) = attr.optional_int_meta_value("min")? {
        let message = format!("{field_ident} must not be less than {min}, value={{value}}");
        if field.is_optional_type() {
            body.push(quote!(
                if let Some(value) = self.#field_ident && value < #min {
                    return Err(framework::validation_error!(message = format!(#message)));
                }
            ));
        } else {
            body.push(quote!(
                let value = self.#field_ident;
                if value < #min {
                    return Err(framework::validation_error!(message = format!(#message)));
                }
            ));
        }
    }

    Ok(body)
}

fn build_length_validator(field: &FieldModel, attr: &AttributeModel) -> Result<Vec<TokenStream>> {
    let field_ident = &field.ident;
    let mut body = vec![];

    if let Some(max) = attr.optional_int_meta_value("max")? {
        let message = format!("{field_ident} length must not be greater than {max}, value={{value}}");
        if field.is_optional_type() {
            body.push(quote!(
                if let Some(ref value) = self.#field_ident && value.len() > #max {
                    let value = value.len();
                    return Err(framework::validation_error!(message = format!(#message)));
                }
            ));
        } else {
            body.push(quote!(
                let value = self.#field_ident.len();
                if value > #max {
                    return Err(framework::validation_error!(message = format!(#message)));
                }
            ));
        }
    }

    if let Some(min) = attr.optional_int_meta_value("min")? {
        let message = format!("{field_ident} length must not be less than {min}, value={{value}}");
        if field.is_optional_type() {
            body.push(quote!(
                if let Some(ref value) = self.#field_ident && value.len() < #min {
                    let value = value.len();
                    return Err(framework::validation_error!(message = format!(#message)));
                }
            ));
        } else {
            body.push(quote!(
                let value = self.#field_ident.len();
                if value < #min {
                    return Err(framework::validation_error!(message = format!(#message)));
                }
            ));
        }
    }

    Ok(body)
}

fn build_not_blank_validator(field: &FieldModel) -> TokenStream {
    let field_ident = &field.ident;
    let message = format!("{field_ident} must not be blank");
    if field.is_optional_type() {
        quote!(
            if let Some(ref value) = self.#field_ident && value.chars().all(char::is_whitespace) {
                return Err(framework::validation_error!(message = #message));
            }
        )
    } else {
        quote!(
            if self.#field_ident.chars().all(char::is_whitespace) {
                return Err(framework::validation_error!(message = #message));
            }
        )
    }
}

fn build_nested_validator(field: &FieldModel) -> TokenStream {
    let field_ident = &field.ident;

    if field.is_optional_type() {
        quote!(
            if let Some(ref value) = self.#field_ident {
                value.validate()?;
            }
        )
    } else {
        quote!(
            self.#field_ident.validate()?;
        )
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::Result;

    use super::build;

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

        let output = build(source)?;

        assert_eq!(output.to_string(), quote! {
            impl framework::validate::Validator for TestBean {
                fn validate(&self) -> Result<(), framework::exception::Exception> {
                    let value = self.col1;
                    if value > 100 {
                        return Err(framework::validation_error!(message = format!("col1 must not be greater than 100, value={value}")));
                    }
                    let value = self.col1;
                    if value < 2 {
                        return Err(framework::validation_error!(message = format!("col1 must not be less than 2, value={value}")));
                    }

                    let value = self.col2.len();
                    if value > 10 {
                        return Err(framework::validation_error!(message = format!("col2 length must not be greater than 10, value={value}")));
                    }
                    let value = self.col2.len();
                    if value < 1 {
                        return Err(framework::validation_error!(message = format!("col2 length must not be less than 1, value={value}")));
                    }

                    if self.col3.chars().all(char::is_whitespace) {
                        return Err(framework::validation_error!(message = "col3 must not be blank"));
                    }

                    self.col4.validate()?;

                    if let Some(ref value) = self.col5 && value.len() < 1 {
                        let value = value.len();
                        return Err(framework::validation_error!(message = format!("col5 length must not be less than 1, value={value}")));
                    }

                    Ok(())
                }
            }
       }.to_string());

        Ok(())
    }
}
