use proc_macro2::TokenStream;
use quote::quote;
use syn::Result;

use crate::model;
use crate::model::AttributeModel;
use crate::model::ConstraintsModel;
use crate::model::DeriveModel;
use crate::model::EnumModel;
use crate::model::StructModel;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    match model::parse_derive_input(tokens)? {
        DeriveModel::Struct(model) => build_struct(&model),
        DeriveModel::Enum(model) => build_enum(&model),
    }
}

// api type uses rust names as wire names, so serde renaming must not be used, otherwise client stubs would not match
fn check_serde_attrs<'a>(attrs: impl Iterator<Item = &'a AttributeModel>) -> Result<()> {
    for attr in attrs {
        for meta in ["rename", "rename_all"] {
            if attr.has_meta(meta) {
                return Err(attr.error(format!("#[serde({meta})] is not supported by #[derive(ApiType)], api uses rust names")));
            }
        }
    }
    Ok(())
}

fn build_struct(model: &StructModel) -> Result<TokenStream> {
    let ident = &model.ident;
    let name = ident.to_string();
    check_serde_attrs(model.attrs("serde"))?;

    let mut fields = vec![];
    for field in &model.fields {
        check_serde_attrs(field.attrs("serde"))?;
        let field_name = field.ident.to_string();
        let ty = &field.ty;
        let constraints = build_constraints(&field.constraints()?);
        fields.push(quote! {
            framework::api::FieldDefinition {
                name: #field_name.to_owned(),
                r#type: <#ty as framework::api::ApiType>::type_ref(registry),
                constraints: #constraints,
            }
        });
    }

    Ok(quote! {
        impl framework::api::ApiType for #ident {
            fn type_ref(registry: &mut framework::api::TypeRegistry) -> framework::api::TypeRef {
                if registry.reserve(#name, ::std::any::type_name::<Self>()) {
                    let fields = vec![#(#fields),*];
                    registry.define(framework::api::TypeDefinition::Struct { name: #name.to_owned(), fields });
                }
                framework::api::TypeRef::Ref { name: #name.to_owned() }
            }
        }
    })
}

fn build_constraints(constraints: &ConstraintsModel) -> TokenStream {
    let mut fields = vec![];
    if constraints.not_blank {
        fields.push(quote!(not_blank: true));
    }
    if let Some(min) = &constraints.min {
        fields.push(quote!(min: Some(#min)));
    }
    if let Some(max) = &constraints.max {
        fields.push(quote!(max: Some(#max)));
    }
    if let Some(min) = &constraints.min_length {
        fields.push(quote!(min_length: Some(#min)));
    }
    if let Some(max) = &constraints.max_length {
        fields.push(quote!(max_length: Some(#max)));
    }

    if fields.is_empty() {
        quote!(framework::api::Constraints::default())
    } else {
        quote!(framework::api::Constraints { #(#fields,)* ..framework::api::Constraints::default() })
    }
}

fn build_enum(model: &EnumModel) -> Result<TokenStream> {
    let ident = &model.ident;
    let name = ident.to_string();
    check_serde_attrs(model.attrs("serde"))?;

    let mut values = vec![];
    for variant in &model.variants {
        check_serde_attrs(variant.attrs("serde"))?;
        values.push(variant.ident.to_string());
    }

    Ok(quote! {
        impl framework::api::ApiType for #ident {
            fn type_ref(registry: &mut framework::api::TypeRegistry) -> framework::api::TypeRef {
                if registry.reserve(#name, ::std::any::type_name::<Self>()) {
                    registry.define(framework::api::TypeDefinition::Enum { name: #name.to_owned(), values: vec![#(#values.to_owned()),*] });
                }
                framework::api::TypeRef::Ref { name: #name.to_owned() }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use syn::Result;

    use super::build;

    #[test]
    fn api_type_struct() -> Result<()> {
        let source = quote! {
            #[derive(ApiType)]
            struct CreateUserRequest {
                #[not_blank]
                #[length(max = 50)]
                name: String,
                #[range(min = 0, max = 10)]
                rating: Option<i32>,
                tags: Vec<String>,
                #[validate]
                child: Child,
            }
        };

        let output = build(source)?;

        assert_eq!(
            output.to_string(),
            quote! {
                impl framework::api::ApiType for CreateUserRequest {
                    fn type_ref(registry: &mut framework::api::TypeRegistry) -> framework::api::TypeRef {
                        if registry.reserve("CreateUserRequest", ::std::any::type_name::<Self>()) {
                            let fields = vec![
                                framework::api::FieldDefinition {
                                    name: "name".to_owned(),
                                    r#type: <String as framework::api::ApiType>::type_ref(registry),
                                    constraints: framework::api::Constraints { not_blank: true, max_length: Some(50), ..framework::api::Constraints::default() },
                                },
                                framework::api::FieldDefinition {
                                    name: "rating".to_owned(),
                                    r#type: <Option<i32> as framework::api::ApiType>::type_ref(registry),
                                    constraints: framework::api::Constraints { min: Some(0), max: Some(10), ..framework::api::Constraints::default() },
                                },
                                framework::api::FieldDefinition {
                                    name: "tags".to_owned(),
                                    r#type: <Vec<String> as framework::api::ApiType>::type_ref(registry),
                                    constraints: framework::api::Constraints::default(),
                                },
                                framework::api::FieldDefinition {
                                    name: "child".to_owned(),
                                    r#type: <Child as framework::api::ApiType>::type_ref(registry),
                                    constraints: framework::api::Constraints::default(),
                                }
                            ];
                            registry.define(framework::api::TypeDefinition::Struct { name: "CreateUserRequest".to_owned(), fields });
                        }
                        framework::api::TypeRef::Ref { name: "CreateUserRequest".to_owned() }
                    }
                }
            }
            .to_string()
        );

        Ok(())
    }

    #[test]
    fn api_type_enum() -> Result<()> {
        let source = quote! {
            #[derive(ApiType)]
            enum Status {
                Active,
                Inactive,
            }
        };

        let output = build(source)?;

        assert_eq!(
            output.to_string(),
            quote! {
                impl framework::api::ApiType for Status {
                    fn type_ref(registry: &mut framework::api::TypeRegistry) -> framework::api::TypeRef {
                        if registry.reserve("Status", ::std::any::type_name::<Self>()) {
                            registry.define(framework::api::TypeDefinition::Enum { name: "Status".to_owned(), values: vec!["Active".to_owned(), "Inactive".to_owned()] });
                        }
                        framework::api::TypeRef::Ref { name: "Status".to_owned() }
                    }
                }
            }
            .to_string()
        );

        Ok(())
    }

    #[test]
    fn api_type_with_serde_rename() {
        let Err(rename_all_error) = build(quote! {
            #[derive(Serialize, ApiType)]
            #[serde(rename_all = "camelCase")]
            struct Request {
                user_name: String,
            }
        }) else {
            panic!("must fail");
        };
        assert_eq!(rename_all_error.to_string(), "#[serde(rename_all)] is not supported by #[derive(ApiType)], api uses rust names");

        let Err(rename_error) = build(quote! {
            struct Request {
                #[serde(default, rename = "userName")]
                user_name: String,
            }
        }) else {
            panic!("must fail");
        };
        assert_eq!(rename_error.to_string(), "#[serde(rename)] is not supported by #[derive(ApiType)], api uses rust names");

        let Err(variant_error) = build(quote! {
            enum Status {
                #[serde(rename = "active")]
                Active,
            }
        }) else {
            panic!("must fail");
        };
        assert_eq!(variant_error.to_string(), "#[serde(rename)] is not supported by #[derive(ApiType)], api uses rust names");

        let Err(enum_error) = build(quote! {
            #[serde(rename_all(serialize = "lowercase"))]
            enum Status {
                Active,
            }
        }) else {
            panic!("must fail");
        };
        assert_eq!(enum_error.to_string(), "#[serde(rename_all)] is not supported by #[derive(ApiType)], api uses rust names");
    }
}
