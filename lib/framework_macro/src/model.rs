use std::fmt::Display;

use proc_macro2::TokenStream;
use quote::ToTokens as _;
use syn::Attribute;
use syn::Data;
use syn::DeriveInput;
use syn::Error;
use syn::Expr;
use syn::Field;
use syn::Fields;
use syn::FieldsNamed;
use syn::Ident;
use syn::Lit;
use syn::LitInt;
use syn::Meta;
use syn::Result;
use syn::Token;
use syn::Type;
use syn::Visibility;
use syn::parse2;
use syn::punctuated::Punctuated;
use syn::token::Comma;

pub(crate) struct StructModel {
    pub(crate) ident: Ident,
    attrs: Vec<AttributeModel>,
    pub(crate) fields: Vec<FieldModel>,
}

impl StructModel {
    pub(crate) fn attrs(&self, attr_name: &'static str) -> impl Iterator<Item = &AttributeModel> {
        self.attrs.iter().filter(move |attr| attr.attr.path().is_ident(attr_name))
    }

    pub(crate) fn attr(&self, attr_name: &'static str) -> Result<&AttributeModel> {
        self.attrs
            .iter()
            .find(|attr| attr.attr.path().is_ident(attr_name))
            .ok_or_else(|| Error::new_spanned(&self.ident, format!("can not find {attr_name} attribute")))
    }
}

pub(crate) struct EnumModel {
    pub(crate) ident: Ident,
    attrs: Vec<AttributeModel>,
    pub(crate) variants: Vec<VariantModel>,
}

impl EnumModel {
    pub(crate) fn attrs(&self, attr_name: &'static str) -> impl Iterator<Item = &AttributeModel> {
        self.attrs.iter().filter(move |attr| attr.attr.path().is_ident(attr_name))
    }
}

pub(crate) struct VariantModel {
    pub(crate) ident: Ident,
    attrs: Vec<AttributeModel>,
}

impl VariantModel {
    pub(crate) fn attrs(&self, attr_name: &'static str) -> impl Iterator<Item = &AttributeModel> {
        self.attrs.iter().filter(move |attr| attr.attr.path().is_ident(attr_name))
    }
}

pub(crate) enum DeriveModel {
    Struct(StructModel),
    Enum(EnumModel),
}

pub(crate) struct FieldModel {
    pub(crate) ident: Ident,
    pub(crate) vis: Visibility,
    pub(crate) ty: Type,
    pub(crate) field_type: String,
    attrs: Vec<AttributeModel>,
}

/// validation rules declared by `#[range]`, `#[length]`, `#[not_blank]`, shared by `Validate` and `ApiType` derives.
#[derive(Default)]
pub(crate) struct ConstraintsModel {
    pub(crate) not_blank: bool,
    pub(crate) min: Option<LitInt>,
    pub(crate) max: Option<LitInt>,
    pub(crate) min_length: Option<LitInt>,
    pub(crate) max_length: Option<LitInt>,
}

impl FieldModel {
    pub(crate) fn is_optional_type(&self) -> bool {
        self.field_type.starts_with("Option<")
    }

    pub(crate) fn is_vec_type(&self) -> bool {
        self.field_type.starts_with("Vec<")
    }

    pub(crate) fn is_optional_vec_type(&self) -> bool {
        self.field_type.starts_with("Option<Vec<")
    }

    // includes Option<String>, since length validation applies to the inner value
    pub(crate) fn is_string_type(&self) -> bool {
        self.field_type == "String" || self.field_type == "Option<String>"
    }

    pub(crate) fn attr(&self, attr_name: &'static str) -> Result<&AttributeModel> {
        self.optional_attr(attr_name)
            .ok_or_else(|| Error::new_spanned(&self.ident, format!("can not find {attr_name} attribute")))
    }

    pub(crate) fn optional_attr(&self, attr_name: &'static str) -> Option<&AttributeModel> {
        self.attrs.iter().find(|attr| attr.attr.path().is_ident(attr_name))
    }

    pub(crate) fn attrs(&self, attr_name: &'static str) -> impl Iterator<Item = &AttributeModel> {
        self.attrs.iter().filter(move |attr| attr.attr.path().is_ident(attr_name))
    }

    pub(crate) fn constraints(&self) -> Result<ConstraintsModel> {
        let mut constraints = ConstraintsModel { not_blank: self.optional_attr("not_blank").is_some(), ..Default::default() };
        if let Some(attr) = self.optional_attr("range") {
            constraints.min = attr.optional_int_meta_value("min")?;
            constraints.max = attr.optional_int_meta_value("max")?;
        }
        if let Some(attr) = self.optional_attr("length") {
            constraints.min_length = attr.optional_int_meta_value("min")?;
            constraints.max_length = attr.optional_int_meta_value("max")?;
        }
        Ok(constraints)
    }
}

pub(crate) struct AttributeModel {
    attr: Attribute,
}

impl AttributeModel {
    pub(crate) fn error(&self, message: impl Display) -> Error {
        Error::new_spanned(&self.attr, message)
    }

    // matches any meta kind, e.g. `rename = "x"`, `rename_all(serialize = "x")` or bare `rename`
    pub(crate) fn has_meta(&self, meta_name: &str) -> bool {
        let Ok(nested) = self.attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) else {
            return false;
        };
        nested.iter().any(|meta| meta.path().is_ident(meta_name))
    }

    // Meta::Path is different from Meta::NameValue
    pub(crate) fn has_meta_path(&self, meta_name: &str) -> bool {
        let Ok(nested) = self.attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) else {
            return false;
        };
        nested.iter().any(|meta| matches!(meta, Meta::Path(path) if path.is_ident(meta_name)))
    }

    pub(crate) fn optional_meta_value(&self, meta_name: &str) -> Result<Option<Lit>> {
        let nested = self.attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in nested {
            if let Meta::NameValue(name_value) = meta
                && name_value.path.is_ident(meta_name)
                && let Expr::Lit(lit) = name_value.value
            {
                return Ok(Some(lit.lit));
            }
        }
        Ok(None)
    }

    pub(crate) fn string_meta_value(&self, meta_name: &str) -> Result<String> {
        let lit = self
            .optional_meta_value(meta_name)?
            .ok_or_else(|| Error::new_spanned(&self.attr, format!("can not find meta {meta_name}")))?;
        if let Lit::Str(value) = lit {
            Ok(value.value())
        } else {
            Err(Error::new_spanned(&self.attr, format!("meta {meta_name} value is not string")))
        }
    }

    pub(crate) fn optional_int_meta_value(&self, meta_name: &str) -> Result<Option<LitInt>> {
        let Some(lit) = self.optional_meta_value(meta_name)? else {
            return Ok(None);
        };
        if let Lit::Int(value) = lit {
            Ok(Some(value))
        } else {
            Err(Error::new_spanned(&self.attr, format!("meta {meta_name} is not int")))
        }
    }
}

pub(crate) fn parse_struct(tokens: TokenStream) -> Result<StructModel> {
    let ast: DeriveInput = parse2(tokens)?;
    struct_model(ast.ident, ast.attrs, ast.data)
}

/// parses struct with named fields, or enum with unit variants only, generic types are not supported.
pub(crate) fn parse_derive_input(tokens: TokenStream) -> Result<DeriveModel> {
    let ast: DeriveInput = parse2(tokens)?;
    if !ast.generics.params.is_empty() {
        return Err(Error::new_spanned(&ast.generics, "derive does not support generic type"));
    }
    match ast.data {
        Data::Enum(data) => {
            let variants = data
                .variants
                .into_iter()
                .map(|variant| {
                    if matches!(variant.fields, Fields::Unit) {
                        let attrs = variant.attrs.into_iter().map(|attr| AttributeModel { attr }).collect();
                        Ok(VariantModel { ident: variant.ident, attrs })
                    } else {
                        Err(Error::new_spanned(&variant, "enum variant must not have fields"))
                    }
                })
                .collect::<Result<Vec<VariantModel>>>()?;
            let attrs = ast.attrs.into_iter().map(|attr| AttributeModel { attr }).collect();
            Ok(DeriveModel::Enum(EnumModel { ident: ast.ident, attrs, variants }))
        }
        data @ (Data::Struct(_) | Data::Union(_)) => Ok(DeriveModel::Struct(struct_model(ast.ident, ast.attrs, data)?)),
    }
}

fn struct_model(ident: Ident, attrs: Vec<Attribute>, data: Data) -> Result<StructModel> {
    let attrs = attrs.into_iter().map(|attr| AttributeModel { attr }).collect();

    let fields: Punctuated<Field, Comma> = if let Data::Struct(data_struct) = data {
        if let Fields::Named(FieldsNamed { named, .. }) = data_struct.fields {
            named
        } else {
            return Err(Error::new_spanned(&ident, "derive struct can only have named fields"));
        }
    } else {
        return Err(Error::new_spanned(&ident, "derive target must be struct"));
    };

    let fields = fields
        .into_iter()
        .map(|field| {
            let field_ident = field.ident.expect("field must be named");
            let field_attrs = field.attrs.into_iter().map(|attr| AttributeModel { attr }).collect();
            let field_type = field.ty.to_token_stream().to_string().replace(' ', "");
            FieldModel { ident: field_ident, vis: field.vis, ty: field.ty, field_type, attrs: field_attrs }
        })
        .collect();

    Ok(StructModel { ident, attrs, fields })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::DeriveModel;
    use super::parse_derive_input;
    use super::parse_struct;

    #[test]
    fn parse_struct_with_entity_macro() -> syn::Result<()> {
        let tokens = quote! {
            #[derive(Entity)]
            #[table(name = "test_entity")]
            struct TestEntity {
                #[primary_key]
                #[column(name = "id")]
                id: i32,
                #[column(name = "col1")]
                col1: String,
                #[column(name = "col2")]
                col2: Option<i32>,
            }
        };

        let model = parse_struct(tokens)?;
        assert_eq!(model.ident, "TestEntity");
        assert_eq!(model.attr("table")?.string_meta_value("name")?, "test_entity");

        assert_eq!(model.fields.len(), 3);
        assert_eq!(model.fields[0].ident, "id");
        assert_eq!(model.fields[0].field_type, "i32");
        assert_eq!(model.fields[1].ident, "col1");
        assert_eq!(model.fields[1].field_type, "String");
        assert_eq!(model.fields[2].ident, "col2");
        assert_eq!(model.fields[2].field_type, "Option<i32>");

        assert_eq!(model.fields[0].attrs.len(), 2);
        assert_eq!(model.fields[0].attr("column")?.string_meta_value("name")?, "id");
        assert!(model.fields[0].optional_attr("primary_key").is_some());
        assert_eq!(model.fields[1].attrs.len(), 1);
        assert_eq!(model.fields[1].attr("column")?.string_meta_value("name")?, "col1");
        assert_eq!(model.fields[2].attr("column")?.string_meta_value("name")?, "col2");

        Ok(())
    }

    #[test]
    fn parse_struct_with_validate_macro() -> syn::Result<()> {
        let tokens = quote! {
            #[derive(Validate)]
            struct TestBean {
                #[range(min = 2, max = 100)]
                col1: i32,
                #[length(min = 1, max = 10)]
                col2: Vec<String>,
                #[not_blank]
                col3: Option<String>,
                #[validate]
                col4: Child,
            }
        };

        let model = parse_struct(tokens)?;
        assert_eq!(model.ident, "TestBean");

        assert_eq!(model.fields.len(), 4);
        assert_eq!(model.fields[0].ident, "col1");
        assert_eq!(model.fields[0].field_type, "i32");
        assert_eq!(model.fields[1].field_type, "Vec<String>");

        assert_eq!(model.fields[0].attrs.len(), 1);
        let range = model.fields[0].attr("range")?;
        assert_eq!(range.optional_int_meta_value("min")?.unwrap().base10_digits(), "2");
        assert_eq!(range.optional_int_meta_value("max")?.unwrap().base10_digits(), "100");

        let length = model.fields[1].attr("length")?;
        assert_eq!(length.optional_int_meta_value("min")?.unwrap().base10_digits(), "1");
        assert_eq!(length.optional_int_meta_value("max")?.unwrap().base10_digits(), "10");

        assert!(model.fields[2].optional_attr("not_blank").is_some());
        assert!(model.fields[2].is_optional_type());
        assert!(model.fields[3].optional_attr("validate").is_some());

        let range_constraints = model.fields[0].constraints()?;
        assert_eq!(range_constraints.min.unwrap().base10_digits(), "2");
        assert_eq!(range_constraints.max.unwrap().base10_digits(), "100");
        assert!(!range_constraints.not_blank);
        let not_blank_constraints = model.fields[2].constraints()?;
        assert!(not_blank_constraints.not_blank);
        assert!(not_blank_constraints.min.is_none());

        Ok(())
    }

    #[test]
    fn parse_derive_input_with_enum() -> syn::Result<()> {
        let tokens = quote! {
            #[derive(ApiType)]
            enum Status {
                Active,
                Inactive,
            }
        };

        let DeriveModel::Enum(model) = parse_derive_input(tokens)? else {
            panic!("must be enum");
        };
        assert_eq!(model.ident, "Status");
        assert_eq!(model.variants.len(), 2);
        assert_eq!(model.variants[0].ident, "Active");

        let Err(variant_error) = parse_derive_input(quote! {
            enum Status {
                Active(String),
            }
        }) else {
            panic!("must fail");
        };
        assert_eq!(variant_error.to_string(), "enum variant must not have fields");

        let Err(generic_error) = parse_derive_input(quote! {
            struct Page<T> {
                items: Vec<T>,
            }
        }) else {
            panic!("must fail");
        };
        assert_eq!(generic_error.to_string(), "derive does not support generic type");

        Ok(())
    }
}
