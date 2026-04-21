use proc_macro2::TokenStream;
use quote::ToTokens;
use syn::Attribute;
use syn::Data::Struct;
use syn::DeriveInput;
use syn::Error;
use syn::Expr;
use syn::Field;
use syn::Fields::Named;
use syn::FieldsNamed;
use syn::Ident;
use syn::Lit;
use syn::Meta;
use syn::Result;
use syn::Token;
use syn::parse2;
use syn::punctuated::Punctuated;
use syn::token::Comma;

pub(crate) struct StructModel {
    pub(crate) ident: Ident,
    pub(crate) attrs: AttributesModel,
    pub(crate) fields: Vec<FieldModel>,
}

impl StructModel {}

pub(crate) struct FieldModel {
    pub(crate) ident: Ident,
    pub(crate) r#type: String,
    pub(crate) attrs: AttributesModel,
}

impl FieldModel {
    pub(crate) fn is_optional(&self) -> bool {
        self.r#type.starts_with("Option<")
    }
}

pub(crate) struct AttributesModel {
    parent_ident: Ident,
    attrs: Vec<AttributeModel>,
}

impl AttributesModel {
    pub(crate) fn get(&self, name: &'static str) -> Result<&AttributeModel> {
        self.get_optional(name)
            .ok_or_else(|| Error::new_spanned(&self.parent_ident, format!("can not find {name} attribute")))
    }

    pub(crate) fn get_optional(&self, name: &'static str) -> Option<&AttributeModel> {
        self.attrs.iter().find(|model| model.attr.path().is_ident(name))
    }
}

pub(crate) struct AttributeModel {
    attr: Attribute,
}

impl AttributeModel {
    pub(crate) fn meta_value(&self, name: &str) -> Result<Lit> {
        let nested = self.attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;

        for meta in nested {
            if let Meta::NameValue(name_value) = meta
                && name_value.path.is_ident(name)
                && let Expr::Lit(lit) = name_value.value
            {
                return Ok(lit.lit);
            }
        }
        Err(Error::new_spanned(&self.attr, format!("can not find meta, name={name}")))
    }

    pub(crate) fn string_meta_value(&self, name: &str) -> Result<String> {
        let lit = self.meta_value(name)?;
        if let Lit::Str(value) = lit {
            Ok(value.value())
        } else {
            Err(Error::new_spanned(&self.attr, format!("meta is not string, name={name}")))
        }
    }

    pub(crate) fn has_meta(&self, name: &str) -> bool {
        let Ok(nested) = self.attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated) else {
            return false;
        };
        nested.iter().any(|meta| matches!(meta, Meta::Path(p) if p.is_ident(name)))
    }

    pub(crate) fn int_meta_value(&self, name: &str) -> Result<i32> {
        let lit = self.meta_value(name)?;
        if let Lit::Int(value) = lit {
            Ok(value.base10_parse::<i32>()?)
        } else {
            Err(Error::new_spanned(&self.attr, format!("meta is not int, name={name}")))
        }
    }
}

pub(crate) fn parse_struct(tokens: TokenStream) -> Result<StructModel> {
    let ast: DeriveInput = parse2(tokens)?;
    let ident = ast.ident;
    let attrs = ast.attrs.into_iter().map(|attr| AttributeModel { attr }).collect();

    let fields: Punctuated<Field, Comma> = if let Struct(data_struct) = ast.data {
        if let Named(FieldsNamed { named, .. }) = data_struct.fields {
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
            let ident = field.ident.expect("field should be named");
            let attrs = field.attrs.into_iter().map(|attr| AttributeModel { attr }).collect();
            let r#type = field.ty.to_token_stream().to_string().replace(" ", "");
            FieldModel { ident: ident.clone(), r#type, attrs: AttributesModel { parent_ident: ident, attrs } }
        })
        .collect();

    Ok(StructModel { ident: ident.clone(), attrs: AttributesModel { parent_ident: ident, attrs }, fields })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::parse_struct;

    #[test]
    fn test_parse_with_entity_macro() -> syn::Result<()> {
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
        assert_eq!(model.attrs.get("table")?.string_meta_value("name")?, "test_entity");

        assert_eq!(model.fields.len(), 3);
        assert_eq!(model.fields[0].ident, "id");
        assert_eq!(model.fields[0].r#type, "i32");
        assert_eq!(model.fields[1].ident, "col1");
        assert_eq!(model.fields[1].r#type, "String");
        assert_eq!(model.fields[2].ident, "col2");
        assert_eq!(model.fields[2].r#type, "Option<i32>");

        assert_eq!(model.fields[0].attrs.attrs.len(), 2);
        assert_eq!(model.fields[0].attrs.get("column")?.string_meta_value("name")?, "id");
        assert!(model.fields[0].attrs.get_optional("primary_key").is_some());
        assert_eq!(model.fields[1].attrs.attrs.len(), 1);
        assert_eq!(model.fields[1].attrs.get("column")?.string_meta_value("name")?, "col1");
        assert_eq!(model.fields[2].attrs.get("column")?.string_meta_value("name")?, "col2");

        Ok(())
    }

    #[test]
    fn test_parse_with_validate_macro() -> syn::Result<()> {
        let tokens = quote! {
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
            }
        };

        let model = parse_struct(tokens)?;
        assert_eq!(model.ident, "TestBean");

        assert_eq!(model.fields.len(), 4);
        assert_eq!(model.fields[0].ident, "col1");
        assert_eq!(model.fields[0].r#type, "i32");
        assert_eq!(model.fields[1].r#type, "Vec<String>");

        assert_eq!(model.fields[0].attrs.attrs.len(), 1);
        assert_eq!(model.fields[0].attrs.get("range")?.int_meta_value("min")?, 2);
        assert_eq!(model.fields[0].attrs.get("range")?.int_meta_value("max")?, 100);

        assert_eq!(model.fields[1].attrs.get("length")?.int_meta_value("min")?, 1);
        assert_eq!(model.fields[1].attrs.get("length")?.int_meta_value("max")?, 10);

        assert!(model.fields[2].attrs.get_optional("not_blank").is_some());
        assert!(model.fields[3].attrs.get_optional("validate").is_some());

        Ok(())
    }
}
