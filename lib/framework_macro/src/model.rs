use proc_macro2::TokenStream;
use quote::ToTokens as _;
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
    pub(crate) fn attr(&self, attr_name: &'static str) -> Result<&AttributeModel> {
        self.attrs
            .iter()
            .find(|attr| attr.attr.path().is_ident(attr_name))
            .ok_or_else(|| Error::new_spanned(&self.ident, format!("can not find {attr_name} attribute")))
    }
}

pub(crate) struct FieldModel {
    pub(crate) ident: Ident,
    pub(crate) vis: Visibility,
    pub(crate) field_type: String,
    attrs: Vec<AttributeModel>,
}

impl FieldModel {
    pub(crate) fn attr(&self, attr_name: &'static str) -> Result<&AttributeModel> {
        self.optional_attr(attr_name)
            .ok_or_else(|| Error::new_spanned(&self.ident, format!("can not find {attr_name} attribute")))
    }

    pub(crate) fn optional_attr(&self, attr_name: &'static str) -> Option<&AttributeModel> {
        self.attrs.iter().find(|attr| attr.attr.path().is_ident(attr_name))
    }
}

pub(crate) struct AttributeModel {
    attr: Attribute,
}

impl AttributeModel {
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
            let field_ident = field.ident.expect("field must be named");
            let field_attrs = field.attrs.into_iter().map(|attr| AttributeModel { attr }).collect();
            let field_type = field.ty.to_token_stream().to_string().replace(' ', "");
            FieldModel { ident: field_ident, vis: field.vis, field_type, attrs: field_attrs }
        })
        .collect();

    Ok(StructModel { ident, attrs, fields })
}

#[cfg(test)]
mod tests {
    use quote::quote;

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
}
