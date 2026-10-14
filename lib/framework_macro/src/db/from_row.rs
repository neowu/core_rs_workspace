use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;
use syn::Result;

use crate::model;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let model = model::parse_struct(tokens)?;
    let columns = model
        .fields
        .iter()
        .map(|field| Ok((&field.ident, field.attr("column")?.string_meta_value("name")?)))
        .collect::<Result<Vec<_>>>()?;
    Ok(from_row_impl(&model.ident, &columns))
}

pub(crate) fn from_row_impl(struct_ident: &Ident, columns: &[(&Ident, String)]) -> TokenStream {
    let assignments = columns.iter().map(|(field, column)| quote! { #field: row.try_get(#column)?, });
    quote! {
        impl framework_db::FromRow for #struct_ident {
            fn try_from(row: framework_db::Row) -> ::std::result::Result<#struct_ident, framework_db::PgError> {
                Ok(#struct_ident {
                    #(#assignments)*
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::build;

    #[test]
    fn build_from_row() {
        let source = quote! {
            #[derive(FromRow)]
            struct TableStats {
                #[column(name = "schema")]
                schema: String,
                #[column(name = "relkind")]
                kind: String,
                #[column(name = "size")]
                size: i64,
            }
        };

        let output = build(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                impl framework_db::FromRow for TableStats {
                    fn try_from(row: framework_db::Row) -> ::std::result::Result<TableStats, framework_db::PgError> {
                        Ok(TableStats {
                            schema: row.try_get("schema")?,
                            kind: row.try_get("relkind")?,
                            size: row.try_get("size")?,
                        })
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn build_from_row_without_column() {
        let source = quote! {
            #[derive(FromRow)]
            struct TableStats {
                #[column(name = "schema")]
                schema: String,
                size: i64,
            }
        };

        let error = build(source).err().unwrap();
        assert_eq!(error.to_string(), "can not find column attribute");
    }
}
