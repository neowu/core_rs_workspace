use proc_macro2::TokenStream;
use quote::quote;
use syn::Result;

use crate::model;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let model = model::parse_struct(tokens)?;
    let table = model.attr("table")?.string_meta_value("name")?;

    let struct_ident = &model.ident;
    let struct_name = struct_ident.to_string();
    let column_names: Vec<String> = model.fields.iter().map(|field| field.ident.to_string()).collect();

    // clickhouse::Row members are #[doc(hidden)] and semver-exempt; this mirrors what
    // clickhouse-macros 0.3.0 generates for an owned struct with named fields, so a
    // clickhouse crate upgrade may require revisiting this impl.
    Ok(quote! {
        #[automatically_derived]
        impl framework_clickhouse::clickhouse::Row for #struct_ident {
            const NAME: &'static str = #struct_name;
            const COLUMN_NAMES: &'static [&'static str] = &[#(#column_names,)*];
            const COLUMN_COUNT: usize = <Self as framework_clickhouse::clickhouse::Row>::COLUMN_NAMES.len();
            const KIND: framework_clickhouse::clickhouse::_priv::RowKind = framework_clickhouse::clickhouse::_priv::RowKind::Struct;
            type Value<'__v> = Self;
        }
        impl framework_clickhouse::Table for #struct_ident {
            const NAME: &'static str = #table;
        }
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::build;

    #[test]
    fn build_row() {
        let source = quote! {
            #[derive(Row, Serialize)]
            #[table(name = "action")]
            struct ActionRow {
                #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
                timestamp: DateTime<Utc>,
                id: String,
                elapsed: i64,
            }
        };

        let output = build(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                #[automatically_derived]
                impl framework_clickhouse::clickhouse::Row for ActionRow {
                    const NAME: &'static str = "ActionRow";
                    const COLUMN_NAMES: &'static [&'static str] = &["timestamp", "id", "elapsed",];
                    const COLUMN_COUNT: usize = <Self as framework_clickhouse::clickhouse::Row>::COLUMN_NAMES.len();
                    const KIND: framework_clickhouse::clickhouse::_priv::RowKind = framework_clickhouse::clickhouse::_priv::RowKind::Struct;
                    type Value<'__v> = Self;
                }
                impl framework_clickhouse::Table for ActionRow {
                    const NAME: &'static str = "action";
                }
            }
            .to_string()
        );
    }

    #[test]
    fn build_row_without_table() {
        let source = quote! {
            #[derive(Row, Serialize)]
            struct ActionRow {
                id: String,
            }
        };

        build(source).unwrap_err();
    }
}
