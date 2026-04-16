use quote::quote;

use crate::FieldInfo;

pub(crate) fn from_row_impl(
    struct_name: &syn::Ident,
    fields: &Vec<(&syn::Field, FieldInfo)>,
) -> proc_macro2::TokenStream {
    // TryFrom<Row, Error=PgError>: map all fields from their column names
    let assignments = fields.iter().map(|(field, info)| {
        let fname = &field.ident;
        let col = &info.column;
        quote! { #fname: row.try_get(#col)?, }
    });

    let try_from_impl = quote! {
        impl ::std::convert::TryFrom<framework::db::Row> for #struct_name {
            type Error = framework::db::PgError;
            fn try_from(row: framework::db::Row) -> ::std::result::Result<#struct_name, framework::db::PgError> {
                Ok(#struct_name {
                    #(#assignments)*
                })
            }
        }
    };
    try_from_impl
}
