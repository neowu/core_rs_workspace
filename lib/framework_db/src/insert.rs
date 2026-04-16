use quote::quote;
use syn::Ident;

use crate::FieldInfo;

pub(crate) fn insert_with_auto_increment_id_impl(
    struct_name: &Ident,
    table_name: &String,
    fields: &Vec<(&syn::Field, FieldInfo)>,
) -> proc_macro2::TokenStream {
    let pk_col = fields
        .iter()
        .find(|(_, i)| i.auto_increment_pk)
        .unwrap()
        .1
        .column
        .as_str();
    let ins_fields: Vec<_> = fields.iter().filter(|(_, i)| !i.auto_increment_pk).collect();
    let cols = ins_fields
        .iter()
        .map(|(_, i)| i.column.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (1..=ins_fields.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO \"{table_name}\" ({cols}) VALUES ({placeholders}) RETURNING {pk_col}");
    let params = ins_fields.iter().map(|(f, _)| {
        let fname = &f.ident;
        quote! { &self.#fname, }
    });
    quote! {
        impl framework::db::InsertWithAutoIncrementId for #struct_name {
            async fn __insert(
                &self,
                client: &tokio_postgres::Client,
            ) -> ::std::result::Result<tokio_postgres::Row, tokio_postgres::Error> {
                client.query_one(#sql, &[#(#params)*]).await
            }
        }
    }
}

pub(crate) fn insert_impl(
    struct_name: &Ident,
    table_name: &str,
    fields: &Vec<(&syn::Field, FieldInfo)>,
) -> proc_macro2::TokenStream {
    let cols = fields
        .iter()
        .map(|(_, i)| i.column.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let placeholders = (1..=fields.len())
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("INSERT INTO \"{table_name}\" ({cols}) VALUES ({placeholders})");
    let params = fields.iter().map(|(f, _)| {
        let fname = &f.ident;
        quote! { &self.#fname, }
    });
    quote! {
        impl framework::db::Insert for #struct_name {
            async fn __insert(
                &self,
                client: &tokio_postgres::Client,
            ) -> ::std::result::Result<u64, tokio_postgres::Error> {
                client.execute(#sql, &[#(#params)*]).await
            }
        }
    }
}
