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
                client: &framework::db::Client,
            ) -> ::std::result::Result<i64, framework::db::PgError> {
                client.query_one_scalar(#sql, &[#(#params)*]).await
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

    let pk_cols = fields
        .iter()
        .filter(|(_, i)| i.assigned_pk)
        .map(|(_, i)| i.column.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let non_pk_fields: Vec<_> = fields.iter().filter(|(_, i)| !i.assigned_pk).collect();
    let update_set = non_pk_fields
        .iter()
        .map(|(_, i)| format!("{} = EXCLUDED.{}", i.column.as_str(), i.column.as_str()))
        .collect::<Vec<_>>()
        .join(", ");

    let sql_ignore = format!("{sql} ON CONFLICT DO NOTHING");
    let sql_upsert =
        format!("{sql} ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set} RETURNING (xmax = 0) AS inserted");

    let params: Vec<_> = fields
        .iter()
        .map(|(f, _)| {
            let fname = &f.ident;
            quote! { &self.#fname, }
        })
        .collect();

    quote! {
        impl framework::db::Insert for #struct_name {
            async fn __insert(
                &self,
                client: &framework::db::Client,
            ) -> ::std::result::Result<u64, framework::db::PgError> {
                client.execute(#sql, &[#(#params)*]).await
            }
            async fn __insert_ignore(
                &self,
                client: &framework::db::Client,
            ) -> ::std::result::Result<u64, framework::db::PgError> {
                client.execute(#sql_ignore, &[#(#params)*]).await
            }
            async fn __upsert(
                &self,
                client: &framework::db::Client,
            ) -> ::std::result::Result<bool, framework::db::PgError> {
                client.query_one_scalar(#sql_upsert, &[#(#params)*]).await
            }
        }
    }
}
