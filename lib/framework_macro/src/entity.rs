use proc_macro2::TokenStream;
use quote::quote;
use syn::Error;
use syn::Ident;
use syn::Result;

use crate::model::FieldModel;
use crate::model::{self};

pub(crate) fn entity_impl(tokens: TokenStream) -> Result<TokenStream> {
    let model = model::parse(tokens)?;
    let name = &model.ident;

    let table_name = model.attrs.get("table")?.string_meta_value("name")?;

    let fields: Vec<EntityField> = model.fields.iter().map(EntityField::parse).collect::<Result<_>>()?;

    let auto_increment_count = fields.iter().filter(|f| f.auto_increment_pk).count();
    if auto_increment_count > 1 {
        return Err(Error::new_spanned(name, "only one auto_increment primary key is allowed"));
    }
    let has_auto_increment_pk = auto_increment_count == 1;
    let has_assigned_pk = fields.iter().any(|f| f.assigned_pk);
    if has_auto_increment_pk && has_assigned_pk {
        return Err(Error::new_spanned(name, "primary key must be either auto increment or assigned"));
    }

    let from_row = from_row_impl(name, &fields);

    let insert_auto_incr = if has_auto_increment_pk {
        insert_with_auto_increment_id_impl(name, &table_name, &fields)
    } else {
        quote! {}
    };

    let insert = if has_assigned_pk {
        insert_impl(name, &table_name, &fields)
    } else {
        quote! {}
    };

    Ok(quote! {
        #from_row
        #insert_auto_incr
        #insert
    })
}

struct EntityField<'a> {
    model: &'a FieldModel,
    column: String,
    auto_increment_pk: bool,
    assigned_pk: bool,
}

impl<'a> EntityField<'a> {
    fn parse(field: &'a FieldModel) -> Result<Self> {
        let column = field.attrs.get("column")?.string_meta_value("name")?;
        let (auto_increment_pk, assigned_pk) = parse_pk(field)?;
        Ok(EntityField { model: field, column, auto_increment_pk, assigned_pk })
    }
}

fn parse_pk(field: &FieldModel) -> Result<(bool, bool)> {
    let Some(pk_attr) = field.attrs.get_optional("primary_key") else {
        return Ok((false, false));
    };
    if pk_attr.has_meta_path("auto_increment") {
        if field.r#type != "Option<i64>" {
            return Err(Error::new_spanned(
                &field.ident,
                "`#[primary_key(auto_increment)]` field must have type `Option<i64>`",
            ));
        }
        Ok((true, false))
    } else {
        Ok((false, true))
    }
}

fn from_row_impl(struct_name: &Ident, fields: &[EntityField]) -> TokenStream {
    let assignments = fields.iter().map(|f| {
        let fname = &f.model.ident;
        let col = &f.column;
        quote! { #fname: row.try_get(#col)?, }
    });
    quote! {
        impl ::std::convert::TryFrom<framework::db::Row> for #struct_name {
            type Error = framework::db::PgError;
            fn try_from(row: framework::db::Row) -> ::std::result::Result<#struct_name, framework::db::PgError> {
                Ok(#struct_name {
                    #(#assignments)*
                })
            }
        }
    }
}

fn insert_with_auto_increment_id_impl(struct_name: &Ident, table_name: &str, fields: &[EntityField]) -> TokenStream {
    let pk_col = fields.iter().find(|f| f.auto_increment_pk).unwrap().column.as_str();
    let ins_fields: Vec<_> = fields.iter().filter(|f| !f.auto_increment_pk).collect();
    let cols = ins_fields.iter().map(|f| f.column.as_str()).collect::<Vec<_>>().join(", ");
    let placeholders = (1..=ins_fields.len()).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO \"{table_name}\" ({cols}) VALUES ({placeholders}) RETURNING {pk_col}");
    let params = ins_fields.iter().map(|f| {
        let fname = &f.model.ident;
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

fn insert_impl(struct_name: &Ident, table_name: &str, fields: &[EntityField]) -> TokenStream {
    let cols = fields.iter().map(|f| f.column.as_str()).collect::<Vec<_>>().join(", ");
    let placeholders = (1..=fields.len()).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO \"{table_name}\" ({cols}) VALUES ({placeholders})");

    let pk_cols = fields.iter().filter(|f| f.assigned_pk).map(|f| f.column.as_str()).collect::<Vec<_>>().join(", ");
    let non_pk_fields: Vec<_> = fields.iter().filter(|f| !f.assigned_pk).collect();
    let update_set =
        non_pk_fields.iter().map(|f| format!("{} = EXCLUDED.{}", f.column, f.column)).collect::<Vec<_>>().join(", ");

    let sql_ignore = format!("{sql} ON CONFLICT DO NOTHING");
    let sql_upsert =
        format!("{sql} ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set} RETURNING (xmax = 0) AS inserted");

    let params: Vec<_> = fields
        .iter()
        .map(|f| {
            let fname = &f.model.ident;
            quote! { &self.#fname, }
        })
        .collect();

    quote! {
        impl framework::db::Insert for #struct_name {
            async fn __insert(&self, client: &framework::db::Client) -> ::std::result::Result<u64, framework::db::PgError> {
                client.execute(#sql, &[#(#params)*]).await
            }
            async fn __insert_ignore(&self, client: &framework::db::Client) -> ::std::result::Result<u64, framework::db::PgError> {
                client.execute(#sql_ignore, &[#(#params)*]).await
            }
            async fn __upsert(&self, client: &framework::db::Client) -> ::std::result::Result<bool, framework::db::PgError> {
                client.query_one_scalar(#sql_upsert, &[#(#params)*]).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::entity_impl;

    #[test]
    fn test_entity_with_assigned_id() {
        let source = quote! {
            #[derive(Entity)]
            #[table(name = "test_entity")]
            struct TestEntity {
                #[primary_key]
                #[column(name = "id")]
                id: i32,
                #[column(name = "col1")]
                col1: String,
            }
        };

        let output = entity_impl(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                impl ::std::convert::TryFrom<framework::db::Row> for TestEntity {
                    type Error = framework::db::PgError;
                    fn try_from(row: framework::db::Row) -> ::std::result::Result<TestEntity, framework::db::PgError> {
                        Ok(TestEntity {
                            id: row.try_get("id")?,
                            col1: row.try_get("col1")?,
                        })
                    }
                }

                impl framework::db::Insert for TestEntity {
                    async fn __insert(
                        &self,
                        client: &framework::db::Client,
                    ) -> ::std::result::Result<u64, framework::db::PgError> {
                        client.execute("INSERT INTO \"test_entity\" (id, col1) VALUES ($1, $2)", &[&self.id, &self.col1,]).await
                    }
                    async fn __insert_ignore(
                        &self,
                        client: &framework::db::Client,
                    ) -> ::std::result::Result<u64, framework::db::PgError> {
                        client.execute("INSERT INTO \"test_entity\" (id, col1) VALUES ($1, $2) ON CONFLICT DO NOTHING", &[&self.id, &self.col1,]).await
                    }
                    async fn __upsert(
                        &self,
                        client: &framework::db::Client,
                    ) -> ::std::result::Result<bool, framework::db::PgError> {
                        client.query_one_scalar("INSERT INTO \"test_entity\" (id, col1) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET col1 = EXCLUDED.col1 RETURNING (xmax = 0) AS inserted", &[&self.id, &self.col1,]).await
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn test_entity_with_auto_increment_id() {
        let source = quote! {
            #[derive(Entity)]
            #[table(name = "test_entity")]
            struct TestEntity {
                #[primary_key(auto_increment)]
                #[column(name = "id")]
                id: Option<i64>,
                #[column(name = "col1")]
                col1: Option<String>,
            }
        };

        let output = entity_impl(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                impl ::std::convert::TryFrom<framework::db::Row> for TestEntity {
                    type Error = framework::db::PgError;
                    fn try_from(row: framework::db::Row) -> ::std::result::Result<TestEntity, framework::db::PgError> {
                        Ok(TestEntity {
                            id: row.try_get("id")?,
                            col1: row.try_get("col1")?,
                        })
                    }
                }

                impl framework::db::InsertWithAutoIncrementId for TestEntity {
                    async fn __insert(
                        &self,
                        client: &framework::db::Client,
                    ) -> ::std::result::Result<i64, framework::db::PgError> {
                        client.query_one_scalar("INSERT INTO \"test_entity\" (col1) VALUES ($1) RETURNING id", &[&self.col1,]).await
                    }
                }
            }
            .to_string()
        );
    }
}
