use proc_macro2::TokenStream;
use quote::quote;
use syn::Error;
use syn::Ident;
use syn::Result;

use crate::model;
use crate::model::StructModel;

pub(crate) fn entity_impl(tokens: TokenStream) -> Result<TokenStream> {
    let model = model::parse_struct(tokens)?;
    let model = parse_entity(model)?;

    let from_row = from_row_impl(&model);

    let insert_auto_increment = if model.primary_key && model.auto_increment {
        insert_with_auto_increment_primary_key_impl(&model)
    } else {
        quote! {}
    };

    let insert = if model.primary_key && !model.auto_increment {
        insert_impl(&model)
    } else {
        quote! {}
    };

    Ok(quote! {
        #from_row
        #insert_auto_increment
        #insert
    })
}

struct EntityModel {
    r#struct: Ident,
    table: String,
    columns: Vec<ColumnModel>,
    primary_key: bool,
    auto_increment: bool,
}

struct ColumnModel {
    field: Ident,
    column: String,
    primary_key: bool,
    auto_increment: bool,
}

fn parse_entity(model: StructModel) -> Result<EntityModel> {
    let table = model.attrs.get("table")?.string_meta_value("name")?;

    let mut columns = vec![];

    let mut primary_key_fields = 0;
    let mut found_auto_increment = false;

    for field in model.fields {
        let mut primary_key = false;
        let mut auto_increment = false;
        if let Some(attr) = field.attrs.get_optional("primary_key") {
            primary_key = true;
            primary_key_fields += 1;

            if attr.has_meta("auto_increment") {
                if field.r#type != "Option<i64>" {
                    return Err(Error::new_spanned(
                        &field.ident,
                        "auto_increment primary_key field must have type `Option<i64>",
                    ));
                }
                if found_auto_increment {
                    return Err(Error::new_spanned(
                        &field.ident,
                        "only one auto_increment primary_key field is allowed",
                    ));
                }
                auto_increment = true;
                found_auto_increment = true;
            }
        }

        if primary_key_fields > 1 && found_auto_increment {
            return Err(Error::new_spanned(&field.ident, "primary_key fields must not mix auto_increment and not"));
        }

        columns.push(ColumnModel {
            field: field.ident,
            column: field.attrs.get("column")?.string_meta_value("name")?,
            primary_key,
            auto_increment,
        });
    }

    Ok(EntityModel {
        r#struct: model.ident,
        table,
        columns,
        primary_key: primary_key_fields > 0,
        auto_increment: found_auto_increment,
    })
}

fn from_row_impl(model: &EntityModel) -> TokenStream {
    let assignments = model.columns.iter().map(|column| {
        let field = &column.field;
        let column = &column.column;
        quote! { #field: row.try_get(#column)?, }
    });
    let struct_name = &model.r#struct;
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

fn insert_with_auto_increment_primary_key_impl(model: &EntityModel) -> TokenStream {
    let struct_name = &model.r#struct;
    let table = &model.table;
    let primary_key = &model.columns.iter().find(|column| column.auto_increment).unwrap().column;
    let insert_fields: Vec<_> = model.columns.iter().filter(|column| !column.auto_increment).collect();
    let insert_columns = insert_fields.iter().map(|column| column.column.as_str()).collect::<Vec<_>>().join(", ");
    let placeholders = (1..=insert_fields.len()).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO \"{table}\" ({insert_columns}) VALUES ({placeholders}) RETURNING {primary_key}");
    let params = insert_fields.iter().map(|column| {
        let field = &column.field;
        quote! { &self.#field, }
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

fn insert_impl(model: &EntityModel) -> TokenStream {
    let struct_name = &model.r#struct;
    let table = &model.table;
    let insert_columns = model.columns.iter().map(|column| column.column.as_str()).collect::<Vec<_>>().join(", ");
    let placeholders = (1..=model.columns.len()).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO \"{table}\" ({insert_columns}) VALUES ({placeholders})");

    let primary_key_columns = model
        .columns
        .iter()
        .filter(|column| column.primary_key)
        .map(|column| column.column.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    let non_primary_key_fields: Vec<_> = model.columns.iter().filter(|column| !column.primary_key).collect();
    let update_set = non_primary_key_fields
        .iter()
        .map(|column| format!("{} = EXCLUDED.{}", column.column, column.column))
        .collect::<Vec<_>>()
        .join(", ");

    let sql_ignore = format!("{sql} ON CONFLICT DO NOTHING");
    let sql_upsert = format!(
        "{sql} ON CONFLICT ({primary_key_columns}) DO UPDATE SET {update_set} RETURNING (xmax = 0) AS inserted"
    );

    let params: Vec<_> = model
        .columns
        .iter()
        .map(|column| {
            let field = &column.field;
            quote! { &self.#field, }
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
