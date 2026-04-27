use proc_macro2::TokenStream;
use quote::quote;
use syn::Error;
use syn::Ident;
use syn::Result;

use crate::model;
use crate::model::StructModel;

pub(crate) fn build(tokens: TokenStream) -> Result<TokenStream> {
    let model = model::parse_struct(tokens)?;
    let model = parse_entity(model)?;

    let from_row_impl = from_row_impl(&model);

    let insert_impl = if model.has_primary_key {
        if model.has_auto_increment_primary_key { insert_auto_increment_impl(&model) } else { insert_impl(&model) }
    } else {
        quote! {}
    };

    let entity_impl = if model.has_primary_key {
        entity_impl(&model)
    } else {
        quote! {}
    };

    Ok(quote! {
        #from_row_impl
        #insert_impl
        #entity_impl
    })
}

struct EntityModel {
    struct_ident: Ident,
    table: String,
    columns: Vec<ColumnModel>,
    has_primary_key: bool,
    has_auto_increment_primary_key: bool,
}

struct ColumnModel {
    field_ident: Ident,
    field_type: String,
    column: String,
    primary_key: bool,
    auto_increment: bool,
}

fn parse_entity(model: StructModel) -> Result<EntityModel> {
    let table = model.attr("table")?.string_meta_value("name")?;

    let mut columns = vec![];

    let mut primary_key_fields = 0;
    let mut found_auto_increment = false;

    for field in model.fields {
        let mut primary_key = false;
        let mut auto_increment = false;
        if let Some(attr) = field.optional_attr("primary_key") {
            primary_key = true;
            primary_key_fields += 1;

            if attr.has_meta_path("auto_increment") {
                if field.field_type != "Option<i64>" {
                    return Err(Error::new_spanned(
                        &field.ident,
                        "#[primary_key(auto_increment)] field must have type `Option<i64>",
                    ));
                }
                if found_auto_increment {
                    return Err(Error::new_spanned(
                        &field.ident,
                        "only one #[primary_key(auto_increment)] field is allowed",
                    ));
                }
                auto_increment = true;
                found_auto_increment = true;
            }
        }

        if primary_key_fields > 1 && found_auto_increment {
            return Err(Error::new_spanned(
                &field.ident,
                "cannot mix #[primary_key] and #[primary_key(auto_increment)] fields",
            ));
        }

        let column = field.attr("column")?.string_meta_value("name")?;

        columns.push(ColumnModel {
            field_ident: field.ident,
            field_type: field.field_type,
            column,
            primary_key,
            auto_increment,
        });
    }

    Ok(EntityModel {
        struct_ident: model.ident,
        table,
        columns,
        has_primary_key: primary_key_fields > 0,
        has_auto_increment_primary_key: found_auto_increment,
    })
}

fn from_row_impl(model: &EntityModel) -> TokenStream {
    let assignments = model.columns.iter().map(|column| {
        let field = &column.field_ident;
        let column = &column.column;
        quote! { #field: row.try_get(#column)?, }
    });
    let struct_name = &model.struct_ident;
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

fn insert_auto_increment_impl(model: &EntityModel) -> TokenStream {
    let struct_name = &model.struct_ident;
    let table = &model.table;
    let primary_key = &model.columns.iter().find(|column| column.auto_increment).unwrap().column;
    let insert_fields = model.columns.iter().filter(|column| !column.auto_increment).collect::<Vec<_>>();
    let insert_columns = insert_fields.iter().map(|column| column.column.as_str()).collect::<Vec<_>>().join(", ");
    let placeholders = (1..=insert_fields.len()).map(|i| format!("${i}")).collect::<Vec<_>>().join(", ");
    let sql = format!("INSERT INTO \"{table}\" ({insert_columns}) VALUES ({placeholders}) RETURNING {primary_key}");
    let params = insert_fields.iter().map(|column| {
        let field = &column.field_ident;
        quote! { &self.#field as &framework::db::QueryParam, }
    });
    quote! {
        impl framework::db::repository::InsertWithAutoIncrementId for #struct_name {
            #[inline]
            fn __insert_sql() -> &'static str {
                #sql
            }
            #[inline]
            fn __insert_params(&self) -> ::std::vec::Vec<&framework::db::QueryParam> {
                vec![#(#params)*]
            }
        }
    }
}

fn insert_impl(model: &EntityModel) -> TokenStream {
    let struct_name = &model.struct_ident;
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
            let field = &column.field_ident;
            quote! { &self.#field as &framework::db::QueryParam, }
        })
        .collect();

    quote! {
        impl framework::db::repository::Insert for #struct_name {
            fn __insert_sql() -> &'static str {
                #sql
            }
            fn __insert_ignore_sql() -> &'static str {
                #sql_ignore
            }
            fn __upsert_sql() -> &'static str {
                #sql_upsert
            }
            fn __insert_params(&self) -> ::std::vec::Vec<&framework::db::QueryParam> {
                vec![#(#params)*]
            }
        }
    }
}

fn entity_impl(model: &EntityModel) -> TokenStream {
    let struct_name = &model.struct_ident;
    let table = &model.table;
    let all_columns = model.columns.iter().map(|column| column.column.as_str()).collect::<Vec<_>>().join(", ");
    let primary_key_columns: Vec<_> = model.columns.iter().filter(|column| column.primary_key).collect();
    let where_clause = primary_key_columns
        .iter()
        .enumerate()
        .map(|(index, column)| format!("{} = ${}", column.column, index + 1))
        .collect::<Vec<_>>()
        .join(" AND ");

    let select_sql = format!("SELECT {all_columns} FROM \"{table}\"");

    let get_sql = format!("{select_sql} WHERE {where_clause}");

    let delete_sql = format!("DELETE FROM \"{table}\" WHERE {where_clause}");

    let id_types: Vec<proc_macro2::TokenStream> = primary_key_columns
        .iter()
        .map(|column| {
            let field_type = if column.auto_increment { "i64" } else { column.field_type.as_ref() };
            field_type.parse().unwrap()
        })
        .collect();

    let (id_type, ids_params) = if primary_key_columns.len() == 1 {
        let id_type = &id_types[0];
        (quote! { #id_type }, quote! { vec![ids as &framework::db::QueryParam] })
    } else {
        let id_indices = (0..primary_key_columns.len()).map(syn::Index::from);
        (quote! { (#(#id_types,)*) }, quote! { vec![#(&ids.#id_indices as &framework::db::QueryParam,)*] })
    };

    quote! {
        impl framework::db::repository::Entity for #struct_name {
            type Id = #id_type;
            #[inline]
            fn __id_params(ids: &Self::Id) -> ::std::vec::Vec<&framework::db::QueryParam> {
                #ids_params
            }
            #[inline]
            fn __get_sql() -> &'static str {
                #get_sql
            }
            #[inline]
            fn __select_sql() -> &'static str {
                #select_sql
            }
            #[inline]
            fn __delete_sql() -> &'static str {
                #delete_sql
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use super::build;

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

        let output = build(source).unwrap();

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

                impl framework::db::repository::Insert for TestEntity {
                    fn __insert_sql() -> &'static str {
                        "INSERT INTO \"test_entity\" (id, col1) VALUES ($1, $2)"
                    }
                    fn __insert_ignore_sql() -> &'static str {
                        "INSERT INTO \"test_entity\" (id, col1) VALUES ($1, $2) ON CONFLICT DO NOTHING"
                    }
                    fn __upsert_sql() -> &'static str {
                        "INSERT INTO \"test_entity\" (id, col1) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET col1 = EXCLUDED.col1 RETURNING (xmax = 0) AS inserted"
                    }
                    fn __insert_params(&self) -> ::std::vec::Vec<&framework::db::QueryParam> {
                        vec![&self.id as &framework::db::QueryParam, &self.col1 as &framework::db::QueryParam,]
                    }
                }

                impl framework::db::repository::Entity for TestEntity {
                    type Id = i32;
                    #[inline]
                    fn __id_params(ids: &Self::Id) -> ::std::vec::Vec<&framework::db::QueryParam> {
                        vec![ids as &framework::db::QueryParam]
                    }
                    #[inline]
                    fn __get_sql() -> &'static str {
                        "SELECT id, col1 FROM \"test_entity\" WHERE id = $1"
                    }
                    #[inline]
                    fn __select_sql() -> &'static str {
                        "SELECT id, col1 FROM \"test_entity\""
                    }
                    #[inline]
                    fn __delete_sql() -> &'static str {
                        "DELETE FROM \"test_entity\" WHERE id = $1"
                    }
                }
            }
            .to_string()
        );
    }

    #[test]
    fn test_entity_with_composite_id() {
        let source = quote! {
            #[derive(Entity)]
            #[table(name = "test_entity")]
            struct TestEntity {
                #[primary_key]
                #[column(name = "id1")]
                id1: i32,
                #[primary_key]
                #[column(name = "id2")]
                id2: String,
                #[column(name = "col1")]
                col1: String,
            }
        };

        let output = build(source).unwrap();

        assert_eq!(
            output.to_string(),
            quote! {
                impl ::std::convert::TryFrom<framework::db::Row> for TestEntity {
                    type Error = framework::db::PgError;
                    fn try_from(row: framework::db::Row) -> ::std::result::Result<TestEntity, framework::db::PgError> {
                        Ok(TestEntity {
                            id1: row.try_get("id1")?,
                            id2: row.try_get("id2")?,
                            col1: row.try_get("col1")?,
                        })
                    }
                }

                impl framework::db::repository::Insert for TestEntity {
                    fn __insert_sql() -> &'static str {
                        "INSERT INTO \"test_entity\" (id1, id2, col1) VALUES ($1, $2, $3)"
                    }
                    fn __insert_ignore_sql() -> &'static str {
                        "INSERT INTO \"test_entity\" (id1, id2, col1) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING"
                    }
                    fn __upsert_sql() -> &'static str {
                        "INSERT INTO \"test_entity\" (id1, id2, col1) VALUES ($1, $2, $3) ON CONFLICT (id1, id2) DO UPDATE SET col1 = EXCLUDED.col1 RETURNING (xmax = 0) AS inserted"
                    }
                    fn __insert_params(&self) -> ::std::vec::Vec<&framework::db::QueryParam> {
                        vec![&self.id1 as &framework::db::QueryParam, &self.id2 as &framework::db::QueryParam, &self.col1 as &framework::db::QueryParam,]
                    }
                }

                impl framework::db::repository::Entity for TestEntity {
                    type Id = (i32, String,);
                    #[inline]
                    fn __id_params(ids: &Self::Id) -> ::std::vec::Vec<&framework::db::QueryParam> {
                        vec![&ids.0 as &framework::db::QueryParam, &ids.1 as &framework::db::QueryParam, ]
                    }
                    #[inline]
                    fn __get_sql() -> &'static str {
                        "SELECT id1, id2, col1 FROM \"test_entity\" WHERE id1 = $1 AND id2 = $2"
                    }
                    #[inline]
                    fn __select_sql() -> &'static str {
                        "SELECT id1, id2, col1 FROM \"test_entity\""
                    }
                    #[inline]
                    fn __delete_sql() -> &'static str {
                        "DELETE FROM \"test_entity\" WHERE id1 = $1 AND id2 = $2"
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

        let output = build(source).unwrap();

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

                impl framework::db::repository::InsertWithAutoIncrementId for TestEntity {
                    #[inline]
                    fn __insert_sql() -> &'static str {
                        "INSERT INTO \"test_entity\" (col1) VALUES ($1) RETURNING id"
                    }
                    #[inline]
                    fn __insert_params(&self) -> ::std::vec::Vec<&framework::db::QueryParam> {
                        vec![&self.col1 as &framework::db::QueryParam,]
                    }
                }

                impl framework::db::repository::Entity for TestEntity {
                    type Id = i64;
                    #[inline]
                    fn __id_params(ids: &Self::Id) -> ::std::vec::Vec<&framework::db::QueryParam> {
                        vec![ids as &framework::db::QueryParam]
                    }
                    #[inline]
                    fn __get_sql() -> &'static str {
                        "SELECT id, col1 FROM \"test_entity\" WHERE id = $1"
                    }
                    #[inline]
                    fn __select_sql() -> &'static str {
                        "SELECT id, col1 FROM \"test_entity\""
                    }
                    #[inline]
                    fn __delete_sql() -> &'static str {
                        "DELETE FROM \"test_entity\" WHERE id = $1"
                    }
                }
            }
            .to_string()
        );
    }
}
