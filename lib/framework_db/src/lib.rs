use proc_macro2::TokenStream;
use quote::quote;
use syn::Data;
use syn::DeriveInput;
use syn::Fields;
use syn::parse2;

mod from_row;
mod insert;

/// Derive `framework::db::Entity<T>` for a struct.
///
/// ## Struct attributes
/// - `#[table(name = "table_name")]` — postgres table name, panic if not defined
///
/// ## Field attributes
/// - `#[primary_key(auto_increment)]` — auto increment pk, excluded from INSERT, must be `Option<i64>`, only one allowed
/// - `#[primary_key]` — assigned pk, included in INSERT
/// - `#[column(name = "col_name")]` — column name override, panic if not defined
#[proc_macro_derive(Entity, attributes(table, column, primary_key))]
pub fn entity(item: proc_macro::TokenStream) -> proc_macro::TokenStream {
    entity_impl(item.into()).into()
}

pub(crate) fn entity_impl(item: TokenStream) -> TokenStream {
    let input: DeriveInput = parse2(item).unwrap();
    let struct_name = &input.ident;
    let table_name = parse_table_name(&input);
    let fields = parse_fields(&input);

    let from_row_impl = from_row::from_row_impl(struct_name, &fields);

    let auto_increment_count = fields.iter().filter(|(_, i)| i.auto_increment_pk).count();
    if auto_increment_count > 1 {
        panic!("only one auto_increment primary key is allowed");
    }
    let has_auto_increment_pk = auto_increment_count == 1;
    let has_assigned_pk = fields.iter().any(|(_, i)| i.assigned_pk);
    if has_auto_increment_pk && has_assigned_pk {
        panic!("primary key must be either auto increment or assigned");
    }

    let insert_with_auto_increment_id_impl = if has_auto_increment_pk {
        insert::insert_with_auto_increment_id_impl(struct_name, &table_name, &fields)
    } else {
        quote! {}
    };

    let insert = if has_assigned_pk {
        insert::insert_impl(struct_name, &table_name, &fields)
    } else {
        quote! {}
    };

    quote! {
        #from_row_impl

        #insert_with_auto_increment_id_impl

        #insert
    }
}

fn parse_fields(input: &DeriveInput) -> Vec<(&syn::Field, FieldInfo)> {
    let named_fields = match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(named) => &named.named,
            _ => panic!("Entity derive only supports structs with named fields"),
        },
        _ => panic!("Entity derive only supports structs"),
    };

    named_fields
        .iter()
        .map(|f| {
            let field_name = f.ident.as_ref().unwrap().to_string();
            let info = parse_field_info(f, &field_name);
            (f, info)
        })
        .collect()
}

struct FieldInfo {
    column: String,
    auto_increment_pk: bool,
    assigned_pk: bool,
}

fn parse_table_name(input: &DeriveInput) -> String {
    let attr = input
        .attrs
        .iter()
        .find(|a| a.path().is_ident("table"))
        .unwrap_or_else(|| panic!("#[table(name = \"...\")] is required on struct `{}`", input.ident));

    let mut name = None;
    let _ = attr.parse_nested_meta(|meta| {
        if meta.path.is_ident("name") {
            let lit: syn::LitStr = meta.value()?.parse()?;
            name = Some(lit.value());
            Ok(())
        } else {
            Err(meta.error("unknown table attribute key"))
        }
    });
    name.unwrap_or_else(|| panic!("#[table(name = \"...\")] name is required on struct `{}`", input.ident))
}

fn parse_field_info(field: &syn::Field, field_name: &str) -> FieldInfo {
    let (auto_increment_pk, assigned_pk) = parse_primary_key(field);

    let col_name = field
        .attrs
        .iter()
        .find(|a| a.path().is_ident("column"))
        .map(|attr| {
            let mut name = None;
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    name = Some(lit.value());
                    Ok(())
                } else {
                    Err(meta.error("unknown column attribute key"))
                }
            });
            name.unwrap_or_else(|| panic!("#[column(name = \"...\")] name is required on field `{field_name}`"))
        })
        .unwrap_or_else(|| panic!("#[column(name = \"...\")] is required on field `{field_name}`"));

    FieldInfo {
        column: col_name,
        auto_increment_pk,
        assigned_pk,
    }
}

fn parse_primary_key(field: &syn::Field) -> (bool, bool) {
    for attr in &field.attrs {
        if attr.path().is_ident("primary_key") {
            let mut auto_increment = false;
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("auto_increment") {
                    auto_increment = true;
                    Ok(())
                } else {
                    Err(meta.error("unknown primary_key argument, expected `auto_increment`"))
                }
            });
            return if auto_increment { (true, false) } else { (false, true) };
        }
    }
    (false, false)
}

#[cfg(test)]
mod tests {
    use quote::quote;

    use crate::entity_impl;

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

        let output = entity_impl(source);

        assert_eq!(output.to_string(), quote! {
                impl From < framework::db::Row> for TestEntity {
                    fn from (row: framework::db::Row) -> TestEntity {
                        TestEntity {
                            id: row.get("id"),
                            col1: row.get("col1"),
                        }
                    }
                }

                impl framework::db::Insert for TestEntity {
                    async fn __insert(&self, client: &tokio_postgres::Client,) -> ::std::result::Result<u64, tokio_postgres::Error> {
                        client.execute("INSERT INTO \"test_entity\" (id, col1) VALUES ($1, $2)", &[&self.id, &self.col1,]).await
                    }
                }
        }.to_string());
    }

    #[test]
    fn test_entity_with_auto_increment_id() {
        let source = quote! {
            #[derive(Entity)]
            #[table(name = "test_entity")]
            struct TestEntity {
                #[primary_key(auto_increment)]
                #[column(name = "id")]
                id: i32,
                #[column(name = "col1")]
                col1: Option<String>,
            }
        };

        let output = entity_impl(source);

        assert_eq!(output.to_string(), quote! {
                impl From < framework::db::Row> for TestEntity {
                    fn from (row: framework::db::Row) -> TestEntity {
                        TestEntity {
                            id: row.get("id"),
                            col1: row.get("col1"),
                        }
                    }
                }

                impl framework::db::InsertWithAutoIncrementId for TestEntity {
                    async fn __insert(&self, client: &tokio_postgres::Client,) -> ::std::result::Result<tokio_postgres::Row, tokio_postgres::Error> {
                        client.query_one("INSERT INTO \"test_entity\" (col1) VALUES ($1) RETURNING id", &[&self.col1,]).await
                    }
                }
        }.to_string());
    }
}
