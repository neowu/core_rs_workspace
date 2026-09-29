use std::fmt::Debug;
use std::marker::PhantomData;

pub use clickhouse;
use clickhouse::_priv::RowKind;
use clickhouse::Client;
use clickhouse::Row;
use clickhouse::RowOwned;
use clickhouse::RowRead;
use clickhouse::error::Error;
use clickhouse::query::Query;
use framework::console;
use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::span;
use framework::stats;
pub use framework_macro::Enum8;
use serde::Serialize;
pub mod types;

// clickhouse's Bind trait is sealed and not object-safe, so params can't be `&[&dyn Bind]`
// like framework_db's `&[&dyn ToSql]`; this wrapper folds each param into query.bind().
pub trait QueryParam: Debug + Sync {
    fn bind(&self, query: Query) -> Query;
}

impl<T: Serialize + Debug + Sync> QueryParam for T {
    fn bind(&self, query: Query) -> Query {
        query.bind(self)
    }
}

pub struct ClickHouse {
    client: Client,
}

impl ClickHouse {
    pub fn new(uri: &str, user: &str, password: &str, database: Option<&str>) -> Self {
        console!("create clickhouse client, uri={uri}, user={user}, db={database:?}");
        let client = Client::default().with_url(uri).with_user(user).with_password(password);
        let client = if let Some(database) = database { client.with_database(database) } else { client };

        Self { client }
    }

    // each `?` in sql is replaced client-side by the corresponding param, in order; use `??` for a literal `?`
    pub async fn execute(&self, sql: &str, params: &[&dyn QueryParam]) -> Result<(), Exception> {
        let _span = span!("clickhouse");
        log!("execute, sql={sql}, params={params:?}");
        let mut query = self.client.query(sql);
        for param in params {
            query = param.bind(query);
        }
        query.execute().await.map_err(|err| {
            exception!(
                format!("failed to execute statement, error={}", clickhouse_error_code(&err)),
                code = "CLICKHOUSE_ERROR",
                source = err
            )
        })
    }

    pub async fn select_one<T>(&self, sql: &str, params: &[&dyn QueryParam]) -> Result<Option<T>, Exception>
    where
        T: RowOwned + RowRead,
    {
        let _span = span!("clickhouse");
        log!("select_one, sql={sql}, params={params:?}");
        let mut query = self.client.query(sql);
        for param in params {
            query = param.bind(query);
        }
        let row = query.fetch_optional().await.map_err(|err| {
            exception!(
                format!("failed to select one, error={}", clickhouse_error_code(&err)),
                code = "CLICKHOUSE_ERROR",
                source = err
            )
        })?;

        stats!(clickhouse_read_rows = if row.is_some() { 1 } else { 0 });

        Ok(row)
    }

    pub async fn select_all<T>(&self, sql: &str, params: &[&dyn QueryParam]) -> Result<Vec<T>, Exception>
    where
        T: RowOwned + RowRead,
    {
        let _span = span!("clickhouse");
        log!("select_all, sql={sql}, params={params:?}");
        let mut query = self.client.query(sql);
        for param in params {
            query = param.bind(query);
        }
        let rows = query.fetch_all().await.map_err(|err| {
            exception!(
                format!("failed to select all, error={}", clickhouse_error_code(&err)),
                code = "CLICKHOUSE_ERROR",
                source = err
            )
        })?;

        stats!(clickhouse_read_rows = rows.len());

        Ok(rows)
    }

    // rows may own their data or borrow it, e.g. a row pointing into the message it is written for,
    // so a batch costs no copy of the data it already has; either way the row type comes from the slice
    pub async fn insert<T>(&self, table: &str, rows: &[T]) -> Result<(), Exception>
    where
        T: Row + Serialize,
    {
        let _span = span!("clickhouse");
        // previously it used setting .with_setting("async_insert", "1").with_setting("wait_for_async_insert", "0");
        // but found silent data loss on clickhouse, no error on both side, no error in "system.asynchronous_insert_log"
        // so here to use without, message handler will wait until success
        let mut inserter = self.client.inserter::<FixedRow<T>>(table);
        for row in rows {
            inserter.write(row).await.map_err(|err| {
                exception!(
                    format!("failed to insert, error={}", clickhouse_error_code(&err)),
                    code = "CLICKHOUSE_ERROR",
                    source = err
                )
            })?;
        }
        let quantities = inserter.end().await.map_err(|err| {
            exception!(
                format!("failed to commit insert, error={}", clickhouse_error_code(&err)),
                code = "CLICKHOUSE_ERROR",
                source = err
            )
        })?;
        stats!(clickhouse_write_rows = quantities.rows, clickhouse_write_bytes = quantities.bytes);
        Ok(())
    }
}

// the full error goes to the trace via source, the message only carries a short name to keep sql/schema out of responses
fn clickhouse_error_code(err: &Error) -> &str {
    match err {
        // e.g. "Code: 60. DB::Exception: ... (UNKNOWN_TABLE) (version 25.8.1.1 (official build))"
        Error::BadResponse(body) => body
            .rfind(" (version ")
            .and_then(|end| body[..end].strip_suffix(')')?.rsplit_once('('))
            .map(|(_, name)| name)
            .filter(|name| {
                !name.is_empty() && name.bytes().all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_')
            })
            .unwrap_or("BAD_RESPONSE"),
        Error::Network(_) => "NETWORK_ERROR",
        Error::TimedOut => "TIMED_OUT",
        Error::SchemaMismatch(_) => "SCHEMA_MISMATCH",
        Error::InvalidParams(_)
        | Error::Compression(_)
        | Error::Decompression(_)
        | Error::DataFormat(_)
        | Error::RowNotFound
        | Error::SequenceMustHaveLength
        | Error::DeserializeAnyNotSupported
        | Error::NotEnoughData
        | Error::InvalidUtf8Encoding(_)
        | Error::InvalidTagEncoding(_)
        | Error::VariantDiscriminatorIsOutOfBound(_)
        | Error::Custom(_)
        | Error::InvalidColumnsHeader(_)
        | Error::Unsupported(_)
        | Error::Other(_)
        | _ => "CLIENT_ERROR",
    }
}

// clickhouse writes a `T::Value<'_>` - `T` re-bound to the slice's lifetime - and a projection
// can't be inverted to infer `T`, while bounding `T: Row<Value<'a> = T>` next to `RowWrite` trips
// rustc ("one type is more general than the other"). a row whose `Value<'_>` is `T` itself, of
// whatever lifetime, takes `&T` as is. forwards clickhouse's `#[doc(hidden)]` row metadata, which
// is only what `#[derive(Row)]` generates, but isn't covered by semver.
struct FixedRow<T>(PhantomData<T>);

impl<T: Row> Row for FixedRow<T> {
    const NAME: &'static str = T::NAME;
    const COLUMN_NAMES: &'static [&'static str] = T::COLUMN_NAMES;
    const COLUMN_COUNT: usize = T::COLUMN_COUNT;
    const KIND: RowKind = T::KIND;
    type Value<'a> = T;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bad_response(body: &str) -> Error {
        Error::BadResponse(body.to_owned())
    }

    #[test]
    fn code_from_server_error() {
        let err = bad_response(
            "Code: 60. DB::Exception: Unknown table expression identifier 'foo' in scope SELECT * FROM foo. (UNKNOWN_TABLE) (version 25.8.1.1 (official build))",
        );
        assert_eq!(clickhouse_error_code(&err), "UNKNOWN_TABLE");
    }

    #[test]
    fn code_ignores_parens_in_message() {
        let err = bad_response(
            "Code: 62. DB::Exception: Syntax error: failed at position 8 ('(') (line 1, col 8): (SELECT. (SYNTAX_ERROR) (version 25.8.1.1 (official build))",
        );
        assert_eq!(clickhouse_error_code(&err), "SYNTAX_ERROR");
    }

    #[test]
    fn code_without_name_in_response() {
        // empty body falls back to the numeric X-ClickHouse-Exception-Code header, or http status
        assert_eq!(clickhouse_error_code(&bad_response("60")), "BAD_RESPONSE");
        assert_eq!(clickhouse_error_code(&bad_response("502 Bad Gateway")), "BAD_RESPONSE");
        assert_eq!(clickhouse_error_code(&bad_response("upstream error (version 1.0)")), "BAD_RESPONSE");
        assert_eq!(clickhouse_error_code(&bad_response("error () (version 1.0)")), "BAD_RESPONSE");
    }

    #[test]
    fn code_from_client_error() {
        assert_eq!(clickhouse_error_code(&Error::TimedOut), "TIMED_OUT");
        assert_eq!(clickhouse_error_code(&Error::SchemaMismatch("column".to_owned())), "SCHEMA_MISMATCH");
        assert_eq!(clickhouse_error_code(&Error::RowNotFound), "CLIENT_ERROR");
    }
}
