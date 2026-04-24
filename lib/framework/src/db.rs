use std::sync::Arc;
use std::time::Duration;

use futures::TryFutureExt;
use tokio::time::timeout;
use tokio_postgres::CancelToken;
pub use tokio_postgres::Client;
pub use tokio_postgres::Config;
pub use tokio_postgres::Error as PgError;
use tokio_postgres::NoTls;
pub use tokio_postgres::Row;
pub use tokio_postgres::types::ToSql;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::error;

use crate::exception;
use crate::exception::Exception;
use crate::pool::ResourceManager;
use crate::pool::ResourcePool;

pub mod repository;

struct Connection {
    client: Client,
    cancel_token: CancelToken,
}

pub type QueryParam = dyn ToSql + Sync;

impl ResourceManager for ConnectionManager {
    type Target = Connection;

    async fn create(&self) -> Result<Self::Target, Exception> {
        let (client, connection) = self.config.connect(NoTls).await?;

        // use native tokio spawn, not wire current span
        tokio::task::spawn(async {
            if let Err(e) = connection.await {
                error!("connection error: {e}");
            }
        });

        let cancel_token = client.cancel_token();

        Ok(Connection { client, cancel_token })
    }

    async fn is_valid(item: &Self::Target) -> bool {
        item.client.check_connection().await.is_ok()
    }

    fn is_closed(item: &Self::Target) -> bool {
        item.client.is_closed()
    }
}

struct ConnectionManager {
    config: Config,
}

pub struct Database {
    pool: Arc<ResourcePool<ConnectionManager>>,
    query_timeout: Duration,
}

impl Database {
    pub fn new(config: Config) -> Self {
        let pool = Arc::new(ResourcePool::new(
            ConnectionManager { config },
            50,
            Duration::from_secs(30),
            Duration::from_hours(1),
            Duration::from_secs(5),
        ));

        Database { pool, query_timeout: Duration::from_secs(5) }
    }
}

pub async fn execute(database: &Database, statement: &str, params: &[&QueryParam]) -> Result<u64, Exception> {
    async {
        let connection = database.pool.get_with_timeout().await?;

        let updated_rows = with_timeout(
            connection
                .client
                .execute(statement, params)
                .map_err(|err| exception!(message = "failed to execute", source = err)),
            database.query_timeout,
            &connection.cancel_token,
        )
        .await?;

        debug!(db_write_rows = updated_rows, "stats");

        Ok(updated_rows)
    }
    .instrument(debug_span!("db"))
    .await
}

pub async fn select_one<T>(database: &Database, statement: &str, params: &[&QueryParam]) -> Result<Option<T>, Exception>
where
    T: TryFrom<Row, Error = PgError>,
{
    async {
        let connection = database.pool.get_with_timeout().await?;

        let row = with_timeout(
            connection
                .client
                .query_opt(statement, params)
                .map_err(|err| exception!(message = "failed to select_one", source = err)),
            database.query_timeout,
            &connection.cancel_token,
        )
        .await?;

        debug!(db_read_rows = if row.is_some() { 1 } else { 0 }, "stats");

        row.map(T::try_from).transpose().map_err(|err| exception!(message = "failed to map row", source = err))
    }
    .instrument(debug_span!("db"))
    .await
}

async fn with_timeout<T>(
    operation: impl Future<Output = Result<T, Exception>>,
    query_timeout: Duration,
    cancel_token: &CancelToken,
) -> Result<T, Exception> {
    let result = timeout(query_timeout, operation).await;
    match result {
        Ok(result) => result,
        Err(_elapsed) => {
            debug!("cancel query");
            let cancel_result = cancel_token.cancel_query(NoTls).await;
            match cancel_result {
                Ok(_) => Err(exception!(message = "query timed out")),
                Err(err) => Err(exception!(message = "query timed out, failed to cancel", source = err)),
            }
        }
    }
}
