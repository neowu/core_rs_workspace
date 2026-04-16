use std::time::Duration;

use tokio_postgres::CancelToken;
use tokio_postgres::Client;
use tokio_postgres::Config;
use tokio_postgres::NoTls;
pub use tokio_postgres::Row;
pub use tokio_postgres::types::ToSql;

use crate::exception;
use crate::exception::Exception;
use crate::pool::ResourceManager;
use crate::pool::ResourcePool;
use crate::task;

pub mod repository;

struct Connection {
    client: Client,
    cancel_token: CancelToken,
}

impl ResourceManager for ConnectionManager {
    type Target = Connection;

    async fn create(&self) -> Result<Self::Target, Exception> {
        let (client, connection) = self.config.connect(NoTls).await?;

        task::spawn_task(async move {
            connection.await?;
            Ok(())
        });

        let cancel_token = client.cancel_token();

        Ok(Connection { client, cancel_token })
    }

    async fn is_valid(item: &Self::Target) -> bool {
        item.client.execute("SELECT 1", &[]).await.is_ok()
    }

    fn is_closed(item: &Self::Target) -> bool {
        item.client.is_closed()
    }
}

struct ConnectionManager {
    config: Config,
}

pub struct Database {
    pool: ResourcePool<ConnectionManager>,
    connection_checkout_timeout: Duration,
}

impl Database {
    pub fn new(config: Config) -> Self {
        Database {
            pool: ResourcePool::new(ConnectionManager { config }, 50, Duration::from_secs(30)),
            connection_checkout_timeout: Duration::from_secs(5),
        }
    }
}

pub async fn execute(database: &Database, statement: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Exception> {
    let connection = database
        .pool
        .get_with_timeout(database.connection_checkout_timeout)
        .await?;

    connection
        .client
        .execute(statement, params)
        .await
        .map_err(|err| exception!(message = "failed to execute", source = err))
}

pub async fn select_one<T>(
    database: &Database,
    statement: &str,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<T>, Exception>
where
    T: From<Row>,
{
    let connection = database
        .pool
        .get_with_timeout(database.connection_checkout_timeout)
        .await?;

    let row = connection
        .client
        .query_one(statement, params)
        .await
        .map_err(|err| exception!(message = "failed to select_one", source = err))?;

    if row.is_empty() {
        Ok(None)
    } else {
        Ok(Some(T::from(row)))
    }
}

#[allow(async_fn_in_trait)]
#[doc(hidden)] // disable auto complete, it's used by framework
pub trait InsertWithAutoIncrementId {
    async fn __insert(&self, client: &tokio_postgres::Client) -> Result<Row, tokio_postgres::Error>;
}

#[allow(async_fn_in_trait)]
#[doc(hidden)] // disable auto complete, it's used by framework
pub trait Insert {
    async fn __insert(&self, client: &tokio_postgres::Client) -> Result<u64, tokio_postgres::Error>;
}
