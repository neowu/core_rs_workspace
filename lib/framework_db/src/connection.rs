use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::time::Duration;

use framework::console;
use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::pool::ResourceManager;
use tokio::time::timeout;
use tokio_postgres::CancelToken;
use tokio_postgres::Client;
use tokio_postgres::Config;
use tokio_postgres::NoTls;
use tokio_postgres::Statement;

use crate::PgError;

#[doc(hidden)] // only reachable via Executor, used by framework
pub struct Connection {
    pub(crate) client: Client,
    cancel_token: CancelToken,
    statement_cache: HashMap<String, Statement>,
    query_timeout: Duration,
    // set on any error, the connection is discarded at release instead of returned to the pool
    broken: AtomicBool,
}

impl Connection {
    pub(crate) async fn prepared_statement(&mut self, sql: &str) -> Result<Statement, Exception> {
        if let Some(statement) = self.statement_cache.get(sql) {
            Ok(statement.clone())
        } else {
            let statement = self.with_timeout(self.client.prepare(sql)).await?;
            self.statement_cache.insert(sql.to_owned(), statement.clone());
            Ok(statement)
        }
    }

    pub(crate) async fn with_timeout<T>(
        &self,
        operation: impl Future<Output = Result<T, PgError>>,
    ) -> Result<T, Exception> {
        // marks broken unless the operation succeeds, including when this future is dropped by caller (e.g. outer
        // timeout or select!), the sent query still runs on the connection and may abort the transaction
        let mut guard = BrokenGuard { connection: self, succeeded: false };
        let result = timeout(self.query_timeout, operation).await;
        match result {
            Ok(Ok(value)) => {
                guard.succeeded = true;
                Ok(value)
            }
            Ok(Err(err)) => Err(exception!("failed to call db", source = err)),
            Err(_elapsed) => {
                // the timed out query is still in flight and cancel is racy, never reuse this connection
                log!("cancel query");
                let cancel_result = self.cancel_token.cancel_query(NoTls).await;
                match cancel_result {
                    Ok(()) => Err(exception!("query timed out")),
                    Err(err) => Err(exception!("query timed out, failed to cancel", source = err)),
                }
            }
        }
    }

    pub(crate) fn mark_broken(&self) {
        self.broken.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_broken(&self) -> bool {
        self.broken.load(Ordering::Relaxed)
    }
}

struct BrokenGuard<'a> {
    connection: &'a Connection,
    succeeded: bool,
}

impl Drop for BrokenGuard<'_> {
    fn drop(&mut self) {
        if !self.succeeded {
            self.connection.mark_broken();
        }
    }
}

pub(crate) struct ConnectionManager {
    pub config: Config,
    pub query_timeout: Duration,
}

impl ResourceManager for ConnectionManager {
    type Target = Connection;

    async fn create(&self) -> Result<Self::Target, Exception> {
        let (client, connection) =
            self.config.connect(NoTls).await.map_err(|err| exception!("failed to connect to db", source = err))?;

        // use native tokio spawn, not wire current span
        tokio::spawn(async {
            if let Err(e) = connection.await {
                console!("ERROR db connection error, {e}");
            }
        });

        let cancel_token = client.cancel_token();

        Ok(Connection {
            client,
            cancel_token,
            statement_cache: HashMap::new(),
            query_timeout: self.query_timeout,
            broken: AtomicBool::new(false),
        })
    }

    async fn is_valid(item: &Self::Target) -> bool {
        item.client.check_connection().await.is_ok()
    }

    fn is_closed(item: &Self::Target) -> bool {
        item.client.is_closed() || item.is_broken()
    }
}
