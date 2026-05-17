use std::collections::HashMap;
use std::time::Duration;

use framework::exception;
use framework::exception::Exception;
use framework::pool::ResourceManager;
use tokio::task;
use tokio::time::timeout;
use tokio_postgres::CancelToken;
use tokio_postgres::Client;
use tokio_postgres::Config;
use tokio_postgres::NoTls;
use tokio_postgres::Statement;
use tracing::debug;
use tracing::error;

use crate::PgError;

pub(crate) struct Connection {
    pub client: Client,
    cancel_token: CancelToken,
    statement_cache: HashMap<String, Statement>,
}

impl Connection {
    pub(crate) async fn prepared_statement(&mut self, sql: &str) -> Result<Statement, Exception> {
        if let Some(statement) = self.statement_cache.get(sql) {
            Ok(statement.clone())
        } else {
            let statement = self
                .client
                .prepare(sql)
                .await
                .map_err(|err| exception!("failed to prepare statement", source = err))?;
            self.statement_cache.insert(sql.to_owned(), statement.clone());
            Ok(statement)
        }
    }

    pub(crate) async fn with_timeout<T>(
        &self,
        operation: impl Future<Output = Result<T, PgError>>,
        query_timeout: Duration,
    ) -> Result<T, Exception> {
        let result = timeout(query_timeout, operation).await;
        match result {
            Ok(result) => result.map_err(|err| exception!("failed to call db", source = err)),
            Err(_elapsed) => {
                debug!("cancel query");
                let cancel_result = self.cancel_token.cancel_query(NoTls).await;
                match cancel_result {
                    Ok(()) => Err(exception!("query timed out")),
                    Err(err) => Err(exception!("query timed out, failed to cancel", source = err)),
                }
            }
        }
    }
}

pub(crate) struct ConnectionManager {
    pub config: Config,
}

impl ResourceManager for ConnectionManager {
    type Target = Connection;

    async fn create(&self) -> Result<Self::Target, Exception> {
        let (client, connection) = self.config.connect(NoTls).await?;

        // use native tokio spawn, not wire current span
        task::spawn(async {
            if let Err(e) = connection.await {
                error!("connection error: {e}");
            }
        });

        let cancel_token = client.cancel_token();

        Ok(Connection { client, cancel_token, statement_cache: HashMap::new() })
    }

    async fn is_valid(item: &Self::Target) -> bool {
        item.client.check_connection().await.is_ok()
    }

    fn is_closed(item: &Self::Target) -> bool {
        item.client.is_closed()
    }
}
