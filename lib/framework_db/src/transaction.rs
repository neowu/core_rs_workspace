use std::ops::DerefMut;

use framework::exception;
use framework::exception::Exception;
use framework::log;
use framework::pool::ResourceGuard;
use framework::span;

use crate::Connection;
use crate::Executor;
use crate::connection::ConnectionManager;

// rollback if dropped without commit/rollback, e.g. returned early by `?`
pub struct Transaction<'a> {
    conn: ResourceGuard<'a, ConnectionManager>,
    completed: bool,
}

impl<'a> Transaction<'a> {
    pub(crate) async fn begin(conn: ResourceGuard<'a, ConnectionManager>) -> Result<Self, Exception> {
        let _span = span!("db");
        log!("begin transaction");
        // simple query protocol, one round trip, no prepare needed
        conn.with_timeout(conn.client.batch_execute("BEGIN")).await?;
        Ok(Transaction { conn, completed: false })
    }

    pub async fn commit(mut self) -> Result<(), Exception> {
        let _span = span!("db");
        log!("commit transaction");
        self.check_broken()?;
        self.conn.with_timeout(self.conn.client.batch_execute("COMMIT")).await?;
        self.completed = true;
        Ok(())
    }

    pub async fn rollback(mut self) -> Result<(), Exception> {
        let _span = span!("db");
        log!("rollback transaction");
        if self.conn.is_broken() {
            // broken connection is discarded at release, server rolls back on disconnect
            self.completed = true;
            return Ok(());
        }
        self.conn.with_timeout(self.conn.client.batch_execute("ROLLBACK")).await?;
        self.completed = true;
        Ok(())
    }

    fn check_broken(&self) -> Result<(), Exception> {
        if self.conn.is_broken() {
            return Err(exception!("transaction connection is broken"));
        }
        Ok(())
    }
}

impl Executor for &mut Transaction<'_> {
    async fn __conn(&mut self) -> Result<impl DerefMut<Target = Connection> + Send + '_, Exception> {
        self.check_broken()?;
        Ok(&mut *self.conn)
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if !self.completed {
            // discard instead of sending ROLLBACK, the connection may still be in transaction or have a query in flight
            log!("transaction dropped without commit or rollback, discard connection");
            self.conn.mark_broken();
        }
    }
}
