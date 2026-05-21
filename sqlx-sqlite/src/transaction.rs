use std::future::Future;

use sqlx_core::transaction::TransactionManager;
use sqlx_core::{error::Error, sql_str::SqlStr};

use crate::{Sqlite, SqliteConnection};

/// Implementation of [`TransactionManager`] for SQLite.
pub struct SqliteTransactionManager;

impl TransactionManager for SqliteTransactionManager {
    type Database = Sqlite;

    async fn begin(conn: &mut SqliteConnection, statement: Option<SqlStr>) -> Result<(), Error> {
        conn.worker.begin(statement).await
    }

    fn commit(conn: &mut SqliteConnection) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        conn.worker.commit()
    }

    fn rollback(
        conn: &mut SqliteConnection,
    ) -> impl Future<Output = Result<(), Error>> + Send + '_ {
        conn.worker.rollback()
    }

    fn start_rollback(conn: &mut SqliteConnection) {
        conn.worker.start_rollback().ok();
    }

    fn get_transaction_depth(conn: &SqliteConnection) -> usize {
        conn.worker.shared.get_transaction_depth()
    }

    fn set_transaction_span(
        conn: &mut SqliteConnection,
        span: tracing::Span,
        parent_at_begin: Option<tracing::Id>,
    ) {
        conn.transaction_span = Some((span, parent_at_begin));
    }

    fn clear_transaction_span(conn: &mut SqliteConnection) {
        conn.transaction_span = None;
    }

    fn current_transaction_span(conn: &SqliteConnection) -> Option<tracing::Span> {
        conn.transaction_span.as_ref().map(|(span, _)| span.clone())
    }

    fn query_parent_span(conn: &SqliteConnection) -> Option<tracing::Span> {
        let (tx_span, parent_at_begin) = conn.transaction_span.as_ref()?;
        if tracing::Span::current().id() == *parent_at_begin {
            Some(tx_span.clone())
        } else {
            None
        }
    }
}
