use std::fmt::{self, Debug, Formatter};
use std::future::{self, Future};
use std::ops::{Deref, DerefMut};

use futures_core::future::BoxFuture;

use crate::database::Database;
use crate::error::Error;
use crate::pool::MaybePoolConnection;
use crate::sql_str::{AssertSqlSafe, SqlSafeStr, SqlStr};

/// Generic management of database transactions.
///
/// This trait should not be used, except when implementing [`Connection`].
pub trait TransactionManager {
    type Database: Database;

    /// Begin a new transaction or establish a savepoint within the active transaction.
    ///
    /// If this is a new transaction, `statement` may be used instead of the
    /// default "BEGIN" statement.
    ///
    /// If we are already inside a transaction and `statement.is_some()`, then
    /// `Error::InvalidSavePoint` is returned without running any statements.
    fn begin(
        conn: &mut <Self::Database as Database>::Connection,
        statement: Option<SqlStr>,
    ) -> impl Future<Output = Result<(), Error>> + Send + '_;

    /// Commit the active transaction or release the most recent savepoint.
    fn commit(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> impl Future<Output = Result<(), Error>> + Send + '_;

    /// Abort the active transaction or restore from the most recent savepoint.
    fn rollback(
        conn: &mut <Self::Database as Database>::Connection,
    ) -> impl Future<Output = Result<(), Error>> + Send + '_;

    /// Starts to abort the active transaction or restore from the most recent snapshot.
    fn start_rollback(conn: &mut <Self::Database as Database>::Connection);

    /// Returns the current transaction depth.
    ///
    /// Transaction depth indicates the level of nested transactions:
    /// - Level 0: No active transaction.
    /// - Level 1: A transaction is active.
    /// - Level 2 or higher: A transaction is active and one or more SAVEPOINTs have been created within it.
    fn get_transaction_depth(conn: &<Self::Database as Database>::Connection) -> usize;

    /// Records the outermost transaction's tracing span on the connection,
    /// together with the caller's `Span::current()` at begin time. Called by
    /// [`Transaction`] only when opening the outermost transaction (not for
    /// nested savepoints, which share the outer span).
    ///
    /// The `parent_at_begin` id drives the user-injected-span heuristic: if
    /// `Span::current()` at query time still matches it, the caller hasn't
    /// entered their own span between begin and the query, so we auto-parent
    /// under the tx span. If they differ, we respect the caller's current.
    ///
    /// Default impl is a no-op. In-tree backends override.
    fn set_transaction_span(
        _conn: &mut <Self::Database as Database>::Connection,
        _span: tracing::Span,
        _parent_at_begin: Option<tracing::Id>,
    ) {
    }

    /// Clears the connection's transaction span. Called by [`Transaction`]
    /// only when the outermost transaction commits, rolls back, or is dropped.
    ///
    /// Default impl is a no-op; in-tree backends clear their `Option`.
    fn clear_transaction_span(_conn: &mut <Self::Database as Database>::Connection) {}

    /// Returns the connection's currently open transaction span, if any.
    /// Used by [`Transaction::begin`] so savepoint transactions can share the
    /// outermost transaction's span.
    ///
    /// Default impl returns `None`; in-tree backends look at their `Option`.
    fn current_transaction_span(
        _conn: &<Self::Database as Database>::Connection,
    ) -> Option<tracing::Span> {
        None
    }

    /// Returns the span the executor should use as the parent for the next
    /// query span on this connection, or `None` to fall back to
    /// `Span::current()`.
    ///
    /// In-tree backends compare `Span::current()` to the stored
    /// `parent_at_begin`: if equal, the caller hasn't entered any span since
    /// `begin`, so the query parents under the tx span; if different, the
    /// caller has wrapped this query in their own span, so we respect that
    /// and return `None`.
    ///
    /// Default impl returns `None`, preserving pre-tracing behavior for
    /// out-of-tree drivers.
    fn query_parent_span(
        _conn: &<Self::Database as Database>::Connection,
    ) -> Option<tracing::Span> {
        None
    }
}

/// An in-progress database transaction or savepoint.
///
/// A transaction starts with a call to [`Pool::begin`] or [`Connection::begin`].
///
/// A transaction should end with a call to [`commit`] or [`rollback`]. If neither are called
/// before the transaction goes out-of-scope, [`rollback`] is called. In other
/// words, [`rollback`] is called on `drop` if the transaction is still in-progress.
///
/// A savepoint is a special mark inside a transaction that allows all commands that are
/// executed after it was established to be rolled back, restoring the transaction state to
/// what it was at the time of the savepoint.
///
/// A transaction can be used as an [`Executor`] when performing queries:
/// ```rust,no_run
/// # use sqlx_core::acquire::Acquire;
/// # async fn example() -> sqlx::Result<()> {
/// # let id = 1;
/// # let mut conn: sqlx::PgConnection = unimplemented!();
/// let mut tx = conn.begin().await?;
///
/// let result = sqlx::query("DELETE FROM \"testcases\" WHERE id = $1")
///     .bind(id)
///     .execute(&mut *tx)
///     .await?
///     .rows_affected();
///
/// tx.commit().await
/// # }
/// ```
/// [`Executor`]: crate::executor::Executor
/// [`Connection::begin`]: crate::connection::Connection::begin()
/// [`Pool::begin`]: crate::pool::Pool::begin()
/// [`commit`]: Self::commit()
/// [`rollback`]: Self::rollback()
pub struct Transaction<'c, DB>
where
    DB: Database,
{
    connection: MaybePoolConnection<'c, DB>,
    open: bool,
    span: tracing::Span,
    /// True for the outermost transaction, false for nested savepoints. The
    /// outermost owns the span: it sets it on `begin`, records the outcome,
    /// and clears it on commit/rollback/drop. Savepoints share the same span
    /// and don't touch the connection's tracing state.
    is_outermost: bool,
}

impl<'c, DB> Transaction<'c, DB>
where
    DB: Database,
{
    #[doc(hidden)]
    pub fn begin(
        conn: impl Into<MaybePoolConnection<'c, DB>>,
        statement: Option<SqlStr>,
    ) -> BoxFuture<'c, Result<Self, Error>> {
        let conn = conn.into();

        Box::pin(async move {
            // Hardcoded INFO level per maintainer review of #3313. Field names follow the
            // OTel database span semantic conventions. `db.transaction.outcome` starts as
            // `Empty` and gets recorded on commit/rollback/drop so the span carries its
            // resolution.
            //
            // The outermost transaction owns the span; nested savepoints share it, so a
            // sequence of BEGIN / SAVEPOINT / queries / RELEASE / COMMIT shows up as a
            // single `db.transaction` span with the per-statement query spans as
            // children. Per-savepoint duration isn't surfaced — the SAVEPOINT / RELEASE
            // query spans themselves serve as the markers.
            let depth_before = DB::TransactionManager::get_transaction_depth(&conn);
            let is_outermost = depth_before == 0;
            let span = if is_outermost {
                tracing::info_span!(
                    target: "sqlx::transaction",
                    parent: &tracing::Span::current(),
                    "db.transaction",
                    "db.system.name" = %DB::NAME.to_ascii_lowercase(),
                    "db.operation.name" = "BEGIN",
                    "db.transaction.outcome" = tracing::field::Empty,
                    "otel.kind" = "client",
                )
            } else {
                // Reuse the outermost transaction's span so nested savepoint queries
                // still parent under it.
                DB::TransactionManager::current_transaction_span(&conn).unwrap_or_else(
                    // Defensive: out-of-tree drivers that don't override the trait
                    // methods can land here. Fall back to a fresh detached span — no
                    // worse than the pre-tracing-spans behavior.
                    tracing::Span::current,
                )
            };

            let mut tx = Self {
                connection: conn,

                // If the call to `begin` fails or doesn't complete we want to attempt a rollback in case the transaction was started.
                open: true,
                span: span.clone(),
                is_outermost,
            };

            // Only the outermost transaction installs the span on the connection;
            // savepoints inherit it. Set before BEGIN so the BEGIN's query span
            // parents under it via the executor's `query_parent_span` lookup. If
            // BEGIN fails, the Drop handler clears it back out (`open: true`).
            //
            // `parent_at_begin` lets later `query_parent_span` calls distinguish
            // "caller is still at the same level as begin" (auto-parent) from
            // "caller has entered their own span in between" (respect their
            // current).
            if is_outermost {
                let parent_at_begin = tracing::Span::current().id();
                DB::TransactionManager::set_transaction_span(
                    &mut tx.connection,
                    span,
                    parent_at_begin,
                );
            }

            DB::TransactionManager::begin(&mut tx.connection, statement).await?;

            Ok(tx)
        })
    }

    /// Returns a handle to the transaction's tracing span.
    ///
    /// Useful when callers want to manually parent their own spans under the
    /// transaction — e.g. wrapping a block of procedural work in
    /// `something.instrument(tx.span())` so user spans inside become children
    /// of the transaction. The returned `Span` is cheap to clone and `Send`.
    pub fn span(&self) -> tracing::Span {
        self.span.clone()
    }

    /// Commits this transaction or savepoint.
    pub async fn commit(mut self) -> Result<(), Error> {
        // The span stays on the connection across the COMMIT call so the executor
        // parents the COMMIT's query span under it; cleared only after success, and
        // only for the outermost transaction (savepoints don't own the span).
        DB::TransactionManager::commit(&mut self.connection).await?;
        self.open = false;
        if self.is_outermost {
            self.span.record("db.transaction.outcome", "committed");
            DB::TransactionManager::clear_transaction_span(&mut self.connection);
        }

        Ok(())
    }

    /// Aborts this transaction or savepoint.
    pub async fn rollback(mut self) -> Result<(), Error> {
        DB::TransactionManager::rollback(&mut self.connection).await?;
        self.open = false;
        if self.is_outermost {
            self.span.record("db.transaction.outcome", "rolled_back");
            DB::TransactionManager::clear_transaction_span(&mut self.connection);
        }

        Ok(())
    }
}

// NOTE: fails to compile due to lack of lazy normalization
// impl<'c, 't, DB: Database> crate::executor::Executor<'t>
//     for &'t mut crate::transaction::Transaction<'c, DB>
// where
//     &'c mut DB::Connection: Executor<'c, Database = DB>,
// {
//     type Database = DB;
//
//
//
//     fn fetch_many<'e, 'q: 'e, E: 'q>(
//         self,
//         query: E,
//     ) -> futures_core::stream::BoxStream<
//         'e,
//         Result<
//             crate::Either<<DB as crate::database::Database>::QueryResult, DB::Row>,
//             crate::error::Error,
//         >,
//     >
//     where
//         't: 'e,
//         E: crate::executor::Execute<'q, Self::Database>,
//     {
//         (&mut **self).fetch_many(query)
//     }
//
//     fn fetch_optional<'e, 'q: 'e, E: 'q>(
//         self,
//         query: E,
//     ) -> futures_core::future::BoxFuture<'e, Result<Option<DB::Row>, crate::error::Error>>
//     where
//         't: 'e,
//         E: crate::executor::Execute<'q, Self::Database>,
//     {
//         (&mut **self).fetch_optional(query)
//     }
//
//     fn prepare_with<'e, 'q: 'e>(
//         self,
//         sql: &'q str,
//         parameters: &'e [<Self::Database as crate::database::Database>::TypeInfo],
//     ) -> futures_core::future::BoxFuture<
//         'e,
//         Result<
//             <Self::Database as crate::database::Database>::Statement<'q>,
//             crate::error::Error,
//         >,
//     >
//     where
//         't: 'e,
//     {
//         (&mut **self).prepare_with(sql, parameters)
//     }
//
//     #[doc(hidden)]
//     #[cfg(feature = "offline")]
//     fn describe<'e, 'q: 'e>(
//         self,
//         query: &'q str,
//     ) -> futures_core::future::BoxFuture<
//         'e,
//         Result<crate::describe::Describe<Self::Database>, crate::error::Error>,
//     >
//     where
//         't: 'e,
//     {
//         (&mut **self).describe(query)
//     }
// }

impl<DB> Debug for Transaction<'_, DB>
where
    DB: Database,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // TODO: Show the full type <..<..<..
        f.debug_struct("Transaction").finish()
    }
}

impl<DB> Deref for Transaction<'_, DB>
where
    DB: Database,
{
    type Target = DB::Connection;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.connection
    }
}

impl<DB> DerefMut for Transaction<'_, DB>
where
    DB: Database,
{
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.connection
    }
}

// Implement `AsMut<DB::Connection>` so `Transaction` can be given to a
// `PgAdvisoryLockGuard`.
//
// See: https://github.com/launchbadge/sqlx/issues/2520
impl<DB: Database> AsMut<DB::Connection> for Transaction<'_, DB> {
    fn as_mut(&mut self) -> &mut DB::Connection {
        &mut self.connection
    }
}

impl<'t, DB: Database> crate::acquire::Acquire<'t> for &'t mut Transaction<'_, DB> {
    type Database = DB;

    type Connection = &'t mut <DB as Database>::Connection;

    #[inline]
    fn acquire(self) -> BoxFuture<'t, Result<Self::Connection, Error>> {
        Box::pin(future::ready(Ok(&mut **self)))
    }

    #[inline]
    fn begin(self) -> BoxFuture<'t, Result<Transaction<'t, DB>, Error>> {
        Transaction::begin(&mut **self, None)
    }
}

impl<DB> Drop for Transaction<'_, DB>
where
    DB: Database,
{
    fn drop(&mut self) {
        if self.open {
            // starts a rollback operation

            // what this does depends on the database but generally this means we queue a rollback
            // operation that will happen on the next asynchronous invocation of the underlying
            // connection (including if the connection is returned to a pool)

            DB::TransactionManager::start_rollback(&mut self.connection);

            // Only the outermost transaction owns the span and the connection's tracing
            // state. Clear after start_rollback so the span is still installed for the
            // duration of the operation, mirroring commit/rollback.
            if self.is_outermost {
                self.span.record("db.transaction.outcome", "dropped");
                DB::TransactionManager::clear_transaction_span(&mut self.connection);
            }
        }
    }
}

pub fn begin_ansi_transaction_sql(depth: usize) -> SqlStr {
    if depth == 0 {
        "BEGIN".into_sql_str()
    } else {
        AssertSqlSafe(format!("SAVEPOINT _sqlx_savepoint_{depth}")).into_sql_str()
    }
}

pub fn commit_ansi_transaction_sql(depth: usize) -> SqlStr {
    if depth == 1 {
        "COMMIT".into_sql_str()
    } else {
        AssertSqlSafe(format!("RELEASE SAVEPOINT _sqlx_savepoint_{}", depth - 1)).into_sql_str()
    }
}

pub fn rollback_ansi_transaction_sql(depth: usize) -> SqlStr {
    if depth == 1 {
        "ROLLBACK".into_sql_str()
    } else {
        AssertSqlSafe(format!(
            "ROLLBACK TO SAVEPOINT _sqlx_savepoint_{}",
            depth - 1
        ))
        .into_sql_str()
    }
}
