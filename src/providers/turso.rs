// Turso provider: Mutex/lock operations should panic on poison
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

use self::sqlx::sqlite::SqlitePool;
use self::sqlx::{Sqlite, Transaction};
#[cfg(feature = "provider-test")]
use std::sync::Arc;
#[cfg(feature = "provider-test")]
use std::sync::atomic::AtomicUsize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::debug;

use super::{
    DeleteInstanceResult, DispatcherCapabilityFilter, ExecutionInfo, InstanceFilter, InstanceInfo, OrchestrationItem,
    Provider, ProviderAdmin, ProviderError, PruneOptions, PruneResult, QueueDepths, ScheduledActivityIdentifier,
    SessionFetchConfig, SystemMetrics, TagFilter, WorkItem,
};
use crate::{Event, EventKind};

mod sqlx {
    use std::collections::HashMap;
    use std::fmt;
    use std::marker::PhantomData;
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;
    use std::sync::Mutex;
    #[cfg(feature = "provider-test")]
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::{OwnedSemaphorePermit, Semaphore};

    pub mod sqlite {
        pub type SqlitePool = super::Pool;
    }

    pub type SqlitePool = Pool;

    pub struct Sqlite;

    pub type Result<T> = std::result::Result<T, Error>;

    #[derive(Debug)]
    pub enum Error {
        Turso(::turso::Error),
        Protocol(String),
        RowNotFound,
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Error::Turso(e) => write!(f, "{e}"),
                Error::Protocol(e) => write!(f, "{e}"),
                Error::RowNotFound => write!(f, "query returned no rows"),
            }
        }
    }

    impl std::error::Error for Error {}

    impl From<::turso::Error> for Error {
        fn from(value: ::turso::Error) -> Self {
            Error::Turso(value)
        }
    }

    impl Error {
        pub fn is_turso_transaction_retryable(&self) -> bool {
            match self {
                Error::Turso(::turso::Error::Busy(_) | ::turso::Error::BusySnapshot(_)) => true,
                Error::Turso(::turso::Error::Error(msg)) => msg.to_ascii_lowercase().contains("conflict"),
                _ => false,
            }
        }
    }

    fn is_no_active_transaction_error(error: &::turso::Error) -> bool {
        matches!(error, ::turso::Error::Error(msg)
            if msg.to_ascii_lowercase().contains("cannot rollback")
                && msg.to_ascii_lowercase().contains("no transaction is active"))
    }

    #[derive(Clone)]
    pub struct Pool {
        idle: Arc<Mutex<Vec<ConnectionState>>>,
        available: Arc<Semaphore>,
        size: usize,
        begin_statement: &'static str,
        #[cfg(feature = "provider-test")]
        commit_conflict_injections: Option<Arc<AtomicUsize>>,
    }

    struct ConnectionState {
        conn: ::turso::Connection,
        rollback_needed: bool,
    }

    pub struct PoolOptions {
        pub max_connections: usize,
        pub busy_timeout: std::time::Duration,
        pub begin_statement: &'static str,
        #[cfg(feature = "provider-test")]
        pub commit_conflict_injections: Option<Arc<AtomicUsize>>,
    }

    impl Pool {
        pub async fn connect(path: &str, options: PoolOptions) -> Result<Self> {
            let db = ::turso::Builder::new_local(path).build().await?;
            let max_connections = options.max_connections.max(1);
            let mut idle = Vec::with_capacity(max_connections);
            for _ in 0..max_connections {
                let conn = db.connect()?;
                conn.busy_timeout(options.busy_timeout)?;
                idle.push(ConnectionState {
                    conn,
                    rollback_needed: false,
                });
            }
            Ok(Self {
                idle: Arc::new(Mutex::new(idle)),
                available: Arc::new(Semaphore::new(max_connections)),
                size: max_connections,
                begin_statement: options.begin_statement,
                #[cfg(feature = "provider-test")]
                commit_conflict_injections: options.commit_conflict_injections,
            })
        }

        pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>> {
            let conn = self.acquire().await?;
            conn.conn().execute(self.begin_statement, ()).await?;
            Ok(Transaction {
                conn,
                active: true,
                _marker: PhantomData,
            })
        }

        pub async fn acquire(&self) -> Result<PooledConnection> {
            let permit = self
                .available
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Error::Protocol("Turso connection pool closed".to_string()))?;
            let state = {
                let mut idle = self.idle.lock().expect("Turso connection pool mutex poisoned");
                idle.pop()
                    .ok_or_else(|| Error::Protocol("Turso connection pool exhausted".to_string()))?
            };
            let mut conn = PooledConnection {
                state: Some(state),
                idle: self.idle.clone(),
                #[cfg(feature = "provider-test")]
                commit_conflict_injections: self.commit_conflict_injections.clone(),
                _permit: permit,
            };
            conn.rollback_if_needed().await?;
            Ok(conn)
        }

        pub async fn execute_on_all(&self, sql: &str) -> Result<()> {
            let mut conns = Vec::with_capacity(self.size);
            for _ in 0..self.size {
                conns.push(self.acquire().await?);
            }
            for conn in &conns {
                fetch_all_on_conn(conn.conn(), sql, Vec::new()).await?;
            }
            Ok(())
        }
    }

    pub struct PooledConnection {
        state: Option<ConnectionState>,
        idle: Arc<Mutex<Vec<ConnectionState>>>,
        #[cfg(feature = "provider-test")]
        commit_conflict_injections: Option<Arc<AtomicUsize>>,
        _permit: OwnedSemaphorePermit,
    }

    pub struct Transaction<'a, DB> {
        conn: PooledConnection,
        active: bool,
        _marker: PhantomData<&'a DB>,
    }

    impl<DB> Transaction<'_, DB> {
        pub async fn commit(mut self) -> Result<()> {
            #[cfg(feature = "provider-test")]
            if let Some(injections) = &self.conn.commit_conflict_injections {
                let injected = injections
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
                    .is_ok();
                if injected {
                    return Err(Error::Turso(::turso::Error::BusySnapshot(
                        "injected BEGIN CONCURRENT commit conflict".to_string(),
                    )));
                }
            }
            self.conn.conn().execute("COMMIT", ()).await?;
            self.active = false;
            Ok(())
        }

        pub async fn rollback(mut self) -> Result<()> {
            self.conn.conn().execute("ROLLBACK", ()).await?;
            self.active = false;
            Ok(())
        }
    }

    impl<DB> Drop for Transaction<'_, DB> {
        fn drop(&mut self) {
            if self.active {
                if let Some(state) = self.conn.state.as_mut() {
                    state.rollback_needed = true;
                }
            }
        }
    }

    impl PooledConnection {
        fn conn(&self) -> &::turso::Connection {
            &self.state.as_ref().expect("checked-out Turso connection missing").conn
        }

        fn conn_mut(&mut self) -> &mut ::turso::Connection {
            &mut self.state.as_mut().expect("checked-out Turso connection missing").conn
        }

        async fn rollback_if_needed(&mut self) -> Result<()> {
            let state = self.state.as_mut().expect("checked-out Turso connection missing");
            if state.rollback_needed {
                match state.conn.execute("ROLLBACK", ()).await {
                    Ok(_) => {}
                    Err(error) if is_no_active_transaction_error(&error) => {}
                    Err(error) => return Err(error.into()),
                }
                state.rollback_needed = false;
            }
            Ok(())
        }
    }

    impl Drop for PooledConnection {
        fn drop(&mut self) {
            if let Some(state) = self.state.take() {
                self.idle
                    .lock()
                    .expect("Turso connection pool mutex poisoned")
                    .push(state);
            }
        }
    }

    impl Deref for PooledConnection {
        type Target = ::turso::Connection;

        fn deref(&self) -> &Self::Target {
            self.conn()
        }
    }

    impl DerefMut for PooledConnection {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.conn_mut()
        }
    }

    impl<DB> Deref for Transaction<'_, DB> {
        type Target = ::turso::Connection;

        fn deref(&self) -> &Self::Target {
            self.conn.conn()
        }
    }

    impl<DB> DerefMut for Transaction<'_, DB> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.conn.conn_mut()
        }
    }

    #[derive(Debug, Clone)]
    pub struct Done {
        rows_affected: u64,
    }

    impl Done {
        pub fn rows_affected(&self) -> u64 {
            self.rows_affected
        }
    }

    #[derive(Debug, Clone)]
    pub struct Query {
        sql: String,
        params: Vec<::turso::Value>,
    }

    pub fn query(sql: &str) -> Query {
        Query {
            sql: sql.to_string(),
            params: Vec::new(),
        }
    }

    pub fn query_scalar<DB, T>(sql: &str) -> QueryScalar<DB, T> {
        QueryScalar {
            query: query(sql),
            _marker: PhantomData,
        }
    }

    pub fn query_as<DB, T>(sql: &str) -> QueryAs<DB, T> {
        QueryAs {
            query: query(sql),
            _marker: PhantomData,
        }
    }

    impl Query {
        pub fn bind<V: IntoDbValue>(mut self, value: V) -> Self {
            self.params.push(value.into_db_value());
            self
        }

        pub async fn execute<E: Executor>(self, mut executor: E) -> Result<Done> {
            executor.execute_query(&self.sql, self.params).await
        }

        pub async fn fetch_all<E: Executor>(self, mut executor: E) -> Result<Vec<DbRow>> {
            executor.fetch_all_query(&self.sql, self.params).await
        }

        pub async fn fetch_optional<E: Executor>(self, executor: E) -> Result<Option<DbRow>> {
            Ok(self.fetch_all(executor).await?.into_iter().next())
        }

        pub async fn fetch_one<E: Executor>(self, executor: E) -> Result<DbRow> {
            self.fetch_optional(executor).await?.ok_or(Error::RowNotFound)
        }
    }

    pub struct QueryScalar<DB, T> {
        query: Query,
        _marker: PhantomData<(DB, T)>,
    }

    impl<DB, T: TryFromValue> QueryScalar<DB, T> {
        pub fn bind<V: IntoDbValue>(mut self, value: V) -> Self {
            self.query = self.query.bind(value);
            self
        }

        pub async fn fetch_one<E: Executor<Database = DB>>(self, executor: E) -> Result<T> {
            let row = self.query.fetch_one(executor).await?;
            row.try_get(0usize)
        }

        pub async fn fetch_optional<E: Executor<Database = DB>>(self, executor: E) -> Result<Option<T>> {
            match self.query.fetch_optional(executor).await? {
                Some(row) => Ok(Some(row.try_get(0usize)?)),
                None => Ok(None),
            }
        }
    }

    pub struct QueryAs<DB, T> {
        query: Query,
        _marker: PhantomData<(DB, T)>,
    }

    impl<DB, T: FromDbRow> QueryAs<DB, T> {
        pub fn bind<V: IntoDbValue>(mut self, value: V) -> Self {
            self.query = self.query.bind(value);
            self
        }

        pub async fn fetch_one<E: Executor<Database = DB>>(self, executor: E) -> Result<T> {
            T::from_row(&self.query.fetch_one(executor).await?)
        }

        pub async fn fetch_optional<E: Executor<Database = DB>>(self, executor: E) -> Result<Option<T>> {
            match self.query.fetch_optional(executor).await? {
                Some(row) => Ok(Some(T::from_row(&row)?)),
                None => Ok(None),
            }
        }
    }

    pub trait Row {
        fn try_get<T, I>(&self, index: I) -> Result<T>
        where
            T: TryFromValue,
            I: ColumnIndex;
    }

    #[derive(Debug, Clone)]
    pub struct DbRow {
        values: Vec<::turso::Value>,
        columns: HashMap<String, usize>,
    }

    impl Row for DbRow {
        fn try_get<T, I>(&self, index: I) -> Result<T>
        where
            T: TryFromValue,
            I: ColumnIndex,
        {
            let idx = index.index(self)?;
            T::try_from_value(self.values.get(idx))
        }
    }

    impl DbRow {
        pub fn try_get<T, I>(&self, index: I) -> Result<T>
        where
            T: TryFromValue,
            I: ColumnIndex,
        {
            <Self as Row>::try_get(self, index)
        }
    }

    pub trait ColumnIndex {
        fn index(&self, row: &DbRow) -> Result<usize>;
    }

    impl ColumnIndex for usize {
        fn index(&self, row: &DbRow) -> Result<usize> {
            if *self < row.values.len() {
                Ok(*self)
            } else {
                Err(Error::Protocol(format!("column index {self} out of bounds")))
            }
        }
    }

    impl ColumnIndex for &str {
        fn index(&self, row: &DbRow) -> Result<usize> {
            row.columns
                .get(*self)
                .copied()
                .ok_or_else(|| Error::Protocol(format!("column '{self}' not found")))
        }
    }

    impl ColumnIndex for String {
        fn index(&self, row: &DbRow) -> Result<usize> {
            self.as_str().index(row)
        }
    }

    pub trait FromDbRow: Sized {
        fn from_row(row: &DbRow) -> Result<Self>;
    }

    impl FromDbRow for (i64,) {
        fn from_row(row: &DbRow) -> Result<Self> {
            Ok((row.try_get(0usize)?,))
        }
    }

    impl FromDbRow for (Option<String>,) {
        fn from_row(row: &DbRow) -> Result<Self> {
            Ok((row.try_get(0usize)?,))
        }
    }

    pub trait TryFromValue: Sized {
        fn try_from_value(value: Option<&::turso::Value>) -> Result<Self>;
    }

    impl TryFromValue for String {
        fn try_from_value(value: Option<&::turso::Value>) -> Result<Self> {
            match value {
                Some(::turso::Value::Text(v)) => Ok(v.clone()),
                Some(::turso::Value::Integer(v)) => Ok(v.to_string()),
                Some(::turso::Value::Real(v)) => Ok(v.to_string()),
                Some(::turso::Value::Blob(v)) => {
                    String::from_utf8(v.clone()).map_err(|e| Error::Protocol(format!("invalid UTF-8 blob: {e}")))
                }
                Some(::turso::Value::Null) => Err(Error::Protocol("unexpected NULL".to_string())),
                None => Err(Error::Protocol("missing column".to_string())),
            }
        }
    }

    impl TryFromValue for Option<String> {
        fn try_from_value(value: Option<&::turso::Value>) -> Result<Self> {
            match value {
                Some(::turso::Value::Null) | None => Ok(None),
                Some(_) => String::try_from_value(value).map(Some),
            }
        }
    }

    impl TryFromValue for i64 {
        fn try_from_value(value: Option<&::turso::Value>) -> Result<Self> {
            match value {
                Some(::turso::Value::Integer(v)) => Ok(*v),
                Some(::turso::Value::Text(v)) => v
                    .parse::<i64>()
                    .map_err(|e| Error::Protocol(format!("failed to parse integer '{v}': {e}"))),
                Some(::turso::Value::Real(v)) => Ok(*v as i64),
                Some(::turso::Value::Null) => Err(Error::Protocol("unexpected NULL".to_string())),
                Some(::turso::Value::Blob(_)) => Err(Error::Protocol("unexpected BLOB".to_string())),
                None => Err(Error::Protocol("missing column".to_string())),
            }
        }
    }

    impl TryFromValue for Option<i64> {
        fn try_from_value(value: Option<&::turso::Value>) -> Result<Self> {
            match value {
                Some(::turso::Value::Null) | None => Ok(None),
                Some(_) => i64::try_from_value(value).map(Some),
            }
        }
    }

    pub trait IntoDbValue {
        fn into_db_value(self) -> ::turso::Value;
    }

    impl IntoDbValue for ::turso::Value {
        fn into_db_value(self) -> ::turso::Value {
            self
        }
    }

    impl IntoDbValue for String {
        fn into_db_value(self) -> ::turso::Value {
            ::turso::Value::Text(self)
        }
    }

    impl IntoDbValue for &String {
        fn into_db_value(self) -> ::turso::Value {
            ::turso::Value::Text(self.clone())
        }
    }

    impl IntoDbValue for &str {
        fn into_db_value(self) -> ::turso::Value {
            ::turso::Value::Text(self.to_string())
        }
    }

    impl IntoDbValue for &&str {
        fn into_db_value(self) -> ::turso::Value {
            ::turso::Value::Text((*self).to_string())
        }
    }

    impl IntoDbValue for i64 {
        fn into_db_value(self) -> ::turso::Value {
            ::turso::Value::Integer(self)
        }
    }

    impl IntoDbValue for &i64 {
        fn into_db_value(self) -> ::turso::Value {
            ::turso::Value::Integer(*self)
        }
    }

    impl IntoDbValue for u64 {
        fn into_db_value(self) -> ::turso::Value {
            ::turso::Value::Integer(self as i64)
        }
    }

    impl IntoDbValue for &u64 {
        fn into_db_value(self) -> ::turso::Value {
            ::turso::Value::Integer(*self as i64)
        }
    }

    impl<T: IntoDbValue> IntoDbValue for Option<T> {
        fn into_db_value(self) -> ::turso::Value {
            match self {
                Some(value) => value.into_db_value(),
                None => ::turso::Value::Null,
            }
        }
    }

    impl<T: Clone + IntoDbValue> IntoDbValue for &Option<T> {
        fn into_db_value(self) -> ::turso::Value {
            self.clone().into_db_value()
        }
    }

    #[async_trait::async_trait]
    pub trait Executor {
        type Database;

        async fn execute_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Done>;
        async fn fetch_all_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Vec<DbRow>>;
    }

    #[async_trait::async_trait]
    impl Executor for &SqlitePool {
        type Database = Sqlite;

        async fn execute_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Done> {
            let conn = self.acquire().await?;
            execute_on_conn(conn.conn(), sql, params).await
        }

        async fn fetch_all_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Vec<DbRow>> {
            let conn = self.acquire().await?;
            fetch_all_on_conn(conn.conn(), sql, params).await
        }
    }

    #[async_trait::async_trait]
    impl Executor for &mut PooledConnection {
        type Database = Sqlite;

        async fn execute_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Done> {
            execute_on_conn(self.conn(), sql, params).await
        }

        async fn fetch_all_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Vec<DbRow>> {
            fetch_all_on_conn(self.conn(), sql, params).await
        }
    }

    #[async_trait::async_trait]
    impl<DB: Send + Sync> Executor for &mut Transaction<'_, DB> {
        type Database = DB;

        async fn execute_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Done> {
            execute_on_conn(self.conn.conn(), sql, params).await
        }

        async fn fetch_all_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Vec<DbRow>> {
            fetch_all_on_conn(self.conn.conn(), sql, params).await
        }
    }

    #[async_trait::async_trait]
    impl Executor for &mut ::turso::Connection {
        type Database = Sqlite;

        async fn execute_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Done> {
            execute_on_conn(self, sql, params).await
        }

        async fn fetch_all_query(&mut self, sql: &str, params: Vec<::turso::Value>) -> Result<Vec<DbRow>> {
            fetch_all_on_conn(self, sql, params).await
        }
    }

    async fn execute_on_conn(conn: &::turso::Connection, sql: &str, params: Vec<::turso::Value>) -> Result<Done> {
        let rows_affected = conn.execute(sql, ::turso::params_from_iter(params)).await?;
        Ok(Done { rows_affected })
    }

    async fn fetch_all_on_conn(
        conn: &::turso::Connection,
        sql: &str,
        params: Vec<::turso::Value>,
    ) -> Result<Vec<DbRow>> {
        let mut rows = conn.query(sql, ::turso::params_from_iter(params)).await?;
        let column_names = rows.column_names();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            let mut values = Vec::with_capacity(row.column_count());
            for idx in 0..row.column_count() {
                values.push(row.get_value(idx)?);
            }
            let columns = column_names
                .iter()
                .enumerate()
                .map(|(idx, name)| (name.clone(), idx))
                .collect();
            out.push(DbRow { values, columns });
        }
        Ok(out)
    }
}

/// Journal mode for Turso's SQLite-compatible engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TursoJournalMode {
    /// Use `MEMORY` for in-memory databases and `WAL` for file-backed databases.
    Auto,
    Delete,
    Truncate,
    Persist,
    Memory,
    Wal,
    Off,
    /// Turso MVCC journal mode. Pair with [`TursoTransactionMode::Concurrent`]
    /// to use optimistic concurrent transactions.
    Mvcc,
    /// Raw PRAGMA value for experimenting with Turso-specific modes.
    Custom(String),
}

impl TursoJournalMode {
    fn pragma_value(&self, is_memory: bool) -> String {
        match self {
            Self::Auto if is_memory => "MEMORY".to_string(),
            Self::Auto => "WAL".to_string(),
            Self::Delete => "DELETE".to_string(),
            Self::Truncate => "TRUNCATE".to_string(),
            Self::Persist => "PERSIST".to_string(),
            Self::Memory => "MEMORY".to_string(),
            Self::Wal => "WAL".to_string(),
            Self::Off => "OFF".to_string(),
            Self::Mvcc => "'mvcc'".to_string(),
            Self::Custom(value) => value.clone(),
        }
    }

    fn requires_mvcc_schema_compat(&self) -> bool {
        match self {
            Self::Mvcc => true,
            Self::Custom(value) => value.to_ascii_lowercase().contains("mvcc"),
            _ => false,
        }
    }
}

impl Default for TursoJournalMode {
    fn default() -> Self {
        Self::Auto
    }
}

/// Synchronous setting for Turso's SQLite-compatible engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TursoSynchronous {
    /// Use `OFF` for in-memory databases and `NORMAL` for file-backed databases.
    Auto,
    Off,
    Normal,
    Full,
    Extra,
    /// Raw PRAGMA value for compatibility with Turso/SQLite-specific behavior.
    Custom(String),
}

impl TursoSynchronous {
    fn pragma_value(&self, is_memory: bool) -> String {
        match self {
            Self::Auto if is_memory => "OFF".to_string(),
            Self::Auto => "NORMAL".to_string(),
            Self::Off => "OFF".to_string(),
            Self::Normal => "NORMAL".to_string(),
            Self::Full => "FULL".to_string(),
            Self::Extra => "EXTRA".to_string(),
            Self::Custom(value) => value.clone(),
        }
    }
}

impl Default for TursoSynchronous {
    fn default() -> Self {
        Self::Auto
    }
}

/// Explicit transaction mode used by TursoProvider for multi-statement provider operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TursoTransactionMode {
    /// Match the existing SQLite-like provider behavior.
    #[default]
    Immediate,
    /// Use Turso's optimistic write transaction mode.
    ///
    /// This is opt-in because conflicts can surface at `COMMIT` and require
    /// replaying the whole provider operation.
    Concurrent,
}

impl TursoTransactionMode {
    fn begin_statement(self) -> &'static str {
        match self {
            Self::Immediate => "BEGIN IMMEDIATE",
            Self::Concurrent => "BEGIN CONCURRENT",
        }
    }
}

/// Configuration options for TursoProvider.
#[derive(Debug, Clone)]
pub struct TursoOptions {
    /// Number of Turso connections in the local provider pool.
    ///
    /// Default: 5, matching the SQLite provider.
    pub max_connections: usize,

    /// Busy timeout applied to each Turso connection.
    ///
    /// Default: 60 seconds.
    pub busy_timeout: Duration,

    /// Journal mode PRAGMA. Pair [`TursoJournalMode::Mvcc`] with
    /// [`TursoTransactionMode::Concurrent`] to use Turso's optimistic
    /// concurrent write path.
    ///
    /// Default: [`TursoJournalMode::Auto`].
    pub journal_mode: TursoJournalMode,

    /// Synchronous PRAGMA.
    ///
    /// Default: [`TursoSynchronous::Auto`].
    pub synchronous: TursoSynchronous,

    /// WAL checkpoint interval for file-backed databases.
    ///
    /// Set to `None` to skip this PRAGMA. Default: `Some(10000)`.
    pub wal_autocheckpoint: Option<u32>,

    /// SQLite/Turso cache size PRAGMA for file-backed databases.
    ///
    /// Negative values are kibibytes. Set to `None` to skip. Default: `Some(-64000)`.
    pub cache_size: Option<i64>,

    /// Transaction mode for multi-statement provider operations.
    ///
    /// Default: [`TursoTransactionMode::Immediate`]. When set to
    /// [`TursoTransactionMode::Concurrent`], retryable Turso commit conflicts
    /// replay the whole provider operation with bounded backoff.
    pub transaction_mode: TursoTransactionMode,

    /// Maximum number of whole-operation retries after a Turso concurrent
    /// transaction conflict.
    ///
    /// Default: 8.
    pub transaction_max_retries: u32,

    /// Initial backoff before retrying a conflicted concurrent transaction.
    ///
    /// Default: 1 millisecond.
    pub transaction_retry_initial_backoff: Duration,

    /// Maximum backoff between conflicted concurrent transaction retries.
    ///
    /// Default: 50 milliseconds.
    pub transaction_retry_max_backoff: Duration,

    /// Additional lock lifetime applied inside `ack_orchestration_item` before
    /// long transactional writes start.
    ///
    /// This update is part of the ack transaction and is only used to prevent
    /// the final in-transaction validity check from failing due to wall-clock
    /// time spent in the same ack transaction. It does not make expired or
    /// stolen tokens valid.
    ///
    /// Set to `None` to disable. Default: 5 minutes.
    pub ack_lock_extension: Option<Duration>,

    /// Test-only delay injected after the ack lock extension.
    #[cfg(feature = "provider-test")]
    #[doc(hidden)]
    pub ack_delay_after_lock_extension: Option<Duration>,

    /// Test-only count of synthetic commit conflicts to inject.
    #[cfg(feature = "provider-test")]
    #[doc(hidden)]
    pub commit_conflict_injections: Option<Arc<AtomicUsize>>,
}

impl Default for TursoOptions {
    fn default() -> Self {
        Self {
            max_connections: 5,
            busy_timeout: Duration::from_secs(60),
            journal_mode: TursoJournalMode::Auto,
            synchronous: TursoSynchronous::Auto,
            wal_autocheckpoint: Some(10000),
            cache_size: Some(-64000),
            transaction_mode: TursoTransactionMode::Immediate,
            transaction_max_retries: 8,
            transaction_retry_initial_backoff: Duration::from_millis(1),
            transaction_retry_max_backoff: Duration::from_millis(50),
            ack_lock_extension: Some(Duration::from_secs(300)),
            #[cfg(feature = "provider-test")]
            ack_delay_after_lock_extension: None,
            #[cfg(feature = "provider-test")]
            commit_conflict_injections: None,
        }
    }
}

/// Turso-backed provider with full transactional support
///
/// This provider offers true ACID guarantees across all operations,
/// eliminating the race conditions present in the filesystem provider.
pub struct TursoProvider {
    pool: SqlitePool,
    transaction_mode: TursoTransactionMode,
    transaction_max_retries: u32,
    transaction_retry_initial_backoff: Duration,
    transaction_retry_max_backoff: Duration,
    ack_lock_extension: Option<Duration>,
    #[cfg(feature = "provider-test")]
    ack_delay_after_lock_extension: Option<Duration>,
}

impl TursoProvider {
    /// Create a new Turso provider
    ///
    /// # Arguments
    /// * `database_url` - Local Turso path (e.g., "data.db", "turso:data.db", or ":memory:")
    /// * `options` - Optional Turso-specific configuration
    ///
    /// # Errors
    ///
    /// Returns an error if database connection or schema initialization fails.
    pub async fn new(database_url: &str, options: Option<TursoOptions>) -> Result<Self, sqlx::Error> {
        let options = options.unwrap_or_default();
        let database_path = Self::normalize_database_path(database_url);
        let is_memory = database_path == ":memory:";
        let pool = SqlitePool::connect(
            &database_path,
            sqlx::PoolOptions {
                max_connections: options.max_connections,
                busy_timeout: options.busy_timeout,
                begin_statement: options.transaction_mode.begin_statement(),
                #[cfg(feature = "provider-test")]
                commit_conflict_injections: options.commit_conflict_injections.clone(),
            },
        )
        .await?;

        // Configure Turso's SQLite-compatible engine with the same practical defaults
        // as the sqlx-backed sqlite provider where the PRAGMAs are supported.
        Self::try_optional_pragma(
            &pool,
            format!("PRAGMA journal_mode = {}", options.journal_mode.pragma_value(is_memory)),
        )
        .await;
        Self::try_optional_pragma(
            &pool,
            format!("PRAGMA synchronous = {}", options.synchronous.pragma_value(is_memory)),
        )
        .await;
        if !is_memory {
            if let Some(wal_autocheckpoint) = options.wal_autocheckpoint {
                Self::try_optional_pragma(&pool, format!("PRAGMA wal_autocheckpoint = {wal_autocheckpoint}")).await;
            }
            if let Some(cache_size) = options.cache_size {
                Self::try_optional_pragma(&pool, format!("PRAGMA cache_size = {cache_size}")).await;
            }
        }
        pool.execute_on_all("PRAGMA foreign_keys = ON").await?;

        let queue_id_column = if options.journal_mode.requires_mvcc_schema_compat() {
            // Turso 0.5.x MVCC rejects AUTOINCREMENT, while plain INTEGER
            // PRIMARY KEY still gives rowid-backed queue ordering.
            "id INTEGER PRIMARY KEY"
        } else {
            "id INTEGER PRIMARY KEY AUTOINCREMENT"
        };
        Self::create_schema_with_queue_id(&pool, queue_id_column).await?;

        Ok(Self {
            pool,
            transaction_mode: options.transaction_mode,
            transaction_max_retries: options.transaction_max_retries,
            transaction_retry_initial_backoff: options.transaction_retry_initial_backoff,
            transaction_retry_max_backoff: options.transaction_retry_max_backoff,
            ack_lock_extension: options.ack_lock_extension,
            #[cfg(feature = "provider-test")]
            ack_delay_after_lock_extension: options.ack_delay_after_lock_extension,
        })
    }

    async fn try_optional_pragma(pool: &SqlitePool, statement: impl AsRef<str>) {
        let statement = statement.as_ref();
        if let Err(error) = pool.execute_on_all(statement).await {
            debug!(%statement, %error, "optional Turso PRAGMA setup failed");
        }
    }

    fn should_retry_transaction_operation(&self, error: &ProviderError, retry_count: u32) -> bool {
        self.transaction_mode == TursoTransactionMode::Concurrent
            && retry_count < self.transaction_max_retries
            && error.is_retryable()
            && error.message.contains("Turso transaction retry")
    }

    fn transaction_retry_backoff(&self, retry_count: u32) -> Duration {
        let multiplier = 1u32.checked_shl(retry_count.min(10)).unwrap_or(1);
        self.transaction_retry_initial_backoff
            .saturating_mul(multiplier)
            .min(self.transaction_retry_max_backoff)
    }

    fn normalize_database_path(database_url: &str) -> String {
        if database_url == ":memory:"
            || database_url == "turso::memory:"
            || database_url == "sqlite::memory:"
            || database_url.contains("mode=memory")
        {
            return ":memory:".to_string();
        }

        database_url
            .strip_prefix("turso:")
            .or_else(|| database_url.strip_prefix("sqlite:"))
            .unwrap_or(database_url)
            .to_string()
    }

    /// Convenience: create an in-memory Turso store for tests.
    ///
    /// # Errors
    ///
    /// Returns an error if database connection or schema initialization fails.
    pub async fn new_in_memory() -> Result<Self, sqlx::Error> {
        Self::new_in_memory_with_options(None).await
    }

    /// Create an in-memory Turso store with custom options
    ///
    /// # Errors
    ///
    /// Returns an error if database connection or schema initialization fails.
    pub async fn new_in_memory_with_options(options: Option<TursoOptions>) -> Result<Self, sqlx::Error> {
        Self::new(":memory:", options).await
    }
}

super::sqlite_common::define_sqlite_like_provider!(
    TursoProvider,
    "turso",
    "duroxide::providers::turso",
    leaf_first_instance_delete,
    extend_ack_lock_at_ack_start,
    retry_concurrent_transactions
);

#[cfg(test)]
mod tests {
    use super::TursoSynchronous;

    #[test]
    fn turso_synchronous_auto_uses_valid_sqlite_values() {
        assert_eq!(TursoSynchronous::Auto.pragma_value(true), "OFF");
        assert_eq!(TursoSynchronous::Auto.pragma_value(false), "NORMAL");
    }
}
