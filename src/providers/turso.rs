// Turso provider: Mutex/lock operations should panic on poison
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]

use self::sqlx::sqlite::SqlitePool;
use self::sqlx::{Sqlite, Transaction};
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

    const MAX_CONNECTIONS: usize = 5;

    #[derive(Clone)]
    pub struct Pool {
        idle: Arc<Mutex<Vec<ConnectionState>>>,
        available: Arc<Semaphore>,
        size: usize,
    }

    struct ConnectionState {
        conn: ::turso::Connection,
        rollback_needed: bool,
    }

    impl Pool {
        pub async fn connect(path: &str) -> Result<Self> {
            let db = ::turso::Builder::new_local(path).build().await?;
            let mut idle = Vec::with_capacity(MAX_CONNECTIONS);
            for _ in 0..MAX_CONNECTIONS {
                let conn = db.connect()?;
                conn.busy_timeout(std::time::Duration::from_secs(60))?;
                idle.push(ConnectionState {
                    conn,
                    rollback_needed: false,
                });
            }
            Ok(Self {
                idle: Arc::new(Mutex::new(idle)),
                available: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
                size: MAX_CONNECTIONS,
            })
        }

        pub async fn begin(&self) -> Result<Transaction<'static, Sqlite>> {
            let conn = self.acquire().await?;
            conn.conn().execute("BEGIN IMMEDIATE", ()).await?;
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
        _permit: OwnedSemaphorePermit,
    }

    pub struct Transaction<'a, DB> {
        conn: PooledConnection,
        active: bool,
        _marker: PhantomData<&'a DB>,
    }

    impl<DB> Transaction<'_, DB> {
        pub async fn commit(mut self) -> Result<()> {
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
                state.conn.execute("ROLLBACK", ()).await?;
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

/// Configuration options for TursoProvider
#[derive(Debug, Clone, Default)]
pub struct TursoOptions {
    // Currently empty - lock timeout moved to RuntimeOptions
    // Kept for future provider-specific options
}

/// Turso-backed provider with full transactional support
///
/// This provider offers true ACID guarantees across all operations,
/// eliminating the race conditions present in the filesystem provider.
pub struct TursoProvider {
    pool: SqlitePool,
}

impl TursoProvider {
    /// Create a new Turso provider
    ///
    /// # Arguments
    /// * `database_url` - Local Turso path (e.g., "data.db", "turso:data.db", or ":memory:")
    /// * `options` - Optional configuration (currently unused, kept for future options)
    ///
    /// # Errors
    ///
    /// Returns an error if database connection or schema initialization fails.
    pub async fn new(database_url: &str, _options: Option<TursoOptions>) -> Result<Self, sqlx::Error> {
        let database_path = Self::normalize_database_path(database_url);
        let is_memory = database_path == ":memory:";
        let pool = SqlitePool::connect(&database_path).await?;

        // Configure Turso's SQLite-compatible engine with the same practical defaults
        // as the sqlx-backed sqlite provider where the PRAGMAs are supported.
        if is_memory {
            Self::try_optional_pragma(&pool, "PRAGMA journal_mode = MEMORY").await;
            Self::try_optional_pragma(&pool, "PRAGMA synchronous = OFF").await;
        } else {
            Self::try_optional_pragma(&pool, "PRAGMA journal_mode = WAL").await;
            Self::try_optional_pragma(&pool, "PRAGMA synchronous = WAL").await;
            Self::try_optional_pragma(&pool, "PRAGMA wal_autocheckpoint = 10000").await;
            Self::try_optional_pragma(&pool, "PRAGMA cache_size = -64000").await;
        }
        pool.execute_on_all("PRAGMA foreign_keys = ON").await?;

        Self::create_schema(&pool).await?;

        Ok(Self { pool })
    }

    async fn try_optional_pragma(pool: &SqlitePool, statement: &'static str) {
        if let Err(error) = pool.execute_on_all(statement).await {
            debug!(%statement, %error, "optional Turso PRAGMA setup failed");
        }
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

super::sqlite_common::define_sqlite_like_provider!(TursoProvider, "turso", "duroxide::providers::turso");
