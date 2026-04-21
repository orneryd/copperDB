//! Connection pool for copperdb driver connections.
//!
//! Equivalent to Go's `pkg/pool` in NornicDB.
//! Manages a pool of reusable database connections to reduce handshake overhead.

use parking_lot::Mutex;
use std::collections::VecDeque;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PoolError {
    #[error("pool exhausted (max {0} connections)")]
    Exhausted(usize),
    #[error("connection error: {0}")]
    ConnectionError(String),
    #[error("pool closed")]
    Closed,
}

/// A pooled connection handle. Returns the connection to the pool on drop.
pub struct PooledConnection<T> {
    inner: Option<T>,
    pool: std::sync::Weak<ConnectionPool<T>>,
}

impl<T> std::ops::Deref for PooledConnection<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.inner.as_ref().expect("connection already returned")
    }
}

impl<T> std::ops::DerefMut for PooledConnection<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.inner.as_mut().expect("connection already returned")
    }
}

impl<T> Drop for PooledConnection<T> {
    fn drop(&mut self) {
        if let (Some(conn), Some(pool)) = (self.inner.take(), self.pool.upgrade()) {
            let mut guard = pool.connections.lock();
            if !pool.closed.load(std::sync::atomic::Ordering::Relaxed) && guard.len() < pool.max_size {
                guard.push_back(conn);
            }
        }
    }
}

/// Generic connection pool.
pub struct ConnectionPool<T> {
    connections: Mutex<VecDeque<T>>,
    max_size: usize,
    closed: std::sync::atomic::AtomicBool,
}

impl<T: Send> ConnectionPool<T> {
    pub fn new(max_size: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Self {
            connections: Mutex::new(VecDeque::new()),
            max_size,
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Seed the pool with pre-created connections.
    pub fn seed(self: &std::sync::Arc<Self>, connections: Vec<T>) {
        let mut guard = self.connections.lock();
        for conn in connections {
            guard.push_back(conn);
        }
    }

    /// Acquire a connection from the pool.
    pub fn acquire(self: &std::sync::Arc<Self>) -> Result<PooledConnection<T>, PoolError> {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(PoolError::Closed);
        }
        let conn = {
            let mut guard = self.connections.lock();
            guard.pop_front()
        };
        conn.map(|inner| PooledConnection {
            inner: Some(inner),
            pool: std::sync::Arc::downgrade(self),
        }).ok_or_else(|| PoolError::Exhausted(self.max_size))
    }

    pub fn available(&self) -> usize {
        self.connections.lock().len()
    }

    pub fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::Relaxed);
        self.connections.lock().clear();
    }
}

/// A simpler, non-RAII generic connection pool.
///
/// Connections are manually acquired and released; there is no automatic
/// return-on-drop. Useful for simple resource management scenarios.
pub struct Pool<C: Send + 'static> {
    connections: std::sync::Arc<Mutex<VecDeque<C>>>,
    max_size: usize,
    created: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl<C: Send + 'static> Pool<C> {
    pub fn new(max_size: usize) -> Self {
        Self {
            connections: std::sync::Arc::new(Mutex::new(VecDeque::new())),
            max_size,
            created: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    /// Acquire a connection from the pool (returns `None` if empty).
    pub fn acquire(&self) -> Option<C> {
        self.connections.lock().pop_front()
    }

    /// Return a connection to the pool. Drops it if the pool is at capacity.
    pub fn release(&self, conn: C) {
        let mut guard = self.connections.lock();
        if guard.len() < self.max_size {
            guard.push_back(conn);
        }
    }

    /// Number of connections currently in the pool (available).
    pub fn available(&self) -> usize {
        self.connections.lock().len()
    }

    /// Track how many connections have been created externally.
    pub fn record_created(&self) {
        self.created.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Number of connections currently in use (created but not in pool).
    pub fn in_use(&self) -> usize {
        let created = self.created.load(std::sync::atomic::Ordering::Relaxed);
        let avail = self.available();
        created.saturating_sub(avail)
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_acquire_and_return() {
        let pool = ConnectionPool::new(2);
        pool.seed(vec![1i32, 2i32]);
        {
            let conn = pool.acquire().unwrap();
            assert_eq!(*conn, 1);
            assert_eq!(pool.available(), 1);
        }
        // Returned on drop
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_pool_exhausted() {
        let pool: std::sync::Arc<ConnectionPool<i32>> = ConnectionPool::new(2);
        assert!(matches!(pool.acquire(), Err(PoolError::Exhausted(2))));
    }

    #[test]
    fn test_simple_pool_acquire_release() {
        let pool: Pool<i32> = Pool::new(3);
        pool.release(10);
        pool.release(20);
        assert_eq!(pool.available(), 2);
        let v = pool.acquire().unwrap();
        assert_eq!(v, 10);
        assert_eq!(pool.available(), 1);
        pool.release(v);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_simple_pool_capacity() {
        let pool: Pool<i32> = Pool::new(2);
        pool.release(1);
        pool.release(2);
        // Releasing a 3rd connection to a full pool drops it
        pool.release(3);
        assert_eq!(pool.available(), 2);
    }

    #[test]
    fn test_simple_pool_empty_acquire() {
        let pool: Pool<i32> = Pool::new(2);
        assert!(pool.acquire().is_none());
    }

    #[test]
    fn test_simple_pool_in_use() {
        let pool: Pool<i32> = Pool::new(3);
        pool.record_created();
        pool.record_created();
        pool.release(1);
        // 2 created, 1 in pool => 1 in use
        assert_eq!(pool.in_use(), 1);
    }
}
