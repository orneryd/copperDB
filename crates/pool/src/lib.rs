//! Connection pool for magnetDB driver connections.
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
}
