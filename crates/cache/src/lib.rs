//! Thread-safe LRU query plan cache with TTL expiration.
//!
//! Equivalent to Go's `pkg/cache` in NornicDB.
//!
//! Avoids re-parsing identical Cypher queries by caching parsed query plans.
//! Uses FNV-1a hashing, LRU eviction, and optional TTL expiry.
//!
//! # Example
//! ```
//! use copperdb_cache::QueryCache;
//! use std::time::Duration;
//!
//! let cache = QueryCache::new(1000, Some(Duration::from_secs(300)));
//! cache.put(42u64, "plan".to_string());
//! assert!(cache.get(42u64).is_some());
//! ```

use fnv::FnvHashMap;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// A thread-safe LRU cache for parsed query plans.
pub struct QueryCache<V> {
    inner: Mutex<CacheInner<V>>,
    hits: AtomicU64,
    misses: AtomicU64,
}

struct CacheInner<V> {
    map: FnvHashMap<u64, CacheEntry<V>>,
    order: VecDeque<u64>,
    max_size: usize,
    ttl: Option<Duration>,
}

struct CacheEntry<V> {
    value: V,
    inserted_at: Instant,
}

impl<V: Clone> QueryCache<V> {
    /// Create a new cache with the given capacity and optional TTL.
    pub fn new(max_size: usize, ttl: Option<Duration>) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
                map: FnvHashMap::with_capacity_and_hasher(max_size, Default::default()),
                order: VecDeque::with_capacity(max_size),
                max_size,
                ttl,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Retrieve a cached value by key.
    pub fn get(&self, key: u64) -> Option<V> {
        let mut inner = self.inner.lock();
        if let Some(entry) = inner.map.get(&key) {
            if let Some(ttl) = inner.ttl {
                if entry.inserted_at.elapsed() > ttl {
                    inner.map.remove(&key);
                    inner.order.retain(|k| *k != key);
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            }
            let value = entry.value.clone();
            // Move to front of LRU
            inner.order.retain(|k| *k != key);
            inner.order.push_front(key);
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Some(value);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert a value. Evicts the least-recently-used entry if at capacity.
    pub fn put(&self, key: u64, value: V) {
        let mut inner = self.inner.lock();
        if inner.map.contains_key(&key) {
            inner.order.retain(|k| *k != key);
        } else if inner.map.len() >= inner.max_size {
            if let Some(evict_key) = inner.order.pop_back() {
                inner.map.remove(&evict_key);
            }
        }
        inner.map.insert(
            key,
            CacheEntry {
                value,
                inserted_at: Instant::now(),
            },
        );
        inner.order.push_front(key);
    }

    /// Cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            size: self.inner.lock().map.len(),
        }
    }

    /// Generate a cache key from a Cypher query and its parameter keys.
    pub fn key(query: &str, param_keys: &[&str]) -> u64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = fnv::FnvHasher::default();
        query.hash(&mut hasher);
        for k in param_keys {
            k.hash(&mut hasher);
        }
        hasher.finish()
    }
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_put_get() {
        let cache: QueryCache<String> = QueryCache::new(10, None);
        cache.put(1, "plan_a".to_string());
        assert_eq!(cache.get(1), Some("plan_a".to_string()));
    }

    #[test]
    fn test_lru_eviction() {
        let cache: QueryCache<i32> = QueryCache::new(2, None);
        cache.put(1, 10);
        cache.put(2, 20);
        cache.put(3, 30); // should evict key=1
        assert!(cache.get(1).is_none());
        assert_eq!(cache.get(2), Some(20));
        assert_eq!(cache.get(3), Some(30));
    }

    #[test]
    fn test_ttl_expiry() {
        let cache: QueryCache<i32> = QueryCache::new(10, Some(Duration::from_millis(1)));
        cache.put(1, 99);
        std::thread::sleep(Duration::from_millis(10));
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_key_generation() {
        let k1 = QueryCache::<i32>::key("MATCH (n) RETURN n", &["id"]);
        let k2 = QueryCache::<i32>::key("MATCH (n) RETURN n", &["id"]);
        let k3 = QueryCache::<i32>::key("MATCH (n) RETURN n", &["name"]);
        assert_eq!(k1, k2);
        assert_ne!(k1, k3);
    }

    #[test]
    fn test_stats() {
        let cache: QueryCache<i32> = QueryCache::new(10, None);
        cache.put(1, 1);
        let _ = cache.get(1);
        let _ = cache.get(2);
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_hit_rate() {
        let cache: QueryCache<i32> = QueryCache::new(10, None);
        cache.put(1, 10);
        cache.get(1);
        cache.get(1);
        cache.get(2); // miss
        let stats = cache.stats();
        let rate = stats.hit_rate();
        assert!((rate - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_overwrite_existing_key() {
        let cache: QueryCache<i32> = QueryCache::new(10, None);
        cache.put(1, 10);
        cache.put(1, 99);
        assert_eq!(cache.get(1), Some(99));
        assert_eq!(cache.stats().size, 1);
    }

    #[test]
    fn test_empty_hit_rate() {
        let cache: QueryCache<i32> = QueryCache::new(10, None);
        assert_eq!(cache.stats().hit_rate(), 0.0);
    }

    #[test]
    fn test_lru_order_maintained_on_access() {
        let cache: QueryCache<i32> = QueryCache::new(2, None);
        cache.put(1, 10);
        cache.put(2, 20);
        // Access key=1 to make it recently used
        cache.get(1);
        // Now insert key=3: should evict key=2 (LRU)
        cache.put(3, 30);
        assert_eq!(cache.get(1), Some(10));
        assert!(cache.get(2).is_none());
        assert_eq!(cache.get(3), Some(30));
    }
}
