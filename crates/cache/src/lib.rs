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
use serde_json::Value;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub const DEFAULT_QUERY_CACHE_SIZE: usize = 1000;

/// A thread-safe LRU cache for parsed query plans.
pub struct QueryCache<V> {
    inner: Mutex<CacheInner<V>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

struct CacheInner<V> {
    map: FnvHashMap<u64, CacheEntry<V>>,
    order: VecDeque<u64>,
    max_size: usize,
    ttl: Option<Duration>,
    enabled: bool,
}

struct CacheEntry<V> {
    value: V,
    inserted_at: Instant,
}

impl<V: Clone> QueryCache<V> {
    /// Create a new cache with the given capacity and optional TTL.
    pub fn new(max_size: usize, ttl: Option<Duration>) -> Self {
        let max_size = if max_size == 0 {
            DEFAULT_QUERY_CACHE_SIZE
        } else {
            max_size
        };

        Self {
            inner: Mutex::new(CacheInner {
                map: FnvHashMap::with_capacity_and_hasher(max_size, Default::default()),
                order: VecDeque::with_capacity(max_size),
                max_size,
                ttl,
                enabled: true,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Retrieve a cached value by key.
    pub fn get(&self, key: u64) -> Option<V> {
        let mut inner = self.inner.lock();
        if !inner.enabled {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        if let Some(entry) = inner.map.get(&key) {
            if let Some(ttl) = inner.ttl {
                if entry.inserted_at.elapsed() > ttl {
                    inner.map.remove(&key);
                    inner.order.retain(|k| *k != key);
                    self.evictions.fetch_add(1, Ordering::Relaxed);
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
        if !inner.enabled {
            return;
        }

        if inner.map.contains_key(&key) {
            inner.order.retain(|k| *k != key);
        } else if inner.map.len() >= inner.max_size {
            if let Some(evict_key) = inner.order.pop_back() {
                inner.map.remove(&evict_key);
                self.evictions.fetch_add(1, Ordering::Relaxed);
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

    /// Remove one cached entry. Missing keys are ignored.
    pub fn remove(&self, key: u64) {
        let mut inner = self.inner.lock();
        if inner.map.remove(&key).is_some() {
            inner.order.retain(|candidate| *candidate != key);
        }
    }

    /// Clear cached entries without changing configuration or statistics.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.map.clear();
        inner.order.clear();
    }

    /// Enable or disable the cache. Disabling also drops resident entries.
    pub fn set_enabled(&self, enabled: bool) {
        let mut inner = self.inner.lock();
        inner.enabled = enabled;
        if !enabled {
            inner.map.clear();
            inner.order.clear();
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.lock().enabled
    }

    pub fn len(&self) -> usize {
        self.inner.lock().map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cache statistics.
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock();
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            size: inner.map.len(),
            max_size: inner.max_size,
            enabled: inner.enabled,
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
    pub evictions: u64,
    pub size: usize,
    pub max_size: usize,
    pub enabled: bool,
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

    pub fn hit_rate_percent(&self) -> f64 {
        self.hit_rate() * 100.0
    }
}

/// Parameter-sensitive LRU cache for read-only query results.
pub struct QueryResultCache<V> {
    inner: QueryCache<V>,
}

impl<V: Clone> QueryResultCache<V> {
    pub fn new(max_size: usize, ttl: Option<Duration>) -> Self {
        Self {
            inner: QueryCache::new(max_size, ttl),
        }
    }

    pub fn get(&self, cypher: &str, params: &BTreeMap<String, Value>) -> Option<V> {
        self.inner.get(Self::key(cypher, params))
    }

    pub fn put(&self, cypher: &str, params: &BTreeMap<String, Value>, value: V) {
        self.inner.put(Self::key(cypher, params), value);
    }

    pub fn remove(&self, cypher: &str, params: &BTreeMap<String, Value>) {
        self.inner.remove(Self::key(cypher, params));
    }

    pub fn invalidate(&self) {
        self.inner.clear();
    }

    pub fn set_enabled(&self, enabled: bool) {
        self.inner.set_enabled(enabled);
    }

    pub fn stats(&self) -> CacheStats {
        self.inner.stats()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn key(cypher: &str, params: &BTreeMap<String, Value>) -> u64 {
        use std::hash::{Hash, Hasher};

        let mut hasher = fnv::FnvHasher::default();
        cypher.hash(&mut hasher);
        for (param_key, param_value) in params {
            param_key.hash(&mut hasher);
            param_value.to_string().hash(&mut hasher);
        }
        hasher.finish()
    }
}

pub trait WriteThroughSink<V> {
    type Error;

    fn put(&self, key: u64, value: &V) -> Result<(), Self::Error>;
    fn remove(&self, key: u64) -> Result<(), Self::Error>;
    fn clear(&self) -> Result<(), Self::Error>;
}

/// Query cache wrapper that writes to its backing sink before updating memory.
pub struct WriteThroughQueryCache<V, S> {
    cache: QueryCache<V>,
    sink: S,
}

impl<V: Clone, S: WriteThroughSink<V>> WriteThroughQueryCache<V, S> {
    pub fn new(max_size: usize, ttl: Option<Duration>, sink: S) -> Self {
        Self {
            cache: QueryCache::new(max_size, ttl),
            sink,
        }
    }

    pub fn get(&self, key: u64) -> Option<V> {
        self.cache.get(key)
    }

    pub fn put(&self, key: u64, value: V) -> Result<(), S::Error> {
        self.sink.put(key, &value)?;
        self.cache.put(key, value);
        Ok(())
    }

    pub fn remove(&self, key: u64) -> Result<(), S::Error> {
        self.sink.remove(key)?;
        self.cache.remove(key);
        Ok(())
    }

    pub fn clear(&self) -> Result<(), S::Error> {
        self.sink.clear()?;
        self.cache.clear();
        Ok(())
    }

    pub fn stats(&self) -> CacheStats {
        self.cache.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        assert_eq!(cache.stats().evictions, 1);
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
        assert_eq!(stats.max_size, 10);
        assert!(stats.enabled);
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

    #[test]
    fn test_zero_capacity_uses_default() {
        let cache: QueryCache<i32> = QueryCache::new(0, None);
        assert_eq!(cache.stats().max_size, DEFAULT_QUERY_CACHE_SIZE);
    }

    #[test]
    fn test_remove_one_entry() {
        let cache: QueryCache<&str> = QueryCache::new(10, None);
        cache.put(1, "one");
        cache.put(2, "two");

        cache.remove(1);

        assert!(cache.get(1).is_none());
        assert_eq!(cache.get(2), Some("two"));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_clear_entries_preserves_statistics() {
        let cache: QueryCache<i32> = QueryCache::new(10, None);
        cache.put(1, 1);
        assert_eq!(cache.get(1), Some(1));

        cache.clear();

        assert!(cache.is_empty());
        assert_eq!(cache.stats().hits, 1);
        assert!(cache.get(1).is_none());
    }

    #[test]
    fn test_set_enabled_disables_and_clears() {
        let cache: QueryCache<i32> = QueryCache::new(10, None);
        cache.put(1, 1);

        cache.set_enabled(false);

        assert!(!cache.is_enabled());
        assert!(cache.is_empty());
        cache.put(2, 2);
        assert!(cache.get(2).is_none());

        cache.set_enabled(true);
        cache.put(2, 2);
        assert_eq!(cache.get(2), Some(2));
    }

    #[test]
    fn test_concurrent_read_write_stays_bounded() {
        use std::sync::Arc;

        let cache = Arc::new(QueryCache::new(32, None));
        let mut handles = Vec::new();

        for worker_id in 0..16u64 {
            let cache = Arc::clone(&cache);
            handles.push(std::thread::spawn(move || {
                for iteration in 0..100u64 {
                    let key = worker_id * 100 + iteration;
                    cache.put(key, key);
                    let _ = cache.get(key);
                }
            }));
        }

        for handle in handles {
            handle.join().expect("cache worker panicked");
        }

        assert!(cache.len() <= 32);
        assert!(cache.stats().hits + cache.stats().misses > 0);
    }

    #[test]
    fn test_result_cache_key_includes_parameter_values() {
        let cache = QueryResultCache::new(10, None);
        let mut alice_params = BTreeMap::new();
        alice_params.insert("name".to_string(), Value::String("Alice".to_string()));
        let mut bob_params = BTreeMap::new();
        bob_params.insert("name".to_string(), Value::String("Bob".to_string()));

        cache.put("MATCH (n {name: $name}) RETURN n", &alice_params, "alice");

        assert_eq!(
            cache.get("MATCH (n {name: $name}) RETURN n", &alice_params),
            Some("alice")
        );
        assert_eq!(
            cache.get("MATCH (n {name: $name}) RETURN n", &bob_params),
            None
        );
    }

    #[test]
    fn test_result_cache_invalidation() {
        let cache = QueryResultCache::new(10, None);
        let params = BTreeMap::new();
        cache.put("MATCH (n) RETURN n", &params, 10);

        cache.invalidate();

        assert!(cache.is_empty());
        assert_eq!(cache.get("MATCH (n) RETURN n", &params), None);
    }

    #[derive(Default)]
    struct RecordingSink {
        values: Mutex<HashMap<u64, String>>,
        fail_put: bool,
    }

    impl WriteThroughSink<String> for RecordingSink {
        type Error = &'static str;

        fn put(&self, key: u64, value: &String) -> Result<(), Self::Error> {
            if self.fail_put {
                return Err("put failed");
            }
            self.values.lock().insert(key, value.clone());
            Ok(())
        }

        fn remove(&self, key: u64) -> Result<(), Self::Error> {
            self.values.lock().remove(&key);
            Ok(())
        }

        fn clear(&self) -> Result<(), Self::Error> {
            self.values.lock().clear();
            Ok(())
        }
    }

    #[test]
    fn test_write_through_updates_sink_before_cache() {
        let cache = WriteThroughQueryCache::new(10, None, RecordingSink::default());

        cache.put(1, "plan".to_string()).expect("write-through put");

        assert_eq!(cache.get(1), Some("plan".to_string()));
        assert_eq!(cache.sink.values.lock().get(&1), Some(&"plan".to_string()));
    }

    #[test]
    fn test_write_through_failure_does_not_fill_cache() {
        let cache = WriteThroughQueryCache::new(
            10,
            None,
            RecordingSink {
                values: Mutex::new(HashMap::new()),
                fail_put: true,
            },
        );

        assert_eq!(cache.put(1, "plan".to_string()), Err("put failed"));
        assert_eq!(cache.get(1), None);
    }
}
