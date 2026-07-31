//! LRU-cached embedder matching NornicDB's `cached_embedder.go`.
//!
//! Wraps any [`Embedder`] with an LRU cache keyed by FNV-1a hash of the input text.
//! Cache hit: ~1µs. Cache miss: delegates to the underlying embedder.
//!
//! # Performance
//! - Cache hit: ~1µs (vs 50-200ms for actual embedding)
//! - Memory: ~4KB per cached embedding (1024 dims × 4 bytes)
//! - 10K cache = ~40MB memory

use super::{EmbedError, Embedder, Embedding};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

/// An LRU ring entry for cache eviction tracking.
struct LruEntry {
    key: String,
    embedding: Embedding,
    prev: Option<usize>,
    next: Option<usize>,
}

/// LRU-cached embedder matching NornicDB's `CachedEmbedder`.
///
/// Thread-safe: all methods protected by a mutex.
pub struct CachedEmbedder {
    base: Arc<dyn Embedder>,
    inner: Mutex<CacheInner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

struct CacheInner {
    map: HashMap<String, usize>,
    entries: Vec<Option<LruEntry>>,
    live_len: usize,
    head: Option<usize>,
    tail: Option<usize>,
    max_size: usize,
    flights: HashMap<String, Arc<Flight>>,
}

struct Flight {
    result: Mutex<Option<Result<Embedding, EmbedError>>>,
    completed: Condvar,
}

/// Snapshot of bounded cache state and request counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub live_entries: usize,
    pub capacity: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub active_flights: usize,
}

impl CachedEmbedder {
    /// Wrap an embedder with LRU caching.
    pub fn new(base: Box<dyn Embedder>, max_size: usize) -> Self {
        Self::from_arc(Arc::from(base), max_size)
    }

    /// Wrap a shared embedder with LRU caching.
    pub fn from_arc(base: Arc<dyn Embedder>, max_size: usize) -> Self {
        let max_size = if max_size == 0 { 10000 } else { max_size };
        Self {
            base,
            inner: Mutex::new(CacheInner {
                map: HashMap::with_capacity(max_size),
                entries: Vec::with_capacity(max_size),
                live_len: 0,
                head: None,
                tail: None,
                max_size,
                flights: HashMap::new(),
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
    pub fn miss_count(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock().unwrap();
        CacheStats {
            live_entries: inner.live_len,
            capacity: inner.max_size,
            hits: self.hit_count(),
            misses: self.miss_count(),
            evictions: self.evictions.load(Ordering::Relaxed),
            active_flights: inner.flights.len(),
        }
    }

    pub fn clear(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.map.clear();
        inner.entries.clear();
        inner.live_len = 0;
        inner.head = None;
        inner.tail = None;
    }

    pub fn embed_batch_sync(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
        texts.iter().map(|text| self.embed_sync(text)).collect()
    }

    pub fn embed_sync(&self, text: &str) -> Result<Embedding, EmbedError> {
        let key = text.to_owned();

        // Check cache
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(&idx) = inner.map.get(&key) {
                // Move to front (most recently used)
                Self::move_to_front(&mut inner, idx);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(inner.entries[idx]
                    .as_ref()
                    .expect("cache map must reference a live entry")
                    .embedding
                    .clone());
            }
        }
        let (flight, is_leader) = {
            let mut inner = self.inner.lock().unwrap();
            match inner.flights.get(&key) {
                Some(flight) => (Arc::clone(flight), false),
                None => {
                    let flight = Arc::new(Flight {
                        result: Mutex::new(None),
                        completed: Condvar::new(),
                    });
                    inner.flights.insert(key.clone(), Arc::clone(&flight));
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    (flight, true)
                }
            }
        };

        if !is_leader {
            let mut result = flight.result.lock().unwrap();
            while result.is_none() {
                result = flight.completed.wait(result).unwrap();
            }
            return result.clone().expect("completed flight must have a result");
        }

        // Delegate to base embedder (synchronous call from blocking thread)
        let result = self
            .base
            .embed_batch_blocking(&[text.to_string()])
            .and_then(|embeddings| {
                embeddings
                    .into_iter()
                    .next()
                    .ok_or_else(|| EmbedError::LocalModel("no embedding returned".into()))
            });

        // Insert into cache
        if let Ok(embedding) = &result {
            let mut inner = self.inner.lock().unwrap();
            if let Some(&idx) = inner.map.get(&key) {
                Self::move_to_front(&mut inner, idx);
                let cached = inner.entries[idx]
                    .as_ref()
                    .expect("cache map must reference a live entry")
                    .embedding
                    .clone();
                drop(inner);
                Self::complete_flight(&self.inner, &key, &flight, Ok(cached));
                return Ok(embedding.clone());
            }
            if inner.live_len >= inner.max_size {
                Self::evict_lru(&mut inner);
                self.evictions.fetch_add(1, Ordering::Relaxed);
            }
            Self::insert_entry(&mut inner, key.clone(), embedding.clone());
        }
        Self::complete_flight(&self.inner, &key, &flight, result.clone());
        result
    }

    fn complete_flight(
        inner: &Mutex<CacheInner>,
        key: &str,
        flight: &Flight,
        result: Result<Embedding, EmbedError>,
    ) {
        {
            let mut completed = flight.result.lock().unwrap();
            *completed = Some(result);
            flight.completed.notify_all();
        }
        inner.lock().unwrap().flights.remove(key);
    }

    fn insert_entry(inner: &mut CacheInner, key: String, embedding: Embedding) {
        let old_head = inner.head;
        let entry = LruEntry {
            key: key.clone(),
            embedding,
            prev: None,
            next: old_head,
        };
        let idx = inner
            .entries
            .iter()
            .position(Option::is_none)
            .unwrap_or_else(|| {
                inner.entries.push(None);
                inner.entries.len() - 1
            });
        inner.entries[idx] = Some(entry);
        if let Some(head) = old_head {
            inner.entries[head]
                .as_mut()
                .expect("LRU head must be live")
                .prev = Some(idx);
        }
        inner.head = Some(idx);
        if inner.tail.is_none() {
            inner.tail = Some(idx);
        }
        inner.map.insert(key, idx);
        inner.live_len += 1;
    }

    fn move_to_front(inner: &mut CacheInner, idx: usize) {
        if inner.head == Some(idx) {
            return;
        }
        // Remove from current position
        let entry = inner.entries[idx].as_ref().expect("LRU entry must be live");
        let prev = entry.prev;
        let next = entry.next;
        if let Some(p) = prev {
            inner.entries[p]
                .as_mut()
                .expect("LRU previous entry must be live")
                .next = next;
        }
        if let Some(n) = next {
            inner.entries[n]
                .as_mut()
                .expect("LRU next entry must be live")
                .prev = prev;
        }
        if inner.tail == Some(idx) {
            inner.tail = prev;
        }
        // Insert at head
        inner.entries[idx]
            .as_mut()
            .expect("LRU entry must be live")
            .prev = None;
        inner.entries[idx]
            .as_mut()
            .expect("LRU entry must be live")
            .next = inner.head;
        if let Some(head) = inner.head {
            inner.entries[head]
                .as_mut()
                .expect("LRU head must be live")
                .prev = Some(idx);
        }
        inner.head = Some(idx);
        if inner.tail.is_none() {
            inner.tail = Some(idx);
        }
    }

    fn evict_lru(inner: &mut CacheInner) {
        if let Some(tail) = inner.tail {
            let entry = inner.entries[tail].take().expect("LRU tail must be live");
            let key = entry.key;
            let prev = entry.prev;
            inner.map.remove(&key);
            if let Some(p) = prev {
                inner.entries[p]
                    .as_mut()
                    .expect("LRU previous entry must be live")
                    .next = None;
            }
            inner.tail = prev;
            if inner.head == Some(tail) {
                inner.head = None;
            }
            inner.live_len -= 1;
        }
    }
}

#[async_trait::async_trait]
impl Embedder for CachedEmbedder {
    async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
        self.embed_batch_sync(texts)
    }

    fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
        self.embed_batch_sync(texts)
    }

    fn dimensions(&self) -> usize {
        self.base.dimensions()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    struct CountingEmbedder {
        calls: AtomicUsize,
        delay: Duration,
        active_calls: AtomicUsize,
        peak_active_calls: AtomicUsize,
    }

    struct FailingEmbedder {
        calls: AtomicUsize,
        delay: Duration,
    }

    impl CountingEmbedder {
        fn new(delay: Duration) -> Self {
            Self {
                calls: AtomicUsize::new(0),
                delay,
                active_calls: AtomicUsize::new(0),
                peak_active_calls: AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl Embedder for CountingEmbedder {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.embed_batch_blocking(texts)
        }

        fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let active = self.active_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak_active_calls.fetch_max(active, Ordering::SeqCst);
            thread::sleep(self.delay);
            let embeddings = texts
                .iter()
                .map(|text| Embedding {
                    text: text.clone(),
                    vector: vec![1.0],
                    model: "test".into(),
                })
                .collect();
            self.active_calls.fetch_sub(1, Ordering::SeqCst);
            Ok(embeddings)
        }

        fn dimensions(&self) -> usize {
            1
        }
    }

    #[test]
    fn same_key_concurrent_misses_share_one_base_call() {
        let base = Arc::new(CountingEmbedder::new(Duration::from_millis(25)));
        let cache = Arc::new(CachedEmbedder::new(
            Box::new(SharedCountingEmbedder(base.clone())),
            2,
        ));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || cache.embed_sync("same-key").unwrap())
            })
            .collect();

        for thread in threads {
            assert_eq!(thread.join().unwrap().text, "same-key");
        }
        assert_eq!(base.calls.load(Ordering::SeqCst), 1);
        assert_eq!(cache.miss_count(), 1);
        assert_eq!(cache.stats().active_flights, 0);
    }

    #[test]
    fn distinct_key_misses_make_progress_concurrently() {
        let base = Arc::new(CountingEmbedder::new(Duration::from_millis(25)));
        let cache = Arc::new(CachedEmbedder::new(
            Box::new(SharedCountingEmbedder(base.clone())),
            2,
        ));
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let threads: Vec<_> = ["first", "second"]
            .into_iter()
            .map(|key| {
                let cache = Arc::clone(&cache);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    cache.embed_sync(key).unwrap();
                })
            })
            .collect();

        barrier.wait();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(base.calls.load(Ordering::SeqCst), 2);
        assert_eq!(base.peak_active_calls.load(Ordering::SeqCst), 2);
    }

    struct SharedCountingEmbedder(Arc<CountingEmbedder>);

    #[async_trait::async_trait]
    impl Embedder for SharedCountingEmbedder {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.0.embed(texts).await
        }

        fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.0.embed_batch_blocking(texts)
        }

        fn dimensions(&self) -> usize {
            self.0.dimensions()
        }
    }

    #[async_trait::async_trait]
    impl Embedder for FailingEmbedder {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.embed_batch_blocking(texts)
        }

        fn embed_batch_blocking(&self, _texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            thread::sleep(self.delay);
            Err(EmbedError::LocalModel("expected failure".into()))
        }

        fn dimensions(&self) -> usize {
            1
        }
    }

    #[test]
    fn capacity_one_replaces_entries_and_remains_bounded() {
        let base = Arc::new(CountingEmbedder::new(Duration::ZERO));
        let cache = CachedEmbedder::new(Box::new(SharedCountingEmbedder(base.clone())), 1);

        cache.embed_sync("a").unwrap();
        cache.embed_sync("b").unwrap();
        cache.embed_sync("a").unwrap();

        assert_eq!(base.calls.load(Ordering::SeqCst), 3);
        assert_eq!(cache.stats().live_entries, 1);
        assert_eq!(cache.stats().evictions, 2);
    }

    #[test]
    fn promotion_evicts_the_least_recently_used_entry() {
        let base = Arc::new(CountingEmbedder::new(Duration::ZERO));
        let cache = CachedEmbedder::new(Box::new(SharedCountingEmbedder(base.clone())), 2);

        cache.embed_sync("a").unwrap();
        cache.embed_sync("b").unwrap();
        cache.embed_sync("a").unwrap();
        cache.embed_sync("c").unwrap();
        cache.embed_sync("a").unwrap();
        cache.embed_sync("b").unwrap();

        assert_eq!(base.calls.load(Ordering::SeqCst), 4);
        assert_eq!(cache.stats().live_entries, 2);
    }

    #[test]
    fn repeated_churn_and_clear_stay_within_capacity() {
        let base = Arc::new(CountingEmbedder::new(Duration::ZERO));
        let cache = CachedEmbedder::new(Box::new(SharedCountingEmbedder(base)), 3);

        for index in 0..300 {
            cache.embed_sync(&format!("key-{index}")).unwrap();
            assert!(cache.stats().live_entries <= 3);
        }
        assert_eq!(cache.stats().live_entries, 3);
        cache.clear();
        assert_eq!(cache.stats().live_entries, 0);
    }

    #[test]
    fn batch_embedding_reuses_the_scalar_cache() {
        let base = Arc::new(CountingEmbedder::new(Duration::ZERO));
        let cache = CachedEmbedder::new(Box::new(SharedCountingEmbedder(base.clone())), 3);
        let texts = vec!["a".to_string(), "b".to_string(), "a".to_string()];

        let embeddings = cache.embed_batch_blocking(&texts).unwrap();

        assert_eq!(embeddings.len(), 3);
        assert_eq!(base.calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.hit_count(), 1);
    }

    #[test]
    fn same_key_concurrent_failures_share_one_base_call() {
        let base = Arc::new(FailingEmbedder {
            calls: AtomicUsize::new(0),
            delay: Duration::from_millis(25),
        });
        let cache = Arc::new(CachedEmbedder::new(
            Box::new(SharedFailingEmbedder(base.clone())),
            2,
        ));
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let cache = Arc::clone(&cache);
                thread::spawn(move || cache.embed_sync("same-key").unwrap_err().to_string())
            })
            .collect();

        for thread in threads {
            assert_eq!(
                thread.join().unwrap(),
                "local model error: expected failure"
            );
        }
        assert_eq!(base.calls.load(Ordering::SeqCst), 1);
        assert_eq!(cache.stats().active_flights, 0);
    }

    struct SharedFailingEmbedder(Arc<FailingEmbedder>);

    #[async_trait::async_trait]
    impl Embedder for SharedFailingEmbedder {
        async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.0.embed(texts).await
        }

        fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
            self.0.embed_batch_blocking(texts)
        }

        fn dimensions(&self) -> usize {
            self.0.dimensions()
        }
    }
}
