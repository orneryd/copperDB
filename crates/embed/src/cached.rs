//! LRU-cached embedder matching NornicDB's `cached_embedder.go`.
//!
//! Wraps any [`Embedder`] with an LRU cache keyed by FNV-1a hash of the input text.
//! Cache hit: ~1µs. Cache miss: delegates to the underlying embedder.
//!
//! # Performance
//! - Cache hit: ~1µs (vs 50-200ms for actual embedding)
//! - Memory: ~4KB per cached embedding (1024 dims × 4 bytes)
//! - 10K cache = ~40MB memory

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use super::{Embedder, EmbedError, Embedding};

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
    base: Box<dyn Embedder>,
    inner: Mutex<CacheInner>,
    hits: AtomicU64,
    misses: AtomicU64,
}

struct CacheInner {
    map: HashMap<String, usize>,
    entries: Vec<Option<LruEntry>>,
    live_len: usize,
    head: Option<usize>,
    tail: Option<usize>,
    max_size: usize,
}

impl CachedEmbedder {
    /// Wrap an embedder with LRU caching.
    pub fn new(base: Box<dyn Embedder>, max_size: usize) -> Self {
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
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    pub fn hit_count(&self) -> u64 { self.hits.load(Ordering::Relaxed) }
    pub fn miss_count(&self) -> u64 { self.misses.load(Ordering::Relaxed) }

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
        self.misses.fetch_add(1, Ordering::Relaxed);

        // Delegate to base embedder (synchronous call from blocking thread)
        let embeddings = self.base.embed_batch_blocking(&[text.to_string()])?;
        let embedding = embeddings.into_iter().next()
            .ok_or_else(|| EmbedError::LocalModel("no embedding returned".into()))?;

        // Insert into cache
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(&idx) = inner.map.get(&key) {
                Self::move_to_front(&mut inner, idx);
                return Ok(inner.entries[idx]
                    .as_ref()
                    .expect("cache map must reference a live entry")
                    .embedding
                    .clone());
            }
            if inner.live_len >= inner.max_size {
                Self::evict_lru(&mut inner);
            }
            let old_head = inner.head;
            let entry = LruEntry {
                key: key.clone(),
                embedding: embedding.clone(),
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
        Ok(embedding)
    }

    fn move_to_front(inner: &mut CacheInner, idx: usize) {
        if inner.head == Some(idx) { return; }
        // Remove from current position
        let entry = inner.entries[idx]
            .as_ref()
            .expect("LRU entry must be live");
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
        if inner.tail == Some(idx) { inner.tail = prev; }
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
        if inner.tail.is_none() { inner.tail = Some(idx); }
    }

    fn evict_lru(inner: &mut CacheInner) {
        if let Some(tail) = inner.tail {
            let entry = inner.entries[tail]
                .take()
                .expect("LRU tail must be live");
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
            if inner.head == Some(tail) { inner.head = None; }
            inner.live_len -= 1;
        }
    }
}
