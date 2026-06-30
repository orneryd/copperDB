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
use std::hash::{Hash, Hasher};
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
    entries: Vec<LruEntry>,
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

    fn cache_key(text: &str) -> String {
        let mut hasher = fnv::FnvHasher::default();
        text.hash(&mut hasher);
        format!("{:x}", hasher.finish())
    }

    pub fn embed_sync(&self, text: &str) -> Result<Embedding, EmbedError> {
        let key = Self::cache_key(text);

        // Check cache
        {
            let mut inner = self.inner.lock().unwrap();
            if let Some(&idx) = inner.map.get(&key) {
                // Move to front (most recently used)
                Self::move_to_front(&mut inner, idx);
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok(inner.entries[idx].embedding.clone());
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
            while inner.entries.len() >= inner.max_size {
                Self::evict_lru(&mut inner);
            }
            let old_head = inner.head;
            let idx = inner.entries.len();
            inner.entries.push(LruEntry {
                key: key.clone(),
                embedding: embedding.clone(),
                prev: None,
                next: old_head,
            });
            if let Some(head) = old_head {
                inner.entries[head].prev = Some(idx);
            }
            inner.head = Some(idx);
            if inner.tail.is_none() {
                inner.tail = Some(idx);
            }
            inner.map.insert(key, idx);
        }
        Ok(embedding)
    }

    fn move_to_front(inner: &mut CacheInner, idx: usize) {
        if inner.head == Some(idx) { return; }
        // Remove from current position
        let prev = inner.entries[idx].prev;
        let next = inner.entries[idx].next;
        if let Some(p) = prev { inner.entries[p].next = next; }
        if let Some(n) = next { inner.entries[n].prev = prev; }
        if inner.tail == Some(idx) { inner.tail = prev; }
        // Insert at head
        inner.entries[idx].prev = None;
        inner.entries[idx].next = inner.head;
        if let Some(head) = inner.head { inner.entries[head].prev = Some(idx); }
        inner.head = Some(idx);
        if inner.tail.is_none() { inner.tail = Some(idx); }
    }

    fn evict_lru(inner: &mut CacheInner) {
        if let Some(tail) = inner.tail {
            let key = inner.entries[tail].key.clone();
            let prev = inner.entries[tail].prev;
            inner.map.remove(&key);
            if let Some(p) = prev { inner.entries[p].next = None; }
            inner.tail = prev;
            if inner.head == Some(tail) { inner.head = None; }
            // Note: we don't shrink entries vec — we reuse slots
        }
    }
}

// Use fnv hasher
mod fnv {
    use std::hash::Hasher;
    pub struct FnvHasher(u64);
    impl Default for FnvHasher { fn default() -> Self { FnvHasher(0xcbf29ce484222325) } }
    impl Hasher for FnvHasher {
        fn write(&mut self, bytes: &[u8]) {
            for &byte in bytes {
                self.0 ^= byte as u64;
                self.0 = self.0.wrapping_mul(0x100000001b3);
            }
        }
        fn finish(&self) -> u64 { self.0 }
    }
}
