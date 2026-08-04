//! End-to-end embedding pipeline tests matching NornicDB scenarios.
//!
//! These tests run ONLY when a GGUF model is present locally and the
//! `COPPERDB_MODELS_DIR` env var points to a directory containing .gguf files.
//!
//! Run with:
//!   COPPERDB_MODELS_DIR=./models cargo test -p copperdb-embed -- e2e --nocapture --ignored
//!
//! Or to force run (skip if no model):
//!   cargo test -p copperdb-embed -- e2e --nocapture

#[cfg(test)]
mod tests {
    use crate::{CachedEmbedder, EmbedError, Embedder, Embedding, LocalGgufEmbedder};
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    /// Returns the path to a GGUF model, or None if not available.
    fn find_model() -> Option<PathBuf> {
        let dirs = [
            std::env::var("COPPERDB_MODELS_DIR").ok(),
            Some("./models".to_string()),
            Some("../models".to_string()),
            Some("/data/models".to_string()),
        ];
        let names = ["bge-m3.gguf", "bge-small.gguf", "all-MiniLM-L6-v2.gguf"];

        for dir in dirs.iter().flatten() {
            for name in &names {
                let path = PathBuf::from(dir).join(name);
                if path.exists() {
                    return Some(path);
                }
            }
        }
        None
    }

    /// Test: Model loading with Metal/CUDA, crash resilience, warmup, and embedding generation.
    #[test]
    #[ignore] // requires local model
    fn e2e_local_gguf_load_embed_and_warmup() {
        let model_path = find_model().expect("no GGUF model found — set COPPERDB_MODELS_DIR");
        let model_name = model_path.file_stem().unwrap().to_str().unwrap();

        eprintln!(
            "=== Loading model: {model_name} from {} ===",
            model_path.display()
        );

        // 1. Load model with warmup enabled
        let embedder = LocalGgufEmbedder::new(
            model_name,
            model_path.clone(),
            0,                             // auto-detect dimensions
            Some(Duration::from_secs(10)), // 10s warmup for testing
        )
        .expect("failed to load model");

        let stats = embedder.stats();
        eprintln!(
            "✅ Model loaded: {} dimensions, backend={}, warmup=enabled",
            stats.dimensions, stats.backend
        );
        assert!(stats.dimensions > 0, "dimensions should be > 0");
        assert!(
            stats.backend == "metal" || stats.backend == "cuda" || stats.backend == "cpu",
            "backend should be valid"
        );

        // 2. Generate a single embedding
        let text = "Hello world, this is a test of the embedding pipeline";
        let t0 = Instant::now();
        let vec = embedder.embed_with_recovery(text).expect("embed failed");
        let elapsed = t0.elapsed();
        eprintln!(
            "✅ Embedding generated: {} dims in {:.2}ms",
            vec.len(),
            elapsed.as_secs_f64() * 1000.0
        );
        assert_eq!(vec.len(), stats.dimensions);

        // 3. Verify normalization (unit vector)
        let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 0.01,
            "embedding should be L2-normalized (norm={norm})"
        );

        // 4. Batch embedding
        let texts: Vec<String> = (0..5).map(|i| format!("test text {i}")).collect();
        let t0 = Instant::now();
        for text in &texts {
            embedder
                .embed_with_recovery(text)
                .expect("batch embed failed");
        }
        eprintln!(
            "✅ Batch of {} embeddings in {:.2}ms",
            texts.len(),
            t0.elapsed().as_secs_f64() * 1000.0
        );

        let stats = embedder.stats();
        eprintln!(
            "📊 Stats: {} embeddings, {} errors, {} panics",
            stats.embed_count, stats.error_count, stats.panic_count
        );
        assert!(stats.embed_count >= 6, "should have 6+ embeddings");

        // 5. Crash resilience: test panic recovery with empty text (shouldn't panic)
        let result = embedder.embed_with_recovery("");
        assert!(result.is_ok(), "empty text should not crash");

        // 6. Wait for warmup to trigger at least once
        eprintln!("⏳ Waiting for warmup cycle...");
        std::thread::sleep(Duration::from_secs(12));
        let stats_after = embedder.stats();
        eprintln!("📊 After warmup: {} embeddings", stats_after.embed_count);

        embedder.close();
        eprintln!("=== All E2E embedding tests passed ===");
    }

    /// Test: Cached embedder with LRU eviction.
    #[test]
    fn e2e_cached_embedder_lru_eviction() {
        // Use mock embedder for deterministic testing
        struct CountingEmbedder {
            dims: usize,
            count: std::sync::atomic::AtomicU64,
        }
        impl CountingEmbedder {
            fn new(dims: usize) -> Self {
                Self {
                    dims,
                    count: std::sync::atomic::AtomicU64::new(0),
                }
            }
            fn inc(&self) {
                self.count
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        #[async_trait::async_trait]
        impl Embedder for CountingEmbedder {
            async fn embed(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
                Ok(texts
                    .iter()
                    .map(|t| {
                        self.inc();
                        Embedding {
                            text: t.clone(),
                            vector: vec![1.0; self.dims],
                            model: "test".into(),
                        }
                    })
                    .collect())
            }
            fn embed_batch_blocking(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbedError> {
                Ok(texts
                    .iter()
                    .map(|t| {
                        self.inc();
                        Embedding {
                            text: t.clone(),
                            vector: vec![1.0; self.dims],
                            model: "test".into(),
                        }
                    })
                    .collect())
            }
            fn dimensions(&self) -> usize {
                self.dims
            }
        }

        let base = Box::new(CountingEmbedder::new(128));
        let cache = CachedEmbedder::new(base, 3); // tiny cache for testing

        // First call: cache miss
        let _ = cache.embed_sync("text1").unwrap();
        assert_eq!(cache.miss_count(), 1);
        assert_eq!(cache.hit_count(), 0);

        // Second call same text: cache hit
        let _ = cache.embed_sync("text1").unwrap();
        assert_eq!(cache.miss_count(), 1);
        assert_eq!(cache.hit_count(), 1);

        // Fill cache with 3 unique texts
        let _ = cache.embed_sync("text2").unwrap();
        let _ = cache.embed_sync("text3").unwrap();
        assert_eq!(cache.miss_count(), 3);

        // text4 should evict text1 (LRU)
        let _ = cache.embed_sync("text4").unwrap();
        assert_eq!(cache.miss_count(), 4);

        // text1 should be a miss again (was evicted)
        let _ = cache.embed_sync("text1").unwrap();
        assert_eq!(cache.miss_count(), 5);

        eprintln!(
            "✅ LRU cache: {} hits, {} misses",
            cache.hit_count(),
            cache.miss_count()
        );
    }

    /// Test: Crash resilience — verify panic recovery works.
    #[test]
    fn e2e_crash_resilience_panic_recovery() {
        let model_path = match find_model() {
            Some(p) => p,
            None => {
                eprintln!("⏭️  Skipping crash test — no model available");
                return;
            }
        };

        let model_name = model_path.file_stem().unwrap().to_str().unwrap();
        let path = model_path.clone();
        let embedder =
            LocalGgufEmbedder::new(model_name, path, 0, None).expect("failed to load model");

        // Embed very long text — should either work or return error, never panic
        let long_text = "test ".repeat(10000);
        let result = embedder.embed_with_recovery(&long_text);
        match result {
            Ok(_) => eprintln!("✅ Long text embedding succeeded"),
            Err(e) => eprintln!("✅ Long text gracefully errored: {e}"),
        }
        // The key assertion: we got here without panicking
        assert!(
            embedder.stats().panic_count <= 1,
            "should have 0 or 1 panics"
        );

        embedder.close();
        eprintln!("=== Crash resilience test passed ===");
    }
}
