# 06: Bounded Embedding Cache Capacity

Status: planned. Priority: P0. Owner: `embed`.

## Objective

Fix `CachedEmbedder` so eviction frees capacity, concurrent same-key misses share work, and memory remains bounded under churn.

## Current Defect

`CacheInner::evict_lru` removes lookup/LRU links without shrinking or reusing the entry slot. The `entries.len() >= max_size` loop can therefore never terminate after capacity. Concurrent same-key misses can also create duplicate work/slots.

## Design

Use either a proven bounded LRU or a reusable arena (`Vec<Option<Entry>>`, free-slot stack, and `live_len`). Maintain O(1) lookup/promotion. Add per-key single-flight state so one base embedding call serves concurrent identical requests; distinct keys remain concurrent. Store enough key identity to prevent hash-collision result confusion.

## Phases

1. Implement reusable eviction and post-computation cache double-check.
2. Add same-key single-flight with success/error wakeup and cancellation-safe cleanup.
3. Add cache statistics: live entries, capacity, hits, misses, evictions, and active flights.
4. Align batch behavior and clear semantics with the scalar API.

## Tests

- Capacity-one `A -> B -> A` replacement.
- Repeated churn at 100 times capacity without deadlock or growth.
- LRU promotion chooses the correct victim.
- Concurrent same-key success and error call the base embedder once per wave.
- Distinct keys make progress concurrently.
- Live entries never exceed capacity; clear returns to zero.

Run `cargo test -p copperdb-embed --lib`; use Miri or a concurrency model test where practical.

## Benchmark And Risks

Measure hit/miss latency and 1/8/32-thread same-key/disjoint throughput. Memory must be proportional to capacity plus active misses. Avoid poisoned waiter state, recursive embedder deadlock, and returning results for a colliding hash.

## Definition Of Done

The cache cannot hang at capacity, performs one base call per identical miss wave, remains bounded under stress, and preserves the public `Embedder` API and configured default capacity behavior.