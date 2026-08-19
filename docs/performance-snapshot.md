## Benchmark Results

Lower latency is better unless noted. A winner is named only when fixtures, execution boundary, storage mode, cache state, assertions, and result quality are sufficiently aligned.

### Valid Comparisons

| Operation | NornicDB | CopperDB | Winner |
|---|---:|---:|---|
| Traversal: count all relationships | 2.177 µs | 1.495 µs | **CopperDB**, 31.3% lower latency |
| Traversal: optional match count | 2.919 µs | 0.547 µs | **CopperDB**, 81.3% lower latency |
| Traversal: two-hop match count | 2.612 µs | 0.583 µs | **CopperDB**, 77.7% lower latency |
| Traversal: shortest path length | 3.750 µs | 0.620 µs | **CopperDB**, 83.5% lower latency |
| HNSW query, 10k × 128D | 74.515 µs | 66.892 µs | **CopperDB**, 10.2% lower latency |
| HNSW query, 10k × 384D | 109.980 µs | 123.790 µs | **NornicDB**, 11.2% lower latency |
| HNSW query, 10k × 1024D | 161.742 µs | 235.880 µs | **NornicDB**, 31.4% lower latency |
| HNSW build, 10k × 128D | 1.7362 s | 1.3482 s | **CopperDB**, 22.3% lower latency |
| HNSW build, 10k × 384D | 2.8877 s | 2.6508 s | **No valid winner**, Nornic recall varied from 0.4–0.5 |
| HNSW build, 10k × 1024D | 5.3592 s | 5.7434 s | **NornicDB**, 6.7% lower latency |
| Exact cosine, 10k × 128D | 2.6917 ms | 680.64 µs | **CopperDB**, 74.7% lower latency |
| Exact cosine, 10k × 384D | 3.5061 ms | 1.9101 ms | **CopperDB**, 45.5% lower latency |
| Exact cosine, 10k × 1024D | 4.7982 ms | 3.6236 ms | **CopperDB**, 24.5% lower latency |
| File-backed rerank, 128D | 964.70 µs | 821.08 µs | **CopperDB**, 14.9% lower latency |
| File-backed rerank, 384D | 1.4004 ms | 1.1181 ms | **CopperDB timing**, 20.2% lower; upstream recall varied once |
| File-backed rerank, 1024D | 1.9729 ms | 1.7046 ms | **CopperDB timing**, 13.6% lower; upstream recall varied once |
| Hybrid RRF, durable | 1.1380 ms | 1.0773 ms | **No valid winner**, ANN candidate identities differ |
| Hybrid RRF, memory | Not collected | 783.36 µs | **No valid winner** |

CopperDB wins 10 of the 14 cleanly comparable rows. NornicDB wins three; the 384D build is excluded because result quality was unstable.

### Supplemental Benchmarks

| Operation | NornicDB | CopperDB | Winner |
|---|---:|---:|---|
| Hybrid RRF, high limit (`k=100`) | Not collected | 2.7008 ms | No valid winner |
| Hybrid RRF after reopen | Not collected | 1.1077 ms | No valid winner |
| Hybrid RRF result-cache hit | Not collected | 16.023 µs | No valid winner |
| Hybrid lexical branch | Not collected | 590.12 µs | No valid winner |
| Hybrid semantic branch | Not collected | 285.14 µs | No valid winner |
| Storage full-text branch | Not collected | 487.15 µs | No valid winner |
| RRF fusion only | Not collected | 192.42 µs | No valid winner |
| Index schema load | Not collected | 6.977 µs | No valid winner |
| Policy metadata load, 40 records | Not collected | 18.565 µs | No valid winner |
| Isolated policy catalog load | Not collected | 9.0762 ms | No valid winner |
| HNSW artifact load, 128D / 384D / 1024D | Not collected | 26.27 / 63.75 / 155.56 ms | No valid winner |
| HNSW update, 128D / 384D / 1024D | Not collected | 2.972 / 6.226 / 14.219 ms | No valid winner |
| HNSW rerank-only, 128D / 384D / 1024D | Not collected | 574.09 / 738.72 / 1,089.15 µs | No valid winner |
| HNSW full rerank pipeline, 128D / 384D / 1024D | Not collected | 846.58 / 1,122.54 / 1,693.78 µs | No valid winner |
| Lucene exact terms, vocab 256 / 2048 | Not collected | 57.35 / 442.78 µs | No valid winner |
| Lucene nested boolean, vocab 256 / 2048 | Not collected | 74.19 / 592.75 µs | No valid winner |
| Lucene leading wildcard, vocab 256 / 2048 | Not collected | 276.39 µs / 2.124 ms | No valid winner |
| Lucene regex, vocab 256 / 2048 | Not collected | 393.23 / 779.53 µs | No valid winner |
| Lucene fuzzy, vocab 256 / 2048 | Not collected | 1.412 / 11.722 ms | No valid winner |
| Full-engine traversal, memory / durable | Not equivalent | 19.91 / 25.17 ms | No valid winner |
| Offline import, CSV chunks 1k / 10k / 100k | Not collected | 5.417 / 1.906 / 1.694 s | No valid winner |
| Offline import, GZIP chunks 1k / 10k / 100k | Not collected | 5.355 / 1.942 / 1.697 s | No valid winner |
| Offline import, ZIP chunks 1k / 10k / 100k | Not collected | 5.360 / 1.926 / 1.675 s | No valid winner |
| Best offline-import throughput: ZIP, chunk 100k | Not collected | 35.823 Krows/s | No valid winner |

The traversal head-to-head numbers are warm result-cache hits in both engines, matching NornicDB’s benchmark boundary. CopperDB’s graph-resident cache-miss diagnostics remain `1.91 ms` for two-hop and `2.26 ms` for shortest path; those are profiling measurements, not cross-engine ratios.

The remaining valid optimization targets are HNSW query at 384D and 1024D, followed by HNSW build at 1024D.

Validation is clean: storage `206/206`, evaluator `344/344`, warning-denied Clippy passed, and `git diff --check` found no whitespace errors. Local scope is 13 files with 1,534 insertions and 85 deletions.