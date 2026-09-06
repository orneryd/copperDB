# NornicDB vs CopperDB — Northwind Benchmark Comparison

- Products seeded: **48,000**, Orders seeded: **48,000**
- Iterations per query: **10** (after 2 warmup iterations)

## Summary

| Metric | NornicDB | CopperDB | Delta | Ratio |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.16 | 0.16 | +3.1% | 0.97× |
| Overall throughput (ops/sec) | 12.11 | 13.06 | -7.3% | 0.93× |
| Seed duration (ms) | 9,388.75 | 9,356.18 | +0.3% | 1.00× |
| Avg CPU power (mW) | 8,698.10 | 7,503.13 | +15.9% | 0.86× |
| Avg GPU power (mW) | 61.28 | 27.48 | +123.0% | 0.45× |
| Avg package power (mW) | 8,759.36 | 7,530.61 | +16.3% | 0.86× |
| Energy during benchmark (J) | 150.29 | 182.42 | -17.6% | 1.21× |
| Benchmark wall-clock (s) | 18.78 | 26.00 | -27.8% | 1.38× |
| Peak memory used (bytes) | 20.6 GiB | 20.5 GiB | +0.3% | 1.00× |
| Raw data files (bytes) | 152,002,560 | 163,586,048 | -7.1% | 1.08× |

_Delta = (NornicDB − CopperDB) / CopperDB. Ratio compares CopperDB to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to CopperDB for throughput (higher is better)._

## Per-Query Latency

| Query | NornicDB mean (ms) | CopperDB mean (ms) | Delta | NornicDB P95 | CopperDB P95 | NornicDB ops/s | CopperDB ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.18 | 0.19 | -8.0% | 0.18 | 0.20 | 4,927.73 | 4,592.25 |
| `customer_category_distinct_orders` | 0.14 | 0.13 | +8.4% | 0.17 | 0.15 | 6,918.02 | 7,482.23 |
| `optional_match_orders_count` | 0.19 | 0.18 | +3.9% | 0.20 | 0.19 | 4,683.48 | 4,855.74 |
| `revenue_by_product` | 0.14 | 0.12 | +13.1% | 0.15 | 0.19 | 6,901.71 | 7,725.49 |

## Correctness

**Seed verification.** Post-seed counts reported by each database (via `MATCH (n:Label) RETURN count(n)` and equivalent edge queries).

| Entity | NornicDB | CopperDB | Match |
|---|---:|---:|:---:|
| Category | 96 | 96 | ✅ |
| Supplier | 144 | 144 | ✅ |
| Customer | 1,200 | 1,200 | ✅ |
| Product | 48,000 | 48,000 | ✅ |
| Order | 48,000 | 48,000 | ✅ |
| PART_OF | 48,000 | 48,000 | ✅ |
| SUPPLIES | 48,000 | 48,000 | ✅ |
| PURCHASED | 48,000 | 48,000 | ✅ |
| ORDERS | 168,050 | 168,050 | ✅ |

**Per-query result fingerprints.** Each engine runs the query on the first (warmup) iteration, canonicalises the full result set, and hashes it with SHA-256. A matching row_count + matching hash means the engines returned the same data.

| Query | NornicDB rows | CopperDB rows | NornicDB hash | CopperDB hash | Match |
|---|---:|---:|---|---|:---:|
| `products_per_category` | 96 | 96 | `91e9f1f06368…` | `91e9f1f06368…` | ✅ |
| `customer_category_distinct_orders` | 10 | 10 | `5da36214d516…` | `5da36214d516…` | ✅ |
| `optional_match_orders_count` | 100 | 100 | `8950fcdaab16…` | `8950fcdaab16…` | ✅ |
| `revenue_by_product` | 10 | 10 | `60b64c678f4c…` | `60b64c678f4c…` | ✅ |

**Intra-run stability.** Every iteration of each query re-fingerprints its result set; a mismatch within a single engine's run is flagged below.

- No intra-run mismatches on either engine.

✅ **All correctness checks passed** — both engines seeded identically and returned identical result sets (by row count and canonical SHA-256 fingerprint) for every benchmark query.

## Storage

Raw data files only (preallocated scratch, WAL, and indexes excluded from the headline):

| Bucket | NornicDB | CopperDB |
|---|---:|---:|
| **Raw data** | 145.0 MiB (152,002,560 B) | 156.0 MiB (163,586,048 B) |
| Indexes / stats | 0 B (0 B) | 4.0 KiB (4,096 B) |
| Write-ahead logs | 260.0 KiB (266,240 B) | 26.7 MiB (28,020,736 B) |
| Metadata | 8.0 KiB (8,192 B) | 0 B (0 B) |
| _Scratch (excluded)_ | 1.0 MiB (1,048,576 B) | 4.0 KiB (4,096 B) |
| _Unclassified_ | 0 B (0 B) | 0 B (0 B) |
| Total `du` | 146.2 MiB | 182.7 MiB |

- **Raw data ratio:** 0.93× CopperDB (smaller)
- Full-dir ratio (includes scratch/WAL): 0.80× CopperDB

## Power

| | NornicDB | CopperDB |
|---|---:|---:|
| Samples | 17 | 24 |
| Duration (s) | 17.16 | 24.22 |
| CPU avg (mW) | 8,698.1 | 7,503.1 |
| GPU avg (mW) | 61.3 | 27.5 |
| Package avg (mW) | 8,759.4 | 7,530.6 |
| Energy (J) | 150.29 | 182.42 |

## Memory Pressure

System-wide memory during each engine's full lifecycle (startup → benchmark → shutdown).

| | NornicDB | CopperDB |
|---|---:|---:|
| Samples | 19 | 26 |
| Avg used (active+wired+compressor) | 20.3 GiB | 20.2 GiB |
| Peak used | 20.6 GiB | 20.5 GiB |
| Avg free | 467.0 MiB | 570.6 MiB |
| Min free | 56.6 MiB | 56.3 MiB |
| Avg compressed (logical) | 21.5 GiB | 21.5 GiB |
| Peak compressed | 21.5 GiB | 21.5 GiB |

## Notes

- Power figures are Apple `powermetrics` estimates; treat as directional, not absolute. Apple's own docs note that reported averages are approximate.
- Both databases were freshly initialized before each run; CopperDB was stopped during the NornicDB run, and vice versa, to isolate measurements.
- Benchmarks ran over the Bolt protocol using the neo4j-go-driver.
- **Storage classification:** NornicDB raw data = `*.sst` + `*.vlog` (LSM records + value log). CopperDB raw data = Fjall keyspace tables and manifests. Preallocated scratch files — Badger's 8 MiB memtable (`*.mem`) and 1 MiB discard log (`DISCARD`), and CopperDB lock/version metadata — are excluded because their size is fixed/preallocated and does not scale with the dataset.