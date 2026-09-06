# NornicDB vs CopperDB — Northwind Benchmark Comparison

- Products seeded: **48,000**, Orders seeded: **48,000**
- Iterations per query: **10** (after 2 warmup iterations)

## Summary

| Metric | NornicDB | CopperDB | Delta | Ratio |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.14 | 0.16 | -11.8% | 1.13× |
| Overall throughput (ops/sec) | 11.89 | 11.80 | +0.8% | 1.01× |
| Seed duration (ms) | 9,776.47 | 9,660.91 | +1.2% | 0.99× |
| Avg CPU power (mW) | 8,701.93 | 8,541.70 | +1.9% | 0.98× |
| Avg GPU power (mW) | 53.75 | 19.17 | +180.4% | 0.36× |
| Avg package power (mW) | 8,755.68 | 8,560.87 | +2.3% | 0.98× |
| Energy during benchmark (J) | 150.19 | 198.69 | -24.4% | 1.32× |
| Benchmark wall-clock (s) | 19.46 | 25.70 | -24.3% | 1.32× |
| Peak memory used (bytes) | 20.1 GiB | 20.3 GiB | -1.1% | 1.01× |
| Raw data files (bytes) | 151,810,048 | 163,606,528 | -7.2% | 1.08× |

_Delta = (NornicDB − CopperDB) / CopperDB. Ratio compares CopperDB to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to CopperDB for throughput (higher is better)._

## Per-Query Latency

| Query | NornicDB mean (ms) | CopperDB mean (ms) | Delta | NornicDB P95 | CopperDB P95 | NornicDB ops/s | CopperDB ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.12 | 0.19 | -33.5% | 0.13 | 0.20 | 6,707.09 | 4,756.90 |
| `customer_category_distinct_orders` | 0.11 | 0.13 | -17.9% | 0.14 | 0.16 | 8,744.45 | 7,197.27 |
| `optional_match_orders_count` | 0.18 | 0.19 | -0.6% | 0.19 | 0.20 | 4,757.94 | 4,778.21 |
| `revenue_by_product` | 0.14 | 0.13 | +10.1% | 0.15 | 0.14 | 6,976.95 | 7,660.63 |

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
| **Raw data** | 144.8 MiB (151,810,048 B) | 156.0 MiB (163,606,528 B) |
| Indexes / stats | 0 B (0 B) | 4.0 KiB (4,096 B) |
| Write-ahead logs | 260.0 KiB (266,240 B) | 26.7 MiB (28,020,736 B) |
| Metadata | 8.0 KiB (8,192 B) | 0 B (0 B) |
| _Scratch (excluded)_ | 1.0 MiB (1,048,576 B) | 4.0 KiB (4,096 B) |
| _Unclassified_ | 0 B (0 B) | 0 B (0 B) |
| Total `du` | 146.0 MiB | 182.8 MiB |

- **Raw data ratio:** 0.93× CopperDB (smaller)
- Full-dir ratio (includes scratch/WAL): 0.80× CopperDB

## Power

| | NornicDB | CopperDB |
|---|---:|---:|
| Samples | 17 | 23 |
| Duration (s) | 17.15 | 23.21 |
| CPU avg (mW) | 8,701.9 | 8,541.7 |
| GPU avg (mW) | 53.8 | 19.2 |
| Package avg (mW) | 8,755.7 | 8,560.9 |
| Energy (J) | 150.19 | 198.69 |

## Memory Pressure

System-wide memory during each engine's full lifecycle (startup → benchmark → shutdown).

| | NornicDB | CopperDB |
|---|---:|---:|
| Samples | 20 | 26 |
| Avg used (active+wired+compressor) | 19.8 GiB | 20.0 GiB |
| Peak used | 20.1 GiB | 20.3 GiB |
| Avg free | 1.3 GiB | 862.6 MiB |
| Min free | 646.5 MiB | 99.4 MiB |
| Avg compressed (logical) | 20.9 GiB | 20.9 GiB |
| Peak compressed | 20.9 GiB | 20.9 GiB |

## Notes

- Power figures are Apple `powermetrics` estimates; treat as directional, not absolute. Apple's own docs note that reported averages are approximate.
- Both databases were freshly initialized before each run; CopperDB was stopped during the NornicDB run, and vice versa, to isolate measurements.
- Benchmarks ran over the Bolt protocol using the neo4j-go-driver.
- **Storage classification:** NornicDB raw data = `*.sst` + `*.vlog` (LSM records + value log). CopperDB raw data = Fjall keyspace tables and manifests. Preallocated scratch files — Badger's 8 MiB memtable (`*.mem`) and 1 MiB discard log (`DISCARD`), and CopperDB lock/version metadata — are excluded because their size is fixed/preallocated and does not scale with the dataset.