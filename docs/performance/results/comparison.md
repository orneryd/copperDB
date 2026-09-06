# NornicDB vs CopperDB — Northwind Benchmark Comparison

- Products seeded: **48,000**, Orders seeded: **48,000**
- Iterations per query: **10** (after 2 warmup iterations)

## Summary

| Metric | NornicDB | CopperDB | Delta | Ratio |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.14 | 0.16 | -11.9% | 1.14× |
| Overall throughput (ops/sec) | 12.00 | 12.89 | -6.9% | 0.93× |
| Seed duration (ms) | 9,705.73 | 9,387.36 | +3.4% | 0.97× |
| Avg CPU power (mW) | 8,904.59 | 8,412.36 | +5.9% | 0.94× |
| Avg GPU power (mW) | 41.32 | 23.74 | +74.1% | 0.57× |
| Avg package power (mW) | 8,945.91 | 8,436.10 | +6.0% | 0.94× |
| Energy during benchmark (J) | 144.40 | 195.78 | -26.2% | 1.36× |
| Benchmark wall-clock (s) | 18.00 | 25.42 | -29.2% | 1.41× |
| Peak memory used (bytes) | 21.0 GiB | 21.1 GiB | -0.1% | 1.00× |
| Raw data files (bytes) | 151,785,472 | 163,594,240 | -7.2% | 1.08× |

_Delta = (NornicDB − CopperDB) / CopperDB. Ratio compares CopperDB to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to CopperDB for throughput (higher is better)._

## Per-Query Latency

| Query | NornicDB mean (ms) | CopperDB mean (ms) | Delta | NornicDB P95 | CopperDB P95 | NornicDB ops/s | CopperDB ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.13 | 0.18 | -26.7% | 0.15 | 0.21 | 6,442.26 | 4,879.14 |
| `customer_category_distinct_orders` | 0.15 | 0.13 | +11.1% | 0.16 | 0.15 | 6,547.90 | 7,248.13 |
| `optional_match_orders_count` | 0.17 | 0.18 | -8.9% | 0.19 | 0.20 | 5,221.02 | 4,790.13 |
| `revenue_by_product` | 0.11 | 0.13 | -19.3% | 0.13 | 0.16 | 8,881.98 | 7,226.96 |

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
| **Raw data** | 144.8 MiB (151,785,472 B) | 156.0 MiB (163,594,240 B) |
| Indexes / stats | 0 B (0 B) | 4.0 KiB (4,096 B) |
| Write-ahead logs | 260.0 KiB (266,240 B) | 26.7 MiB (28,020,736 B) |
| Metadata | 8.0 KiB (8,192 B) | 0 B (0 B) |
| _Scratch (excluded)_ | 1.0 MiB (1,048,576 B) | 4.0 KiB (4,096 B) |
| _Unclassified_ | 0 B (0 B) | 0 B (0 B) |
| Total `du` | 146.0 MiB | 182.7 MiB |

- **Raw data ratio:** 0.93× CopperDB (smaller)
- Full-dir ratio (includes scratch/WAL): 0.80× CopperDB

## Power

| | NornicDB | CopperDB |
|---|---:|---:|
| Samples | 16 | 23 |
| Duration (s) | 16.14 | 23.21 |
| CPU avg (mW) | 8,904.6 | 8,412.4 |
| GPU avg (mW) | 41.3 | 23.7 |
| Package avg (mW) | 8,945.9 | 8,436.1 |
| Energy (J) | 144.40 | 195.78 |

## Memory Pressure

System-wide memory during each engine's full lifecycle (startup → benchmark → shutdown).

| | NornicDB | CopperDB |
|---|---:|---:|
| Samples | 18 | 26 |
| Avg used (active+wired+compressor) | 20.7 GiB | 20.6 GiB |
| Peak used | 21.0 GiB | 21.1 GiB |
| Avg free | 447.8 MiB | 640.2 MiB |
| Min free | 62.5 MiB | 58.0 MiB |
| Avg compressed (logical) | 23.1 GiB | 23.2 GiB |
| Peak compressed | 23.1 GiB | 23.4 GiB |

## Notes

- Power figures are Apple `powermetrics` estimates; treat as directional, not absolute. Apple's own docs note that reported averages are approximate.
- Both databases were freshly initialized before each run; CopperDB was stopped during the NornicDB run, and vice versa, to isolate measurements.
- Benchmarks ran over the Bolt protocol using the neo4j-go-driver.
- **Storage classification:** NornicDB raw data = `*.sst` + `*.vlog` (LSM records + value log). CopperDB raw data = Fjall keyspace tables and manifests. Preallocated scratch files — Badger's 8 MiB memtable (`*.mem`) and 1 MiB discard log (`DISCARD`), and CopperDB lock/version metadata — are excluded because their size is fixed/preallocated and does not scale with the dataset.