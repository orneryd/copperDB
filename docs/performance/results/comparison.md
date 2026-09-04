# NornicDB vs CopperDB — Northwind Benchmark Comparison

- Products seeded: **48,000**, Orders seeded: **48,000**
- Iterations per query: **10** (after 2 warmup iterations)

## Summary

| Metric | NornicDB | CopperDB | Delta | Ratio |
|---|---:|---:|---:|---:|
| Overall mean latency (ms) | 0.16 | 0.18 | -9.1% | 1.10× |
| Overall throughput (ops/sec) | 12.36 | 13.28 | -6.9% | 0.93× |
| Seed duration (ms) | 9,304.86 | 10,832.47 | -14.1% | 1.16× |
| Avg CPU power (mW) | 9,286.67 | 8,549.07 | +8.6% | 0.92× |
| Avg GPU power (mW) | 26.97 | 14.62 | +84.5% | 0.54× |
| Avg package power (mW) | 9,313.65 | 8,563.69 | +8.8% | 0.92× |
| Energy during benchmark (J) | 140.91 | 207.47 | -32.1% | 1.47× |
| Benchmark wall-clock (s) | 17.46 | 26.02 | -32.9% | 1.49× |
| Peak memory used (bytes) | 19.1 GiB | 19.3 GiB | -1.2% | 1.01× |
| Raw data files (bytes) | 151,707,648 | 256,196,608 | -40.8% | 1.69× |

_Delta = (NornicDB − CopperDB) / CopperDB. Ratio compares CopperDB to NornicDB for metrics where lower is better (latency, energy, disk), and NornicDB to CopperDB for throughput (higher is better)._

## Per-Query Latency

| Query | NornicDB mean (ms) | CopperDB mean (ms) | Delta | NornicDB P95 | CopperDB P95 | NornicDB ops/s | CopperDB ops/s |
|---|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.18 | 0.22 | -16.0% | 0.19 | 0.25 | 4,795.59 | 4,130.10 |
| `customer_category_distinct_orders` | 0.15 | 0.16 | -5.9% | 0.17 | 0.18 | 6,521.56 | 6,159.37 |
| `optional_match_orders_count` | 0.19 | 0.20 | -5.4% | 0.19 | 0.21 | 4,660.83 | 4,400.60 |
| `revenue_by_product` | 0.14 | 0.15 | -7.4% | 0.16 | 0.16 | 7,034.82 | 6,545.93 |

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
| **Raw data** | 144.7 MiB (151,707,648 B) | 244.3 MiB (256,196,608 B) |
| Indexes / stats | 0 B (0 B) | 4.0 KiB (4,096 B) |
| Write-ahead logs | 260.0 KiB (266,240 B) | 596.7 MiB (625,680,384 B) |
| Metadata | 8.0 KiB (8,192 B) | 0 B (0 B) |
| _Scratch (excluded)_ | 1.0 MiB (1,048,576 B) | 4.0 KiB (4,096 B) |
| _Unclassified_ | 0 B (0 B) | 0 B (0 B) |
| Total `du` | 145.9 MiB | 841.0 MiB |

- **Raw data ratio:** 0.59× CopperDB (smaller)
- Full-dir ratio (includes scratch/WAL): 0.17× CopperDB

## Power

| | NornicDB | CopperDB |
|---|---:|---:|
| Samples | 15 | 24 |
| Duration (s) | 15.13 | 24.23 |
| CPU avg (mW) | 9,286.7 | 8,549.1 |
| GPU avg (mW) | 27.0 | 14.6 |
| Package avg (mW) | 9,313.6 | 8,563.7 |
| Energy (J) | 140.91 | 207.47 |

## Memory Pressure

System-wide memory during each engine's full lifecycle (startup → benchmark → shutdown).

| | NornicDB | CopperDB |
|---|---:|---:|
| Samples | 18 | 26 |
| Avg used (active+wired+compressor) | 18.7 GiB | 19.0 GiB |
| Peak used | 19.1 GiB | 19.3 GiB |
| Avg free | 1.1 GiB | 659.4 MiB |
| Min free | 515.6 MiB | 55.5 MiB |
| Avg compressed (logical) | 17.4 GiB | 17.4 GiB |
| Peak compressed | 17.4 GiB | 17.4 GiB |

## Notes

- Power figures are Apple `powermetrics` estimates; treat as directional, not absolute. Apple's own docs note that reported averages are approximate.
- Both databases were freshly initialized before each run; CopperDB was stopped during the NornicDB run, and vice versa, to isolate measurements.
- Benchmarks ran over the Bolt protocol using the neo4j-go-driver.
- **Storage classification:** NornicDB raw data = `*.sst` + `*.vlog` (LSM records + value log). CopperDB raw data = Fjall keyspace tables and manifests. Preallocated scratch files — Badger's 8 MiB memtable (`*.mem`) and 1 MiB discard log (`DISCARD`), and CopperDB lock/version metadata — are excluded because their size is fixed/preallocated and does not scale with the dataset.