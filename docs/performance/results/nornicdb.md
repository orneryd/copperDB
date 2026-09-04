# NornicDB — Northwind Benchmark Report

**Run:** `2026-09-04T00:51:33.679652-07:00` → `2026-09-04T00:51:49.165528-07:00`
**Endpoint:** `bolt://127.0.0.1:17687` (database `nornic`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **9,304.86 ms**
- Iterations per query: **10**

## Query Latency

| Query | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.18 | 0.18 | 0.19 | 0.20 | 0.17 | 0.20 | 0.01 | 4,795.59 |
| `customer_category_distinct_orders` | 0.15 | 0.15 | 0.17 | 0.17 | 0.14 | 0.17 | 0.01 | 6,521.56 |
| `optional_match_orders_count` | 0.19 | 0.19 | 0.19 | 0.19 | 0.18 | 0.20 | 0.00 | 4,660.83 |
| `revenue_by_product` | 0.14 | 0.13 | 0.16 | 0.16 | 0.12 | 0.16 | 0.01 | 7,034.82 |

- **Overall mean latency:** 0.16 ms
- **Overall throughput:** 12.36 ops/sec
- **Total benchmark wall-clock (sampled):** 17.462 s

## Correctness

Seed counts (from the database's own `count(...)` queries):

| Entity | Count |
|---|---:|
| Category | 96 |
| Supplier | 144 |
| Customer | 1,200 |
| Product | 48,000 |
| Order | 48,000 |
| PART_OF edges | 48,000 |
| SUPPLIES edges | 48,000 |
| PURCHASED edges | 48,000 |
| ORDERS edges | 168,050 |

Per-query result fingerprints (SHA-256 over canonicalised rows):

| Query | Rows | Hash | Stable across iterations |
|---|---:|---|:---:|
| `products_per_category` | 96 | `91e9f1f063680a6d…` | ✅ |
| `customer_category_distinct_orders` | 10 | `5da36214d5163220…` | ✅ |
| `optional_match_orders_count` | 100 | `8950fcdaab16eaeb…` | ✅ |
| `revenue_by_product` | 10 | `60b64c678f4c01fd…` | ✅ |

✅ No intra-run correctness errors.

## Power Consumption

- Samples collected: **15** (~1s each)
- Sampled duration: **15.13 s**
- Avg CPU power: **9,286.7 mW**
- Avg GPU power: **27.0 mW**
- Avg package power: **9,313.6 mW**
- Estimated energy (benchmark window): **140.91 J**

## Memory Pressure

- Samples collected: **18** (~1s each)
- Avg used (active + wired + compressor): **18.7 GiB**
- Peak used: **19.1 GiB**
- Avg free: **1.1 GiB**
- Min free: **515.6 MiB**
- Avg compressed (logical): **17.4 GiB**
- Peak compressed: **17.4 GiB**

## Storage

- **Raw data files:** 144.7 MiB (151,707,648 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 260.0 KiB (266,240 bytes)
- Metadata/bookkeeping: 8.0 KiB (8,192 bytes)
- Preallocated scratch (excluded): 1.0 MiB (1,048,576 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 145.9 MiB (153,030,656 bytes)
- Classified sum: 145.9 MiB (153,030,656 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated scratch files and write-ahead logs are excluded from raw-data comparisons because they do not represent durable graph records._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `000002.sst` | 52.4 MiB |
| `000003.sst` | 51.8 MiB |
| `000004.sst` | 40.4 MiB |
| `000001.sst` | 4.0 KiB |
| `000001.vlog` | 4.0 KiB |
| `000002.vlog` | 4.0 KiB |

</details>

## Queries

### `products_per_category`

```cypher
MATCH (c:Category)<-[:PART_OF]-(p:Product)
			RETURN c.categoryName AS categoryName, count(p) AS productCount
			ORDER BY productCount DESC
```

### `customer_category_distinct_orders`

```cypher
MATCH (c:Customer)-[:PURCHASED]->(o:Order)-[:ORDERS]->(p:Product)-[:PART_OF]->(cat:Category)
			RETURN c.companyName AS companyName, cat.categoryName AS categoryName, count(DISTINCT o) AS orders
			ORDER BY orders DESC, companyName ASC, categoryName ASC
			LIMIT 10
```

### `optional_match_orders_count`

```cypher
MATCH (p:Product)
			OPTIONAL MATCH (p)<-[r:ORDERS]-(o:Order)
			RETURN p.productName AS productName, count(o) AS orderCount
			ORDER BY orderCount DESC, productName ASC
			LIMIT 100
```

### `revenue_by_product`

```cypher
MATCH (p:Product)<-[r:ORDERS]-(:Order)
			WITH p, sum(p.unitPrice * r.quantity) AS revenue
			RETURN p.productName AS productName, revenue
			ORDER BY revenue DESC, productName ASC
			LIMIT 10
```
