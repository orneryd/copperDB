# NornicDB — Northwind Benchmark Report

**Run:** `2026-09-06T08:46:34.510863-07:00` → `2026-09-06T08:46:50.17969-07:00`
**Endpoint:** `bolt://127.0.0.1:17687` (database `nornic`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **9,388.75 ms**
- Iterations per query: **10**

## Query Latency

| Query | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.18 | 0.18 | 0.18 | 0.18 | 0.17 | 0.18 | 0.01 | 4,927.73 |
| `customer_category_distinct_orders` | 0.14 | 0.14 | 0.17 | 0.18 | 0.10 | 0.18 | 0.02 | 6,918.02 |
| `optional_match_orders_count` | 0.19 | 0.19 | 0.20 | 0.21 | 0.17 | 0.21 | 0.01 | 4,683.48 |
| `revenue_by_product` | 0.14 | 0.14 | 0.15 | 0.15 | 0.13 | 0.15 | 0.01 | 6,901.71 |

- **Overall mean latency:** 0.16 ms
- **Overall throughput:** 12.11 ops/sec
- **Total benchmark wall-clock (sampled):** 18.776 s

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

- Samples collected: **17** (~1s each)
- Sampled duration: **17.16 s**
- Avg CPU power: **8,698.1 mW**
- Avg GPU power: **61.3 mW**
- Avg package power: **8,759.4 mW**
- Estimated energy (benchmark window): **150.29 J**

## Memory Pressure

- Samples collected: **19** (~1s each)
- Avg used (active + wired + compressor): **20.3 GiB**
- Peak used: **20.6 GiB**
- Avg free: **467.0 MiB**
- Min free: **56.6 MiB**
- Avg compressed (logical): **21.5 GiB**
- Peak compressed: **21.5 GiB**

## Storage

- **Raw data files:** 145.0 MiB (152,002,560 bytes)
- Indexes/stats: 0 B (0 bytes)
- Write-ahead logs: 260.0 KiB (266,240 bytes)
- Metadata/bookkeeping: 8.0 KiB (8,192 bytes)
- Preallocated scratch (excluded): 1.0 MiB (1,048,576 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 146.2 MiB (153,325,568 bytes)
- Classified sum: 146.2 MiB (153,325,568 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated scratch files and write-ahead logs are excluded from raw-data comparisons because they do not represent durable graph records._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `000002.sst` | 52.5 MiB |
| `000003.sst` | 52.0 MiB |
| `000004.sst` | 40.5 MiB |
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
