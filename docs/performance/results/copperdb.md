# CopperDB — Northwind Benchmark Report

**Run:** `2026-09-04T00:51:52.215268-07:00` → `2026-09-04T00:52:15.603734-07:00`
**Endpoint:** `bolt://127.0.0.1:17688` (database `copperdb`)

## Workload

- Categories: **96**  |  Suppliers: **144**  |  Customers: **1,200**
- Products seeded: **48,000**
- Orders seeded: **48,000** (1..6 lines each)
- Random seed: `42` (deterministic dataset)
- Seed nodes: **97,440**
- Seed relationships: **312,050**
- Approx. seed payload (JSON-serialized): **35.4 MiB**
- Seed duration: **10,832.47 ms**
- Iterations per query: **10**

## Query Latency

| Query | Mean (ms) | Median (ms) | P95 (ms) | P99 (ms) | Min (ms) | Max (ms) | StdDev (ms) | Ops/sec |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `products_per_category` | 0.22 | 0.21 | 0.25 | 0.26 | 0.18 | 0.26 | 0.02 | 4,130.10 |
| `customer_category_distinct_orders` | 0.16 | 0.15 | 0.18 | 0.19 | 0.14 | 0.19 | 0.01 | 6,159.37 |
| `optional_match_orders_count` | 0.20 | 0.20 | 0.21 | 0.21 | 0.18 | 0.21 | 0.01 | 4,400.60 |
| `revenue_by_product` | 0.15 | 0.15 | 0.16 | 0.17 | 0.14 | 0.17 | 0.01 | 6,545.93 |

- **Overall mean latency:** 0.18 ms
- **Overall throughput:** 13.28 ops/sec
- **Total benchmark wall-clock (sampled):** 26.022 s

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

- Samples collected: **24** (~1s each)
- Sampled duration: **24.23 s**
- Avg CPU power: **8,549.1 mW**
- Avg GPU power: **14.6 mW**
- Avg package power: **8,563.7 mW**
- Estimated energy (benchmark window): **207.47 J**

## Memory Pressure

- Samples collected: **26** (~1s each)
- Avg used (active + wired + compressor): **19.0 GiB**
- Peak used: **19.3 GiB**
- Avg free: **659.4 MiB**
- Min free: **55.5 MiB**
- Avg compressed (logical): **17.4 GiB**
- Peak compressed: **17.4 GiB**

## Storage

- **Raw data files:** 244.3 MiB (256,196,608 bytes)
- Indexes/stats: 4.0 KiB (4,096 bytes)
- Write-ahead logs: 596.7 MiB (625,680,384 bytes)
- Metadata/bookkeeping: 0 B (0 bytes)
- Preallocated scratch (excluded): 4.0 KiB (4,096 bytes)
- Unclassified (other): 0 B (0 bytes)
- Full data directory `du`: 841.0 MiB (881,885,184 bytes)
- Classified sum: 841.0 MiB (881,885,184 bytes, Δ vs du = +0 bytes)

_Raw-data size is the comparison headline. Preallocated scratch files and write-ahead logs are excluded from raw-data comparisons because they do not represent durable graph records._

<details><summary>Top raw-data files</summary>

| File | Size |
|---|---:|
| `keyspaces/3/tables/0` | 51.0 MiB |
| `keyspaces/1/tables/0` | 50.6 MiB |
| `keyspaces/1/tables/1` | 48.9 MiB |
| `keyspaces/4/tables/2` | 35.0 MiB |
| `keyspaces/4/tables/1` | 30.6 MiB |
| `keyspaces/4/tables/0` | 28.1 MiB |
| `keyspaces/0/tables/3` | 8.0 KiB |
| `keyspaces/0/v6` | 4.0 KiB |
| `keyspaces/0/v1` | 4.0 KiB |
| `keyspaces/0/v0` | 4.0 KiB |

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
