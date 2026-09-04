#!/usr/bin/env python3
"""Render the immutable upstream Northwind result schema as NornicDB vs CopperDB."""

import argparse
import importlib.util
from pathlib import Path


def load_upstream(upstream_root: Path):
    source = upstream_root / "scripts" / "northwind_report.py"
    spec = importlib.util.spec_from_file_location("northwind_report", source)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load upstream report generator: {source}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dir", required=True, type=Path)
    parser.add_argument("--upstream-root", required=True, type=Path)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--products", type=int, default=2000)
    parser.add_argument("--orders", type=int, default=2000)
    args = parser.parse_args()

    report = load_upstream(args.upstream_root)
    report.NEO4J_RULES = [
        ("skip", report._exact("lock")),
        ("skip", report._exact("version")),
        ("logs", report._ext("*.jnl")),
        ("logs", report._ext("*.wal.rmp")),
        ("index", report._exact("vectors.hnsw")),
        ("raw_data", lambda _rel, _name: True),
    ]

    runs = {
        "nornicdb": report.load_run(args.dir, "nornicdb"),
        "copperdb": report.load_run(args.dir, "copperdb"),
    }
    for label, run in runs.items():
        single = report.render_single_report(run, args.iterations, args.products, args.orders)
        single = single.replace("# copperdb", "# CopperDB")
        single = single.replace("NORNIC_RULES / NEO4J_RULES", "NORNIC_RULES / COPPERDB_RULES")
        single = single.replace(
            "Preallocated memtable/WAL scratch files (8 MiB memtable on Badger, 1 MiB GC discard log, etc.) are excluded because they hold the same bytes regardless of dataset size.",
            "Preallocated scratch files and write-ahead logs are excluded from raw-data comparisons because they do not represent durable graph records.",
        )
        (args.dir / f"{label}.md").write_text(single)

    comparison_runs = {"nornicdb": runs["nornicdb"], "neo4j": runs["copperdb"]}
    comparison = report.render_comparison(comparison_runs, args.iterations, args.products, args.orders)
    comparison = comparison.replace("Neo4j", "CopperDB").replace("neo4j", "copperdb")
    comparison = comparison.replace("NEO4J_RULES", "COPPERDB_RULES")
    comparison = comparison.replace("copperdb-go-driver", "neo4j-go-driver")
    comparison = comparison.replace(
        "CopperDB raw data = `neostore*store.db*` (record stores)",
        "CopperDB raw data = Fjall keyspace tables and manifests",
    )
    comparison = comparison.replace(
        "and CopperDB empty `*.id` allocation files",
        "and CopperDB lock/version metadata",
    )
    (args.dir / "comparison.md").write_text(comparison)


if __name__ == "__main__":
    main()