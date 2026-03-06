#!/usr/bin/env python3
"""Performance benchmark for Japan Culture MCP Server.

Tests FTS5 search, R-Tree spatial search, and connection graph traversal
performance. Prints results as a formatted table.

Usage:
    python scripts/benchmark.py [--db-path /path/to/db] [--iterations 100]

Environment:
    DB_PATH: Path to ontology database (default: ontology/culture_ontology.db)
"""

import argparse
import os
import sqlite3
import statistics
import sys
import time


def benchmark_fts5(conn, iterations=100):
    """Benchmark FTS5 full-text search latency."""
    keywords = [
        "北斎", "京都", "アニメ", "寺", "浮世絵",
        "Hokusai", "Kyoto", "temple", "anime", "festival",
        "祭り", "能", "歌舞伎", "茶道", "鬼滅",
    ]
    timings = []
    for i in range(iterations):
        kw = keywords[i % len(keywords)]
        start = time.perf_counter()
        conn.execute(
            "SELECT rowid, label_ja, label_en FROM entities_fts WHERE entities_fts MATCH ? LIMIT 20",
            (kw,),
        ).fetchall()
        elapsed = (time.perf_counter() - start) * 1000  # ms
        timings.append(elapsed)
    return timings


def benchmark_like(conn, iterations=100):
    """Benchmark LIKE search latency (for comparison)."""
    keywords = [
        "%北斎%", "%京都%", "%アニメ%", "%寺%", "%浮世絵%",
        "%Hokusai%", "%Kyoto%", "%temple%", "%anime%", "%festival%",
    ]
    timings = []
    for i in range(iterations):
        kw = keywords[i % len(keywords)]
        start = time.perf_counter()
        conn.execute(
            "SELECT id, label_ja, label_en FROM entities WHERE label_ja LIKE ? OR label_en LIKE ? LIMIT 20",
            (kw, kw),
        ).fetchall()
        elapsed = (time.perf_counter() - start) * 1000
        timings.append(elapsed)
    return timings


def benchmark_rtree(conn, iterations=100):
    """Benchmark R-Tree spatial search latency."""
    # Various bounding boxes around Japan
    boxes = [
        (34.9, 35.1, 135.7, 135.9),   # Kyoto
        (35.6, 35.8, 139.6, 139.9),   # Tokyo
        (34.6, 34.8, 135.7, 135.9),   # Nara
        (35.2, 35.4, 139.4, 139.6),   # Kamakura
        (33.1, 33.3, 129.8, 130.0),   # Arita
        (40.7, 40.9, 140.7, 140.8),   # Aomori
        (34.0, 34.2, 134.5, 134.6),   # Tokushima
        (34.2, 34.4, 132.2, 132.4),   # Itsukushima
        (37.3, 37.5, 136.8, 137.0),   # Wajima
        (36.2, 36.4, 136.3, 136.5),   # Kutani
    ]
    timings = []
    for i in range(iterations):
        box = boxes[i % len(boxes)]
        start = time.perf_counter()
        conn.execute("""
            SELECT e.id, e.label_ja, e.lat, e.lon
            FROM entities e
            JOIN entities_rtree r ON e.id = r.id
            WHERE r.min_lat >= ? AND r.max_lat <= ?
              AND r.min_lon >= ? AND r.max_lon <= ?
            LIMIT 50
        """, box).fetchall()
        elapsed = (time.perf_counter() - start) * 1000
        timings.append(elapsed)
    return timings


def benchmark_connection_traversal(conn, iterations=50):
    """Benchmark connection graph traversal (BFS depth 2)."""
    # Get some entity IDs
    entity_ids = [
        row[0] for row in
        conn.execute("SELECT id FROM entities LIMIT 20").fetchall()
    ]
    if not entity_ids:
        return [0.0]

    timings = []
    for i in range(iterations):
        eid = entity_ids[i % len(entity_ids)]
        start = time.perf_counter()

        # Depth 1
        level1 = conn.execute("""
            SELECT CASE WHEN entity_a_id = ? THEN entity_b_id ELSE entity_a_id END AS other_id,
                   connection_type, serendipity_score
            FROM connections
            WHERE (entity_a_id = ? OR entity_b_id = ?) AND llm_verdict = 'keep'
            ORDER BY serendipity_score DESC
            LIMIT 20
        """, (eid, eid, eid)).fetchall()

        # Depth 2
        for row in level1[:5]:
            other_id = row[0]
            conn.execute("""
                SELECT CASE WHEN entity_a_id = ? THEN entity_b_id ELSE entity_a_id END AS other_id,
                       connection_type, serendipity_score
                FROM connections
                WHERE (entity_a_id = ? OR entity_b_id = ?) AND llm_verdict = 'keep'
                ORDER BY serendipity_score DESC
                LIMIT 10
            """, (other_id, other_id, other_id)).fetchall()

        elapsed = (time.perf_counter() - start) * 1000
        timings.append(elapsed)
    return timings


def benchmark_entity_by_id(conn, iterations=200):
    """Benchmark entity lookup by primary key."""
    entity_ids = [
        row[0] for row in
        conn.execute("SELECT id FROM entities LIMIT 50").fetchall()
    ]
    if not entity_ids:
        return [0.0]

    timings = []
    for i in range(iterations):
        eid = entity_ids[i % len(entity_ids)]
        start = time.perf_counter()
        conn.execute(
            "SELECT * FROM entities WHERE id = ?", (eid,)
        ).fetchone()
        elapsed = (time.perf_counter() - start) * 1000
        timings.append(elapsed)
    return timings


def benchmark_tag_lookup(conn, iterations=100):
    """Benchmark entity tag lookup."""
    entity_ids = [
        row[0] for row in
        conn.execute("SELECT DISTINCT entity_id FROM entity_tags LIMIT 30").fetchall()
    ]
    if not entity_ids:
        return [0.0]

    timings = []
    for i in range(iterations):
        eid = entity_ids[i % len(entity_ids)]
        start = time.perf_counter()
        conn.execute(
            "SELECT axis, value_code FROM entity_tags WHERE entity_id = ?",
            (eid,),
        ).fetchall()
        elapsed = (time.perf_counter() - start) * 1000
        timings.append(elapsed)
    return timings


def format_stats(timings):
    """Format timing statistics."""
    if not timings:
        return {"mean": 0, "median": 0, "p95": 0, "min": 0, "max": 0}
    return {
        "mean": round(statistics.mean(timings), 3),
        "median": round(statistics.median(timings), 3),
        "p95": round(sorted(timings)[int(len(timings) * 0.95)], 3),
        "min": round(min(timings), 3),
        "max": round(max(timings), 3),
    }


def print_results_table(results):
    """Print benchmark results as a formatted table."""
    header = f"{'Benchmark':<35} {'Mean (ms)':>10} {'Median (ms)':>12} {'P95 (ms)':>10} {'Min (ms)':>10} {'Max (ms)':>10}"
    separator = "-" * len(header)

    print()
    print("=" * len(header))
    print("  Japan Culture MCP Server - Performance Benchmark")
    print("=" * len(header))
    print()
    print(header)
    print(separator)

    for name, stats in results:
        print(
            f"{name:<35} {stats['mean']:>10.3f} {stats['median']:>12.3f} "
            f"{stats['p95']:>10.3f} {stats['min']:>10.3f} {stats['max']:>10.3f}"
        )

    print(separator)
    print()


def main():
    parser = argparse.ArgumentParser(description="Benchmark Japan Culture MCP")
    parser.add_argument(
        "--db-path",
        default=os.environ.get("DB_PATH", "ontology/culture_ontology.db"),
        help="Path to SQLite database",
    )
    parser.add_argument(
        "--iterations", "-n",
        type=int,
        default=100,
        help="Number of iterations per benchmark (default: 100)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.db_path):
        print(f"Error: Database not found at {args.db_path}")
        print("Run 'python scripts/create_test_db.py' first, or set --db-path")
        sys.exit(1)

    conn = sqlite3.connect(args.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")
    conn.execute("PRAGMA mmap_size=268435456")

    # Get DB stats
    entity_count = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conn_count = conn.execute("SELECT COUNT(*) FROM connections").fetchone()[0]

    print(f"\nDatabase: {args.db_path}")
    print(f"Entities: {entity_count:,}")
    print(f"Connections: {conn_count:,}")
    print(f"Iterations: {args.iterations}")

    n = args.iterations
    results = []

    # Warm up
    conn.execute("SELECT COUNT(*) FROM entities").fetchone()
    conn.execute("SELECT COUNT(*) FROM connections").fetchone()

    print("\nRunning benchmarks...")

    # 1. FTS5
    print("  [1/6] FTS5 full-text search...")
    timings = benchmark_fts5(conn, n)
    results.append(("FTS5 Full-Text Search", format_stats(timings)))

    # 2. LIKE (comparison)
    print("  [2/6] LIKE search (comparison)...")
    timings = benchmark_like(conn, n)
    results.append(("LIKE Search (baseline)", format_stats(timings)))

    # 3. R-Tree
    print("  [3/6] R-Tree spatial search...")
    timings = benchmark_rtree(conn, n)
    results.append(("R-Tree Spatial Search", format_stats(timings)))

    # 4. Connection traversal
    print("  [4/6] Connection graph BFS (depth 2)...")
    timings = benchmark_connection_traversal(conn, min(n, 50))
    results.append(("Connection Graph BFS (depth 2)", format_stats(timings)))

    # 5. Entity by ID
    print("  [5/6] Entity lookup by ID...")
    timings = benchmark_entity_by_id(conn, n * 2)
    results.append(("Entity Lookup by ID", format_stats(timings)))

    # 6. Tag lookup
    print("  [6/6] Tag lookup by entity...")
    timings = benchmark_tag_lookup(conn, n)
    results.append(("Tag Lookup by Entity", format_stats(timings)))

    conn.close()

    # Print results
    print_results_table(results)

    # FTS5 vs LIKE comparison
    fts5_mean = results[0][1]["mean"]
    like_mean = results[1][1]["mean"]
    if fts5_mean > 0 and like_mean > 0:
        speedup = like_mean / fts5_mean
        print(f"FTS5 speedup over LIKE: {speedup:.1f}x")
    print()


if __name__ == "__main__":
    main()
