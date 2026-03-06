"""
Phase 17 Step 5: Verification and CSV/Markdown reports for release_year.

Outputs:
  - reports/phase17_year_distribution.csv
  - reports/phase17_source_breakdown.csv
  - reports/phase17_summary.md

Verification checks:
  - Year range outliers
  - Source distribution sanity
  - 10 random samples per source for spot-check
"""
import sqlite3
import csv
import time
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
REPORTS_DIR = os.path.join(os.path.dirname(__file__), "..", "reports")


def open_db():
    db = sqlite3.connect(ORIG_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=30000")
    db.execute("PRAGMA cache_size=-64000")
    return db


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 17 Step 5: Verification & Reports", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    db = open_db()

    # === Basic counts ===
    total_entities = db.execute(
        "SELECT COUNT(*) FROM entities WHERE is_dormant = 0"
    ).fetchone()[0]
    total_with_year = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]
    active_with_year = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL AND is_dormant = 0"
    ).fetchone()[0]
    total_anilist = db.execute(
        "SELECT COUNT(*) FROM entities WHERE anilist_id IS NOT NULL AND is_dormant = 0"
    ).fetchone()[0]

    print(f"\nActive entities:          {total_entities:,}", flush=True)
    print(f"Total with release_year:  {total_with_year:,}", flush=True)
    print(f"Active with release_year: {active_with_year:,}", flush=True)
    print(f"Active with anilist_id:   {total_anilist:,}", flush=True)
    print(f"Coverage: {100*active_with_year/max(total_entities,1):.2f}%", flush=True)

    # === Year distribution (by year) ===
    print("\n--- Year distribution (CSV) ---", flush=True)
    rows = db.execute("""
        SELECT release_year, COUNT(*) as cnt
        FROM entities
        WHERE release_year IS NOT NULL
        GROUP BY release_year
        ORDER BY release_year
    """).fetchall()

    csv_path = os.path.join(REPORTS_DIR, "phase17_year_distribution.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["year", "count"])
        for year, cnt in rows:
            writer.writerow([year, cnt])
    print(f"  Written {len(rows)} rows to {csv_path}", flush=True)

    # Decade summary for console
    print("\n--- Decade distribution ---", flush=True)
    decades = db.execute("""
        SELECT (release_year / 10) * 10 as decade, COUNT(*) as cnt
        FROM entities
        WHERE release_year IS NOT NULL
        GROUP BY decade
        ORDER BY decade
    """).fetchall()
    for dec, cnt in decades:
        print(f"  {dec}s: {cnt:>10,}", flush=True)

    # === Source breakdown ===
    print("\n--- Source breakdown (CSV) ---", flush=True)
    rows = db.execute("""
        SELECT release_year_source, entity_type, COUNT(*) as cnt
        FROM entities
        WHERE release_year IS NOT NULL
        GROUP BY release_year_source, entity_type
        ORDER BY release_year_source, cnt DESC
    """).fetchall()

    csv_path2 = os.path.join(REPORTS_DIR, "phase17_source_breakdown.csv")
    with open(csv_path2, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["release_year_source", "entity_type", "count"])
        for src, etype, cnt in rows:
            writer.writerow([src, etype, cnt])
    print(f"  Written {len(rows)} rows to {csv_path2}", flush=True)

    # Source totals for console
    print("\n--- Source totals ---", flush=True)
    src_totals = db.execute("""
        SELECT release_year_source, COUNT(*) as cnt
        FROM entities WHERE release_year IS NOT NULL
        GROUP BY release_year_source ORDER BY cnt DESC
    """).fetchall()
    for src, cnt in src_totals:
        print(f"  {str(src):30s} {cnt:>10,}", flush=True)

    # === Outlier check ===
    print("\n--- Outlier check ---", flush=True)
    outliers_low = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE release_year IS NOT NULL AND release_year < 1400
    """).fetchone()[0]
    outliers_high = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE release_year IS NOT NULL AND release_year > 2026
    """).fetchone()[0]
    print(f"  Year < 1400: {outliers_low:,}", flush=True)
    print(f"  Year > 2026: {outliers_high:,}", flush=True)

    if outliers_low > 0:
        samples = db.execute("""
            SELECT id, label_ja, release_year, release_year_source
            FROM entities WHERE release_year < 1400
            LIMIT 5
        """).fetchall()
        for eid, label, yr, src in samples:
            print(f"    id={eid}: {label} year={yr} src={src}", flush=True)

    if outliers_high > 0:
        samples = db.execute("""
            SELECT id, label_ja, release_year, release_year_source
            FROM entities WHERE release_year > 2026
            LIMIT 5
        """).fetchall()
        for eid, label, yr, src in samples:
            print(f"    id={eid}: {label} year={yr} src={src}", flush=True)

    # === Random samples per source ===
    print("\n--- Random samples per source (10 each) ---", flush=True)
    sample_lines = []
    for src, _ in src_totals:
        print(f"\n  Source: {src}", flush=True)
        samples = db.execute("""
            SELECT id, label_ja, release_year
            FROM entities
            WHERE release_year_source = ?
            ORDER BY RANDOM()
            LIMIT 10
        """, (src,)).fetchall()
        for eid, label, yr in samples:
            label_short = (label[:50] + "...") if label and len(label) > 50 else label
            line = f"    id={eid}: [{yr}] {label_short}"
            print(line, flush=True)
            sample_lines.append((src, eid, yr, label_short))

    # === AniList-specific checks ===
    print("\n--- AniList match quality ---", flush=True)
    anilist_with_year = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE anilist_id IS NOT NULL AND release_year IS NOT NULL AND is_dormant = 0
    """).fetchone()[0]
    anilist_sources = db.execute("""
        SELECT release_year_source, COUNT(*) FROM entities
        WHERE anilist_id IS NOT NULL AND release_year IS NOT NULL AND is_dormant = 0
        GROUP BY release_year_source ORDER BY COUNT(*) DESC
    """).fetchall()
    print(f"  AniList entities with release_year: {anilist_with_year:,}", flush=True)
    for src, cnt in anilist_sources:
        print(f"    {str(src):30s} {cnt:>8,}", flush=True)

    # CCDM-relevant subset: creative works with year
    ccdm_count = db.execute("""
        SELECT COUNT(*) FROM entities
        WHERE release_year IS NOT NULL
          AND is_dormant = 0
          AND entity_type IN ('work', 'film', 'music', 'game', 'anime', 'character')
          AND release_year BETWEEN 1980 AND 2025
    """).fetchone()[0]
    print(f"\n  CCDM-relevant works (1980-2025): {ccdm_count:,}", flush=True)

    # === Generate Markdown summary ===
    md_path = os.path.join(REPORTS_DIR, "phase17_summary.md")
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Phase 17: release_year Summary\n\n")
        f.write(f"Generated: {now}\n\n")
        f.write("## Overview\n\n")
        f.write(f"| Metric | Value |\n")
        f.write(f"|--------|-------|\n")
        f.write(f"| Active entities | {total_entities:,} |\n")
        f.write(f"| With release_year | {active_with_year:,} |\n")
        f.write(f"| Coverage | {100*active_with_year/max(total_entities,1):.2f}% |\n")
        f.write(f"| With anilist_id | {total_anilist:,} |\n")
        f.write(f"| CCDM-relevant (1980-2025) | {ccdm_count:,} |\n")
        f.write(f"| Year outliers (<1400) | {outliers_low:,} |\n")
        f.write(f"| Year outliers (>2026) | {outliers_high:,} |\n\n")

        f.write("## Source Breakdown\n\n")
        f.write("| Source | Count |\n")
        f.write("|--------|-------|\n")
        for src, cnt in src_totals:
            f.write(f"| {src} | {cnt:,} |\n")
        f.write("\n")

        f.write("## Decade Distribution\n\n")
        f.write("| Decade | Count |\n")
        f.write("|--------|-------|\n")
        for dec, cnt in decades:
            f.write(f"| {dec}s | {cnt:,} |\n")
        f.write("\n")

        f.write("## AniList Match Quality\n\n")
        f.write(f"AniList entities with release_year: {anilist_with_year:,}\n\n")
        if anilist_sources:
            f.write("| Source | Count |\n")
            f.write("|--------|-------|\n")
            for src, cnt in anilist_sources:
                f.write(f"| {src} | {cnt:,} |\n")
            f.write("\n")

    print(f"\n  Summary written to {md_path}", flush=True)

    elapsed = time.time() - t0

    print(f"\n{'='*70}", flush=True)
    print("PHASE 17 STEP 5 SUMMARY", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  Active entities:        {total_entities:,}", flush=True)
    print(f"  With release_year:      {active_with_year:,}", flush=True)
    print(f"  Coverage:               {100*active_with_year/max(total_entities,1):.2f}%", flush=True)
    print(f"  AniList with year:      {anilist_with_year:,}", flush=True)
    print(f"  CCDM-relevant:          {ccdm_count:,}", flush=True)
    print(f"  Year outliers:          {outliers_low + outliers_high:,}", flush=True)
    print(f"  Duration:               {elapsed:.1f}s", flush=True)

    db.close()
    print("\nPhase 17 Step 5 complete.", flush=True)


if __name__ == "__main__":
    main()
