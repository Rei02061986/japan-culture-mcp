"""
Phase 13 A5: JapanSearch additional expansion.
Phase 12 scanned offset 0-9.5M via rdfs:label pagination.
This script tries alternative SPARQL approaches to find more unique data:
  1. Type-based queries (schema:additionalType)
  2. Date-based queries (schema:datePublished)
  3. Subject-based queries (schema:about)
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
from datetime import datetime

DB_PATH = "/tmp/culture_ontology_work.db"
ENDPOINT = "https://jpsearch.go.jp/rdf/sparql/"
UA = "japan-culture-mcp/0.9 (teddykmk@gmail.com)"
PAGE_SIZE = 10000
BATCH = 50000


def sparql_query(query, retries=3):
    for a in range(retries):
        try:
            data = urllib.parse.urlencode({
                "query": query
            }).encode("utf-8")
            req = urllib.request.Request(ENDPOINT, data=data, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded",
            })
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read().decode("utf-8")).get("results", {}).get("bindings", [])
        except Exception as e:
            print(f"  SPARQL error: {e}", flush=True)
            if a < retries - 1:
                time.sleep(15 * (a + 1))
    return []


def open_db():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def load_existing_labels(db):
    existing = set()
    cursor = db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL")
    while True:
        rows = cursor.fetchmany(200000)
        if not rows:
            break
        for (label,) in rows:
            existing.add(label)
    return existing


def strategy_date_based(db, existing, target=1500000):
    """Query by datePublished decade ranges to find items missed by label pagination."""
    print("\n--- Strategy 1: Date-based queries ---", flush=True)

    # Date ranges that likely have lots of items
    date_ranges = [
        ("1600", "1700"), ("1700", "1750"), ("1750", "1800"),
        ("1800", "1830"), ("1830", "1860"), ("1860", "1880"),
        ("1880", "1900"), ("1900", "1910"), ("1910", "1920"),
        ("1920", "1930"), ("1930", "1940"), ("1940", "1950"),
        ("1950", "1955"), ("1955", "1960"), ("1960", "1965"),
        ("1965", "1970"), ("1970", "1975"), ("1975", "1980"),
        ("1980", "1985"), ("1985", "1990"), ("1990", "1995"),
        ("1995", "2000"), ("2000", "2005"), ("2005", "2010"),
        ("2010", "2015"), ("2015", "2020"), ("2020", "2025"),
    ]

    total = 0
    batch_pending = 0

    for start_year, end_year in date_ranges:
        if total >= target:
            break

        offset = 0
        empty_streak = 0
        range_new = 0

        while offset < 500000 and total < target:
            query = f"""
SELECT ?item ?label WHERE {{
  ?item schema:datePublished ?date .
  ?item rdfs:label ?label .
  FILTER(?date >= "{start_year}" && ?date < "{end_year}")
  FILTER(STRLEN(?label) > 2)
}}
LIMIT {PAGE_SIZE} OFFSET {offset}
"""
            bindings = sparql_query(query)
            if not bindings:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                offset += PAGE_SIZE
                time.sleep(2)
                continue

            empty_streak = 0
            page_new = 0
            for b in bindings:
                label = b.get("label", {}).get("value", "")
                if not label or label in existing:
                    continue
                if label.startswith("Q") and label[1:].isdigit():
                    continue
                db.execute(
                    'INSERT OR IGNORE INTO entities (label_ja, entity_type, source) VALUES (?, "work", "jps_date_p13")',
                    (label,)
                )
                existing.add(label)
                page_new += 1
                batch_pending += 1

            total += page_new
            range_new += page_new

            if batch_pending >= BATCH:
                db.commit()
                batch_pending = 0

            offset += PAGE_SIZE
            time.sleep(2)

        if range_new > 0:
            print(f"  {start_year}-{end_year}: +{range_new:,}", flush=True)

    if batch_pending > 0:
        db.commit()
    print(f"  Date-based total: +{total:,}", flush=True)
    return total


def strategy_type_based(db, existing, target=1500000):
    """Query by schema:additionalType to find categorized items."""
    print("\n--- Strategy 2: Type-based queries ---", flush=True)

    # Common JapanSearch types
    type_queries = [
        ("books", 'FILTER(CONTAINS(STR(?type), "Book") || CONTAINS(STR(?type), "book"))'),
        ("images", 'FILTER(CONTAINS(STR(?type), "Image") || CONTAINS(STR(?type), "image") || CONTAINS(STR(?type), "Photo"))'),
        ("maps", 'FILTER(CONTAINS(STR(?type), "Map") || CONTAINS(STR(?type), "map"))'),
        ("audio", 'FILTER(CONTAINS(STR(?type), "Audio") || CONTAINS(STR(?type), "audio") || CONTAINS(STR(?type), "Sound"))'),
        ("video", 'FILTER(CONTAINS(STR(?type), "Video") || CONTAINS(STR(?type), "video") || CONTAINS(STR(?type), "Movie"))'),
    ]

    total = 0
    batch_pending = 0

    for type_name, type_filter in type_queries:
        if total >= target:
            break

        offset = 0
        empty_streak = 0
        type_new = 0

        while offset < 500000 and total < target:
            query = f"""
SELECT ?item ?label WHERE {{
  ?item schema:additionalType ?type .
  ?item rdfs:label ?label .
  {type_filter}
  FILTER(STRLEN(?label) > 2)
}}
LIMIT {PAGE_SIZE} OFFSET {offset}
"""
            bindings = sparql_query(query)
            if not bindings:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                offset += PAGE_SIZE
                time.sleep(2)
                continue

            empty_streak = 0
            page_new = 0
            for b in bindings:
                label = b.get("label", {}).get("value", "")
                if not label or label in existing:
                    continue
                if label.startswith("Q") and label[1:].isdigit():
                    continue

                etype = "work"
                if type_name == "audio":
                    etype = "music"
                elif type_name == "images":
                    etype = "artifact"

                db.execute(
                    "INSERT OR IGNORE INTO entities (label_ja, entity_type, source) VALUES (?, ?, ?)",
                    (label, etype, f"jps_type_{type_name}_p13")
                )
                existing.add(label)
                page_new += 1
                batch_pending += 1

            total += page_new
            type_new += page_new

            if batch_pending >= BATCH:
                db.commit()
                batch_pending = 0

            offset += PAGE_SIZE
            time.sleep(2)

        if type_new > 0:
            print(f"  {type_name}: +{type_new:,}", flush=True)

    if batch_pending > 0:
        db.commit()
    print(f"  Type-based total: +{total:,}", flush=True)
    return total


def main():
    print("=" * 60, flush=True)
    print("Phase 13 A5: JapanSearch Additional Expansion", flush=True)
    print("=" * 60, flush=True)

    start = datetime.now()
    db = open_db()
    before = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"Entities before: {before:,}", flush=True)

    print("Loading existing labels...", flush=True)
    existing = load_existing_labels(db)
    print(f"Existing labels: {len(existing):,}", flush=True)

    t1 = strategy_date_based(db, existing, target=1500000)
    t2 = strategy_type_based(db, existing, target=1500000)

    after = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"SUMMARY", flush=True)
    print(f"  Date-based: +{t1:,}", flush=True)
    print(f"  Type-based: +{t2:,}", flush=True)
    print(f"  Total new:  +{t1+t2:,}", flush=True)
    print(f"  Entities:   {before:,} -> {after:,}", flush=True)
    print(f"  Duration:   {datetime.now() - start}", flush=True)
    db.close()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
