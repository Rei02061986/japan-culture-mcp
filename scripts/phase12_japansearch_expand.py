"""
Phase 12 Stream C: JapanSearch expansion to 5M entities.
Direct pagination through rdfs:label items.
Endpoint caps at 10K per page, ~2.3s per query.
Target: ~3M new entities.
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
from datetime import datetime

DB_PATH = "ontology/culture_ontology.db"
ENDPOINT = "https://jpsearch.go.jp/rdf/sparql/"
UA = "japan-culture-mcp/0.8 (teddykmk@gmail.com)"

PAGE_SIZE = 10000
BATCH_COMMIT = 50000
SLEEP = 2
TARGET_NEW = 3_000_000
MAX_OFFSET = 40_000_000  # safety limit


def sparql_page(offset, retries=3):
    query = f"""SELECT ?item ?label WHERE {{
  ?item rdfs:label ?label .
  FILTER(STRLEN(?label) > 2)
}} LIMIT {PAGE_SIZE} OFFSET {offset}"""

    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({"query": query}).encode("utf-8")
            req = urllib.request.Request(ENDPOINT, data=data, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded",
            })
            with urllib.request.urlopen(req, timeout=180) as resp:
                result = json.loads(resp.read().decode("utf-8"))
                return result.get("results", {}).get("bindings", [])
        except Exception as e:
            wait = 15 * (attempt + 1)
            print(f"    Error at offset {offset:,} (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(wait)
    return []


def db_commit_retry(db, retries=5):
    for i in range(retries):
        try:
            db.commit()
            return True
        except sqlite3.OperationalError as e:
            print(f"  Commit retry {i+1}: {e}", flush=True)
            time.sleep(3)
    return False


def main():
    start = datetime.now()
    print(f"Phase 12 Stream C: JapanSearch → 5M", flush=True)
    print(f"Started: {start.isoformat()}", flush=True)

    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    # Load existing labels
    print("Loading existing labels...", flush=True)
    existing = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing.add(row[0])
    initial = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"  Existing labels: {len(existing):,}", flush=True)
    print(f"  Total entities: {initial:,}", flush=True)

    grand_new = 0
    batch_pending = 0
    offset = 0
    consecutive_empty = 0
    pages = 0

    while offset < MAX_OFFSET and grand_new < TARGET_NEW:
        bindings = sparql_page(offset)
        pages += 1

        if not bindings:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print(f"  3 consecutive empty pages at offset {offset:,}, stopping.", flush=True)
                break
            offset += PAGE_SIZE
            time.sleep(SLEEP)
            continue

        consecutive_empty = 0
        page_new = 0

        for b in bindings:
            label = b.get("label", {}).get("value", "")
            if not label or label in existing:
                continue
            # Skip Q-ids and garbage
            if label.startswith("Q") and label[1:].isdigit():
                continue

            db.execute("""
                INSERT OR IGNORE INTO entities (label_ja, entity_type, source)
                VALUES (?, 'work', 'japansearch_phase12')
            """, (label,))
            existing.add(label)
            page_new += 1
            batch_pending += 1

        grand_new += page_new

        if batch_pending >= BATCH_COMMIT:
            db_commit_retry(db)
            batch_pending = 0

        if pages % 50 == 0 or page_new > 500:
            elapsed = (datetime.now() - start).total_seconds()
            rate = grand_new / max(elapsed, 1) * 3600
            print(f"  page={pages:>5} offset={offset:>10,} new={page_new:>5,} "
                  f"total_new={grand_new:>10,} rate={rate:,.0f}/hr", flush=True)

        offset += PAGE_SIZE
        time.sleep(SLEEP)

    # Final commit
    if batch_pending > 0:
        db_commit_retry(db)

    final = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    elapsed = datetime.now() - start

    print(f"\n{'='*60}", flush=True)
    print(f"=== Phase 12 Stream C Results ===", flush=True)
    print(f"Duration: {elapsed}", flush=True)
    print(f"Pages fetched: {pages:,}", flush=True)
    print(f"New entities: {grand_new:,}", flush=True)
    print(f"Initial: {initial:,}", flush=True)
    print(f"Final: {final:,}", flush=True)

    # Source breakdown
    print(f"\nJapanSearch sources:", flush=True)
    for row in db.execute("""
        SELECT source, COUNT(*) FROM entities
        WHERE source LIKE 'jps_%' OR source LIKE 'japansearch_%'
        GROUP BY source ORDER BY COUNT(*) DESC LIMIT 20
    """):
        print(f"  {row[0]}: {row[1]:,}", flush=True)

    db.close()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
