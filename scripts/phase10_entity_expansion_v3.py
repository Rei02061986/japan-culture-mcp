"""
Phase 10 Task #66 v3: Push to 2.5M entities
JapanSearch broad paginated queries at high offsets
"""
import requests
import time
import sqlite3

DB_PATH = "ontology/culture_ontology.db"
JPSEARCH_SPARQL = "https://jpsearch.go.jp/rdf/sparql"


def sparql_jpsearch(query, retries=4):
    for attempt in range(retries):
        try:
            resp = requests.post(
                JPSEARCH_SPARQL,
                data={'query': query},
                headers={
                    'Accept': 'application/sparql-results+json',
                    'User-Agent': 'japan-culture-mcp/0.7',
                },
                timeout=180,
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (503, 500, 400):
                print(f"    HTTP {resp.status_code}, retry {attempt+1}", flush=True)
                time.sleep(30 * (attempt + 1))
            else:
                print(f"    HTTP {resp.status_code}", flush=True)
                time.sleep(15)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(15)
    return None


def main():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])

    current_total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"Existing labels: {len(existing_labels):,}", flush=True)
    print(f"Current total: {current_total:,}", flush=True)
    print(f"Need: {max(0, 2500000 - current_total):,} more entities", flush=True)

    grand_total = 0

    # Paginated queries at different offsets — each returns ~10K results, ~50% new
    # Need ~50 batches at ~5K new/batch = ~250K new
    base_query = """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?item ?label WHERE {{
  ?item rdfs:label ?label .
  ?item schema:datePublished ?date .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
  FILTER(STRLEN(?label) >= 2 && STRLEN(?label) <= 200)
}}
LIMIT 10000 OFFSET {offset}
"""

    # Start from offset 2,300,000 (previous batches covered up to 2,300,000)
    start_offset = 2300000
    batch_size = 10000
    num_batches = 60

    for i in range(num_batches):
        offset = start_offset + (i * batch_size)
        source = f"jps_broad_{offset // 1000}k"

        print(f"\n=== Batch {i+1}/{num_batches} (offset={offset:,}, source={source}) ===", flush=True)

        query = base_query.format(offset=offset)
        result = sparql_jpsearch(query)

        if not result:
            print("  Failed, skipping", flush=True)
            continue

        bindings = result.get('results', {}).get('bindings', [])
        if not bindings:
            print("  No results, stopping", flush=True)
            break

        new_count = 0
        for b in bindings:
            label = b.get('label', {}).get('value', '').strip()
            if not label or len(label) < 2 or len(label) > 300 or label in existing_labels:
                continue

            try:
                db.execute(
                    "INSERT INTO entities (label_ja, entity_type, source) VALUES (?, 'work', ?)",
                    (label, source),
                )
                existing_labels.add(label)
                new_count += 1
            except sqlite3.IntegrityError:
                continue

        if new_count > 0:
            db.execute(f"""
                INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                SELECT id, 'experience', 'intellectual', '{source}', 0.6
                FROM entities WHERE source = '{source}'
                AND id NOT IN (SELECT entity_id FROM entity_tags WHERE axis='experience' AND source='{source}')
            """)

        db.commit()
        grand_total += new_count
        remaining = max(0, 2500000 - (current_total + grand_total))
        print(f"  Results: {len(bindings)}, New: {new_count:,}, Running: {grand_total:,}, Need: {remaining:,} more", flush=True)

        if remaining == 0:
            print("  Target reached!", flush=True)
            break

        time.sleep(5)

    # Summary
    total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    sources = db.execute("SELECT COUNT(DISTINCT source) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== Entity Expansion v3 Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total:,}", flush=True)
    print(f"Total sources: {sources}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
