"""
Phase 18 C1: Wikidata P577 (publication date) SPARQL enrichment.

Fetches P577 dates from Wikidata for entities that have a wikidata_id
but no release_year. Updates release_year + release_year_source.

Expected yield: ~15K new release_year values.
"""
import os
import shutil
import sqlite3
import time
import urllib.request
import urllib.parse
import json

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ORIG_DB = os.path.join(SCRIPT_DIR, "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p18_wikidata.db"

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
BATCH_SIZE = 100
RATE_LIMIT_SECS = 10
MAX_RETRIES = 3


def sparql_query(query_str):
    """Execute SPARQL query against Wikidata, return JSON results."""
    params = urllib.parse.urlencode({
        "query": query_str,
        "format": "json",
    })
    url = f"{WIKIDATA_SPARQL}?{params}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "JapanCultureMCP/1.3.0 (research; teddykmk@gmail.com)",
        "Accept": "application/sparql-results+json",
    })

    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = int(e.headers.get("Retry-After", 30))
                print(f"    429 rate limited, waiting {retry_after}s...", flush=True)
                time.sleep(retry_after)
            elif e.code == 504:
                print(f"    504 timeout, retry {attempt + 1}/{MAX_RETRIES}...", flush=True)
                time.sleep(RATE_LIMIT_SECS * 2)
            else:
                raise
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    Error: {e}, retry {attempt + 1}...", flush=True)
                time.sleep(RATE_LIMIT_SECS)
            else:
                raise
    return None


def main():
    t0 = time.time()

    print("=" * 70, flush=True)
    print("Phase 18 C1: Wikidata P577 Enrichment", flush=True)
    print("=" * 70, flush=True)

    if not os.path.exists(ORIG_DB):
        print(f"ERROR: DB not found at {ORIG_DB}", flush=True)
        return

    # Copy DB to /tmp
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = sqlite3.connect(WORK_DB, timeout=60)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=60000")
    db.row_factory = sqlite3.Row

    # Count candidates
    candidates = db.execute("""
        SELECT id, wikidata_id FROM entities
        WHERE wikidata_id IS NOT NULL
          AND release_year IS NULL
          AND is_dormant = 0
    """).fetchall()
    print(f"\nCandidates (wikidata_id + no release_year): {len(candidates):,}", flush=True)

    # Check existing release_year count
    existing_count = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]
    print(f"Existing release_year: {existing_count:,}", flush=True)

    # Process in batches
    total_updated = 0
    total_found = 0
    batch_count = 0
    errors = 0

    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i:i + BATCH_SIZE]
        batch_count += 1

        # Build SPARQL VALUES clause
        qids = [row["wikidata_id"] for row in batch if row["wikidata_id"].startswith("Q")]
        if not qids:
            continue

        values_str = " ".join(f"wd:{qid}" for qid in qids)
        sparql = f"""
        SELECT ?item ?year WHERE {{
            VALUES ?item {{ {values_str} }}
            ?item wdt:P577 ?date .
            BIND(YEAR(?date) AS ?year)
        }}
        """

        if batch_count % 10 == 1:
            print(f"\n  Batch {batch_count} ({i:,}/{len(candidates):,})...", flush=True)

        # Rate limit
        time.sleep(RATE_LIMIT_SECS)

        try:
            result = sparql_query(sparql)
        except Exception as e:
            errors += 1
            print(f"    SPARQL error: {e}", flush=True)
            continue

        if not result or "results" not in result:
            continue

        # Parse results
        year_map = {}
        for binding in result["results"]["bindings"]:
            qid = binding["item"]["value"].split("/")[-1]
            try:
                year = int(float(binding["year"]["value"]))
            except (ValueError, KeyError):
                continue
            if 1400 <= year <= 2026:
                year_map[qid] = year

        total_found += len(year_map)

        # Update DB
        updates = []
        for row in batch:
            qid = row["wikidata_id"]
            if qid in year_map:
                updates.append((year_map[qid], "wikidata_p577", row["id"]))

        if updates:
            db.executemany(
                "UPDATE entities SET release_year = ?, release_year_source = ? WHERE id = ?",
                updates,
            )
            total_updated += len(updates)

        # Commit every 10 batches
        if batch_count % 10 == 0:
            db.commit()
            print(f"    Found: {total_found:,} / Updated: {total_updated:,}", flush=True)

    db.commit()

    # Final count
    new_count = db.execute(
        "SELECT COUNT(*) FROM entities WHERE release_year IS NOT NULL"
    ).fetchone()[0]

    print(f"\n{'='*70}", flush=True)
    print(f"Summary:", flush=True)
    print(f"  Batches processed: {batch_count}", flush=True)
    print(f"  P577 dates found: {total_found:,}", flush=True)
    print(f"  Entities updated: {total_updated:,}", flush=True)
    print(f"  Errors: {errors}", flush=True)
    print(f"  release_year before: {existing_count:,}", flush=True)
    print(f"  release_year after:  {new_count:,} (+{new_count - existing_count:,})", flush=True)

    db.close()

    # Copy back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)

    try:
        os.unlink(WORK_DB)
    except OSError:
        pass

    elapsed = time.time() - t0
    print(f"\nTotal duration: {elapsed:.1f}s", flush=True)
    print("Phase 18 C1 complete.", flush=True)


if __name__ == "__main__":
    main()
