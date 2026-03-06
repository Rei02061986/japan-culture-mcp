"""
Phase 16 D1: Wikidata music entities enrichment.

Fetch Japanese music-related entities from Wikidata:
  - Musical groups (P31=Q215380) from Japan (P495=Q17)
  - Musical works (P31=Q105543609) from Japan
  - Musicians (P27=Q17, P106=Q177220)
  - Albums, singles, songs by Japanese artists

Target: +10,000 entities
Source: wikidata_music
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import shutil
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p16d.db"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/1.2 (teddykmk@gmail.com)"
SOURCE = "wikidata_music"


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def db_commit_retry(db, retries=5):
    for i in range(retries):
        try:
            db.commit()
            return True
        except sqlite3.OperationalError as e:
            print(f"  Commit retry {i+1}: {e}", flush=True)
            time.sleep(3)
    return False


def sparql_query(query, retries=5):
    for attempt in range(retries):
        try:
            params = urllib.parse.urlencode({"query": query, "format": "json"})
            url = WIKIDATA_URL + "?" + params
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=120) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("results", {}).get("bindings", [])
        except Exception as e:
            status = ""
            if hasattr(e, "code"):
                status = f" (HTTP {e.code})"
            print(f"    SPARQL error{status} (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = min(10 * (2 ** attempt), 120)
                if hasattr(e, "code") and e.code in (429, 503, 504):
                    wait = max(wait, 30)
                time.sleep(wait)
    return []


# Music SPARQL queries
QUERIES = [
    # 1. Japanese musical groups/bands
    ("Japanese musical groups", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords WHERE {
  ?item wdt:P31/wdt:P279* wd:Q215380 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "ja")
  OPTIONAL { ?item rdfs:label ?itemLabel_en . FILTER(LANG(?itemLabel_en) = "en") }
}
LIMIT 3000
"""),
    # 2. Japanese musicians (born in Japan)
    ("Japanese musicians", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords WHERE {
  ?item wdt:P106 wd:Q177220 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "ja")
  OPTIONAL { ?item rdfs:label ?itemLabel_en . FILTER(LANG(?itemLabel_en) = "en") }
}
LIMIT 5000
"""),
    # 3. Japanese singers
    ("Japanese singers", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords WHERE {
  ?item wdt:P106 wd:Q177220 .
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P106 wd:Q639669 .
  OPTIONAL { ?item wdt:P625 ?coords }
  ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "ja")
  OPTIONAL { ?item rdfs:label ?itemLabel_en . FILTER(LANG(?itemLabel_en) = "en") }
}
LIMIT 3000
"""),
    # 4. Japanese composers
    ("Japanese composers", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords WHERE {
  ?item wdt:P106 wd:Q36834 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "ja")
  OPTIONAL { ?item rdfs:label ?itemLabel_en . FILTER(LANG(?itemLabel_en) = "en") }
}
LIMIT 3000
"""),
    # 5. J-pop / J-rock albums (genre = J-pop Q217597 or J-rock Q849380)
    ("J-pop/J-rock albums", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en WHERE {
  {
    ?item wdt:P31 wd:Q482994 .
    ?item wdt:P136 wd:Q217597 .
  } UNION {
    ?item wdt:P31 wd:Q482994 .
    ?item wdt:P136 wd:Q849380 .
  }
  ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "ja")
  OPTIONAL { ?item rdfs:label ?itemLabel_en . FILTER(LANG(?itemLabel_en) = "en") }
}
LIMIT 3000
"""),
    # 6. Anime/game soundtracks
    ("Anime/game soundtracks", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en WHERE {
  ?item wdt:P31 wd:Q63186042 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "ja")
  OPTIONAL { ?item rdfs:label ?itemLabel_en . FILTER(LANG(?itemLabel_en) = "en") }
}
LIMIT 2000
"""),
    # 7. Japanese record labels
    ("Japanese record labels", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords WHERE {
  ?item wdt:P31/wdt:P279* wd:Q18127 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "ja")
  OPTIONAL { ?item rdfs:label ?itemLabel_en . FILTER(LANG(?itemLabel_en) = "en") }
}
LIMIT 1000
"""),
    # 8. Traditional Japanese music instruments
    ("Traditional Japanese instruments", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en WHERE {
  ?item wdt:P31/wdt:P279* wd:Q34379 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "ja")
  OPTIONAL { ?item rdfs:label ?itemLabel_en . FILTER(LANG(?itemLabel_en) = "en") }
}
LIMIT 500
"""),
]


def parse_point(wkt):
    import re
    if not wkt:
        return None, None
    m = re.search(r"Point\(\s*([-\d.]+)\s+([-\d.]+)\s*\)", wkt)
    if m:
        lon, lat = float(m.group(1)), float(m.group(2))
        if -90 <= lat <= 90 and -180 <= lon <= 180:
            return lat, lon
    return None, None


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 16 D1: Wikidata Music Entities Enrichment", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    # Copy DB to /tmp
    print(f"\nCopying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("  Done.", flush=True)

    db = open_db()

    # Counts before
    entity_count_before = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    music_count_before = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source = ?", (SOURCE,)
    ).fetchone()[0]
    print(f"\nTotal entities before: {entity_count_before:,}", flush=True)
    print(f"Music entities before: {music_count_before:,}", flush=True)

    # Load existing wikidata_ids
    print("Loading existing wikidata_ids...", flush=True)
    existing_wids = set()
    cursor = db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL")
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for (wid,) in rows:
            existing_wids.add(wid)
    print(f"  Existing wikidata_ids: {len(existing_wids):,}", flush=True)

    # Process each query
    total_inserted = 0
    total_skipped = 0
    total_fetched = 0

    for query_name, sparql in QUERIES:
        print(f"\n--- {query_name} ---", flush=True)
        time.sleep(2)  # Rate limit

        bindings = sparql_query(sparql)
        print(f"  SPARQL results: {len(bindings):,}", flush=True)
        total_fetched += len(bindings)

        query_inserted = 0
        query_skipped = 0

        for b in bindings:
            item_uri = b.get("item", {}).get("value", "")
            qid = item_uri.split("/entity/")[-1] if "/entity/" in item_uri else None
            if not qid:
                continue

            if qid in existing_wids:
                query_skipped += 1
                continue

            label_ja = b.get("itemLabel", {}).get("value", "")
            label_en = b.get("itemLabel_en", {}).get("value", "")
            coords_val = b.get("coords", {}).get("value", "")

            lat, lon = parse_point(coords_val)

            # Determine entity_type based on query
            if "group" in query_name.lower() or "label" in query_name.lower():
                entity_type = "organization"
            elif "musician" in query_name.lower() or "singer" in query_name.lower() or "composer" in query_name.lower():
                entity_type = "person"
            elif "instrument" in query_name.lower():
                entity_type = "artifact"
            else:
                entity_type = "music"

            try:
                db.execute("""
                    INSERT OR IGNORE INTO entities
                        (wikidata_id, label_ja, label_en, entity_type, lat, lon, source, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (qid, label_ja or None, label_en or None, entity_type,
                      lat, lon, SOURCE, now))
                if db.execute("SELECT changes()").fetchone()[0] > 0:
                    existing_wids.add(qid)
                    query_inserted += 1
                else:
                    query_skipped += 1
            except sqlite3.IntegrityError:
                query_skipped += 1

        db_commit_retry(db)
        total_inserted += query_inserted
        total_skipped += query_skipped
        print(f"  Inserted: +{query_inserted:,}  Skipped: {query_skipped:,}", flush=True)

    # Counts after
    entity_count_after = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    music_count_after = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source = ?", (SOURCE,)
    ).fetchone()[0]

    db.close()

    # Copy back
    print(f"\nCopying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("  Done.", flush=True)

    # Summary
    elapsed = time.time() - t0
    print(f"\n{'='*70}", flush=True)
    print("PHASE 16 D1 SUMMARY", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  SPARQL results fetched:  {total_fetched:,}", flush=True)
    print(f"  New entities inserted:   +{total_inserted:,}", flush=True)
    print(f"  Skipped (existing):      {total_skipped:,}", flush=True)
    print(f"  Total entities: {entity_count_before:,} -> {entity_count_after:,}", flush=True)
    print(f"  Music entities: {music_count_before:,} -> {music_count_after:,}", flush=True)
    print(f"  Duration: {elapsed:.1f}s", flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
