"""
Phase 13: Japanese Architecture - Wikidata SPARQL bulk import.
Target: 10,000+ new building entities with connections.

Queries:
  1. Buildings with architect (P84) in Japan
  2. Registered tangible cultural properties (P31=Q3395498)
  3. Buildings with architectural style (P149) in Japan
  4. Important cultural properties that are buildings (P31=Q916305)
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import re
from collections import defaultdict

DB_PATH = "/tmp/culture_ontology_work.db"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/0.9 (teddykmk@gmail.com)"
BATCH_SIZE = 5000
SPARQL_SLEEP = 10  # seconds between queries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def open_db():
    db = sqlite3.connect(DB_PATH, timeout=30)
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


def run_sparql(query, retries=3):
    """Execute a SPARQL query against Wikidata and return parsed JSON."""
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({
                "query": query,
                "format": "json",
            }).encode()
            req = urllib.request.Request(WIKIDATA_ENDPOINT, data=data, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  SPARQL error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = 15 * (attempt + 1)
                print(f"  Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
    return None


def parse_coord(wkt):
    """Parse WKT Point(lon lat) into (lat, lon)."""
    if not wkt:
        return None, None
    m = re.search(r'Point\(([-\d.]+)\s+([-\d.]+)\)', wkt)
    if m:
        return float(m.group(2)), float(m.group(1))
    return None, None


def extract_qid(uri):
    """Extract Wikidata Q-id from a URI."""
    if not uri:
        return None
    parts = uri.rsplit("/", 1)
    if len(parts) == 2 and parts[1].startswith("Q"):
        return parts[1]
    return None


def extract_label(binding, field="itemLabel"):
    """Extract a label from SPARQL binding, rejecting Q-id fallbacks."""
    val = binding.get(field, {}).get("value", "")
    if not val or len(val) < 2:
        return None
    # Reject if the label is just a Q-id (Wikidata fallback)
    if re.match(r'^Q\d+$', val):
        return None
    return val


def insert_connection(db, a_id, b_id, conn_type, confidence, explanation, source,
                      existing_pairs, serendipity=0.5):
    """Insert a single connection if the pair does not already exist."""
    pair = (min(a_id, b_id), max(a_id, b_id))
    if pair in existing_pairs:
        return False
    try:
        db.execute("""
            INSERT OR IGNORE INTO connections
                (entity_a_id, entity_b_id, connection_type, serendipity_score,
                 explanation, source, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (pair[0], pair[1], conn_type, serendipity, explanation, source, confidence))
        existing_pairs.add(pair)
        return True
    except sqlite3.IntegrityError:
        return False


# ---------------------------------------------------------------------------
# SPARQL Queries
# ---------------------------------------------------------------------------

QUERIES = [
    # Query 1: Buildings with architect (P84) in Japan
    ("architect_buildings", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords ?architectLabel ?inception ?styleLabel WHERE {
  ?item wdt:P17 wd:Q17 .
  ?item wdt:P84 ?architect .
  OPTIONAL { ?item wdt:P625 ?coords }
  OPTIONAL { ?item wdt:P571 ?inception }
  OPTIONAL { ?item wdt:P149 ?style }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "ja" .
    ?item rdfs:label ?itemLabel .
    ?architect rdfs:label ?architectLabel .
    ?style rdfs:label ?styleLabel .
  }
  OPTIONAL {
    ?item rdfs:label ?itemLabel_en .
    FILTER(LANG(?itemLabel_en) = "en")
  }
}
LIMIT 50000
"""),

    # Query 2: Registered tangible cultural properties in Japan
    ("registered_cultural_properties", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords WHERE {
  ?item wdt:P31 wd:Q3395498 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "ja" .
    ?item rdfs:label ?itemLabel .
  }
  OPTIONAL {
    ?item rdfs:label ?itemLabel_en .
    FILTER(LANG(?itemLabel_en) = "en")
  }
}
LIMIT 50000
"""),

    # Query 3: Buildings with architectural style (P149) in Japan
    ("architectural_style_buildings", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords ?styleLabel WHERE {
  ?item wdt:P17 wd:Q17 .
  ?item wdt:P149 ?style .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "ja" .
    ?item rdfs:label ?itemLabel .
    ?style rdfs:label ?styleLabel .
  }
  OPTIONAL {
    ?item rdfs:label ?itemLabel_en .
    FILTER(LANG(?itemLabel_en) = "en")
  }
}
LIMIT 50000
"""),

    # Query 4: Important cultural properties (buildings)
    ("important_cultural_buildings", """
SELECT DISTINCT ?item ?itemLabel ?itemLabel_en ?coords WHERE {
  ?item wdt:P31 wd:Q916305 .
  ?item wdt:P31/wdt:P279* wd:Q41176 .
  OPTIONAL { ?item wdt:P625 ?coords }
  SERVICE wikibase:label {
    bd:serviceParam wikibase:language "ja" .
    ?item rdfs:label ?itemLabel .
  }
  OPTIONAL {
    ?item rdfs:label ?itemLabel_en .
    FILTER(LANG(?itemLabel_en) = "en")
  }
}
LIMIT 50000
"""),
]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70, flush=True)
    print("Phase 13: Japanese Architecture - Wikidata SPARQL Import", flush=True)
    print("=" * 70, flush=True)

    db = open_db()

    # Load existing wikidata_ids and labels for dedup
    existing_wdids = set()
    existing_labels = set()
    for row in db.execute("SELECT wikidata_id, label_ja FROM entities"):
        if row[0]:
            existing_wdids.add(row[0])
        if row[1]:
            existing_labels.add(row[1])
    print(f"Existing entities: labels={len(existing_labels):,}, "
          f"wikidata_ids={len(existing_wdids):,}", flush=True)

    # Load existing connection pairs
    existing_pairs = set()
    try:
        cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
        while True:
            rows = cursor.fetchmany(50000)
            if not rows:
                break
            for a, b in rows:
                existing_pairs.add((min(a, b), max(a, b)))
        print(f"Existing connection pairs: {len(existing_pairs):,}", flush=True)
    except sqlite3.OperationalError:
        print("  Connections table not found or empty, starting fresh.", flush=True)

    grand_total_entities = 0
    grand_total_connections = 0

    # Track architect -> [entity_ids] and style -> [entity_ids] for connections
    architect_to_entities = defaultdict(list)
    style_to_entities = defaultdict(list)

    for query_name, sparql in QUERIES:
        print(f"\n{'='*60}", flush=True)
        print(f"Query: {query_name}", flush=True)
        print(f"{'='*60}", flush=True)

        result = run_sparql(sparql)
        if not result or "results" not in result:
            print(f"  No results or error for {query_name}", flush=True)
            time.sleep(SPARQL_SLEEP)
            continue

        bindings = result["results"]["bindings"]
        print(f"  Raw results: {len(bindings):,}", flush=True)

        new_count = 0
        batch_count = 0

        for b in bindings:
            # Extract wikidata_id
            wikidata_id = extract_qid(b.get("item", {}).get("value", ""))
            if not wikidata_id:
                continue

            # Skip if already exists
            if wikidata_id in existing_wdids:
                # Still track for connections if we have architect/style info
                architect_label = extract_label(b, "architectLabel")
                style_label = extract_label(b, "styleLabel")
                # Lookup entity id for connection building
                row = db.execute(
                    "SELECT id FROM entities WHERE wikidata_id = ?", (wikidata_id,)
                ).fetchone()
                if row:
                    eid = row[0]
                    if architect_label:
                        architect_to_entities[architect_label].append(eid)
                    if style_label:
                        style_to_entities[style_label].append(eid)
                continue

            # Extract label (Japanese)
            label_ja = extract_label(b, "itemLabel")
            if not label_ja:
                continue

            # Skip duplicate labels
            if label_ja in existing_labels:
                continue

            # Extract English label
            label_en = extract_label(b, "itemLabel_en")

            # Extract coordinates
            lat, lon = parse_coord(b.get("coords", {}).get("value", ""))

            # Insert entity
            try:
                cur = db.execute("""
                    INSERT OR IGNORE INTO entities
                        (wikidata_id, label_ja, label_en, entity_type, lat, lon, source)
                    VALUES (?, ?, ?, 'building', ?, ?, ?)
                """, (wikidata_id, label_ja, label_en, lat, lon,
                      f"wikidata_arch_{query_name}"))

                if cur.rowcount > 0:
                    eid = cur.lastrowid
                    existing_wdids.add(wikidata_id)
                    existing_labels.add(label_ja)
                    new_count += 1
                    batch_count += 1

                    # Track architect and style for connections
                    architect_label = extract_label(b, "architectLabel")
                    style_label = extract_label(b, "styleLabel")
                    if architect_label:
                        architect_to_entities[architect_label].append(eid)
                    if style_label:
                        style_to_entities[style_label].append(eid)

                    if batch_count >= BATCH_SIZE:
                        db_commit_retry(db)
                        batch_count = 0
                        print(f"    ... committed batch, {new_count:,} new so far",
                              flush=True)
            except sqlite3.IntegrityError:
                continue

        # Final commit for this query
        if batch_count > 0:
            db_commit_retry(db)

        grand_total_entities += new_count
        print(f"  New entities: {new_count:,} (running total: {grand_total_entities:,})",
              flush=True)

        # Sleep between queries
        print(f"  Sleeping {SPARQL_SLEEP}s...", flush=True)
        time.sleep(SPARQL_SLEEP)

    # ---------------------------------------------------------------------------
    # Build connections: buildings sharing the same architect
    # ---------------------------------------------------------------------------
    print(f"\n{'='*60}", flush=True)
    print("Building connections: same architect", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Unique architects with 2+ buildings: "
          f"{sum(1 for v in architect_to_entities.values() if len(v) >= 2):,}",
          flush=True)

    conn_count = 0
    batch_count = 0
    for architect, eids in architect_to_entities.items():
        if len(eids) < 2:
            continue
        # Cap to avoid quadratic explosion
        cap = min(len(eids), 100)
        for i in range(cap):
            for j in range(i + 1, cap):
                explanation = f"同じ建築家による建築: {architect}"
                if len(explanation) > 200:
                    explanation = explanation[:197] + "..."
                inserted = insert_connection(
                    db, eids[i], eids[j], "same_architect", 0.8,
                    explanation, "phase13_arch_architect", existing_pairs,
                    serendipity=0.6,
                )
                if inserted:
                    conn_count += 1
                    batch_count += 1
                if batch_count >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch_count = 0
                    print(f"    ... committed batch, {conn_count:,} connections",
                          flush=True)

    if batch_count > 0:
        db_commit_retry(db)
    print(f"  Same-architect connections: +{conn_count:,}", flush=True)
    grand_total_connections += conn_count

    # ---------------------------------------------------------------------------
    # Build connections: buildings sharing the same style
    # ---------------------------------------------------------------------------
    print(f"\n{'='*60}", flush=True)
    print("Building connections: same architectural style", flush=True)
    print(f"{'='*60}", flush=True)
    print(f"  Unique styles with 2+ buildings: "
          f"{sum(1 for v in style_to_entities.values() if len(v) >= 2):,}",
          flush=True)

    conn_count_style = 0
    batch_count = 0
    for style, eids in style_to_entities.items():
        if len(eids) < 2:
            continue
        cap = min(len(eids), 100)
        for i in range(cap):
            for j in range(i + 1, cap):
                explanation = f"同じ建築様式: {style}"
                if len(explanation) > 200:
                    explanation = explanation[:197] + "..."
                inserted = insert_connection(
                    db, eids[i], eids[j], "same_style", 0.7,
                    explanation, "phase13_arch_style", existing_pairs,
                    serendipity=0.5,
                )
                if inserted:
                    conn_count_style += 1
                    batch_count += 1
                if batch_count >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch_count = 0
                    print(f"    ... committed batch, {conn_count_style:,} connections",
                          flush=True)

    if batch_count > 0:
        db_commit_retry(db)
    print(f"  Same-style connections: +{conn_count_style:,}", flush=True)
    grand_total_connections += conn_count_style

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_buildings = db.execute(
        "SELECT COUNT(*) FROM entities WHERE entity_type='building'"
    ).fetchone()[0]

    try:
        total_connections = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    except sqlite3.OperationalError:
        total_connections = 0

    print(f"\n{'='*70}", flush=True)
    print("SUMMARY: Phase 13 Architecture", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  New building entities:      +{grand_total_entities:,}", flush=True)
    print(f"  New connections (architect): +{grand_total_connections - conn_count_style:,}",
          flush=True)
    print(f"  New connections (style):     +{conn_count_style:,}", flush=True)
    print(f"  Total new connections:       +{grand_total_connections:,}", flush=True)
    print(f"  Total entities in DB:        {total_entities:,}", flush=True)
    print(f"  Total building entities:     {total_buildings:,}", flush=True)
    print(f"  Total connections in DB:     {total_connections:,}", flush=True)

    if grand_total_entities >= 10000:
        print(f"\n  TARGET REACHED (10,000+ buildings)!", flush=True)
    else:
        print(f"\n  Gap to target: {10000 - grand_total_entities:,}", flush=True)

    # Source breakdown
    print(f"\nBy source (architecture):", flush=True)
    for row in db.execute("""
        SELECT source, COUNT(*) FROM entities
        WHERE source LIKE 'wikidata_arch_%'
        GROUP BY source ORDER BY COUNT(*) DESC
    """):
        print(f"  {row[0]}: {row[1]:,}", flush=True)

    db.close()
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
