"""
Phase 11 Stream B-3: Expand pilgrimage data via Wikidata
Pull P840 (narrative location) and P915 (filming location) with broader queries
Also pull P276 (location) for anime/manga works
Target: ≥2000 total pilgrimage spots
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import re

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/0.7 (teddykmk@gmail.com)"


def run_sparql(query, retries=3):
    """Run SPARQL query against Wikidata."""
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({"query": query, "format": "json"}).encode()
            req = urllib.request.Request(WIKIDATA_ENDPOINT, data=data, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=90) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  SPARQL error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
    return None


def parse_coord(wkt):
    """Parse WKT Point(lon lat)."""
    m = re.search(r'Point\(([-\d.]+)\s+([-\d.]+)\)', wkt)
    if m:
        return float(m.group(2)), float(m.group(1))  # lat, lon
    return None, None


QUERIES = [
    # 1. All anime with narrative locations (P840) - broader
    ("anime_narrative_broad", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31/wdt:P279* wd:Q63952888 .  # anime series
  ?work wdt:P840 ?loc .
  ?loc wdt:P625 ?coord .
  ?loc wdt:P17 wd:Q17 .  # in Japan
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    # 2. All manga with narrative locations
    ("manga_narrative", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31/wdt:P279* wd:Q21198342 .  # manga series
  ?work wdt:P840 ?loc .
  ?loc wdt:P625 ?coord .
  ?loc wdt:P17 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    # 3. Japanese animated films with locations
    ("anime_film_narrative", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31 wd:Q20650540 .  # animated film
  ?work wdt:P495 wd:Q17 .  # from Japan
  ?work wdt:P840 ?loc .
  ?loc wdt:P625 ?coord .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    # 4. Visual novels with locations
    ("visual_novel", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31/wdt:P279* wd:Q689445 .  # visual novel
  ?work wdt:P495 wd:Q17 .
  ?work wdt:P840 ?loc .
  ?loc wdt:P625 ?coord .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 2000
"""),

    # 5. Japanese TV dramas with locations
    ("tv_drama", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31/wdt:P279* wd:Q21191270 .  # TV series episode or similar
  ?work wdt:P495 wd:Q17 .
  ?work wdt:P840 ?loc .
  ?loc wdt:P625 ?coord .
  ?loc wdt:P17 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    # 6. Japanese films - filming locations P915
    ("film_filming", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31 wd:Q11424 .  # film
  ?work wdt:P495 wd:Q17 .
  ?work wdt:P915 ?loc .
  ?loc wdt:P625 ?coord .
  ?loc wdt:P17 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    # 7. Japanese works set in specific locations (P276 - location)
    ("work_location", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P495 wd:Q17 .
  ?work wdt:P276 ?loc .
  ?loc wdt:P625 ?coord .
  ?loc wdt:P17 wd:Q17 .
  {?work wdt:P31/wdt:P279* wd:Q11424} UNION {?work wdt:P31/wdt:P279* wd:Q5398426} .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    # 8. Light novels with narrative locations
    ("light_novel", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31/wdt:P279* wd:Q747381 .  # light novel
  ?work wdt:P840 ?loc .
  ?loc wdt:P625 ?coord .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 3000
"""),

    # 9. Video games set in Japan
    ("game_narrative", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31/wdt:P279* wd:Q7889 .  # video game
  ?work wdt:P495 wd:Q17 .
  ?work wdt:P840 ?loc .
  ?loc wdt:P625 ?coord .
  ?loc wdt:P17 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 5000
"""),

    # 10. All creative works with P840 narrative location in Japan (very broad)
    ("creative_work_japan", """
SELECT DISTINCT ?work ?workLabel ?loc ?locLabel ?coord WHERE {
  ?work wdt:P31/wdt:P279* wd:Q17537576 .  # creative work
  ?work wdt:P495 wd:Q17 .
  ?work wdt:P840 ?loc .
  ?loc wdt:P625 ?coord .
  ?loc wdt:P17 wd:Q17 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en" }
}
LIMIT 10000
"""),
]


def main():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")

    # Get existing entity labels for matching
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities"):
        existing_labels.add(row[0])

    total_new_entities = 0
    total_new_connections = 0
    total_results = 0

    for query_name, query in QUERIES:
        print(f"\n{'='*50}", flush=True)
        print(f"Query: {query_name}", flush=True)

        result = run_sparql(query)
        if not result or "results" not in result:
            print(f"  No results or error", flush=True)
            time.sleep(10)
            continue

        bindings = result["results"]["bindings"]
        print(f"  Raw results: {len(bindings)}", flush=True)
        total_results += len(bindings)

        new_ent = 0
        new_conn = 0

        for b in bindings:
            work_label = b.get("workLabel", {}).get("value", "")
            loc_label = b.get("locLabel", {}).get("value", "")
            coord_str = b.get("coord", {}).get("value", "")

            if not work_label or not loc_label or not coord_str:
                continue

            lat, lon = parse_coord(coord_str)
            if lat is None:
                continue

            # Filter: only Japan roughly
            if not (24 <= lat <= 46 and 122 <= lon <= 154):
                continue

            # Create/find work entity
            work_row = db.execute(
                "SELECT id FROM entities WHERE label_ja = ? LIMIT 1",
                (work_label,)
            ).fetchone()

            if not work_row:
                db.execute("""
                    INSERT INTO entities (label_ja, label_en, entity_type, source)
                    VALUES (?, ?, 'work', ?)
                """, (work_label, work_label, f"wikidata_{query_name}"))
                work_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                new_ent += 1
                existing_labels.add(work_label)
            else:
                work_id = work_row[0]

            # Create/find location entity
            loc_row = db.execute(
                "SELECT id FROM entities WHERE label_ja = ? AND lat IS NOT NULL LIMIT 1",
                (loc_label,)
            ).fetchone()

            if not loc_row:
                db.execute("""
                    INSERT INTO entities (label_ja, label_en, entity_type, lat, lon, source)
                    VALUES (?, ?, 'place', ?, ?, ?)
                """, (loc_label, loc_label, lat, lon, f"wikidata_{query_name}"))
                loc_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                new_ent += 1
                existing_labels.add(loc_label)
            else:
                loc_id = loc_row[0]

            # Create connection if not exists
            conn_type = "pilgrimage_filming" if "filming" in query_name else "pilgrimage_narrative"
            exists = db.execute("""
                SELECT 1 FROM connections
                WHERE connection_type LIKE 'pilgrimage%'
                AND ((entity_a_id = ? AND entity_b_id = ?) OR (entity_a_id = ? AND entity_b_id = ?))
            """, (work_id, loc_id, loc_id, work_id)).fetchone()

            if not exists:
                explanation = f"作品「{work_label}」の{'撮影地' if 'filming' in query_name else '舞台'}「{loc_label}」[wikidata]"
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type, confidence, explanation)
                    VALUES (?, ?, ?, 0.85, ?)
                """, (work_id, loc_id, conn_type, explanation))
                new_conn += 1

        # Commit with retry
        for retry in range(5):
            try:
                db.commit()
                break
            except sqlite3.OperationalError as e:
                print(f"  Commit retry {retry+1}: {e}", flush=True)
                time.sleep(3)
        total_new_entities += new_ent
        total_new_connections += new_conn
        print(f"  New entities: {new_ent}, New connections: {new_conn}", flush=True)
        time.sleep(5)

    # Final stats
    pilgrim = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'"
    ).fetchone()[0]
    total_conn = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    total_ent = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    # Count unique pilgrimage locations (spots with coordinates)
    spots = db.execute("""
        SELECT COUNT(DISTINCT e_loc.id) FROM connections c
        JOIN entities e_loc ON (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e_loc.lat IS NOT NULL
        AND e_loc.entity_type = 'place'
    """).fetchone()[0]

    # Count unique works with pilgrimage data
    works = db.execute("""
        SELECT COUNT(DISTINCT e_work.id) FROM connections c
        JOIN entities e_work ON (c.entity_a_id = e_work.id OR c.entity_b_id = e_work.id)
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e_work.entity_type = 'work'
    """).fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print(f"=== Phase 11 Wikidata Pilgrimage Import Results ===", flush=True)
    print(f"Total raw results across queries: {total_results:,}", flush=True)
    print(f"New entities: {total_new_entities:,}", flush=True)
    print(f"New connections: {total_new_connections:,}", flush=True)
    print(f"\nTotal pilgrimage connections: {pilgrim:,}", flush=True)
    print(f"Total connections: {total_conn:,}", flush=True)
    print(f"Total entities: {total_ent:,}", flush=True)
    print(f"Unique pilgrimage locations: {spots:,}", flush=True)
    print(f"Unique works with pilgrimage: {works:,}", flush=True)

    db.close()


if __name__ == "__main__":
    main()
