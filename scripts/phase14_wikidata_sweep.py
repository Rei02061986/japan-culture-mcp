"""
Phase 14 A3: Wikidata 15-category sweep.
Japanese gardens, dialects, fashion, martial arts, tea ceremony,
sumo, ukiyo-e, pottery, castles, swords, kabuki, noh, light novels,
visual novels, folklore/yokai.
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
WORK_DB = "/tmp/culture_ontology_p14.db"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/1.0 (teddykmk@gmail.com)"
PAGE_SIZE = 10000
BATCH_SIZE = 1000
SOURCE = "wd_sweep_p14"


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def sparql_query(query, retries=5):
    for attempt in range(retries):
        try:
            params = urllib.parse.urlencode({
                "query": query,
                "format": "json",
            })
            url = WIKIDATA_URL + "?" + params
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8")).get("results", {}).get("bindings", [])
        except Exception as e:
            status = ""
            if hasattr(e, "code"):
                status = f" (HTTP {e.code})"
            print(f"    SPARQL error{status} (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = min(10 * (2 ** attempt), 120)
                if hasattr(e, "code") and e.code in (429, 504):
                    wait = max(wait, 30)
                print(f"    Waiting {wait}s...", flush=True)
                time.sleep(wait)
    return []


def extract_wikidata_id(uri):
    if uri and "/entity/" in uri:
        return uri.split("/entity/")[-1]
    return None


def load_existing_wdids(db):
    existing = set()
    cursor = db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL")
    while True:
        rows = cursor.fetchmany(200000)
        if not rows:
            break
        for (wid,) in rows:
            existing.add(wid)
    return existing


# Category definitions: (name, entity_type, sparql_template)
# Each sparql_template must use {limit} and {offset} placeholders.
# May optionally select ?lat, ?lon, ?related (for connections).
CATEGORIES = [
    # 1. Japanese gardens
    {
        "name": "Japanese Gardens",
        "entity_type": "place",
        "sparql": """
SELECT ?item ?labelJa ?labelEn ?lat ?lon WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q15773439 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat) BIND(geof:longitude(?coord) AS ?lon) }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 2. Dialects of Japan
    {
        "name": "Japanese Dialects",
        "entity_type": "concept",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  {{ ?item wdt:P31/wdt:P279* wd:Q33273 . ?item wdt:P17 wd:Q17 . }}
  UNION
  {{ ?item wdt:P31/wdt:P279* wd:Q34770 . ?item wdt:P17 wd:Q17 . }}
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 3. Fashion designers + brands + magazines
    {
        "name": "Japanese Fashion Designers",
        "entity_type": "person",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P106 wd:Q3501317 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    {
        "name": "Japanese Fashion Brands",
        "entity_type": "organization",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q1070990 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    {
        "name": "Japanese Fashion Magazines",
        "entity_type": "work",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q5272157 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 4. Martial arts originating in Japan
    {
        "name": "Japanese Martial Arts",
        "entity_type": "concept",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q11417 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 5. Tea ceremony, tea schools, tea houses
    {
        "name": "Tea Ceremony Items",
        "entity_type": "concept",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  {{ ?item wdt:P31/wdt:P279* wd:Q184665 . }}
  UNION {{ ?item wdt:P31/wdt:P279* wd:Q1335720 . }}
  UNION {{ ?item wdt:P31/wdt:P279* wd:Q211841 . ?item wdt:P17 wd:Q17 . }}
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 6. Sumo wrestlers
    {
        "name": "Sumo Wrestlers",
        "entity_type": "person",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P106 wd:Q11606265 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 7. Ukiyo-e artists
    {
        "name": "Ukiyo-e Artists",
        "entity_type": "person",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P101 wd:Q186847 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 8. Japanese pottery/kilns
    {
        "name": "Japanese Pottery Kilns",
        "entity_type": "place",
        "sparql": """
SELECT ?item ?labelJa ?labelEn ?lat ?lon WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q13217483 .
  ?item wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat) BIND(geof:longitude(?coord) AS ?lon) }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 9. Japanese castles
    {
        "name": "Japanese Castles",
        "entity_type": "place",
        "sparql": """
SELECT ?item ?labelJa ?labelEn ?lat ?lon WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q28455455 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
  OPTIONAL {{ ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat) BIND(geof:longitude(?coord) AS ?lon) }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 10. Japanese swords
    {
        "name": "Japanese Swords",
        "entity_type": "artifact",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q111329 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 11. Kabuki plays
    {
        "name": "Kabuki Plays",
        "entity_type": "work",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q217164 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 12. Noh plays
    {
        "name": "Noh Plays",
        "entity_type": "work",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q180963 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 13. Light novels
    {
        "name": "Light Novels",
        "entity_type": "work",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q747381 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 14. Visual novels from Japan
    {
        "name": "Visual Novels",
        "entity_type": "work",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q689445 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
    # 15. Japanese folklore / yokai
    {
        "name": "Japanese Folklore and Yokai",
        "entity_type": "concept",
        "sparql": """
SELECT ?item ?labelJa ?labelEn WHERE {{
  {{ ?item wdt:P31/wdt:P279* wd:Q178706 . ?item wdt:P495 wd:Q17 . }}
  UNION {{ ?item wdt:P31/wdt:P279* wd:Q2239243 . }}
  UNION {{ ?item wdt:P31/wdt:P279* wd:Q159979 . ?item wdt:P495 wd:Q17 . }}
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""",
    },
]


def fetch_category(db, existing_wdids, cat):
    """Fetch one category with pagination."""
    name = cat["name"]
    entity_type = cat["entity_type"]
    sparql_tmpl = cat["sparql"]

    print(f"\n  --- {name} ---", flush=True)
    total_new = 0
    offset = 0
    empty_streak = 0
    batch_pending = 0
    new_entities = []  # (wikidata_id, entity_type)

    while True:
        query = sparql_tmpl.format(limit=PAGE_SIZE, offset=offset)
        bindings = sparql_query(query)

        if not bindings:
            empty_streak += 1
            if empty_streak >= 2:
                break
            offset += PAGE_SIZE
            time.sleep(1)
            continue

        empty_streak = 0
        page_new = 0

        for b in bindings:
            wid = extract_wikidata_id(b.get("item", {}).get("value", ""))
            if not wid or wid in existing_wdids:
                continue

            label_ja = b.get("labelJa", {}).get("value", "")
            label_en = b.get("labelEn", {}).get("value", "")
            if not label_ja and not label_en:
                continue

            lat = None
            lon = None
            if "lat" in b and "lon" in b:
                try:
                    lat = float(b["lat"]["value"])
                    lon = float(b["lon"]["value"])
                except (ValueError, KeyError):
                    pass

            try:
                db.execute(
                    "INSERT OR IGNORE INTO entities (wikidata_id, label_ja, label_en, entity_type, lat, lon, source) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (wid, label_ja or None, label_en or None, entity_type, lat, lon, SOURCE)
                )
                existing_wdids.add(wid)
                new_entities.append((wid, entity_type))
                page_new += 1
                batch_pending += 1
            except sqlite3.IntegrityError:
                pass

            if batch_pending >= BATCH_SIZE:
                db.commit()
                batch_pending = 0

        total_new += page_new
        offset += PAGE_SIZE

        if total_new > 0 and total_new % 5000 < PAGE_SIZE:
            print(f"    {name}: +{total_new:,} (offset {offset:,})", flush=True)

        if len(bindings) < PAGE_SIZE:
            break

        time.sleep(1)

    if batch_pending > 0:
        db.commit()

    print(f"    {name}: total +{total_new:,}", flush=True)
    return total_new, new_entities


def generate_connections(db, category_entities):
    """Generate connections from SPARQL properties using secondary queries."""
    print("\n  --- Generating cross-category connections ---", flush=True)

    # Load existing pairs
    existing_pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for a, b in rows:
            existing_pairs.add((min(a, b), max(a, b)))

    # Build wikidata_id -> entity id for new entities
    all_new_wids = set()
    for wid, _ in category_entities:
        all_new_wids.add(wid)

    wid_to_id = {}
    new_wids_list = list(all_new_wids)
    # Batch lookup
    for i in range(0, len(new_wids_list), 5000):
        batch = new_wids_list[i:i+5000]
        placeholders = ",".join(["?"] * len(batch))
        rows = db.execute(
            f"SELECT id, wikidata_id FROM entities WHERE wikidata_id IN ({placeholders})",
            batch
        ).fetchall()
        for eid, wid in rows:
            wid_to_id[wid] = eid

    # Connection queries: author->work, founder->art, etc.
    conn_queries = [
        # author -> work
        ("author_work", """
SELECT ?author ?work WHERE {{
  ?work wdt:P50 ?author .
  VALUES ?work {{ {values} }}
}}
"""),
        # creator -> work
        ("creator_work", """
SELECT ?creator ?work WHERE {{
  ?work wdt:P170 ?creator .
  VALUES ?work {{ {values} }}
}}
"""),
        # founder -> organization/school
        ("founder_art", """
SELECT ?founder ?org WHERE {{
  ?org wdt:P112 ?founder .
  VALUES ?org {{ {values} }}
}}
"""),
    ]

    total_conns = 0
    batch_pending = 0

    # Process in batches of 100 wikidata IDs
    wid_list = list(wid_to_id.keys())
    for conn_type, sparql_tmpl in conn_queries:
        type_conns = 0

        for i in range(0, len(wid_list), 100):
            batch_wids = wid_list[i:i+100]
            values_str = " ".join(f"wd:{w}" for w in batch_wids)
            query = sparql_tmpl.format(values=values_str)

            bindings = sparql_query(query)
            if not bindings:
                time.sleep(1)
                continue

            for b in bindings:
                # Get both entities
                keys = list(b.keys())
                if len(keys) < 2:
                    continue
                wid_a = extract_wikidata_id(b[keys[0]].get("value", ""))
                wid_b = extract_wikidata_id(b[keys[1]].get("value", ""))
                if not wid_a or not wid_b:
                    continue

                id_a = wid_to_id.get(wid_a)
                id_b = wid_to_id.get(wid_b)
                if not id_a:
                    # Try to find in DB
                    row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (wid_a,)).fetchone()
                    if row:
                        id_a = row[0]
                        wid_to_id[wid_a] = id_a
                if not id_b:
                    row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (wid_b,)).fetchone()
                    if row:
                        id_b = row[0]
                        wid_to_id[wid_b] = id_b

                if not id_a or not id_b or id_a == id_b:
                    continue

                pair = (min(id_a, id_b), max(id_a, id_b))
                if pair in existing_pairs:
                    continue

                try:
                    db.execute("""
                        INSERT OR IGNORE INTO connections
                            (entity_a_id, entity_b_id, connection_type, serendipity_score,
                             explanation, source, confidence)
                        VALUES (?, ?, ?, 0.6, ?, ?, 0.8)
                    """, (pair[0], pair[1], conn_type, f"Wikidata {conn_type}", "wd_sweep_conn_p14"))
                    existing_pairs.add(pair)
                    type_conns += 1
                    batch_pending += 1
                except sqlite3.IntegrityError:
                    pass

                if batch_pending >= BATCH_SIZE:
                    db.commit()
                    batch_pending = 0

            time.sleep(1)

        total_conns += type_conns
        if type_conns > 0:
            print(f"    {conn_type}: +{type_conns:,}", flush=True)

    # Same-category connections (entities from same category are related)
    # Group by category (entity_type is a rough proxy)
    type_groups = {}
    for wid, etype in category_entities:
        eid = wid_to_id.get(wid)
        if eid:
            if etype not in type_groups:
                type_groups[etype] = []
            type_groups[etype].append(eid)

    same_cat_conns = 0
    for etype, eids in type_groups.items():
        if len(eids) < 2:
            continue
        sample = eids[:500]
        for i in range(len(sample)):
            for j in range(i + 1, min(i + 3, len(sample))):
                pair = (min(sample[i], sample[j]), max(sample[i], sample[j]))
                if pair in existing_pairs:
                    continue
                try:
                    db.execute("""
                        INSERT OR IGNORE INTO connections
                            (entity_a_id, entity_b_id, connection_type, serendipity_score,
                             explanation, source, confidence)
                        VALUES (?, ?, 'same_theme', 0.4, ?, ?, 0.6)
                    """, (pair[0], pair[1], f"Same category ({etype})", "wd_sweep_conn_p14"))
                    existing_pairs.add(pair)
                    same_cat_conns += 1
                    batch_pending += 1
                except sqlite3.IntegrityError:
                    pass
                if batch_pending >= BATCH_SIZE:
                    db.commit()
                    batch_pending = 0

    if batch_pending > 0:
        db.commit()

    total_conns += same_cat_conns
    print(f"    Same-category: +{same_cat_conns:,}", flush=True)
    print(f"    Total connections: +{total_conns:,}", flush=True)
    return total_conns


def main():
    print("=" * 60, flush=True)
    print("Phase 14 A3: Wikidata 15-Category Sweep", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp
    print(f"Copying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()
    before_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    before_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"Entities before: {before_entities:,}", flush=True)
    print(f"Connections before: {before_conns:,}", flush=True)

    existing_wdids = load_existing_wdids(db)
    print(f"Existing wikidata_ids: {len(existing_wdids):,}", flush=True)

    all_category_entities = []
    category_results = {}

    for cat in CATEGORIES:
        try:
            count, new_ents = fetch_category(db, existing_wdids, cat)
            category_results[cat["name"]] = count
            all_category_entities.extend(new_ents)
        except Exception as e:
            print(f"    ERROR in {cat['name']}: {e}", flush=True)
            category_results[cat["name"]] = 0

    # Generate connections
    total_conns = 0
    try:
        total_conns = generate_connections(db, all_category_entities)
    except Exception as e:
        print(f"    Connection generation error: {e}", flush=True)

    after_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    after_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print("CATEGORY RESULTS", flush=True)
    for name, count in category_results.items():
        print(f"  {name}: +{count:,}", flush=True)
    print(f"\nSUMMARY", flush=True)
    print(f"  New entities: +{after_entities - before_entities:,}", flush=True)
    print(f"  New connections: +{after_conns - before_conns:,}", flush=True)
    print(f"  Entities: {before_entities:,} -> {after_entities:,}", flush=True)
    print(f"  Connections: {before_conns:,} -> {after_conns:,}", flush=True)
    print(f"  Duration: {datetime.now() - start}", flush=True)
    db.close()

    # Copy back
    print(f"Copying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
