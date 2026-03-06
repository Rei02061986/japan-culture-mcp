"""
Phase 10 Stream D: 聖地巡礼データ取得
Wikidata P840 (narrative location) + P915 (filming location)
Target: 8,000+ work×place pilgrimage connections
"""
import requests
import time
import sqlite3
import re

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    'User-Agent': 'japan-culture-mcp/0.6 (teddykmk@gmail.com)',
    'Accept': 'application/sparql-results+json',
}

# ── P840: Narrative location (anime/manga/film/game set in Japan) ──

QUERIES = [
    # 1. Anime set in Japanese locations
    {
        'name': 'anime_narrative_location',
        'source': 'wd_pilgrimage_anime',
        'work_type': 'anime',
        'query': """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  ?work wdt:P31/wdt:P279* wd:Q63952888 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 10000
""",
    },
    # 2. Manga set in Japanese locations
    {
        'name': 'manga_narrative_location',
        'source': 'wd_pilgrimage_manga',
        'work_type': 'manga',
        'query': """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  ?work wdt:P31/wdt:P279* wd:Q21198342 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 10000
""",
    },
    # 3. Film set in Japanese locations
    {
        'name': 'film_narrative_location',
        'source': 'wd_pilgrimage_film',
        'work_type': 'film',
        'query': """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  ?work wdt:P31/wdt:P279* wd:Q11424 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 10000
""",
    },
    # 4. TV series set in Japanese locations
    {
        'name': 'tv_narrative_location',
        'source': 'wd_pilgrimage_tv',
        'work_type': 'tv',
        'query': """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  ?work wdt:P31/wdt:P279* wd:Q5398426 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 10000
""",
    },
    # 5. Video game set in Japanese locations
    {
        'name': 'game_narrative_location',
        'source': 'wd_pilgrimage_game',
        'work_type': 'game',
        'query': """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  ?work wdt:P31/wdt:P279* wd:Q7889 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 10000
""",
    },
    # 6. Light novel set in Japanese locations
    {
        'name': 'lightnovel_narrative_location',
        'source': 'wd_pilgrimage_ln',
        'work_type': 'light_novel',
        'query': """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  ?work wdt:P31/wdt:P279* wd:Q747381 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 10000
""",
    },
    # 7. P915 Filming location in Japan (all works)
    {
        'name': 'filming_location',
        'source': 'wd_pilgrimage_filming',
        'work_type': 'film',
        'query': """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P915 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 10000
""",
    },
    # 8. Broader: any creative work with P840 in Japan
    {
        'name': 'creative_work_narrative',
        'source': 'wd_pilgrimage_creative',
        'work_type': 'work',
        'query': """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  ?work wdt:P31/wdt:P279* wd:Q17537576 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 10000
""",
    },
]


def parse_coord(coord_str):
    """Parse 'Point(lon lat)' WKT to (lat, lon)."""
    m = re.search(r'Point\(([-\d.]+)\s+([-\d.]+)\)', coord_str)
    if m:
        return float(m.group(2)), float(m.group(1))
    return None, None


def extract_qid(uri):
    """Extract Q-id from Wikidata URI."""
    if uri and '/Q' in uri:
        return uri.split('/')[-1]
    return None


def run_sparql(query, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={'query': query, 'format': 'json'},
                headers=HEADERS,
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code}", flush=True)
                time.sleep(30)
        except Exception as e:
            print(f"  Error: {e}", flush=True)
            time.sleep(30)
    return None


def main():
    db = sqlite3.connect(DB_PATH)

    # Load existing entities for dedup
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])

    existing_wikidata = set()
    for row in db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL"):
        existing_wikidata.add(row[0])

    print(f"Existing: {len(existing_labels):,} labels, {len(existing_wikidata):,} wikidata IDs", flush=True)

    total_connections = 0
    total_new_entities = 0

    for q in QUERIES:
        print(f"\n=== {q['name']} ===", flush=True)

        result = run_sparql(q['query'])
        if not result:
            print("  Failed to fetch", flush=True)
            continue

        bindings = result.get('results', {}).get('bindings', [])
        print(f"  Results: {len(bindings)}", flush=True)

        new_entities = 0
        new_connections = 0

        for b in bindings:
            work_label = b.get('workLabel', {}).get('value', '')
            location_label = b.get('locationLabel', {}).get('value', '')
            work_uri = b.get('work', {}).get('value', '')
            location_uri = b.get('location', {}).get('value', '')
            coord_str = b.get('coord', {}).get('value', '')

            if not work_label or not location_label:
                continue

            work_qid = extract_qid(work_uri)
            location_qid = extract_qid(location_uri)
            lat, lon = parse_coord(coord_str)

            # Ensure work entity exists
            work_id = None
            if work_qid and work_qid in existing_wikidata:
                row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (work_qid,)).fetchone()
                if row:
                    work_id = row[0]
            elif work_label in existing_labels:
                row = db.execute("SELECT id FROM entities WHERE label_ja = ?", (work_label,)).fetchone()
                if row:
                    work_id = row[0]

            if work_id is None:
                # Create work entity
                medium_map = {
                    'anime': 'anime_tv', 'manga': 'manga', 'film': 'film',
                    'tv': 'tv_drama', 'game': 'game', 'light_novel': 'light_novel',
                    'work': 'literature',
                }
                entity_type_map = {
                    'anime': 'anime', 'manga': 'manga', 'film': 'film',
                    'tv': 'tv', 'game': 'game', 'light_novel': 'light_novel',
                    'work': 'work',
                }
                try:
                    cur = db.execute(
                        "INSERT INTO entities (wikidata_id, label_ja, entity_type, source) VALUES (?, ?, ?, ?)",
                        (work_qid, work_label, entity_type_map.get(q['work_type'], 'work'), q['source']),
                    )
                    work_id = cur.lastrowid
                    existing_labels.add(work_label)
                    if work_qid:
                        existing_wikidata.add(work_qid)

                    # Tag the work
                    medium = medium_map.get(q['work_type'], 'literature')
                    db.execute(
                        "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, ?, 0.9)",
                        (work_id, medium, q['source']),
                    )
                    new_entities += 1
                except sqlite3.IntegrityError:
                    # wikidata_id conflict
                    row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (work_qid,)).fetchone()
                    if row:
                        work_id = row[0]
                    else:
                        continue

            # Ensure location entity exists
            location_id = None
            if location_qid and location_qid in existing_wikidata:
                row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (location_qid,)).fetchone()
                if row:
                    location_id = row[0]
            elif location_label in existing_labels:
                row = db.execute("SELECT id FROM entities WHERE label_ja = ?", (location_label,)).fetchone()
                if row:
                    location_id = row[0]

            if location_id is None:
                try:
                    cur = db.execute(
                        "INSERT INTO entities (wikidata_id, label_ja, entity_type, source, lat, lon) VALUES (?, ?, 'place', ?, ?, ?)",
                        (location_qid, location_label, q['source'], lat, lon),
                    )
                    location_id = cur.lastrowid
                    existing_labels.add(location_label)
                    if location_qid:
                        existing_wikidata.add(location_qid)

                    # Tag location
                    db.execute(
                        "INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'physical', ?, 0.8)",
                        (location_id, q['source']),
                    )
                    new_entities += 1
                except sqlite3.IntegrityError:
                    row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (location_qid,)).fetchone()
                    if row:
                        location_id = row[0]
                    else:
                        continue
            else:
                # Update coordinates if missing
                if lat and lon:
                    db.execute(
                        "UPDATE entities SET lat = ?, lon = ? WHERE id = ? AND lat IS NULL",
                        (lat, lon, location_id),
                    )

            if work_id and location_id:
                # Check if connection already exists
                exists = db.execute(
                    "SELECT 1 FROM connections WHERE (entity_a_id=? AND entity_b_id=?) OR (entity_a_id=? AND entity_b_id=?)",
                    (work_id, location_id, location_id, work_id),
                ).fetchone()

                if not exists:
                    conn_type = 'pilgrimage_narrative' if 'filming' not in q['name'] else 'pilgrimage_filming'
                    db.execute("""
                        INSERT INTO connections
                        (entity_a_id, entity_b_id, connection_type, serendipity_score,
                         explanation, source, confidence, llm_verdict)
                        VALUES (?, ?, ?, 0.8, ?, ?, 0.9, 'keep')
                    """, (
                        work_id, location_id, conn_type,
                        f"聖地巡礼: 「{work_label}」の舞台・ロケ地 → {location_label}",
                        q['source'],
                    ))
                    new_connections += 1

        db.commit()
        total_new_entities += new_entities
        total_connections += new_connections
        print(f"  New entities: {new_entities}, New connections: {new_connections}", flush=True)
        time.sleep(5)  # Rate limit between queries

    # ── Additional: Japanese-specific anime pilgrimage locations ──
    print("\n=== Additional: anime/manga with Japanese setting (broader) ===", flush=True)

    broader_queries = [
        # Anime series with setting in Japanese prefectures
        """
SELECT DISTINCT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P131* ?pref .
  ?pref wdt:P31 wd:Q50337 .
  ?location wdt:P625 ?coord .
  { ?work wdt:P31 wd:Q63952888 } UNION { ?work wdt:P31 wd:Q21198342 } UNION { ?work wdt:P31 wd:Q1107 }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 5000
""",
        # Works set in specific famous anime cities
        """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  VALUES ?location { wd:Q131287 wd:Q167770 wd:Q181902 wd:Q209945 wd:Q178336 wd:Q170141 wd:Q131408 wd:Q207753 }
  ?work wdt:P840 ?location .
  ?location wdt:P625 ?coord .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 5000
""",
        # Visual novel set in Japan
        """
SELECT ?work ?workLabel ?location ?locationLabel ?coord WHERE {
  ?work wdt:P840 ?location .
  ?location wdt:P17 wd:Q17 .
  ?location wdt:P625 ?coord .
  ?work wdt:P31/wdt:P279* wd:Q689445 .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "ja,en". }
}
LIMIT 5000
""",
    ]

    for i, bq in enumerate(broader_queries):
        print(f"\n  Broader query {i+1}/{len(broader_queries)}...", flush=True)
        result = run_sparql(bq)
        if not result:
            continue

        bindings = result.get('results', {}).get('bindings', [])
        print(f"  Results: {len(bindings)}", flush=True)
        new_conns = 0

        for b in bindings:
            work_label = b.get('workLabel', {}).get('value', '')
            location_label = b.get('locationLabel', {}).get('value', '')
            work_uri = b.get('work', {}).get('value', '')
            location_uri = b.get('location', {}).get('value', '')
            coord_str = b.get('coord', {}).get('value', '')

            if not work_label or not location_label:
                continue

            work_qid = extract_qid(work_uri)
            location_qid = extract_qid(location_uri)
            lat, lon = parse_coord(coord_str)

            # Find or create work
            work_id = None
            if work_qid:
                row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (work_qid,)).fetchone()
                if row:
                    work_id = row[0]
            if not work_id and work_label in existing_labels:
                row = db.execute("SELECT id FROM entities WHERE label_ja = ? LIMIT 1", (work_label,)).fetchone()
                if row:
                    work_id = row[0]
            if work_id is None:
                try:
                    cur = db.execute(
                        "INSERT INTO entities (wikidata_id, label_ja, entity_type, source) VALUES (?, ?, 'work', 'wd_pilgrimage_broad')",
                        (work_qid, work_label),
                    )
                    work_id = cur.lastrowid
                    existing_labels.add(work_label)
                    if work_qid:
                        existing_wikidata.add(work_qid)
                    total_new_entities += 1
                except sqlite3.IntegrityError:
                    row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (work_qid,)).fetchone()
                    if row:
                        work_id = row[0]

            # Find or create location
            location_id = None
            if location_qid:
                row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (location_qid,)).fetchone()
                if row:
                    location_id = row[0]
            if not location_id and location_label in existing_labels:
                row = db.execute("SELECT id FROM entities WHERE label_ja = ? LIMIT 1", (location_label,)).fetchone()
                if row:
                    location_id = row[0]
            if location_id is None:
                try:
                    cur = db.execute(
                        "INSERT INTO entities (wikidata_id, label_ja, entity_type, source, lat, lon) VALUES (?, ?, 'place', 'wd_pilgrimage_broad', ?, ?)",
                        (location_qid, location_label, lat, lon),
                    )
                    location_id = cur.lastrowid
                    existing_labels.add(location_label)
                    if location_qid:
                        existing_wikidata.add(location_qid)
                    total_new_entities += 1
                except sqlite3.IntegrityError:
                    row = db.execute("SELECT id FROM entities WHERE wikidata_id = ?", (location_qid,)).fetchone()
                    if row:
                        location_id = row[0]

            if work_id and location_id:
                exists = db.execute(
                    "SELECT 1 FROM connections WHERE (entity_a_id=? AND entity_b_id=?) OR (entity_a_id=? AND entity_b_id=?)",
                    (work_id, location_id, location_id, work_id),
                ).fetchone()
                if not exists:
                    db.execute("""
                        INSERT INTO connections
                        (entity_a_id, entity_b_id, connection_type, serendipity_score,
                         explanation, source, confidence, llm_verdict)
                        VALUES (?, ?, 'pilgrimage_narrative', 0.8, ?, 'wd_pilgrimage_broad', 0.9, 'keep')
                    """, (work_id, location_id, f"聖地巡礼: 「{work_label}」→ {location_label}"))
                    new_conns += 1

        db.commit()
        total_connections += new_conns
        print(f"  New connections: {new_conns}", flush=True)
        time.sleep(10)

    # ── Summary ──
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    pilgrimage_conns = db.execute(
        "SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'"
    ).fetchone()[0]
    total_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print(f"=== Pilgrimage Data Import Complete ===", flush=True)
    print(f"New entities: {total_new_entities:,}", flush=True)
    print(f"New pilgrimage connections: {total_connections:,}", flush=True)
    print(f"Total pilgrimage connections: {pilgrimage_conns:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    print(f"Total connections: {total_conns:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
