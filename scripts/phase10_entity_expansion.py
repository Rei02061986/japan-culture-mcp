"""
Phase 10 Task #66: エンティティ数を2.5M以上に押し上げ
JapanSearch keyword queries + Wikidata additional categories
Target: 300,000+ new entities
"""
import requests
import time
import sqlite3

DB_PATH = "ontology/culture_ontology.db"
JPSEARCH_SPARQL = "https://jpsearch.go.jp/rdf/sparql"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"


def sparql_jpsearch(query, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.post(
                JPSEARCH_SPARQL,
                data={'query': query},
                headers={
                    'Accept': 'application/sparql-results+json',
                    'User-Agent': 'japan-culture-mcp/0.6',
                },
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (503, 500):
                time.sleep(30 * (attempt + 1))
            else:
                print(f"  JPS HTTP {resp.status_code}", flush=True)
                time.sleep(15)
        except Exception as e:
            print(f"  JPS Error: {e}", flush=True)
            time.sleep(15)
    return None


def sparql_wikidata(query, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={'query': query, 'format': 'json'},
                headers={
                    'User-Agent': 'japan-culture-mcp/0.6 (teddykmk@gmail.com)',
                    'Accept': 'application/sparql-results+json',
                },
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                time.sleep(60 * (attempt + 1))
            else:
                print(f"  WD HTTP {resp.status_code}", flush=True)
                time.sleep(30)
        except Exception as e:
            print(f"  WD Error: {e}", flush=True)
            time.sleep(30)
    return None


# ── JapanSearch keyword-based queries for new entities ──

JPS_KEYWORD_QUERIES = [
    # Deep paginated queries for large uncovered collections
    {
        'name': '和書 (Japanese books)',
        'source': 'jps_washo',
        'entity_type': 'work',
        'tags': [('medium', 'literature', 0.7), ('experience', 'intellectual', 0.7)],
        'query_template': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX jps: <https://jpsearch.go.jp/term/property#>
SELECT ?item ?label WHERE {{
  ?item a <http://purl.org/dc/dcmitype/Text> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
}}
LIMIT 50000 OFFSET {offset}
""",
        'max_pages': 10,
    },
    {
        'name': '美術品 (Art objects)',
        'source': 'jps_art_objects',
        'entity_type': 'artwork',
        'tags': [('experience', 'aesthetic', 0.8), ('medium', 'visual_art', 0.7)],
        'query_template': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?item ?label WHERE {{
  ?item a <http://purl.org/dc/dcmitype/Image> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
}}
LIMIT 50000 OFFSET {offset}
""",
        'max_pages': 6,
    },
    {
        'name': '地図・図面 (Maps & Diagrams)',
        'source': 'jps_maps',
        'entity_type': 'work',
        'tags': [('experience', 'intellectual', 0.7)],
        'query_template': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?item ?label WHERE {{
  ?item a <http://purl.org/dc/dcmitype/StillImage> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
}}
LIMIT 50000 OFFSET {offset}
""",
        'max_pages': 6,
    },
]

# ── Wikidata additional categories ──

WIKIDATA_QUERIES = [
    {
        'name': 'Japanese songs/music',
        'source': 'wd_music_exp',
        'entity_type': 'music',
        'tags': [('medium', 'music', 0.9), ('experience', 'aesthetic', 0.7)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q134556 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese albums',
        'source': 'wd_album',
        'entity_type': 'music',
        'tags': [('medium', 'music', 0.9)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q482994 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese buildings/architecture',
        'source': 'wd_building',
        'entity_type': 'building',
        'tags': [('medium', 'architecture', 0.9), ('experience', 'aesthetic', 0.7)],
        'query': """
SELECT ?item ?label ?coord WHERE {
  ?item wdt:P31/wdt:P279* wd:Q41176 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label .
  OPTIONAL { ?item wdt:P625 ?coord }
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese mountains',
        'source': 'wd_mountain',
        'entity_type': 'place',
        'tags': [('experience', 'physical', 0.8), ('theme', 'nature_communion', 0.7)],
        'query': """
SELECT ?item ?label ?coord WHERE {
  ?item wdt:P31/wdt:P279* wd:Q8502 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label .
  OPTIONAL { ?item wdt:P625 ?coord }
  FILTER(LANG(?label) = "ja")
}
LIMIT 30000
""",
    },
    {
        'name': 'Japanese islands',
        'source': 'wd_island',
        'entity_type': 'place',
        'tags': [('experience', 'physical', 0.7), ('theme', 'nature_communion', 0.7)],
        'query': """
SELECT ?item ?label ?coord WHERE {
  ?item wdt:P31/wdt:P279* wd:Q23442 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label .
  OPTIONAL { ?item wdt:P625 ?coord }
  FILTER(LANG(?label) = "ja")
}
LIMIT 20000
""",
    },
    {
        'name': 'Japanese historical figures',
        'source': 'wd_historical_person',
        'entity_type': 'person',
        'tags': [('experience', 'intellectual', 0.7)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P106/wdt:P279* wd:Q82955 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 30000
""",
    },
    {
        'name': 'Japanese writers',
        'source': 'wd_writer',
        'entity_type': 'person',
        'tags': [('medium', 'literature', 0.8), ('experience', 'intellectual', 0.8)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P106 wd:Q36180 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 30000
""",
    },
    {
        'name': 'Japanese musicians',
        'source': 'wd_musician',
        'entity_type': 'person',
        'tags': [('medium', 'music', 0.8)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P106/wdt:P279* wd:Q639669 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese voice actors',
        'source': 'wd_seiyuu',
        'entity_type': 'person',
        'tags': [('medium', 'anime_tv', 0.8), ('theme', 'otaku_culture', 0.6)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P106 wd:Q622807 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 20000
""",
    },
    {
        'name': 'Japanese anime characters',
        'source': 'wd_anime_char',
        'entity_type': 'character',
        'tags': [('medium', 'anime_tv', 0.8)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P31 wd:Q95074 .
  ?item wdt:P1080/wdt:P495 wd:Q17 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 30000
""",
    },
    {
        'name': 'Japanese organizations/NGOs',
        'source': 'wd_org',
        'entity_type': 'organization',
        'tags': [('experience', 'social', 0.6)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q43229 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
]

import re


def parse_coord(coord_str):
    if not coord_str:
        return None, None
    m = re.search(r'Point\(([-\d.]+)\s+([-\d.]+)\)', coord_str)
    if m:
        return float(m.group(2)), float(m.group(1))
    return None, None


def main():
    db = sqlite3.connect(DB_PATH)

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    grand_total = 0

    # ── Part 1: JapanSearch paginated queries ──
    print("\n=== Part 1: JapanSearch Expansion ===", flush=True)

    for jq in JPS_KEYWORD_QUERIES:
        print(f"\n--- {jq['name']} ---", flush=True)
        new_count = 0

        for page in range(jq['max_pages']):
            offset = page * 50000
            query = jq['query_template'].format(offset=offset)

            result = sparql_jpsearch(query)
            if not result:
                print(f"  Page {page+1}: failed", flush=True)
                break

            bindings = result.get('results', {}).get('bindings', [])
            if not bindings:
                print(f"  Page {page+1}: no results, done", flush=True)
                break

            page_new = 0
            for b in bindings:
                label = b.get('label', {}).get('value', '').strip()
                if not label or len(label) < 2 or len(label) > 300 or label in existing_labels:
                    continue

                db.execute(
                    "INSERT INTO entities (label_ja, entity_type, source) VALUES (?, ?, ?)",
                    (label, jq['entity_type'], jq['source']),
                )
                existing_labels.add(label)
                page_new += 1

            new_count += page_new
            print(f"  Page {page+1}: {len(bindings)} results, {page_new} new (total: {new_count})", flush=True)

            if page_new == 0 and page > 0:
                print(f"  No new entities, stopping pagination", flush=True)
                break

            db.commit()
            time.sleep(5)

        # Tag new entities
        if new_count > 0:
            for axis, value, conf in jq['tags']:
                db.execute(f"""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    SELECT id, '{axis}', '{value}', '{jq['source']}', {conf}
                    FROM entities WHERE source = '{jq['source']}'
                    AND id NOT IN (SELECT entity_id FROM entity_tags WHERE axis='{axis}' AND source='{jq['source']}')
                """)
            db.commit()

        grand_total += new_count
        print(f"  {jq['name']}: {new_count:,} new (running: {grand_total:,})", flush=True)

    # ── Part 2: Wikidata additional categories ──
    print("\n\n=== Part 2: Wikidata Expansion ===", flush=True)

    for wq in WIKIDATA_QUERIES:
        print(f"\n--- {wq['name']} ---", flush=True)

        result = sparql_wikidata(wq['query'])
        if not result:
            print("  Failed to fetch", flush=True)
            continue

        bindings = result.get('results', {}).get('bindings', [])
        print(f"  Results: {len(bindings)}", flush=True)

        new_count = 0
        for b in bindings:
            label = b.get('label', {}).get('value', '').strip()
            if not label or len(label) < 2 or len(label) > 300 or label in existing_labels:
                continue

            qid = b.get('item', {}).get('value', '').split('/')[-1] if 'item' in b else None
            coord_str = b.get('coord', {}).get('value', '') if 'coord' in b else ''
            lat, lon = parse_coord(coord_str)

            try:
                db.execute(
                    "INSERT INTO entities (wikidata_id, label_ja, entity_type, source, lat, lon) VALUES (?, ?, ?, ?, ?, ?)",
                    (qid, label, wq['entity_type'], wq['source'], lat, lon),
                )
                existing_labels.add(label)
                new_count += 1
            except sqlite3.IntegrityError:
                continue

        # Tag
        if new_count > 0:
            for axis, value, conf in wq['tags']:
                db.execute(f"""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    SELECT id, '{axis}', '{value}', '{wq['source']}', {conf}
                    FROM entities WHERE source = '{wq['source']}'
                    AND id NOT IN (SELECT entity_id FROM entity_tags WHERE axis='{axis}' AND source='{wq['source']}')
                """)
            db.commit()

        grand_total += new_count
        print(f"  {wq['name']}: {new_count:,} new (running: {grand_total:,})", flush=True)
        time.sleep(5)

    # ── Summary ──
    total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    sources = db.execute("SELECT COUNT(DISTINCT source) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== Entity Expansion Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total:,}", flush=True)
    print(f"Total sources: {sources}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
