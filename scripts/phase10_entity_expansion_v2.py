"""
Phase 10 Task #66 v2: エンティティ数を2.5M以上に押し上げ
Wikidata + JapanSearch broader queries
Target: 300,000+ new entities
"""
import requests
import time
import sqlite3
import re

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
JPSEARCH_SPARQL = "https://jpsearch.go.jp/rdf/sparql"


def sparql_wikidata(query, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={'query': query, 'format': 'json'},
                headers={
                    'User-Agent': 'japan-culture-mcp/0.7 (teddykmk@gmail.com)',
                    'Accept': 'application/sparql-results+json',
                },
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
            elif resp.status_code == 504:
                print(f"  Timeout (504), retrying...", flush=True)
                time.sleep(30)
            else:
                print(f"  HTTP {resp.status_code}", flush=True)
                time.sleep(30)
        except Exception as e:
            print(f"  Error: {e}", flush=True)
            time.sleep(30)
    return None


def sparql_jpsearch(query, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.post(
                JPSEARCH_SPARQL,
                data={'query': query},
                headers={
                    'Accept': 'application/sparql-results+json',
                    'User-Agent': 'japan-culture-mcp/0.7',
                },
                timeout=120,
            )
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code in (503, 500, 400):
                print(f"  JPS HTTP {resp.status_code}", flush=True)
                time.sleep(30 * (attempt + 1))
            else:
                print(f"  JPS HTTP {resp.status_code}", flush=True)
                time.sleep(15)
        except Exception as e:
            print(f"  JPS Error: {e}", flush=True)
            time.sleep(15)
    return None


def parse_coord(coord_str):
    if not coord_str:
        return None, None
    m = re.search(r'Point\(([-\d.]+)\s+([-\d.]+)\)', coord_str)
    if m:
        return float(m.group(2)), float(m.group(1))
    return None, None


# ── Wikidata queries ──

WIKIDATA_QUERIES = [
    {
        'name': 'Japanese songs',
        'source': 'wd_song_exp',
        'entity_type': 'music',
        'tags': [('medium', 'music', 0.9)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P31 wd:Q134556 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese albums',
        'source': 'wd_album_exp',
        'entity_type': 'music',
        'tags': [('medium', 'music', 0.9)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P31 wd:Q482994 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese buildings',
        'source': 'wd_building_exp',
        'entity_type': 'building',
        'tags': [('medium', 'architecture', 0.9)],
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
        'source': 'wd_mountain_exp',
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.8), ('experience', 'physical', 0.7)],
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
        'source': 'wd_island_exp',
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.7)],
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
        'name': 'Japanese writers',
        'source': 'wd_writer_exp',
        'entity_type': 'person',
        'tags': [('medium', 'literature', 0.8)],
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
        'source': 'wd_musician_exp',
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
        'source': 'wd_seiyuu_exp',
        'entity_type': 'person',
        'tags': [('medium', 'anime_tv', 0.8)],
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
        'name': 'Japanese organizations',
        'source': 'wd_org_exp',
        'entity_type': 'organization',
        'tags': [('experience', 'social', 0.6)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P17 wd:Q17 .
  ?item wdt:P31/wdt:P279* wd:Q43229 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese actors',
        'source': 'wd_actor_exp',
        'entity_type': 'person',
        'tags': [('medium', 'film', 0.7)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P106 wd:Q33999 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 30000
""",
    },
    {
        'name': 'Japanese manga characters',
        'source': 'wd_manga_char',
        'entity_type': 'character',
        'tags': [('medium', 'manga', 0.9)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P31 wd:Q1114461 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese anime characters',
        'source': 'wd_anime_char_exp',
        'entity_type': 'character',
        'tags': [('medium', 'anime_tv', 0.9)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P31 wd:Q95074 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese educational institutions',
        'source': 'wd_education',
        'entity_type': 'organization',
        'tags': [('experience', 'intellectual', 0.7)],
        'query': """
SELECT ?item ?label ?coord WHERE {
  ?item wdt:P17 wd:Q17 .
  ?item wdt:P31/wdt:P279* wd:Q2385804 .
  ?item rdfs:label ?label .
  OPTIONAL { ?item wdt:P625 ?coord }
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese TV programs',
        'source': 'wd_tvprog_exp',
        'entity_type': 'tv',
        'tags': [('medium', 'tv_drama', 0.7)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P495 wd:Q17 .
  ?item wdt:P31/wdt:P279* wd:Q15416 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese books (published works)',
        'source': 'wd_book_exp',
        'entity_type': 'work',
        'tags': [('medium', 'literature', 0.8)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P495 wd:Q17 .
  ?item wdt:P31 wd:Q571 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese newspapers/magazines',
        'source': 'wd_periodical',
        'entity_type': 'work',
        'tags': [('medium', 'literature', 0.6)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P495 wd:Q17 .
  { ?item wdt:P31 wd:Q1002697 } UNION { ?item wdt:P31 wd:Q41298 }
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 30000
""",
    },
    {
        'name': 'Japanese cultural heritage sites',
        'source': 'wd_heritage_exp',
        'entity_type': 'cultural_property',
        'tags': [('experience', 'aesthetic', 0.8)],
        'query': """
SELECT ?item ?label ?coord WHERE {
  ?item wdt:P17 wd:Q17 .
  { ?item wdt:P1435 ?heritage }
  ?item rdfs:label ?label .
  OPTIONAL { ?item wdt:P625 ?coord }
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
    {
        'name': 'Japanese rivers',
        'source': 'wd_river_exp',
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.7)],
        'query': """
SELECT ?item ?label ?coord WHERE {
  ?item wdt:P17 wd:Q17 .
  ?item wdt:P31/wdt:P279* wd:Q4022 .
  ?item rdfs:label ?label .
  OPTIONAL { ?item wdt:P625 ?coord }
  FILTER(LANG(?label) = "ja")
}
LIMIT 30000
""",
    },
    {
        'name': 'Japanese sports athletes',
        'source': 'wd_athlete_exp',
        'entity_type': 'person',
        'tags': [('experience', 'physical', 0.8)],
        'query': """
SELECT ?item ?label WHERE {
  ?item wdt:P27 wd:Q17 .
  ?item wdt:P31 wd:Q5 .
  ?item wdt:P106/wdt:P279* wd:Q2066131 .
  ?item rdfs:label ?label .
  FILTER(LANG(?label) = "ja")
}
LIMIT 50000
""",
    },
]

# ── JapanSearch broader queries ──

JPS_QUERIES = [
    {
        'name': 'JapanSearch - all items batch 1',
        'source': 'jps_broad1',
        'entity_type': 'work',
        'tags': [('experience', 'intellectual', 0.6)],
        'query': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
PREFIX jps: <https://jpsearch.go.jp/term/property#>
SELECT ?item ?label WHERE {
  ?item rdfs:label ?label .
  ?item schema:datePublished ?date .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
  FILTER(STRLEN(?label) >= 2 && STRLEN(?label) <= 200)
}
LIMIT 50000 OFFSET 2000000
""",
    },
    {
        'name': 'JapanSearch - all items batch 2',
        'source': 'jps_broad2',
        'entity_type': 'work',
        'tags': [('experience', 'intellectual', 0.6)],
        'query': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?item ?label WHERE {
  ?item rdfs:label ?label .
  ?item schema:datePublished ?date .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
  FILTER(STRLEN(?label) >= 2 && STRLEN(?label) <= 200)
}
LIMIT 50000 OFFSET 2050000
""",
    },
    {
        'name': 'JapanSearch - all items batch 3',
        'source': 'jps_broad3',
        'entity_type': 'work',
        'tags': [('experience', 'intellectual', 0.6)],
        'query': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?item ?label WHERE {
  ?item rdfs:label ?label .
  ?item schema:datePublished ?date .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
  FILTER(STRLEN(?label) >= 2 && STRLEN(?label) <= 200)
}
LIMIT 50000 OFFSET 2100000
""",
    },
    {
        'name': 'JapanSearch - all items batch 4',
        'source': 'jps_broad4',
        'entity_type': 'work',
        'tags': [('experience', 'intellectual', 0.6)],
        'query': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?item ?label WHERE {
  ?item rdfs:label ?label .
  ?item schema:datePublished ?date .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
  FILTER(STRLEN(?label) >= 2 && STRLEN(?label) <= 200)
}
LIMIT 50000 OFFSET 2150000
""",
    },
    {
        'name': 'JapanSearch - all items batch 5',
        'source': 'jps_broad5',
        'entity_type': 'work',
        'tags': [('experience', 'intellectual', 0.6)],
        'query': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?item ?label WHERE {
  ?item rdfs:label ?label .
  ?item schema:datePublished ?date .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
  FILTER(STRLEN(?label) >= 2 && STRLEN(?label) <= 200)
}
LIMIT 50000 OFFSET 2200000
""",
    },
    {
        'name': 'JapanSearch - all items batch 6',
        'source': 'jps_broad6',
        'entity_type': 'work',
        'tags': [('experience', 'intellectual', 0.6)],
        'query': """
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX schema: <http://schema.org/>
SELECT ?item ?label WHERE {
  ?item rdfs:label ?label .
  ?item schema:datePublished ?date .
  FILTER(LANG(?label) = "ja" || LANG(?label) = "")
  FILTER(STRLEN(?label) >= 2 && STRLEN(?label) <= 200)
}
LIMIT 50000 OFFSET 2250000
""",
    },
]


def main():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    existing_wikidata = set()
    for row in db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL"):
        existing_wikidata.add(row[0])
    print(f"Existing wikidata IDs: {len(existing_wikidata):,}", flush=True)

    grand_total = 0

    # ── Part 1: Wikidata ──
    print("\n=== Part 1: Wikidata Expansion ===", flush=True)

    for wq in WIKIDATA_QUERIES:
        print(f"\n--- {wq['name']} ---", flush=True)

        result = sparql_wikidata(wq['query'])
        if not result:
            print("  Failed to fetch", flush=True)
            continue

        bindings = result.get('results', {}).get('bindings', [])
        print(f"  Results: {len(bindings)}", flush=True)

        new_count = 0
        batch = []
        for b in bindings:
            label = b.get('label', {}).get('value', '').strip()
            if not label or len(label) < 2 or len(label) > 300 or label in existing_labels:
                continue

            qid = b.get('item', {}).get('value', '').split('/')[-1] if 'item' in b else None
            if qid and qid in existing_wikidata:
                continue

            coord_str = b.get('coord', {}).get('value', '') if 'coord' in b else ''
            lat, lon = parse_coord(coord_str)

            batch.append((qid, label, wq['entity_type'], wq['source'], lat, lon))
            existing_labels.add(label)
            if qid:
                existing_wikidata.add(qid)
            new_count += 1

        # Batch insert
        for item in batch:
            try:
                db.execute(
                    "INSERT INTO entities (wikidata_id, label_ja, entity_type, source, lat, lon) VALUES (?, ?, ?, ?, ?, ?)",
                    item,
                )
            except sqlite3.IntegrityError:
                continue

        # Tag
        if new_count > 0:
            for axis, value, conf in wq['tags']:
                db.execute(f"""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    SELECT id, '{axis}', '{value}', '{wq['source']}', {conf}
                    FROM entities WHERE source = '{wq['source']}'
                    AND id NOT IN (SELECT entity_id FROM entity_tags WHERE axis='{axis}' AND source='{wq['source']}')
                """)
            db.commit()

        grand_total += new_count
        print(f"  {wq['name']}: {new_count:,} new (running: {grand_total:,})", flush=True)
        time.sleep(5)

    # ── Part 2: JapanSearch broad queries ──
    print("\n\n=== Part 2: JapanSearch Broad Expansion ===", flush=True)

    for jq in JPS_QUERIES:
        print(f"\n--- {jq['name']} ---", flush=True)

        result = sparql_jpsearch(jq['query'])
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

            try:
                db.execute(
                    "INSERT INTO entities (label_ja, entity_type, source) VALUES (?, ?, ?)",
                    (label, jq['entity_type'], jq['source']),
                )
                existing_labels.add(label)
                new_count += 1
            except sqlite3.IntegrityError:
                continue

        if new_count > 0:
            for axis, value, conf in jq['tags']:
                db.execute(f"""
                    INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    SELECT id, '{axis}', '{value}', '{jq['source']}', {conf}
                    FROM entities WHERE source = '{jq['source']}'
                    AND id NOT IN (SELECT entity_id FROM entity_tags WHERE axis='{axis}' AND source='{jq['source']}')
                """)
            db.commit()

        grand_total += new_count
        print(f"  {jq['name']}: {new_count:,} new (running: {grand_total:,})", flush=True)
        time.sleep(5)

    # ── Summary ──
    total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    sources = db.execute("SELECT COUNT(DISTINCT source) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== Entity Expansion v2 Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total:,}", flush=True)
    print(f"Total sources: {sources}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
