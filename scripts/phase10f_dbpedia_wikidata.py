"""
Phase 10F: DBpedia Japanese + Additional Wikidata categories.
Target: 200,000+ new entities from diverse categories for source diversity.
"""
import requests
import time
import sqlite3
import re

DB_PATH = "ontology/culture_ontology.db"

WIKIDATA_URL = "https://query.wikidata.org/sparql"
DBPEDIA_URL = "https://ja.dbpedia.org/sparql"
HEADERS_WD = {'Accept': 'application/sparql-results+json', 'User-Agent': 'japan-culture-mcp/0.5'}
HEADERS_DB = {'Accept': 'application/sparql-results+json', 'User-Agent': 'japan-culture-mcp/0.5'}

# Wikidata categories to add
WIKIDATA_QUERIES = [
    {
        'name': 'Japanese_foods',
        'source': 'wd_food',
        'entity_type': 'tradition',
        'tags': [('theme', 'food_drink', 0.9), ('experience', 'social', 0.7)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q2095 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 10000''',
    },
    {
        'name': 'Japanese_companies',
        'source': 'wd_company',
        'entity_type': 'organization',
        'tags': [('experience', 'social', 0.6)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q4830453 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'Japanese_train_stations',
        'source': 'wd_station',
        'entity_type': 'place',
        'tags': [('theme', 'journey_boundary', 0.7), ('experience', 'physical', 0.5)],
        'query': '''SELECT ?item ?label ?lat ?lon WHERE {
  ?item wdt:P31/wdt:P279* wd:Q928830 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
  OPTIONAL { ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat)
    BIND(geof:longitude(?coord) AS ?lon)
  }
} LIMIT 20000''',
    },
    {
        'name': 'Japanese_rivers',
        'source': 'wd_river',
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.8), ('experience', 'aesthetic', 0.6)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q4022 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 10000''',
    },
    {
        'name': 'Japanese_parks',
        'source': 'wd_park',
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.8), ('theme', 'seasonal_beauty', 0.6), ('experience', 'aesthetic', 0.7)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q22698 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 10000''',
    },
    {
        'name': 'Japanese_schools',
        'source': 'wd_school',
        'entity_type': 'organization',
        'tags': [('experience', 'intellectual', 0.8)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q3914 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'Japanese_museums',
        'source': 'wd_museum',
        'entity_type': 'place',
        'tags': [('theme', 'visual_arts', 0.7), ('experience', 'aesthetic', 0.8), ('experience', 'intellectual', 0.7)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q33506 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 10000''',
    },
    {
        'name': 'Japanese_films',
        'source': 'wd_film',
        'entity_type': 'work',
        'tags': [('medium', 'anime', 0.3), ('experience', 'aesthetic', 0.7)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31 wd:Q11424 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'Japanese_songs',
        'source': 'wd_song',
        'entity_type': 'work',
        'tags': [('medium', 'music', 0.9), ('experience', 'aesthetic', 0.8)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q7366 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'Japanese_TV_programs',
        'source': 'wd_tv',
        'entity_type': 'work',
        'tags': [('medium', 'anime_tv', 0.3), ('experience', 'social', 0.6)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q15416 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'Japanese_video_games',
        'source': 'wd_game',
        'entity_type': 'work',
        'tags': [('medium', 'game', 0.9), ('theme', 'game_culture', 0.7), ('experience', 'physical', 0.6)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q7889 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'Japanese_light_novels',
        'source': 'wd_lightnovel',
        'entity_type': 'work',
        'tags': [('medium', 'literature', 0.8), ('theme', 'literary_arts', 0.7), ('experience', 'intellectual', 0.7)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q747381 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 20000''',
    },
]

# DBpedia Japanese categories
DBPEDIA_QUERIES = [
    {
        'name': 'dbp_places',
        'source': 'dbpedia_place',
        'entity_type': 'place',
        'tags': [('experience', 'aesthetic', 0.5)],
        'query': '''SELECT ?item ?label WHERE {
  ?item a <http://dbpedia.org/ontology/Place> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'dbp_works',
        'source': 'dbpedia_work',
        'entity_type': 'work',
        'tags': [('experience', 'intellectual', 0.6)],
        'query': '''SELECT ?item ?label WHERE {
  ?item a <http://dbpedia.org/ontology/Work> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'dbp_persons',
        'source': 'dbpedia_person',
        'entity_type': 'person',
        'tags': [],
        'query': '''SELECT ?item ?label WHERE {
  ?item a <http://dbpedia.org/ontology/Person> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label)="ja")
} LIMIT 100000''',
    },
    {
        'name': 'dbp_buildings',
        'source': 'dbpedia_building',
        'entity_type': 'place',
        'tags': [('medium', 'architecture', 0.7), ('experience', 'aesthetic', 0.6)],
        'query': '''SELECT ?item ?label WHERE {
  ?item a <http://dbpedia.org/ontology/Building> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'dbp_events',
        'source': 'dbpedia_event',
        'entity_type': 'event',
        'tags': [('experience', 'social', 0.7)],
        'query': '''SELECT ?item ?label WHERE {
  ?item a <http://dbpedia.org/ontology/Event> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'dbp_organisms',
        'source': 'dbpedia_organism',
        'entity_type': 'artifact',
        'tags': [('theme', 'nature_communion', 0.8), ('experience', 'intellectual', 0.8)],
        'query': '''SELECT ?item ?label WHERE {
  ?item a <http://dbpedia.org/ontology/Species> .
  ?item rdfs:label ?label .
  FILTER(LANG(?label)="ja")
} LIMIT 100000''',
    },
]


def sparql_fetch(endpoint, query, headers, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(endpoint, params={'query': query}, headers=headers, timeout=120)
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            elif resp.status_code == 429:
                time.sleep(60 * (attempt + 1))
            else:
                print(f"    HTTP {resp.status_code}", flush=True)
                if attempt < retries - 1:
                    time.sleep(30)
                else:
                    return []
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(30)
    return []


def main():
    db = sqlite3.connect(DB_PATH)

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    grand_total = 0

    # Wikidata queries
    print("\n=== Wikidata Additional Categories ===", flush=True)
    for config in WIKIDATA_QUERIES:
        print(f"\n  --- {config['name']} ---", flush=True)
        bindings = sparql_fetch(WIKIDATA_URL, config['query'], HEADERS_WD)
        print(f"  Got {len(bindings):,} results", flush=True)

        new_count = 0
        for b in bindings:
            label = b.get('label', {}).get('value', '').strip()
            if not label or len(label) < 2 or len(label) > 300 or label in existing_labels:
                continue

            lat = float(b['lat']['value']) if b.get('lat') and b['lat'].get('value') else None
            lon = float(b['lon']['value']) if b.get('lon') and b['lon'].get('value') else None

            cur = db.execute("""
                INSERT INTO entities (label_ja, entity_type, source, lat, lon)
                VALUES (?, ?, ?, ?, ?)
            """, (label, config['entity_type'], config['source'], lat, lon))
            eid = cur.lastrowid

            for axis, value_code, confidence in config['tags']:
                db.execute("""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, ?, ?, ?, ?)
                """, (eid, axis, value_code, config['source'], confidence))

            existing_labels.add(label)
            new_count += 1

        db.commit()
        grand_total += new_count
        print(f"  {config['name']}: {new_count:,} new (total: {grand_total:,})", flush=True)
        time.sleep(5)

    # DBpedia queries
    print("\n=== DBpedia Japanese ===", flush=True)
    for config in DBPEDIA_QUERIES:
        print(f"\n  --- {config['name']} ---", flush=True)
        bindings = sparql_fetch(DBPEDIA_URL, config['query'], HEADERS_DB)
        print(f"  Got {len(bindings):,} results", flush=True)

        new_count = 0
        for b in bindings:
            label = b.get('label', {}).get('value', '').strip()
            if not label or len(label) < 2 or len(label) > 300 or label in existing_labels:
                continue

            cur = db.execute("""
                INSERT INTO entities (label_ja, entity_type, source)
                VALUES (?, ?, ?)
            """, (label, config['entity_type'], config['source']))
            eid = cur.lastrowid

            for axis, value_code, confidence in config['tags']:
                db.execute("""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, ?, ?, ?, ?)
                """, (eid, axis, value_code, config['source'], confidence))

            existing_labels.add(label)
            new_count += 1

        db.commit()
        grand_total += new_count
        print(f"  {config['name']}: {new_count:,} new (total: {grand_total:,})", flush=True)
        time.sleep(5)

    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    sources = db.execute("SELECT COUNT(DISTINCT source) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== DBpedia + Wikidata Import Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    print(f"Unique sources: {sources}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
