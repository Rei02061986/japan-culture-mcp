"""
Phase 10G: Wikidata diverse categories for source diversity + entity count.
Target: 100,000+ new entities from 15+ new source categories.
"""
import requests
import time
import sqlite3

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
HEADERS = {'Accept': 'application/sparql-results+json', 'User-Agent': 'japan-culture-mcp/0.5'}

QUERIES = [
    {
        'name': 'Temples',
        'source': 'wd_temple',
        'entity_type': 'place',
        'tags': [('theme', 'sacred_profane', 0.9), ('experience', 'reflective', 0.8)],
        'query': '''SELECT ?item ?label ?lat ?lon WHERE {
  ?item wdt:P31/wdt:P279* wd:Q160742 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
  OPTIONAL { ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat) BIND(geof:longitude(?coord) AS ?lon) }
} LIMIT 50000''',
    },
    {
        'name': 'Bridges',
        'source': 'wd_bridge',
        'entity_type': 'place',
        'tags': [('medium', 'architecture', 0.7), ('theme', 'journey_boundary', 0.6)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q12280 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 20000''',
    },
    {
        'name': 'Lakes',
        'source': 'wd_lake',
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.9), ('experience', 'aesthetic', 0.8)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q23397 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 10000''',
    },
    {
        'name': 'Hot_springs',
        'source': 'wd_onsen',
        'entity_type': 'place',
        'tags': [('theme', 'nature_communion', 0.8), ('experience', 'physical', 0.9)],
        'query': '''SELECT ?item ?label ?lat ?lon WHERE {
  ?item wdt:P31/wdt:P279* wd:Q177380 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
  OPTIONAL { ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat) BIND(geof:longitude(?coord) AS ?lon) }
} LIMIT 10000''',
    },
    {
        'name': 'Castles',
        'source': 'wd_castle',
        'entity_type': 'place',
        'tags': [('theme', 'samurai', 0.8), ('medium', 'architecture', 0.9), ('experience', 'aesthetic', 0.8)],
        'query': '''SELECT ?item ?label ?lat ?lon WHERE {
  ?item wdt:P31/wdt:P279* wd:Q751876 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
  OPTIONAL { ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat) BIND(geof:longitude(?coord) AS ?lon) }
} LIMIT 10000''',
    },
    {
        'name': 'Sports_teams',
        'source': 'wd_sports_team',
        'entity_type': 'organization',
        'tags': [('theme', 'sports', 0.9), ('experience', 'physical', 0.8)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q12973014 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 20000''',
    },
    {
        'name': 'Historical_events',
        'source': 'wd_history',
        'entity_type': 'event',
        'tags': [('experience', 'intellectual', 0.9)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q13418847 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 20000''',
    },
    {
        'name': 'Manga_series',
        'source': 'wd_manga',
        'entity_type': 'work',
        'tags': [('medium', 'manga', 0.9), ('experience', 'aesthetic', 0.7)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q21198342 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'Anime_series',
        'source': 'wd_anime',
        'entity_type': 'work',
        'tags': [('medium', 'anime_tv', 0.9), ('experience', 'aesthetic', 0.7)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q63952888 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 50000''',
    },
    {
        'name': 'Administrative_divs',
        'source': 'wd_admin',
        'entity_type': 'place',
        'tags': [('experience', 'social', 0.5)],
        'query': '''SELECT ?item ?label ?lat ?lon WHERE {
  ?item wdt:P31/wdt:P279* wd:Q1187580 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
  OPTIONAL { ?item wdt:P625 ?coord .
    BIND(geof:latitude(?coord) AS ?lat) BIND(geof:longitude(?coord) AS ?lon) }
} LIMIT 20000''',
    },
    {
        'name': 'Japanese_cuisine',
        'source': 'wd_cuisine',
        'entity_type': 'tradition',
        'tags': [('theme', 'food_drink', 0.9), ('experience', 'social', 0.8)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q746549 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 10000''',
    },
    {
        'name': 'Martial_arts',
        'source': 'wd_martial',
        'entity_type': 'tradition',
        'tags': [('theme', 'martial_arts', 0.9), ('experience', 'physical', 0.9)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q11417 .
  ?item wdt:P495 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 10000''',
    },
    {
        'name': 'Performing_arts',
        'source': 'wd_performing',
        'entity_type': 'tradition',
        'tags': [('theme', 'performing_arts', 0.9), ('experience', 'aesthetic', 0.9)],
        'query': '''SELECT ?item ?label WHERE {
  { ?item wdt:P31/wdt:P279* wd:Q7777570 . }
  UNION
  { ?item wdt:P31/wdt:P279* wd:Q3932621 . ?item wdt:P495 wd:Q17 . }
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 10000''',
    },
    {
        'name': 'Hospitals',
        'source': 'wd_hospital',
        'entity_type': 'place',
        'tags': [('experience', 'social', 0.6)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q16917 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 30000''',
    },
    {
        'name': 'Neighborhoods',
        'source': 'wd_neighborhood',
        'entity_type': 'place',
        'tags': [('theme', 'everyday_beauty', 0.5)],
        'query': '''SELECT ?item ?label WHERE {
  ?item wdt:P31/wdt:P279* wd:Q123705 .
  ?item wdt:P17 wd:Q17 .
  ?item rdfs:label ?label . FILTER(LANG(?label)="ja")
} LIMIT 30000''',
    },
]


def sparql_fetch(query, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(WIKIDATA_URL, params={'query': query}, headers=HEADERS, timeout=120)
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

    for config in QUERIES:
        print(f"\n  --- {config['name']} ---", flush=True)
        bindings = sparql_fetch(config['query'])
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

    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    sources = db.execute("SELECT COUNT(DISTINCT source) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== Wikidata Diverse Import Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    print(f"Unique sources: {sources}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
