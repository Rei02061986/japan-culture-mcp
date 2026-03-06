"""
Phase 10E: JapanSearch SPARQL wave 3 — large specimen & newspaper collections.
Target: 3,000,000+ new entities.
"""
import requests
import time
import sqlite3

DB_PATH = "ontology/culture_ontology.db"
ENDPOINT = "https://jpsearch.go.jp/rdf/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.5',
    'Content-Type': 'application/x-www-form-urlencoded',
}

TYPE_CONFIG = {
    '新聞': {
        'entity_type': 'work',
        'tags': [('medium', 'literature', 0.5), ('experience', 'intellectual', 0.7)],
        'limit_total': 310000,
        'source': 'jps_phase10w3',
    },
    '写真': {
        'entity_type': 'artifact',
        'tags': [('experience', 'aesthetic', 0.6)],
        'limit_total': 130000,
        'source': 'jps_phase10w3',
    },
    '動物標本': {
        'entity_type': 'artifact',
        'tags': [('theme', 'nature_communion', 0.9), ('experience', 'intellectual', 0.9)],
        'limit_total': 1500000,
        'source': 'jps_animal',
    },
    '植物標本': {
        'entity_type': 'artifact',
        'tags': [('theme', 'nature_communion', 0.9), ('experience', 'intellectual', 0.9)],
        'limit_total': 1500000,
        'source': 'jps_plant',
    },
}


def sparql_fetch(query, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.post(ENDPOINT, data={'query': query}, headers=HEADERS, timeout=300)
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            elif resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
            elif resp.status_code >= 500:
                print(f"    HTTP {resp.status_code}, retry {attempt+1}...", flush=True)
                time.sleep(30 * (attempt + 1))
            else:
                print(f"    HTTP {resp.status_code}", flush=True)
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
    page_size = 10000

    for type_name, config in TYPE_CONFIG.items():
        type_uri = f"https://jpsearch.go.jp/term/type/{type_name}"
        limit_total = config['limit_total']
        source = config['source']
        print(f"\n{'='*60}", flush=True)
        print(f"=== {type_name} (limit: {limit_total:,}) ===", flush=True)

        offset = 0
        type_new = 0

        while offset < limit_total:
            query = f"""SELECT ?item ?label WHERE {{
  ?item a <{type_uri}> ;
        rdfs:label ?label .
}} LIMIT {page_size} OFFSET {offset}"""

            bindings = sparql_fetch(query)
            if not bindings:
                print(f"  No results at offset {offset}, moving on", flush=True)
                break

            print(f"  offset={offset:,}, got={len(bindings):,}", flush=True)

            batch_new = 0
            for b in bindings:
                label = b.get('label', {}).get('value', '').strip()
                if not label or len(label) < 2 or len(label) > 300:
                    continue
                if label in existing_labels:
                    continue

                cur = db.execute("""
                    INSERT INTO entities (label_ja, entity_type, source)
                    VALUES (?, ?, ?)
                """, (label, config['entity_type'], source))
                eid = cur.lastrowid

                for axis, value_code, confidence in config['tags']:
                    db.execute("""
                        INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, ?, ?, ?, ?)
                    """, (eid, axis, value_code, source, confidence))

                existing_labels.add(label)
                batch_new += 1
                type_new += 1

            if batch_new > 0 and offset % 50000 == 0:
                db.commit()

            if len(bindings) < page_size:
                break

            offset += page_size
            time.sleep(3)

        db.commit()
        grand_total += type_new
        print(f"  {type_name}: {type_new:,} new (running total: {grand_total:,})", flush=True)

    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== JapanSearch Wave 3 Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
