"""
Phase 9 Stream C: JapanSearch SPARQL import.
Fetch additional cultural items from Japan Search (ジャパンサーチ).
Target: 20,000+ new entities from 264+ cultural institutions.
"""
import requests
import time
import sqlite3
import re

DB_PATH = "ontology/culture_ontology.db"
JAPAN_SEARCH_SPARQL = "https://jpsearch.go.jp/rdf/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.4'
}

# Query categories with SPARQL
QUERIES = {
    'ukiyoe': {
        'query': """SELECT DISTINCT ?item ?label ?creator ?creatorLabel WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "浮世絵") || CONTAINS(?genre, "錦絵"))
  ?item rdfs:label ?label .
  OPTIONAL {{ ?item schema:creator/rdfs:label ?creatorLabel }}
  OPTIONAL {{ ?item schema:creator ?creator }}
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'ukiyoe', 0.9), ('theme', 'everyday_beauty', 0.6),
                 ('experience', 'aesthetic', 0.8)],
    },
    'paintings': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "絵画") || CONTAINS(?genre, "日本画"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'painting', 0.9), ('experience', 'aesthetic', 0.9)],
    },
    'ceramics': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "陶磁") || CONTAINS(?genre, "焼物") || CONTAINS(?genre, "陶器"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'craft', 0.9), ('theme', 'craft_mastery', 0.8),
                 ('experience', 'aesthetic', 0.8)],
    },
    'textiles': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "染織") || CONTAINS(?genre, "着物") || CONTAINS(?genre, "織物"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'craft', 0.9), ('theme', 'craft_mastery', 0.8),
                 ('theme', 'everyday_beauty', 0.6), ('experience', 'aesthetic', 0.7)],
    },
    'sculptures': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "彫刻") || CONTAINS(?genre, "仏像"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'sculpture', 0.9), ('theme', 'sacred_profane', 0.6),
                 ('experience', 'aesthetic', 0.9)],
    },
    'historical_docs': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "古文書") || CONTAINS(?genre, "文書") || CONTAINS(?genre, "記録"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'literature', 0.7), ('theme', 'community_tradition', 0.6),
                 ('experience', 'intellectual', 0.8)],
    },
    'maps_old': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "地図") || CONTAINS(?genre, "絵図"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('theme', 'journey_boundary', 0.7), ('experience', 'intellectual', 0.8)],
    },
    'musical_instruments': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "楽器") || CONTAINS(?genre, "琴") || CONTAINS(?genre, "笛") || CONTAINS(?genre, "太鼓"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'music', 0.8), ('theme', 'music_performance', 0.8),
                 ('experience', 'aesthetic', 0.7)],
    },
    'lacquerware': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "漆工") || CONTAINS(?genre, "蒔絵") || CONTAINS(?genre, "漆器"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'craft', 0.9), ('theme', 'craft_mastery', 0.9),
                 ('experience', 'aesthetic', 0.8)],
    },
    'metalwork': {
        'query': """SELECT DISTINCT ?item ?label WHERE {{
  ?item schema:about/rdfs:label ?genre .
  FILTER(CONTAINS(?genre, "金工") || CONTAINS(?genre, "刀剣") || CONTAINS(?genre, "甲冑"))
  ?item rdfs:label ?label .
}} LIMIT 2000 OFFSET {offset}""",
        'entity_type': 'artifact',
        'tags': [('medium', 'craft', 0.9), ('theme', 'samurai', 0.6),
                 ('theme', 'craft_mastery', 0.8), ('experience', 'aesthetic', 0.7)],
    },
}


def sparql_fetch(query_template, offset=0):
    q = query_template.format(offset=offset)
    for attempt in range(3):
        try:
            resp = requests.post(
                JAPAN_SEARCH_SPARQL,
                data={'query': q},
                headers={**HEADERS, 'Content-Type': 'application/x-www-form-urlencoded'},
                timeout=120
            )
            if resp.status_code == 429:
                time.sleep(60 * (attempt + 1))
                continue
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            print(f"    HTTP {resp.status_code}", flush=True)
            if resp.status_code >= 500:
                time.sleep(30)
            else:
                time.sleep(10)
        except Exception as e:
            print(f"    ERROR: {e}", flush=True)
            time.sleep(30)
    return []


def main():
    db = sqlite3.connect(DB_PATH)

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    total_new = 0

    for cat_name, config in QUERIES.items():
        print(f"\n=== {cat_name} ===", flush=True)
        all_bindings = []
        offset = 0

        while True:
            bindings = sparql_fetch(config['query'], offset)
            if not bindings:
                break
            all_bindings.extend(bindings)
            print(f"  offset={offset}, got={len(bindings)}, total={len(all_bindings)}", flush=True)
            if len(bindings) < 2000:
                break
            offset += 2000
            time.sleep(5)

        cat_new = 0
        for b in all_bindings:
            label = b.get('label', {}).get('value', '')
            if not label:
                continue

            # Clean label
            label = label.strip()
            if len(label) < 2 or len(label) > 200:
                continue

            if label in existing_labels:
                continue

            cur = db.execute("""
                INSERT INTO entities (label_ja, entity_type, source)
                VALUES (?, ?, 'jps_phase9')
            """, (label, config['entity_type']))
            eid = cur.lastrowid

            for axis, value_code, confidence in config['tags']:
                db.execute("""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, ?, ?, 'jps_phase9', ?)
                """, (eid, axis, value_code, confidence))

            existing_labels.add(label)
            cat_new += 1

        db.commit()
        total_new += cat_new
        print(f"  New entities: {cat_new:,} (running total: {total_new:,})", flush=True)

    # Romanize
    print("\n=== Adding English labels ===", flush=True)
    try:
        import pykakasi
        kks = pykakasi.kakasi()
        missing = db.execute("""
            SELECT id, label_ja FROM entities
            WHERE source = 'jps_phase9' AND label_en IS NULL AND label_ja IS NOT NULL
        """).fetchall()

        updated = 0
        for eid, label_ja in missing:
            try:
                result = kks.convert(label_ja)
                romaji = ' '.join(item['hepburn'] for item in result)
                if romaji and romaji != label_ja:
                    db.execute("UPDATE entities SET label_en = ? WHERE id = ?", (romaji, eid))
                    updated += 1
            except:
                pass

        db.commit()
        print(f"  Romanized: {updated:,}", flush=True)
    except ImportError:
        print("  pykakasi not available", flush=True)

    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n=== JapanSearch Import Complete ===", flush=True)
    print(f"New entities: {total_new:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
