"""
Phase 9 Stream A1: Wikidata structural connections.
Extract relationships from Wikidata properties and map to existing entities.
Target: 15,000+ new keep connections.
"""
import requests
import time
import sqlite3
import json
import re

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.3 (contact@example.com)'
}

RELATION_QUERIES = {
    'creator_work': {
        'query': """SELECT ?creator ?creatorLabel ?work ?workLabel WHERE {{
  ?work wdt:P170|wdt:P50 ?creator .
  ?creator wdt:P27 wd:Q17 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'type': 'creator_work',
        'explanation': '作者と作品: {a}は{b}の創作者',
        'score': 0.6,
    },
    'adaptation': {
        'query': """SELECT ?original ?originalLabel ?derivative ?derivativeLabel WHERE {{
  ?derivative wdt:P144 ?original .
  {{ ?derivative wdt:P495 wd:Q17 }} UNION {{ ?original wdt:P495 wd:Q17 }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'type': 'adaptation',
        'explanation': '原作と翻案: {a}から{b}への展開',
        'score': 0.8,
    },
    'studio_work': {
        'query': """SELECT ?studio ?studioLabel ?work ?workLabel WHERE {{
  ?work wdt:P272 ?studio .
  ?studio wdt:P17 wd:Q17 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'type': 'studio_work',
        'explanation': 'スタジオと作品: {a}による{b}の制作',
        'score': 0.5,
    },
    'heritage_location': {
        'query': """SELECT ?heritage ?heritageLabel ?location ?locationLabel WHERE {{
  ?heritage wdt:P1435 ?designation .
  ?heritage wdt:P131 ?location .
  ?heritage wdt:P17 wd:Q17 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'type': 'heritage_location',
        'explanation': '文化財と所在地: {a}は{b}に所在',
        'score': 0.5,
    },
    'work_location': {
        'query': """SELECT ?work ?workLabel ?location ?locationLabel WHERE {{
  ?work wdt:P276 ?location .
  ?location wdt:P17 wd:Q17 .
  ?work wdt:P31/wdt:P279* wd:Q838948 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'type': 'work_location',
        'explanation': '作品の所蔵: {a}は{b}に所蔵',
        'score': 0.5,
    },
    'genre_work': {
        'query': """SELECT ?work ?workLabel ?genre ?genreLabel WHERE {{
  ?work wdt:P136 ?genre .
  ?work wdt:P495 wd:Q17 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'type': 'shared_genre',
        'explanation': 'ジャンルの共有: {a}と{b}は同じジャンルに属する',
        'score': 0.4,
    },
    'based_on': {
        'query': """SELECT ?work ?workLabel ?source ?sourceLabel WHERE {{
  ?work wdt:P144 ?source .
  ?work wdt:P495 wd:Q17 .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}} LIMIT 2000 OFFSET {offset}""",
        'type': 'based_on',
        'explanation': '原作関係: {b}に基づく{a}',
        'score': 0.7,
    },
}


def sparql_fetch(query_template, offset=0):
    q = query_template.format(offset=offset)
    for attempt in range(3):
        try:
            resp = requests.get(WIKIDATA_SPARQL, params={'query': q}, headers=HEADERS, timeout=120)
            if resp.status_code == 429:
                time.sleep(60 * (attempt + 1))
                continue
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            print(f"    HTTP {resp.status_code}", flush=True)
            time.sleep(30)
        except Exception as e:
            print(f"    ERROR: {e}", flush=True)
            time.sleep(30)
    return []


def main():
    db = sqlite3.connect(DB_PATH)

    # Build lookup indices
    print("Building entity indices...", flush=True)
    qid_to_eid = {}
    for row in db.execute("SELECT id, wikidata_id FROM entities WHERE wikidata_id IS NOT NULL"):
        qid_to_eid[row[1]] = row[0]

    label_to_eid = {}
    for row in db.execute("SELECT id, label_ja FROM entities WHERE label_ja IS NOT NULL"):
        if row[1] not in label_to_eid:  # first match wins
            label_to_eid[row[1]] = row[0]

    print(f"  QID index: {len(qid_to_eid):,}", flush=True)
    print(f"  Label index: {len(label_to_eid):,}", flush=True)

    # Load existing connection pairs
    existing_pairs = set()
    for row in db.execute("SELECT entity_a_id, entity_b_id FROM connections"):
        a, b = row
        existing_pairs.add((min(a, b), max(a, b)))
    print(f"  Existing connections: {len(existing_pairs):,}", flush=True)

    total_new = 0

    for rel_name, config in RELATION_QUERIES.items():
        print(f"\n=== {rel_name} ===", flush=True)
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
            time.sleep(10)

        # Process bindings
        rel_new = 0
        # Determine field names based on query
        if rel_name in ('creator_work',):
            a_field, b_field = 'creator', 'work'
            a_label_field, b_label_field = 'creatorLabel', 'workLabel'
        elif rel_name in ('adaptation', 'based_on'):
            a_field, b_field = 'original', 'derivative'
            a_label_field, b_label_field = 'originalLabel', 'derivativeLabel'
            if rel_name == 'based_on':
                a_field, b_field = 'source', 'work'
                a_label_field, b_label_field = 'sourceLabel', 'workLabel'
        elif rel_name == 'studio_work':
            a_field, b_field = 'studio', 'work'
            a_label_field, b_label_field = 'studioLabel', 'workLabel'
        elif rel_name == 'heritage_location':
            a_field, b_field = 'heritage', 'location'
            a_label_field, b_label_field = 'heritageLabel', 'locationLabel'
        elif rel_name == 'work_location':
            a_field, b_field = 'work', 'location'
            a_label_field, b_label_field = 'workLabel', 'locationLabel'
        elif rel_name == 'genre_work':
            a_field, b_field = 'work', 'genre'
            a_label_field, b_label_field = 'workLabel', 'genreLabel'
        else:
            continue

        for binding in all_bindings:
            a_uri = binding.get(a_field, {}).get('value', '')
            b_uri = binding.get(b_field, {}).get('value', '')
            a_label = binding.get(a_label_field, {}).get('value', '')
            b_label = binding.get(b_label_field, {}).get('value', '')

            if not a_label or not b_label:
                continue
            if a_label.startswith('Q') or b_label.startswith('Q'):
                continue

            # Resolve to entity IDs
            a_qid = a_uri.split('/')[-1] if 'wikidata.org' in a_uri else None
            b_qid = b_uri.split('/')[-1] if 'wikidata.org' in b_uri else None

            a_eid = None
            b_eid = None

            if a_qid and a_qid in qid_to_eid:
                a_eid = qid_to_eid[a_qid]
            elif a_label in label_to_eid:
                a_eid = label_to_eid[a_label]

            if b_qid and b_qid in qid_to_eid:
                b_eid = qid_to_eid[b_qid]
            elif b_label in label_to_eid:
                b_eid = label_to_eid[b_label]

            if not a_eid or not b_eid or a_eid == b_eid:
                continue

            pair = (min(a_eid, b_eid), max(a_eid, b_eid))
            if pair in existing_pairs:
                continue

            explanation = config['explanation'].format(a=a_label, b=b_label)
            db.execute("""
                INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                    serendipity_score, explanation, source, confidence, llm_verdict)
                VALUES (?, ?, ?, ?, ?, ?, 0.9, 'keep')
            """, (a_eid, b_eid, config['type'], config['score'], explanation,
                  f"wikidata_{rel_name}"))
            existing_pairs.add(pair)
            rel_new += 1

        db.commit()
        total_new += rel_new
        print(f"  New connections: {rel_new:,}", flush=True)

    # Summary
    keep_count = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    total_count = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"\n=== Wikidata Structural Connections Complete ===", flush=True)
    print(f"New connections: {total_new:,}", flush=True)
    print(f"Keep connections: {keep_count:,}", flush=True)
    print(f"Total connections: {total_count:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
