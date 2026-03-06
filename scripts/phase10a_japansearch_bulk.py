"""
Phase 10A: JapanSearch SPARQL bulk import.
Target: 2,000,000+ new entities from Japan's unified cultural metadata portal.
Endpoint: https://jpsearch.go.jp/rdf/sparql (認証不要, CC BY 4.0)
"""
import requests
import time
import sqlite3
import re

DB_PATH = "ontology/culture_ontology.db"
ENDPOINT = "https://jpsearch.go.jp/rdf/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.5',
    'Content-Type': 'application/x-www-form-urlencoded',
}

# Type → (entity_type, theme, medium, experience)
TYPE_CONFIG = {
    # P0: 最高優先度 — 日本文化のコア
    '版画': {
        'entity_type': 'artifact',
        'tags': [('medium', 'ukiyoe', 0.8), ('theme', 'everyday_beauty', 0.6),
                 ('experience', 'aesthetic', 0.9)],
        'limit_total': 200000,
    },
    '古書・古文書': {
        'entity_type': 'artifact',
        'tags': [('medium', 'literature', 0.8), ('medium', 'classical_text', 0.7),
                 ('theme', 'literary_arts', 0.6), ('experience', 'intellectual', 0.8)],
        'limit_total': 500000,
    },
    '歴史資料': {
        'entity_type': 'artifact',
        'tags': [('theme', 'community_tradition', 0.7), ('experience', 'intellectual', 0.8)],
        'limit_total': 500000,
    },
    '博物資料': {
        'entity_type': 'artifact',
        'tags': [('theme', 'community_tradition', 0.6), ('experience', 'aesthetic', 0.7),
                 ('experience', 'intellectual', 0.7)],
        'limit_total': 234000,
    },
    '絵画': {
        'entity_type': 'artifact',
        'tags': [('medium', 'painting', 0.9), ('theme', 'visual_arts', 0.8),
                 ('experience', 'aesthetic', 0.9)],
        'limit_total': 100000,
    },
    '彫刻': {
        'entity_type': 'artifact',
        'tags': [('medium', 'sculpture', 0.9), ('theme', 'sacred_profane', 0.5),
                 ('experience', 'aesthetic', 0.9)],
        'limit_total': 50000,
    },
    '工芸品': {
        'entity_type': 'artifact',
        'tags': [('medium', 'craft', 0.9), ('theme', 'craft_mastery', 0.8),
                 ('experience', 'aesthetic', 0.8)],
        'limit_total': 200000,
    },

    # P1: 高優先度
    '地図資料': {
        'entity_type': 'artifact',
        'tags': [('theme', 'journey_boundary', 0.7), ('experience', 'intellectual', 0.8)],
        'limit_total': 200000,
    },
    '録音資料': {
        'entity_type': 'work',
        'tags': [('medium', 'music', 0.8), ('experience', 'aesthetic', 0.7)],
        'limit_total': 200000,
    },
    '映像資料': {
        'entity_type': 'work',
        'tags': [('medium', 'anime', 0.3), ('experience', 'aesthetic', 0.7)],
        'limit_total': 182000,
    },
    '記録写真': {
        'entity_type': 'artifact',
        'tags': [('theme', 'community_tradition', 0.5), ('experience', 'aesthetic', 0.6)],
        'limit_total': 300000,
    },
    '静止画資料': {
        'entity_type': 'artifact',
        'tags': [('medium', 'painting', 0.5), ('experience', 'aesthetic', 0.7)],
        'limit_total': 281000,
    },

    # P2: 書籍・雑誌
    '図書': {
        'entity_type': 'work',
        'tags': [('medium', 'literature', 0.7), ('experience', 'intellectual', 0.7)],
        'limit_total': 500000,
    },
}

# era detection patterns
ERA_PATTERNS = [
    (r'縄文|弥生|古墳|飛鳥|奈良時代|平安', 'ancient'),
    (r'鎌倉|室町|南北朝|戦国', 'medieval'),
    (r'安土桃山|慶長|元和|寛永|正保|慶安|承応|明暦|万治|寛文|延宝|天和|貞享|元禄', 'edo_early'),
    (r'宝永|正徳|享保|元文|寛保|延享|寛延|宝暦|明和|安永|天明|寛政|享和|文化|文政|天保|弘化|嘉永|安政|万延|文久|元治|慶応', 'edo_late'),
    (r'明治|大正', 'meiji_taisho'),
    (r'昭和[^戦]|昭和初|昭和[0-9]|昭和1[0-9]', 'showa_prewar'),
    (r'昭和[2-6][0-9]|昭和戦後|戦後', 'showa_postwar'),
    (r'平成', 'heisei'),
    (r'令和', 'reiwa'),
]


def sparql_fetch(query, retries=3):
    """Execute SPARQL query with retries."""
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


def detect_era(text):
    """Detect era from temporal text."""
    if not text:
        return None
    for pattern, era in ERA_PATTERNS:
        if re.search(pattern, text):
            return era
    # Try year detection
    m = re.search(r'(\d{3,4})年', text)
    if m:
        year = int(m.group(1))
        if year < 1185: return 'ancient'
        if year < 1573: return 'medieval'
        if year < 1700: return 'edo_early'
        if year < 1868: return 'edo_late'
        if year < 1926: return 'meiji_taisho'
        if year < 1945: return 'showa_prewar'
        if year < 1989: return 'showa_postwar'
        if year < 2019: return 'heisei'
        return 'reiwa'
    return None


def detect_geography(text):
    """Detect geography from spatial text."""
    if not text:
        return None
    geo_map = {
        '北海道': 'hokkaido',
        '青森|岩手|宮城|秋田|山形|福島': 'tohoku',
        '茨城|栃木|群馬|埼玉|千葉|神奈川': 'kanto',
        '東京': 'tokyo',
        '新潟|富山|石川|福井|山梨|長野|岐阜|静岡|愛知': 'chubu',
        '三重|滋賀|大阪|兵庫|和歌山': 'kinki',
        '京都': 'kyoto',
        '奈良': 'nara',
        '鳥取|島根|岡山|広島|山口': 'chugoku',
        '徳島|香川|愛媛|高知': 'shikoku',
        '福岡|佐賀|長崎|熊本|大分|宮崎|鹿児島|沖縄': 'kyushu',
    }
    for pattern, geo in geo_map.items():
        if re.search(pattern, text):
            return geo
    return None


def main():
    db = sqlite3.connect(DB_PATH)

    # Build existing label set
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    grand_total = 0
    page_size = 10000

    for type_name, config in TYPE_CONFIG.items():
        type_uri = f"https://jpsearch.go.jp/term/type/{type_name}"
        limit_total = config['limit_total']
        print(f"\n{'='*60}", flush=True)
        print(f"=== {type_name} (limit: {limit_total:,}) ===", flush=True)

        offset = 0
        type_new = 0

        while offset < limit_total:
            # Simple query - just label + optional creator and temporal
            query = f"""SELECT ?item ?label ?creator ?temporal ?spatial WHERE {{
  ?item a <{type_uri}> ;
        rdfs:label ?label .
  OPTIONAL {{ ?item schema:creator/rdfs:label ?creator }}
  OPTIONAL {{ ?item schema:temporal/rdfs:label ?temporal }}
  OPTIONAL {{ ?item schema:spatial/rdfs:label ?spatial }}
}} LIMIT {page_size} OFFSET {offset}"""

            bindings = sparql_fetch(query)
            if not bindings:
                # Try simpler query without OPTIONALs
                query_simple = f"""SELECT ?item ?label WHERE {{
  ?item a <{type_uri}> ;
        rdfs:label ?label .
}} LIMIT {page_size} OFFSET {offset}"""
                bindings = sparql_fetch(query_simple)
                if not bindings:
                    print(f"  No results at offset {offset}, moving on", flush=True)
                    break

            print(f"  offset={offset:,}, got={len(bindings):,}", flush=True)

            # Process bindings
            batch_new = 0
            for b in bindings:
                label = b.get('label', {}).get('value', '').strip()
                if not label or len(label) < 2 or len(label) > 300:
                    continue
                if label in existing_labels:
                    continue

                creator = b.get('creator', {}).get('value', '').strip() if b.get('creator') else None
                temporal = b.get('temporal', {}).get('value', '').strip() if b.get('temporal') else None
                spatial = b.get('spatial', {}).get('value', '').strip() if b.get('spatial') else None

                # Insert entity
                cur = db.execute("""
                    INSERT INTO entities (label_ja, entity_type, source)
                    VALUES (?, ?, 'jps_phase10')
                """, (label, config['entity_type']))
                eid = cur.lastrowid

                # Add type-specific tags
                for axis, value_code, confidence in config['tags']:
                    db.execute("""
                        INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, ?, ?, 'jps_phase10', ?)
                    """, (eid, axis, value_code, confidence))

                # Era from temporal
                era = detect_era(temporal)
                if era:
                    db.execute("""
                        INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, 'era', ?, 'jps_phase10_temporal', 0.7)
                    """, (eid, era))

                # Geography from spatial
                geo = detect_geography(spatial)
                if geo:
                    db.execute("""
                        INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, 'geography', ?, 'jps_phase10_spatial', 0.7)
                    """, (eid, geo))

                existing_labels.add(label)
                batch_new += 1
                type_new += 1

            if batch_new > 0 and offset % 50000 == 0:
                db.commit()

            if len(bindings) < page_size:
                break

            offset += page_size
            time.sleep(5)  # Rate limit

        db.commit()
        grand_total += type_new
        print(f"  {type_name}: {type_new:,} new (running total: {grand_total:,})", flush=True)

    # Final stats
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== JapanSearch Bulk Import Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
