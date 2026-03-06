"""
Phase 8 Stage 3: ToMuCo (Tokyo Museum Collection) bulk import.
REST JSON-LD API. 100 items per page.
Target: >= 30,000 entities.
"""
import requests
import json
import time
import sqlite3
import os

DB_PATH = "ontology/culture_ontology.db"
TOMUCO_BASE = "https://museumcollection.tokyo/works/"

# Genre → theme mapping
GENRE_THEME = {
    '浮世絵': 'ukiyoe_craft',
    '版画': 'ukiyoe_craft',
    '絵画': 'visual_arts',
    '日本画': 'visual_arts',
    '洋画': 'visual_arts',
    '彫刻': 'visual_arts',
    '写真': 'visual_arts',
    '工芸': 'traditional_craft',
    '陶磁': 'traditional_craft',
    '染織': 'traditional_craft',
    '漆工': 'traditional_craft',
    '金工': 'traditional_craft',
    '木竹工': 'traditional_craft',
    '書': 'calligraphy',
    '映像': 'visual_arts',
    'デザイン': 'visual_arts',
    '建築': 'architecture',
    '考古': 'historical_event',
    '歴史資料': 'historical_event',
    '民俗': 'community_tradition',
    '海外写真': 'visual_arts',
    '現代美術': 'visual_arts',
}

# Genre → medium mapping
GENRE_MEDIUM = {
    '浮世絵': 'ukiyoe',
    '版画': 'ukiyoe',
    '絵画': 'painting',
    '日本画': 'painting',
    '洋画': 'painting',
    '彫刻': 'sculpture',
    '写真': 'photography',
    '工芸': 'craft',
    '陶磁': 'craft',
    '染織': 'craft',
    '漆工': 'craft',
    '金工': 'craft',
    '木竹工': 'craft',
    '書': 'literature',
    '建築': 'architecture',
}

# Museum keywords → geography
MUSEUM_GEO = {
    '江戸東京': 'kanto',
    '都写真美術館': 'kanto',
    '都現代美術館': 'kanto',
    '都庭園美術館': 'kanto',
    '都美術館': 'kanto',
    'たてもの園': 'kanto',
}

def year_to_era(year):
    if year < 1185: return 'ancient'
    if year < 1573: return 'medieval'
    if year < 1700: return 'edo_early'
    if year < 1868: return 'edo_late'
    if year < 1926: return 'meiji_taisho'
    if year < 1945: return 'showa_prewar'
    if year < 1989: return 'showa_postwar'
    if year < 2019: return 'heisei'
    return 'reiwa'

def extract_year(date_str):
    """Extract year from various date formats."""
    if not date_str:
        return None
    import re
    # Try 4-digit year
    m = re.search(r'(\d{4})', str(date_str))
    if m:
        return int(m.group(1))
    return None


def parse_item(item):
    """Parse a ToMuCo JSON-LD item into a flat record."""
    result = {}

    # Name (ja/en)
    names = item.get('schema:name', [])
    if isinstance(names, dict):
        names = [names]
    for n in names:
        lang = n.get('@lang', '')
        val = n.get('@value', '')
        if lang == 'ja':
            result['name_ja'] = val
        elif lang == 'en':
            result['name_en'] = val

    # Genre
    genres = item.get('schema:genre', [])
    if isinstance(genres, dict):
        genres = [genres]
    genre_labels = []
    for g in genres:
        pref = g.get('skos:preflabel', {})
        if isinstance(pref, dict):
            val = pref.get('@value', '')
            if val:
                genre_labels.append(val)
    result['genres'] = genre_labels

    # Creator
    creators = item.get('schema:creator', [])
    if isinstance(creators, dict):
        creators = [creators]
    creator_names = []
    for c in creators:
        cnames = c.get('schema:name', [])
        if isinstance(cnames, dict):
            cnames = [cnames]
        for cn in cnames:
            if cn.get('@lang') == 'ja':
                creator_names.append(cn.get('@value', ''))
                break
    result['creators'] = creator_names

    # Production date
    date_field = item.get('schema:dateCreated', '')
    if isinstance(date_field, dict):
        date_field = date_field.get('@value', '')
    result['date'] = str(date_field) if date_field else ''

    # Museum/provider
    provider = item.get('schema:provider', {})
    if isinstance(provider, dict):
        pnames = provider.get('schema:name', [])
        if isinstance(pnames, dict):
            pnames = [pnames]
        for pn in pnames:
            if pn.get('@lang') == 'ja':
                result['museum'] = pn.get('@value', '')
                break

    # Thumbnail
    thumb = item.get('schema:thumbnail', '')
    if isinstance(thumb, dict):
        thumb = thumb.get('@id', '')
    result['thumbnail'] = thumb

    return result


def fetch_page(page, genre=None):
    """Fetch one page of ToMuCo results."""
    params = {'output': 'json', 'page': page}
    if genre:
        params['genre'] = genre

    for attempt in range(3):
        try:
            resp = requests.get(TOMUCO_BASE, params=params, timeout=60)
            if resp.status_code == 200 and resp.text.strip():
                data = resp.json()
                return data.get('@graph', [])
            elif resp.status_code == 429:
                time.sleep(30 * (attempt + 1))
            else:
                time.sleep(10)
        except Exception as e:
            print(f"    ERROR page {page}: {e}", flush=True)
            time.sleep(10)
    return []


def main():
    db = sqlite3.connect(DB_PATH)
    os.makedirs('data/tomuco', exist_ok=True)

    existing = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing.add(row[0])
    print(f"Existing entities: {len(existing):,}", flush=True)

    total_new = 0
    total_creators = 0
    creators_seen = set()
    target = 35000

    # Fetch pages sequentially
    page = 1
    consecutive_empty = 0

    while total_new < target:
        if page % 50 == 1:
            print(f"  Page {page}, new entities: {total_new:,}, creators: {total_creators:,}", flush=True)

        items = fetch_page(page)

        if not items:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                print(f"  3 consecutive empty pages at {page}, stopping", flush=True)
                break
            page += 1
            time.sleep(2)
            continue

        consecutive_empty = 0

        for item in items:
            rec = parse_item(item)
            name_ja = rec.get('name_ja', '').strip()

            if not name_ja or name_ja in existing or len(name_ja) < 2:
                continue

            name_en = rec.get('name_en', '')
            genres = rec.get('genres', [])

            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, source)
                VALUES (?, ?, 'artifact', 'tomuco_phase8')
            """, (name_ja, name_en or None))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            existing.add(name_ja)

            # Tag from genre
            theme_set = False
            for genre in genres:
                for gk, theme in GENRE_THEME.items():
                    if gk in genre:
                        db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'tomuco_genre', 0.8)", (eid, theme))
                        theme_set = True
                        break
                if theme_set:
                    break

            if not theme_set:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', 'visual_arts', 'tomuco_default', 0.5)", (eid,))

            for genre in genres:
                for gk, medium in GENRE_MEDIUM.items():
                    if gk in genre:
                        db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'tomuco_genre', 0.7)", (eid, medium))
                        break
                break

            # Experience
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'aesthetic', 'tomuco', 0.7)", (eid,))

            # Geography (all ToMuCo is Tokyo)
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'geography', 'kanto', 'tomuco', 0.9)", (eid,))

            # Era from date
            year = extract_year(rec.get('date', ''))
            if year and 500 < year < 2030:
                era = year_to_era(year)
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'era', ?, 'tomuco_date', 0.7)", (eid, era))

            total_new += 1

            # Also add creators as person entities
            for creator in rec.get('creators', []):
                if creator and creator not in existing and creator not in creators_seen:
                    creators_seen.add(creator)
                    db.execute("""
                        INSERT INTO entities (label_ja, entity_type, source)
                        VALUES (?, 'person', 'tomuco_phase8')
                    """, (creator,))
                    ceid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                    existing.add(creator)
                    db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', 'visual_arts', 'tomuco_creator', 0.7)", (ceid,))
                    db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'aesthetic', 'tomuco', 0.7)", (ceid,))
                    total_creators += 1

        if page % 100 == 0:
            db.commit()

        page += 1
        time.sleep(1)  # Rate limit

    db.commit()

    tomuco_count = db.execute("SELECT COUNT(*) FROM entities WHERE source='tomuco_phase8'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"\n=== ToMuCo Import Complete ===", flush=True)
    print(f"New works: {total_new:,}", flush=True)
    print(f"New creators: {total_creators:,}", flush=True)
    print(f"ToMuCo entities: {tomuco_count:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
