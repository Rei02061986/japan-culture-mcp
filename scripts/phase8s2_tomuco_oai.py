"""
Phase 8 Stage 3: ToMuCo (Tokyo Museum Collection) OAI-PMH bulk import.
Uses OAI-PMH endpoint for proper pagination (210K+ records).
Target: >= 30,000 new entities.
"""
import requests
import xml.etree.ElementTree as ET
import time
import sqlite3
import os
import re

DB_PATH = "ontology/culture_ontology.db"
OAI_BASE = "https://museumcollection.tokyo/wp-json/jpsearch/v1/oai-pmh"

NS = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/',
}

# Museum set → geography mapping
SET_GEO = {
    'edo-tokyo-museum': 'kanto',
    'photography-museum': 'kanto',
    'contemporary-art-museum': 'kanto',
    'teien-museum': 'kanto',
    'tokyo-metropolitan-art-museum': 'kanto',
    'open-air-architectural-museum': 'kanto',
}

# Title/type keyword → theme mapping
KEYWORD_THEME = {
    '浮世絵': 'ukiyoe_craft', '錦絵': 'ukiyoe_craft', '木版': 'ukiyoe_craft',
    '版画': 'ukiyoe_craft',
    '絵画': 'visual_arts', '油彩': 'visual_arts', '水彩': 'visual_arts',
    '日本画': 'visual_arts', '洋画': 'visual_arts', '屏風': 'visual_arts',
    '彫刻': 'visual_arts', '塑像': 'visual_arts',
    '写真': 'visual_arts', '撮影': 'visual_arts',
    '陶磁': 'traditional_craft', '焼': 'traditional_craft', '磁器': 'traditional_craft',
    '染織': 'traditional_craft', '織': 'traditional_craft', '染': 'traditional_craft',
    '漆': 'traditional_craft', '蒔絵': 'traditional_craft',
    '金工': 'traditional_craft', '鍔': 'traditional_craft', '刀': 'samurai',
    '太刀': 'samurai', '甲冑': 'samurai', '兜': 'samurai',
    '茶碗': 'tea_ceremony', '茶': 'tea_ceremony', '花入': 'tea_ceremony',
    '仏像': 'sacred_profane', '仏': 'sacred_profane', '観音': 'sacred_profane',
    '着物': 'traditional_craft', '小袖': 'traditional_craft', '振袖': 'traditional_craft',
    '歌舞伎': 'kabuki_theater', '能': 'noh_theater', '狂言': 'noh_theater',
    '書': 'calligraphy', '巻': 'literary_arts',
    '地図': 'historical_event', '古文書': 'historical_event',
    '民俗': 'community_tradition', '祭': 'matsuri',
    '建築': 'architecture', '住宅': 'architecture',
    '映像': 'visual_arts', 'デザイン': 'visual_arts',
    '考古': 'historical_event',
}

# Title keyword → medium mapping
KEYWORD_MEDIUM = {
    '浮世絵': 'ukiyoe', '錦絵': 'ukiyoe', '木版': 'ukiyoe', '版画': 'ukiyoe',
    '絵画': 'painting', '油彩': 'painting', '水彩': 'painting',
    '日本画': 'painting', '洋画': 'painting', '屏風': 'painting',
    '彫刻': 'sculpture', '塑像': 'sculpture',
    '写真': 'photography', '撮影': 'photography',
    '陶磁': 'craft', '焼': 'craft', '磁器': 'craft',
    '染織': 'craft', '織': 'craft', '漆': 'craft', '蒔絵': 'craft',
    '着物': 'craft', '小袖': 'craft', '刀': 'craft', '太刀': 'craft',
    '書': 'literature', '巻': 'literature',
    '建築': 'architecture',
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


def fetch_oai_page(resumption_token=None):
    """Fetch one page of OAI-PMH records."""
    params = {}
    if resumption_token:
        params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}
    else:
        params = {'verb': 'ListRecords', 'metadataPrefix': 'oai_dc'}

    for attempt in range(3):
        try:
            resp = requests.get(OAI_BASE, params=params, timeout=120)
            if resp.status_code == 200 and resp.text.strip():
                return resp.text
            elif resp.status_code == 429 or resp.status_code == 503:
                wait = 60 * (attempt + 1)
                print(f"    Rate limited ({resp.status_code}), waiting {wait}s", flush=True)
                time.sleep(wait)
            else:
                print(f"    HTTP {resp.status_code}", flush=True)
                time.sleep(15)
        except Exception as e:
            print(f"    ERROR: {e}", flush=True)
            time.sleep(15)
    return None


def parse_records(xml_text):
    """Parse OAI-PMH XML into records and resumption token."""
    root = ET.fromstring(xml_text)
    records = []

    for rec in root.findall('.//oai:record', NS):
        header = rec.find('oai:header', NS)
        if header is None:
            continue

        identifier = ''
        setspec = ''
        id_elem = header.find('oai:identifier', NS)
        if id_elem is not None:
            identifier = id_elem.text or ''
        set_elem = header.find('oai:setSpec', NS)
        if set_elem is not None:
            setspec = set_elem.text or ''

        metadata = rec.find('.//oai_dc:dc', NS)
        if metadata is None:
            continue

        titles = [t.text for t in metadata.findall('dc:title', NS) if t.text]
        creators = [c.text for c in metadata.findall('dc:creator', NS) if c.text]
        dates = [d.text for d in metadata.findall('dc:date', NS) if d.text]
        types = [t.text for t in metadata.findall('dc:type', NS) if t.text]
        subjects = [s.text for s in metadata.findall('dc:subject', NS) if s.text]

        records.append({
            'identifier': identifier,
            'setspec': setspec,
            'titles': titles,
            'creators': creators,
            'dates': dates,
            'types': types,
            'subjects': subjects,
        })

    # Get resumption token
    token_elem = root.find('.//oai:resumptionToken', NS)
    token = None
    complete_size = 0
    if token_elem is not None:
        token = token_elem.text
        try:
            complete_size = int(token_elem.get('completeListSize', '0'))
        except ValueError:
            pass

    return records, token, complete_size


def infer_tags(title, subjects, types, setspec):
    """Infer theme, medium, era from title keywords."""
    combined = title + ' ' + ' '.join(subjects) + ' ' + ' '.join(types)

    theme = None
    for kw, t in KEYWORD_THEME.items():
        if kw in combined:
            theme = t
            break
    if not theme:
        theme = 'visual_arts'  # default for museum items

    medium = None
    for kw, m in KEYWORD_MEDIUM.items():
        if kw in combined:
            medium = m
            break

    geo = SET_GEO.get(setspec, 'kanto')  # default kanto for Tokyo museums

    return theme, medium, geo


def main():
    db = sqlite3.connect(DB_PATH)
    os.makedirs('data/tomuco', exist_ok=True)

    # Load existing labels (use composite key: label + source prefix to allow museum items)
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    # Track ToMuCo identifiers to avoid reimporting
    existing_tomuco = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE source='tomuco_oai_phase8'"):
        existing_tomuco.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)
    print(f"Existing ToMuCo: {len(existing_tomuco):,}", flush=True)

    total_new = 0
    total_creators = 0
    creators_seen = set()
    target = 35000
    page_num = 0
    resumption_token = None

    while total_new < target:
        page_num += 1

        xml_text = fetch_oai_page(resumption_token)
        if not xml_text:
            print(f"  Failed to fetch page {page_num}, stopping", flush=True)
            break

        records, new_token, complete_size = parse_records(xml_text)

        if page_num == 1:
            print(f"Total records available: {complete_size:,}", flush=True)

        if not records:
            print(f"  No records on page {page_num}, stopping", flush=True)
            break

        for rec in records:
            titles = rec['titles']
            if not titles:
                continue

            name_ja = titles[0].strip()
            if not name_ja or len(name_ja) < 2:
                continue

            # Skip if already in DB (any source)
            if name_ja in existing_labels:
                continue

            # Get English title if available (second title often is)
            name_en = titles[1].strip() if len(titles) > 1 else None

            theme, medium, geo = infer_tags(
                name_ja, rec['subjects'], rec['types'], rec['setspec']
            )

            db.execute("""
                INSERT INTO entities (label_ja, label_en, entity_type, source)
                VALUES (?, ?, 'artifact', 'tomuco_oai_phase8')
            """, (name_ja, name_en))
            eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
            existing_labels.add(name_ja)

            # Theme tag
            conf = 0.7 if theme != 'visual_arts' else 0.5
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', ?, 'tomuco_oai', ?)", (eid, theme, conf))

            # Medium tag
            if medium:
                db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'tomuco_oai', 0.7)", (eid, medium))

            # Experience
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'aesthetic', 'tomuco_oai', 0.7)", (eid,))

            # Geography
            db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'geography', ?, 'tomuco_oai', 0.9)", (eid, geo))

            # Era from dates
            for date_str in rec['dates']:
                m = re.search(r'(\d{4})', date_str)
                if m:
                    year = int(m.group(1))
                    if 500 < year < 2030:
                        era = year_to_era(year)
                        db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'era', ?, 'tomuco_date', 0.7)", (eid, era))
                        break

            total_new += 1

            # Add creators as person entities
            for creator in rec['creators']:
                creator = creator.strip()
                if creator and len(creator) >= 2 and creator not in existing_labels and creator not in creators_seen:
                    creators_seen.add(creator)
                    db.execute("""
                        INSERT INTO entities (label_ja, entity_type, source)
                        VALUES (?, 'person', 'tomuco_oai_phase8')
                    """, (creator,))
                    ceid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                    existing_labels.add(creator)
                    db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'theme', 'visual_arts', 'tomuco_creator', 0.7)", (ceid,))
                    db.execute("INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'experience', 'aesthetic', 'tomuco', 0.7)", (ceid,))
                    total_creators += 1

        if page_num % 10 == 0:
            db.commit()
            print(f"  Page {page_num}: new={total_new:,}, creators={total_creators:,}", flush=True)

        if not new_token:
            print(f"  No more pages after {page_num}", flush=True)
            break

        resumption_token = new_token
        time.sleep(1.5)  # Rate limit

    db.commit()

    tomuco_count = db.execute("SELECT COUNT(*) FROM entities WHERE source='tomuco_oai_phase8'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"\n=== ToMuCo OAI-PMH Import Complete ===", flush=True)
    print(f"New works: {total_new:,}", flush=True)
    print(f"New creators: {total_creators:,}", flush=True)
    print(f"ToMuCo entities: {tomuco_count:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
