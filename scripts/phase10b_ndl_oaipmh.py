"""
Phase 10B: NDL (National Diet Library) OAI-PMH bulk import.
Target: 100,000+ entities from digital collections.
Endpoint: https://ndlsearch.ndl.go.jp/api/oaipmh
"""
import requests
import time
import sqlite3
import re
import xml.etree.ElementTree as ET

DB_PATH = "ontology/culture_ontology.db"
NDL_OAI = "https://ndlsearch.ndl.go.jp/api/oaipmh"
HEADERS = {'User-Agent': 'japan-culture-mcp/0.5'}

# OAI-PMH sets to harvest (actual NDL set codes)
SETS = [
    {
        'set': 'A00003',  # 古典籍資料（貴重書等）
        'name': '古典籍資料（貴重書等）',
        'entity_type': 'artifact',
        'tags': [('medium', 'classical_text', 0.8), ('experience', 'intellectual', 0.8)],
        'limit': 100000,
    },
    {
        'set': 'A00004',  # 錦絵
        'name': '錦絵',
        'entity_type': 'artifact',
        'tags': [('medium', 'ukiyoe', 0.9), ('experience', 'aesthetic', 0.9)],
        'limit': 50000,
    },
    {
        'set': 'A00001',  # 図書
        'name': '図書',
        'entity_type': 'work',
        'tags': [('medium', 'literature', 0.7), ('experience', 'intellectual', 0.7)],
        'limit': 200000,
    },
    {
        'set': 'A00006',  # 重要文化財
        'name': '重要文化財',
        'entity_type': 'artifact',
        'tags': [('theme', 'craft_mastery', 0.7), ('experience', 'aesthetic', 0.8)],
        'limit': 50000,
    },
    {
        'set': 'A00024',  # 歴史的音源
        'name': '歴史的音源',
        'entity_type': 'work',
        'tags': [('medium', 'music', 0.8), ('experience', 'aesthetic', 0.7)],
        'limit': 50000,
    },
    {
        'set': 'A00078',  # 児童書
        'name': '児童書',
        'entity_type': 'work',
        'tags': [('medium', 'literature', 0.7), ('theme', 'everyday_beauty', 0.5)],
        'limit': 50000,
    },
]

DC_NS = {
    'oai': 'http://www.openarchives.org/OAI/2.0/',
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcterms': 'http://purl.org/dc/terms/',
    'dcndl': 'http://ndl.go.jp/dcndl/terms/',
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


def detect_medium(dc_type, subject):
    """Detect medium from DC type and subject."""
    text = f"{dc_type} {subject}".lower()
    if '地図' in text or 'map' in text: return 'painting'
    if '写真' in text or 'photo' in text: return 'painting'
    if '音' in text or '録音' in text or 'sound' in text: return 'music'
    if '映像' in text or 'video' in text: return 'anime'
    if '古典' in text or '古書' in text or '写本' in text: return 'classical_text'
    if '漫画' in text or 'manga' in text: return 'manga'
    if '雑誌' in text: return 'literature'
    return 'literature'


def fetch_oai(params, retries=3):
    """Fetch OAI-PMH response."""
    for attempt in range(retries):
        try:
            resp = requests.get(NDL_OAI, params=params, headers=HEADERS, timeout=120)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 503:
                retry_after = int(resp.headers.get('Retry-After', 60))
                print(f"    503, waiting {retry_after}s...", flush=True)
                time.sleep(retry_after)
            else:
                print(f"    HTTP {resp.status_code}", flush=True)
                time.sleep(30)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(30)
    return None


def parse_records(xml_text):
    """Parse OAI-PMH ListRecords response."""
    records = []
    resumption_token = None

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        print(f"    XML parse error: {e}", flush=True)
        return records, None

    # Find resumption token
    for rt in root.iter('{http://www.openarchives.org/OAI/2.0/}resumptionToken'):
        if rt.text:
            resumption_token = rt.text.strip()

    # Parse records
    for record in root.iter('{http://www.openarchives.org/OAI/2.0/}record'):
        metadata = record.find('{http://www.openarchives.org/OAI/2.0/}metadata')
        if metadata is None:
            continue

        rec = {'title': '', 'creator': '', 'date': '', 'subject': '', 'type': '', 'identifier': ''}

        # Try Dublin Core
        for dc in metadata.iter():
            tag = dc.tag.split('}')[-1] if '}' in dc.tag else dc.tag
            text = (dc.text or '').strip()
            if not text:
                continue

            if tag == 'title' and not rec['title']:
                rec['title'] = text
            elif tag == 'creator' and not rec['creator']:
                rec['creator'] = text
            elif tag == 'date' and not rec['date']:
                rec['date'] = text
            elif tag == 'subject':
                rec['subject'] = f"{rec['subject']} {text}".strip()
            elif tag == 'type' and not rec['type']:
                rec['type'] = text
            elif tag == 'identifier' and not rec['identifier']:
                rec['identifier'] = text

        if rec['title']:
            records.append(rec)

    return records, resumption_token


def main():
    db = sqlite3.connect(DB_PATH)

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    grand_total = 0

    for set_config in SETS:
        set_name = set_config['set']
        limit = set_config['limit']
        print(f"\n=== {set_config['name']} (set: {set_name}) ===", flush=True)

        # Initial request
        params = {
            'verb': 'ListRecords',
            'metadataPrefix': 'oai_dc',
            'set': set_name,
        }

        set_new = 0
        page = 0
        resumption_token = None

        while True:
            if set_new >= limit:
                print(f"  Reached limit {limit:,}", flush=True)
                break

            if resumption_token:
                params = {'verb': 'ListRecords', 'resumptionToken': resumption_token}

            xml_text = fetch_oai(params)
            if not xml_text:
                break

            records, resumption_token = parse_records(xml_text)
            page += 1

            if not records:
                break

            batch_new = 0
            for rec in records:
                title = rec['title']
                if not title or len(title) < 2 or len(title) > 300:
                    continue
                if title in existing_labels:
                    continue

                # Detect medium
                medium = detect_medium(rec['type'], rec['subject'])

                # Insert entity
                cur = db.execute("""
                    INSERT INTO entities (label_ja, entity_type, source, ndl_id)
                    VALUES (?, ?, 'ndl_phase10', ?)
                """, (title, set_config['entity_type'], rec['identifier'] or None))
                eid = cur.lastrowid

                # Tags
                for axis, value_code, confidence in set_config['tags']:
                    db.execute("""
                        INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                        VALUES (?, ?, ?, 'ndl_phase10', ?)
                    """, (eid, axis, value_code, confidence))

                # Medium tag
                db.execute("""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'medium', ?, 'ndl_phase10', 0.7)
                """, (eid, medium))

                # Era from date
                date_str = rec['date']
                if date_str:
                    m = re.search(r'(\d{3,4})', date_str)
                    if m:
                        year = int(m.group(1))
                        if 500 <= year <= 2030:
                            era = year_to_era(year)
                            db.execute("""
                                INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                                VALUES (?, 'era', ?, 'ndl_phase10_date', 0.8)
                            """, (eid, era))

                existing_labels.add(title)
                batch_new += 1
                set_new += 1

            if page % 10 == 0:
                db.commit()
                print(f"  Page {page}: {len(records)} records, {batch_new} new (total: {set_new:,})", flush=True)

            if not resumption_token:
                break

            time.sleep(2)

        db.commit()
        grand_total += set_new
        print(f"  {set_config['name']}: {set_new:,} new", flush=True)

    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n=== NDL OAI-PMH Import Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
