"""
Phase 10H: NDL SRU bulk import.
Uses broad searches to maximize record coverage.
Target: 2,000,000+ new entities.
"""
import requests
import time
import sqlite3
import xml.etree.ElementTree as ET

DB_PATH = "ontology/culture_ontology.db"
NDL_SRU = "https://ndlsearch.ndl.go.jp/api/sru"
HEADERS = {'User-Agent': 'japan-culture-mcp/0.5'}

SRW_NS = {'srw': 'http://www.loc.gov/zing/srw/'}
DC_NS = 'http://purl.org/dc/elements/1.1/'

# Search queries to cover broad range of Japanese titles
SEARCHES = [
    {'query': 'title any "の"', 'source': 'ndl_sru_no', 'limit': 500000},     # Particle "no" - very common
    {'query': 'title any "と"', 'source': 'ndl_sru_to', 'limit': 500000},     # Particle "to"
    {'query': 'title any "に"', 'source': 'ndl_sru_ni', 'limit': 500000},     # Particle "ni"
    {'query': 'title any "を"', 'source': 'ndl_sru_wo', 'limit': 500000},     # Particle "wo"
    {'query': 'title any "日本"', 'source': 'ndl_sru_nihon', 'limit': 500000},
    {'query': 'title any "東京"', 'source': 'ndl_sru_tokyo', 'limit': 200000},
    {'query': 'title any "研究"', 'source': 'ndl_sru_kenkyu', 'limit': 300000},
    {'query': 'title any "文化"', 'source': 'ndl_sru_bunka', 'limit': 200000},
    {'query': 'title any "物語"', 'source': 'ndl_sru_monogatari', 'limit': 200000},
    {'query': 'title any "歴史"', 'source': 'ndl_sru_rekishi', 'limit': 200000},
]

PAGE_SIZE = 200


def fetch_sru(query, start_record, retries=3):
    params = {
        'operation': 'searchRetrieve',
        'version': '1.2',
        'query': query,
        'startRecord': start_record,
        'maximumRecords': PAGE_SIZE,
        'recordPacking': 'xml',
        'recordSchema': 'dc',
    }
    for attempt in range(retries):
        try:
            resp = requests.get(NDL_SRU, params=params, headers=HEADERS, timeout=60)
            if resp.status_code == 200:
                return resp.text
            elif resp.status_code == 503:
                wait = int(resp.headers.get('Retry-After', 30))
                print(f"    503, waiting {wait}s...", flush=True)
                time.sleep(wait)
            else:
                print(f"    HTTP {resp.status_code}", flush=True)
                time.sleep(10)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(10)
    return None


def parse_sru_records(xml_text):
    records = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return records, 0

    total = 0
    total_el = root.find('.//srw:numberOfRecords', SRW_NS)
    if total_el is not None:
        total = int(total_el.text)

    for record in root.iter('{http://www.loc.gov/zing/srw/}record'):
        title = ''
        creator = ''
        subject = ''
        date = ''

        for dc_el in record.iter():
            tag = dc_el.tag.split('}')[-1] if '}' in dc_el.tag else dc_el.tag
            text = (dc_el.text or '').strip()
            if not text:
                continue
            if tag == 'title' and not title:
                title = text
            elif tag == 'creator' and not creator:
                creator = text
            elif tag == 'subject':
                subject = f"{subject} {text}".strip()
            elif tag == 'date' and not date:
                date = text

        if title:
            records.append({'title': title, 'creator': creator, 'subject': subject, 'date': date})

    return records, total


def detect_entity_type(title, subject):
    text = f"{title} {subject}".lower()
    if any(k in text for k in ['漫画', 'manga', 'コミック']): return 'work'
    if any(k in text for k in ['小説', '物語', '文学']): return 'work'
    if any(k in text for k in ['映画', 'film']): return 'work'
    if any(k in text for k in ['音楽', '歌', 'music']): return 'work'
    return 'work'


def main():
    db = sqlite3.connect(DB_PATH)

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    grand_total = 0

    for search_config in SEARCHES:
        query = search_config['query']
        source = search_config['source']
        limit = search_config['limit']
        print(f"\n=== {query} (source: {source}, limit: {limit:,}) ===", flush=True)

        start_record = 1
        search_new = 0
        total_available = None

        while True:
            if search_new >= limit:
                print(f"  Reached limit {limit:,}", flush=True)
                break

            xml_text = fetch_sru(query, start_record)
            if not xml_text:
                break

            records, total = parse_sru_records(xml_text)
            if total_available is None:
                total_available = total
                print(f"  Total available: {total:,}", flush=True)

            if not records:
                break

            batch_new = 0
            for rec in records:
                title = rec['title']
                if not title or len(title) < 2 or len(title) > 300:
                    continue
                if title in existing_labels:
                    continue

                entity_type = detect_entity_type(title, rec['subject'])
                cur = db.execute("""
                    INSERT INTO entities (label_ja, entity_type, source)
                    VALUES (?, ?, ?)
                """, (title, entity_type, source))
                eid = cur.lastrowid

                db.execute("""
                    INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
                    VALUES (?, 'experience', 'intellectual', ?, 0.6)
                """, (eid, source))

                existing_labels.add(title)
                batch_new += 1
                search_new += 1

            start_record += PAGE_SIZE

            if start_record % 10000 == 1:
                db.commit()
                print(f"  record={start_record:,}, batch_new={batch_new}, search_new={search_new:,}", flush=True)

            if len(records) < PAGE_SIZE:
                break

            time.sleep(1)  # Rate limit

        db.commit()
        grand_total += search_new
        print(f"  {query}: {search_new:,} new (running total: {grand_total:,})", flush=True)

    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    sources = db.execute("SELECT COUNT(DISTINCT source) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== NDL SRU Bulk Import Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    print(f"Unique sources: {sources}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
