"""
Phase 10H v2: NDL SRU bulk import with proper pagination.
Uses character-range searches to cover broad catalog.
Target: 2,000,000+ new entities.
"""
import requests
import time
import sqlite3
import xml.etree.ElementTree as ET

DB_PATH = "ontology/culture_ontology.db"
NDL_SRU = "https://ndlsearch.ndl.go.jp/api/sru"
HEADERS = {'User-Agent': 'japan-culture-mcp/0.5'}
PAGE_SIZE = 200

# Japanese syllables for broad search coverage
HIRAGANA = list('あいうえおかきくけこさしすせそたちつてとなにぬねのはひふへほまみむめもやゆよらりるれろわ')


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
                time.sleep(30)
            else:
                print(f"    HTTP {resp.status_code}", flush=True)
                time.sleep(10)
        except Exception as e:
            print(f"    Error: {e}", flush=True)
            time.sleep(10)
    return None


def parse_sru(xml_text):
    titles = []
    total = 0
    next_record = 0
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return titles, 0, 0

    total_el = root.find('.//{http://www.loc.gov/zing/srw/}numberOfRecords')
    if total_el is not None:
        total = int(total_el.text)

    next_el = root.find('.//{http://www.loc.gov/zing/srw/}nextRecordPosition')
    if next_el is not None:
        next_record = int(next_el.text)

    for rec in root.iter('{http://www.loc.gov/zing/srw/}record'):
        title = ''
        for el in rec.iter():
            tag = el.tag.split('}')[-1] if '}' in el.tag else el.tag
            if tag == 'title' and not title and el.text:
                title = el.text.strip()
        if title:
            titles.append(title)

    return titles, total, next_record


def main():
    db = sqlite3.connect(DB_PATH)

    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    grand_total = 0
    limit_per_char = 100000  # Max records per hiragana prefix

    for i, char in enumerate(HIRAGANA):
        query = f'title = "{char}*"'
        print(f"\n=== [{i+1}/{len(HIRAGANA)}] '{char}' ===", flush=True)

        start = 1
        char_new = 0
        first_page = True

        while char_new < limit_per_char:
            xml_text = fetch_sru(query, start)
            if not xml_text:
                break

            titles, total, next_record = parse_sru(xml_text)

            if first_page:
                print(f"  Total available: {total:,}", flush=True)
                first_page = False

            if not titles:
                break

            batch_new = 0
            for title in titles:
                if len(title) < 2 or len(title) > 300 or title in existing_labels:
                    continue

                db.execute("""
                    INSERT INTO entities (label_ja, entity_type, source)
                    VALUES (?, 'work', 'ndl_sru')
                """, (title,))

                existing_labels.add(title)
                batch_new += 1
                char_new += 1

            if start % 5000 == 1 and start > 1:
                db.commit()
                print(f"  record={start:,}, new={char_new:,}", flush=True)

            if next_record == 0 or next_record <= start:
                break

            start = next_record
            time.sleep(0.5)

        db.commit()
        grand_total += char_new
        print(f"  '{char}': {char_new:,} new (running: {grand_total:,})", flush=True)

    # Tag all new NDL SRU entities
    print("\nTagging new entities...", flush=True)
    db.execute("""
        INSERT INTO entity_tags (entity_id, axis, value_code, source, confidence)
        SELECT id, 'experience', 'intellectual', 'ndl_sru', 0.6
        FROM entities WHERE source = 'ndl_sru'
    """)
    db.commit()

    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    sources = db.execute("SELECT COUNT(DISTINCT source) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== NDL SRU Bulk Import Complete ===", flush=True)
    print(f"New entities: {grand_total:,}", flush=True)
    print(f"Total entities: {total_entities:,}", flush=True)
    print(f"Sources: {sources}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
