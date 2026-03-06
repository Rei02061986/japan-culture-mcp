"""
Phase 8 Stage 4: English label expansion.
1. Wikidata batch lookup for entities with QIDs
2. Romanization via pykakasi for remaining Japanese labels
Target: >= 60% English label coverage.
"""
import sqlite3
import requests
import time
import pykakasi

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
HEADERS = {'User-Agent': 'japan-culture-mcp/0.3 (contact@example.com)'}


def batch_wikidata_labels(qids):
    """Fetch English labels for a batch of QIDs from Wikidata."""
    if not qids:
        return {}
    ids_str = '|'.join(qids)
    params = {
        'action': 'wbgetentities',
        'ids': ids_str,
        'props': 'labels',
        'languages': 'en|ja',
        'format': 'json',
    }
    try:
        resp = requests.get(WIKIDATA_API, params=params, headers=HEADERS, timeout=60)
        if resp.status_code == 200:
            data = resp.json()
            results = {}
            for qid, entity in data.get('entities', {}).items():
                labels = entity.get('labels', {})
                en_label = labels.get('en', {}).get('value', '')
                if en_label:
                    results[qid] = en_label
            return results
    except Exception as e:
        print(f"    Wikidata API error: {e}", flush=True)
    return {}


def romanize_japanese(text, kakasi):
    """Convert Japanese text to romaji."""
    if not text:
        return ''
    result = kakasi.convert(text)
    parts = []
    for item in result:
        hepburn = item.get('hepburn', item.get('orig', ''))
        if hepburn:
            parts.append(hepburn)
    return ' '.join(parts).strip()


def main():
    db = sqlite3.connect(DB_PATH)

    total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    has_en = db.execute("SELECT COUNT(*) FROM entities WHERE label_en IS NOT NULL AND label_en != ''").fetchone()[0]
    print(f"Before: {has_en:,}/{total:,} ({100*has_en/total:.1f}%)", flush=True)

    # Step 1: Wikidata batch lookup
    print("\n=== Step 1: Wikidata English Labels ===", flush=True)
    rows = db.execute("""
        SELECT id, wikidata_id FROM entities
        WHERE wikidata_id IS NOT NULL
        AND (label_en IS NULL OR label_en = '')
    """).fetchall()
    print(f"  Entities needing English labels (with QIDs): {len(rows):,}", flush=True)

    updated_wd = 0
    batch_size = 50
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        qid_map = {row[1]: row[0] for row in batch}
        qids = list(qid_map.keys())

        labels = batch_wikidata_labels(qids)
        for qid, en_label in labels.items():
            eid = qid_map.get(qid)
            if eid and en_label:
                db.execute("UPDATE entities SET label_en = ? WHERE id = ?", (en_label, eid))
                updated_wd += 1

        if (i // batch_size) % 20 == 0:
            db.commit()
            print(f"  Progress: {i+len(batch):,}/{len(rows):,}, updated={updated_wd:,}", flush=True)

        time.sleep(0.5)

    db.commit()
    print(f"  Wikidata English labels updated: {updated_wd:,}", flush=True)

    # Step 2: Romanization for remaining entities
    print("\n=== Step 2: Japanese Romanization ===", flush=True)
    kakasi = pykakasi.kakasi()

    remaining = db.execute("""
        SELECT id, label_ja FROM entities
        WHERE (label_en IS NULL OR label_en = '')
        AND label_ja IS NOT NULL AND label_ja != ''
    """).fetchall()
    print(f"  Entities needing romanization: {len(remaining):,}", flush=True)

    updated_romaji = 0
    for i, (eid, label_ja) in enumerate(remaining):
        romaji = romanize_japanese(label_ja, kakasi)
        if romaji and romaji != label_ja:
            # Capitalize first letter of each word for readability
            romaji_cap = ' '.join(w.capitalize() for w in romaji.split())
            db.execute("UPDATE entities SET label_en = ? WHERE id = ?", (romaji_cap, eid))
            updated_romaji += 1

        if (i + 1) % 10000 == 0:
            db.commit()
            print(f"  Progress: {i+1:,}/{len(remaining):,}, updated={updated_romaji:,}", flush=True)

    db.commit()
    print(f"  Romanized labels: {updated_romaji:,}", flush=True)

    # Final stats
    has_en_after = db.execute("SELECT COUNT(*) FROM entities WHERE label_en IS NOT NULL AND label_en != ''").fetchone()[0]
    print(f"\n=== English Label Completion ===", flush=True)
    print(f"Before: {has_en:,}/{total:,} ({100*has_en/total:.1f}%)", flush=True)
    print(f"After: {has_en_after:,}/{total:,} ({100*has_en_after/total:.1f}%)", flush=True)
    print(f"Updated: Wikidata={updated_wd:,}, Romanized={updated_romaji:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
