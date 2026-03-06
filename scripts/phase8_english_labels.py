"""
Phase 8B-3: English label completion.
Fill in label_en for entities with wikidata_id or anilist_id.
Target: >= 70% of place/person entities have English labels.
"""
import sqlite3
import json
import time
import requests

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.3 (contact@example.com)'
}


def sparql_fetch(query):
    for attempt in range(3):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={'query': query},
                headers=HEADERS,
                timeout=90
            )
            if resp.status_code == 429:
                time.sleep(60 * (attempt + 1))
                continue
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            else:
                print(f"    HTTP {resp.status_code}", flush=True)
                time.sleep(30)
        except Exception as e:
            print(f"    ERROR: {e}", flush=True)
            time.sleep(30)
    return []


def step1_wikidata_english_labels(db):
    """Fetch English labels from Wikidata for entities with wikidata_id."""
    print("\n=== Step 1: Wikidata English Labels ===", flush=True)

    # Get entities with wikidata_id but no label_en
    rows = db.execute("""
        SELECT id, wikidata_id FROM entities
        WHERE wikidata_id IS NOT NULL AND (label_en IS NULL OR label_en = '')
    """).fetchall()

    print(f"  Entities needing English labels: {len(rows):,}", flush=True)

    # Process in batches of 200 (Wikidata VALUES limit)
    batch_size = 200
    updated = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        qids = [r[1] for r in batch]
        qid_to_eid = {r[1]: r[0] for r in batch}

        # Build SPARQL VALUES query
        values_str = " ".join([f"wd:{qid}" for qid in qids])
        query = f"""
SELECT ?item ?itemLabel WHERE {{
  VALUES ?item {{ {values_str} }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""
        bindings = sparql_fetch(query)

        for rec in bindings:
            uri = rec.get('item', {}).get('value', '')
            qid = uri.split('/')[-1] if uri else None
            label_en = rec.get('itemLabel', {}).get('value', '')

            if qid and label_en and not label_en.startswith('Q') and qid in qid_to_eid:
                eid = qid_to_eid[qid]
                db.execute("UPDATE entities SET label_en = ? WHERE id = ?", (label_en, eid))
                updated += 1

        db.commit()

        if (i // batch_size) % 10 == 0:
            print(f"  Progress: {i+len(batch):,}/{len(rows):,}, updated={updated:,}", flush=True)

        time.sleep(5)  # Respect Wikidata rate limits

    print(f"  Wikidata English labels updated: {updated:,}", flush=True)
    return updated


def step2_anilist_english_labels(db):
    """Fill English labels from AniList data for anime/manga."""
    print("\n=== Step 2: AniList English Labels ===", flush=True)

    # Check if we have AniList data files
    import os
    anilist_files = []
    for fname in ['anime.json', 'manga.json']:
        path = f'data/anilist/{fname}'
        if os.path.exists(path):
            anilist_files.append(path)

    if not anilist_files:
        print("  No AniList data files found", flush=True)
        return 0

    # Build title mapping from AniList data
    title_map = {}  # japanese_title -> english_title
    for fpath in anilist_files:
        with open(fpath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for item in data:
            title = item.get('title', {})
            native = title.get('native', '')
            english = title.get('english', '') or title.get('romaji', '')
            if native and english:
                title_map[native] = english

    print(f"  AniList title mappings: {len(title_map):,}", flush=True)

    # Update entities
    rows = db.execute("""
        SELECT id, label_ja FROM entities
        WHERE (label_en IS NULL OR label_en = '')
        AND entity_type = 'work'
    """).fetchall()

    updated = 0
    for eid, label_ja in rows:
        if label_ja in title_map:
            db.execute("UPDATE entities SET label_en = ? WHERE id = ?",
                       (title_map[label_ja], eid))
            updated += 1

    db.commit()
    print(f"  AniList English labels updated: {updated:,}", flush=True)
    return updated


def step3_romanize_persons(db):
    """For persons without English labels, try to get from Wikidata description."""
    print("\n=== Step 3: Person Name Romanization ===", flush=True)

    # Get persons without English labels
    rows = db.execute("""
        SELECT id, wikidata_id FROM entities
        WHERE entity_type = 'person'
        AND wikidata_id IS NOT NULL
        AND (label_en IS NULL OR label_en = '')
    """).fetchall()

    print(f"  Persons needing romanization: {len(rows):,}", flush=True)

    # Batch query for English descriptions (often contain romanized names)
    batch_size = 200
    updated = 0

    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        qids = [r[1] for r in batch]
        qid_to_eid = {r[1]: r[0] for r in batch}

        values_str = " ".join([f"wd:{qid}" for qid in qids])
        query = f"""
SELECT ?item ?itemLabel ?itemDescription WHERE {{
  VALUES ?item {{ {values_str} }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en" . }}
}}
"""
        bindings = sparql_fetch(query)

        for rec in bindings:
            uri = rec.get('item', {}).get('value', '')
            qid = uri.split('/')[-1] if uri else None
            label = rec.get('itemLabel', {}).get('value', '')

            if qid and label and not label.startswith('Q') and qid in qid_to_eid:
                eid = qid_to_eid[qid]
                db.execute("UPDATE entities SET label_en = ? WHERE id = ?", (label, eid))
                updated += 1

        db.commit()

        if (i // batch_size) % 10 == 0:
            print(f"  Progress: {i+len(batch):,}/{len(rows):,}, updated={updated:,}", flush=True)

        time.sleep(5)

    print(f"  Person names romanized: {updated:,}", flush=True)
    return updated


def main():
    db = sqlite3.connect(DB_PATH)

    # Stats before
    total_place = db.execute("SELECT COUNT(*) FROM entities WHERE entity_type IN ('place', 'architecture')").fetchone()[0]
    total_person = db.execute("SELECT COUNT(*) FROM entities WHERE entity_type = 'person'").fetchone()[0]
    en_place = db.execute("SELECT COUNT(*) FROM entities WHERE entity_type IN ('place', 'architecture') AND label_en IS NOT NULL AND label_en != ''").fetchone()[0]
    en_person = db.execute("SELECT COUNT(*) FROM entities WHERE entity_type = 'person' AND label_en IS NOT NULL AND label_en != ''").fetchone()[0]
    total_en = db.execute("SELECT COUNT(*) FROM entities WHERE label_en IS NOT NULL AND label_en != ''").fetchone()[0]
    total_all = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    print(f"Before:", flush=True)
    print(f"  Place with EN: {en_place:,}/{total_place:,} ({en_place/max(total_place,1)*100:.1f}%)", flush=True)
    print(f"  Person with EN: {en_person:,}/{total_person:,} ({en_person/max(total_person,1)*100:.1f}%)", flush=True)
    print(f"  Total with EN: {total_en:,}/{total_all:,} ({total_en/max(total_all,1)*100:.1f}%)", flush=True)

    # Step 1: Wikidata
    upd1 = step1_wikidata_english_labels(db)

    # Step 2: AniList
    upd2 = step2_anilist_english_labels(db)

    # Step 3: Person romanization
    upd3 = step3_romanize_persons(db)

    # Stats after
    en_place_after = db.execute("SELECT COUNT(*) FROM entities WHERE entity_type IN ('place', 'architecture') AND label_en IS NOT NULL AND label_en != ''").fetchone()[0]
    en_person_after = db.execute("SELECT COUNT(*) FROM entities WHERE entity_type = 'person' AND label_en IS NOT NULL AND label_en != ''").fetchone()[0]
    total_en_after = db.execute("SELECT COUNT(*) FROM entities WHERE label_en IS NOT NULL AND label_en != ''").fetchone()[0]

    print(f"\n=== English Label Completion ===", flush=True)
    print(f"Place with EN: {en_place:,} → {en_place_after:,}/{total_place:,} ({en_place_after/max(total_place,1)*100:.1f}%)", flush=True)
    print(f"Person with EN: {en_person:,} → {en_person_after:,}/{total_person:,} ({en_person_after/max(total_person,1)*100:.1f}%)", flush=True)
    print(f"Total with EN: {total_en:,} → {total_en_after:,}/{total_all:,} ({total_en_after/max(total_all,1)*100:.1f}%)", flush=True)
    print(f"Updated: Wikidata={upd1:,}, AniList={upd2:,}, Persons={upd3:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
