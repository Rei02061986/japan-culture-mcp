"""
Phase 14 A1: Wikidata music data expansion.
Japanese singles, albums, musical artists.
Target: +30,000 music entities + 50,000 connections.
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import shutil
import os
from datetime import datetime

ORIG_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p14.db"
WIKIDATA_URL = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/1.0 (teddykmk@gmail.com)"
PAGE_SIZE = 10000
BATCH_SIZE = 1000
SOURCE = "wd_music_p14"


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def sparql_query(query, retries=5):
    for attempt in range(retries):
        try:
            params = urllib.parse.urlencode({
                "query": query,
                "format": "json",
            })
            url = WIKIDATA_URL + "?" + params
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=120) as resp:
                return json.loads(resp.read().decode("utf-8")).get("results", {}).get("bindings", [])
        except Exception as e:
            status = ""
            if hasattr(e, "code"):
                status = f" (HTTP {e.code})"
            print(f"  SPARQL error{status} (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = min(10 * (2 ** attempt), 120)
                if hasattr(e, "code") and e.code in (429, 504):
                    wait = max(wait, 30)
                print(f"  Waiting {wait}s...", flush=True)
                time.sleep(wait)
    return []


def extract_wikidata_id(uri):
    if uri and "/entity/" in uri:
        return uri.split("/entity/")[-1]
    return None


def load_existing_wdids(db):
    existing = set()
    cursor = db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL")
    while True:
        rows = cursor.fetchmany(200000)
        if not rows:
            break
        for (wid,) in rows:
            existing.add(wid)
    return existing


def fetch_music_category(db, existing_wdids, category_name, sparql, entity_type):
    """Fetch a music category with pagination."""
    print(f"\n  --- {category_name} ---", flush=True)
    total_new = 0
    offset = 0
    empty_streak = 0
    batch_pending = 0
    results = []  # (wikidata_id, label_ja, label_en, entity_type)

    while True:
        query = sparql.format(limit=PAGE_SIZE, offset=offset)
        bindings = sparql_query(query)

        if not bindings:
            empty_streak += 1
            if empty_streak >= 2:
                break
            offset += PAGE_SIZE
            time.sleep(1)
            continue

        empty_streak = 0
        page_new = 0

        for b in bindings:
            wid = extract_wikidata_id(b.get("item", {}).get("value", ""))
            if not wid or wid in existing_wdids:
                continue

            label_ja = b.get("labelJa", {}).get("value", "")
            label_en = b.get("labelEn", {}).get("value", "")
            if not label_ja and not label_en:
                continue

            try:
                db.execute(
                    "INSERT OR IGNORE INTO entities (wikidata_id, label_ja, label_en, entity_type, source) VALUES (?, ?, ?, ?, ?)",
                    (wid, label_ja or None, label_en or None, entity_type, SOURCE)
                )
                existing_wdids.add(wid)
                results.append((wid, label_ja, label_en, entity_type))
                page_new += 1
                batch_pending += 1
            except sqlite3.IntegrityError:
                pass

            if batch_pending >= BATCH_SIZE:
                db.commit()
                batch_pending = 0

        total_new += page_new
        offset += PAGE_SIZE

        if total_new % 5000 < PAGE_SIZE:
            print(f"    {category_name}: +{total_new:,} so far (offset {offset:,})", flush=True)

        if len(bindings) < PAGE_SIZE:
            break

        time.sleep(1)

    if batch_pending > 0:
        db.commit()

    print(f"    {category_name}: total +{total_new:,}", flush=True)
    return results


def fetch_performer_links(existing_wdids):
    """Fetch performer-work relationships from Wikidata."""
    print("\n  --- Fetching performer-work links ---", flush=True)
    links = []  # (performer_wid, work_wid)
    offset = 0
    empty_streak = 0

    while True:
        query = f"""
SELECT ?performer ?work WHERE {{
  ?work wdt:P175 ?performer .
  ?performer wdt:P27 wd:Q17 .
  {{?work wdt:P31 wd:Q134556}} UNION {{?work wdt:P31 wd:Q482994}}
}}
LIMIT {PAGE_SIZE} OFFSET {offset}
"""
        bindings = sparql_query(query)
        if not bindings:
            empty_streak += 1
            if empty_streak >= 2:
                break
            offset += PAGE_SIZE
            time.sleep(1)
            continue

        empty_streak = 0
        for b in bindings:
            p_wid = extract_wikidata_id(b.get("performer", {}).get("value", ""))
            w_wid = extract_wikidata_id(b.get("work", {}).get("value", ""))
            if p_wid and w_wid:
                links.append((p_wid, w_wid))

        offset += PAGE_SIZE
        if len(bindings) < PAGE_SIZE:
            break
        time.sleep(1)

    print(f"    Performer-work links found: {len(links):,}", flush=True)
    return links


def fetch_genre_links(existing_wdids):
    """Fetch genre information for works."""
    print("\n  --- Fetching genre links ---", flush=True)
    genre_map = {}  # wid -> genre_wid
    offset = 0
    empty_streak = 0

    while True:
        query = f"""
SELECT ?item ?genre WHERE {{
  {{?item wdt:P31 wd:Q134556}} UNION {{?item wdt:P31 wd:Q482994}}
  ?item wdt:P136 ?genre .
  ?item wdt:P495 wd:Q17 .
}}
LIMIT {PAGE_SIZE} OFFSET {offset}
"""
        bindings = sparql_query(query)
        if not bindings:
            empty_streak += 1
            if empty_streak >= 2:
                break
            offset += PAGE_SIZE
            time.sleep(1)
            continue

        empty_streak = 0
        for b in bindings:
            wid = extract_wikidata_id(b.get("item", {}).get("value", ""))
            gid = extract_wikidata_id(b.get("genre", {}).get("value", ""))
            if wid and gid:
                genre_map[wid] = gid

        offset += PAGE_SIZE
        if len(bindings) < PAGE_SIZE:
            break
        time.sleep(1)

    print(f"    Genre-tagged items: {len(genre_map):,}", flush=True)
    return genre_map


def generate_connections(db, performer_links, genre_map):
    """Generate connections from performer-work and shared-genre links."""
    print("\n  --- Generating connections ---", flush=True)

    # Load existing pairs
    existing_pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for a, b in rows:
            existing_pairs.add((min(a, b), max(a, b)))

    # Build wikidata_id -> entity id mapping for music entities
    wid_to_id = {}
    cursor = db.execute("SELECT id, wikidata_id FROM entities WHERE wikidata_id IS NOT NULL AND source = ?", (SOURCE,))
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for eid, wid in rows:
            wid_to_id[wid] = eid

    # Also get IDs for existing entities that might be performers
    all_music_wids = set()
    for p_wid, w_wid in performer_links:
        all_music_wids.add(p_wid)
        all_music_wids.add(w_wid)
    for wid in genre_map:
        all_music_wids.add(wid)

    if all_music_wids:
        placeholders = ",".join(["?"] * min(len(all_music_wids), 50000))
        wids_batch = list(all_music_wids)[:50000]
        rows = db.execute(
            f"SELECT id, wikidata_id FROM entities WHERE wikidata_id IN ({placeholders})",
            wids_batch
        ).fetchall()
        for eid, wid in rows:
            if wid not in wid_to_id:
                wid_to_id[wid] = eid

    total_conns = 0
    batch_pending = 0

    # Creator-work connections (performer -> work)
    for p_wid, w_wid in performer_links:
        p_id = wid_to_id.get(p_wid)
        w_id = wid_to_id.get(w_wid)
        if not p_id or not w_id:
            continue
        pair = (min(p_id, w_id), max(p_id, w_id))
        if pair in existing_pairs:
            continue
        try:
            db.execute("""
                INSERT OR IGNORE INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     explanation, source, confidence)
                VALUES (?, ?, 'creator_work', 0.7, ?, ?, 0.9)
            """, (pair[0], pair[1], "Performer of work", "wd_music_conn_p14"))
            existing_pairs.add(pair)
            total_conns += 1
            batch_pending += 1
        except sqlite3.IntegrityError:
            pass
        if batch_pending >= BATCH_SIZE:
            db.commit()
            batch_pending = 0

    print(f"    Creator-work connections: {total_conns:,}", flush=True)

    # Shared genre connections
    genre_groups = {}
    for wid, gid in genre_map.items():
        eid = wid_to_id.get(wid)
        if eid:
            if gid not in genre_groups:
                genre_groups[gid] = []
            genre_groups[gid].append(eid)

    genre_conns = 0
    for gid, eids in genre_groups.items():
        if len(eids) < 2:
            continue
        eids_sample = eids[:100]  # Limit per genre to avoid explosion
        for i in range(len(eids_sample)):
            for j in range(i + 1, min(i + 5, len(eids_sample))):
                pair = (min(eids_sample[i], eids_sample[j]), max(eids_sample[i], eids_sample[j]))
                if pair in existing_pairs:
                    continue
                try:
                    db.execute("""
                        INSERT OR IGNORE INTO connections
                            (entity_a_id, entity_b_id, connection_type, serendipity_score,
                             explanation, source, confidence)
                        VALUES (?, ?, 'shared_genre', 0.5, ?, ?, 0.7)
                    """, (pair[0], pair[1], "Same music genre", "wd_music_conn_p14"))
                    existing_pairs.add(pair)
                    genre_conns += 1
                    batch_pending += 1
                except sqlite3.IntegrityError:
                    pass
                if batch_pending >= BATCH_SIZE:
                    db.commit()
                    batch_pending = 0

    if batch_pending > 0:
        db.commit()

    total_conns += genre_conns
    print(f"    Shared genre connections: {genre_conns:,}", flush=True)
    print(f"    Total connections: {total_conns:,}", flush=True)
    return total_conns


def main():
    print("=" * 60, flush=True)
    print("Phase 14 A1: Wikidata Music Expansion", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp for heavy writes
    print(f"Copying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()
    before_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    before_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"Entities before: {before_entities:,}", flush=True)
    print(f"Connections before: {before_conns:,}", flush=True)

    existing_wdids = load_existing_wdids(db)
    print(f"Existing wikidata_ids: {len(existing_wdids):,}", flush=True)

    # Category SPARQL queries
    categories = [
        ("Japanese Singles (Q134556)", """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31 wd:Q134556 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""", "music"),
        ("Japanese Albums (Q482994)", """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31 wd:Q482994 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""", "music"),
        ("Japanese Musical Artists (P27=Q17)", """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P106/wdt:P279* wd:Q639669 .
  ?item wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""", "person"),
        ("Japanese Bands", """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31 wd:Q215380 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""", "organization"),
        ("Japanese Songs", """
SELECT ?item ?labelJa ?labelEn WHERE {{
  ?item wdt:P31 wd:Q7366 .
  ?item wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item rdfs:label ?labelJa . FILTER(LANG(?labelJa) = "ja") }}
  OPTIONAL {{ ?item rdfs:label ?labelEn . FILTER(LANG(?labelEn) = "en") }}
}}
LIMIT {limit} OFFSET {offset}
""", "music"),
    ]

    all_results = []
    for cat_name, cat_sparql, cat_type in categories:
        results = fetch_music_category(db, existing_wdids, cat_name, cat_sparql, cat_type)
        all_results.extend(results)

    # Fetch performer-work and genre links for connections
    performer_links = fetch_performer_links(existing_wdids)
    genre_map = fetch_genre_links(existing_wdids)
    total_conns = generate_connections(db, performer_links, genre_map)

    after_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    after_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]

    print(f"\n{'='*60}", flush=True)
    print("SUMMARY", flush=True)
    print(f"  New entities: +{after_entities - before_entities:,}", flush=True)
    print(f"  New connections: +{after_conns - before_conns:,}", flush=True)
    print(f"  Entities: {before_entities:,} -> {after_entities:,}", flush=True)
    print(f"  Connections: {before_conns:,} -> {after_conns:,}", flush=True)
    print(f"  Duration: {datetime.now() - start}", flush=True)
    db.close()

    # Copy back
    print(f"Copying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
