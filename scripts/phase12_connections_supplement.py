"""
Phase 12 Stream D supplement: Close the gap to 450K connections.
Additional strategies:
  D5: Source-based connections (same source → topical link)
  D6: Wider proximity (1km radius instead of 500m)
  D7: Wikidata additional structural queries
  D8: Regional cluster with wider grid (0.2 degree)
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import math
from collections import defaultdict

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/0.8 (teddykmk@gmail.com)"
BATCH_SIZE = 5000


def open_db():
    db = sqlite3.connect(DB_PATH, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def db_commit_retry(db, retries=5):
    for i in range(retries):
        try:
            db.commit()
            return True
        except sqlite3.OperationalError as e:
            print(f"  Commit retry {i+1}: {e}", flush=True)
            time.sleep(3)
    return False


def run_sparql(query, retries=3):
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({"query": query, "format": "json"}).encode()
            req = urllib.request.Request(WIKIDATA_ENDPOINT, data=data, headers={
                "User-Agent": UA, "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  SPARQL error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
    return None


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_existing_pairs(db):
    pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for a, b in rows:
            pairs.add((min(a, b), max(a, b)))
    return pairs


def insert_conn(db, a_id, b_id, conn_type, confidence, explanation, source, pairs, seren=0.5):
    pair = (min(a_id, b_id), max(a_id, b_id))
    if pair in pairs:
        return False
    try:
        db.execute("""
            INSERT OR IGNORE INTO connections
                (entity_a_id, entity_b_id, connection_type, serendipity_score,
                 explanation, source, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (pair[0], pair[1], conn_type, seren, explanation, source, confidence))
        pairs.add(pair)
        return True
    except sqlite3.IntegrityError:
        return False


def d5_source_connections(db, pairs, target=30000):
    """Connect entities from the same specific source (non-geo, based on source string)."""
    print("\n" + "=" * 60, flush=True)
    print(f"D5: Source-based connections (target +{target:,})", flush=True)

    # Get sources with many entities
    sources = db.execute("""
        SELECT source, COUNT(*) as cnt FROM entities
        WHERE source IS NOT NULL
        GROUP BY source HAVING cnt >= 10 AND cnt <= 5000
        ORDER BY cnt DESC LIMIT 200
    """).fetchall()
    print(f"  Sources with 10-5000 entities: {len(sources)}", flush=True)

    total = 0
    batch = 0
    for source, cnt in sources:
        if total >= target:
            break
        eids = [r[0] for r in db.execute(
            "SELECT id FROM entities WHERE source = ? LIMIT 200", (source,)
        )]
        max_per = min(len(eids), 100)
        eids = eids[:max_per]
        for i in range(len(eids)):
            if total >= target:
                break
            for j in range(i + 1, min(i + 5, len(eids))):  # max 5 neighbors per entity
                if total >= target:
                    break
                inserted = insert_conn(
                    db, eids[i], eids[j], "same_source", 0.5,
                    f"同一データソース: {source}", "phase12_d5_source", pairs, 0.4
                )
                if inserted:
                    total += 1
                    batch += 1
                if batch >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch = 0

    if batch > 0:
        db_commit_retry(db)
    print(f"  D5 total: +{total:,}", flush=True)
    return total


def d6_wider_proximity(db, pairs, target=30000):
    """Wider proximity: 1km radius, allow same type too."""
    print("\n" + "=" * 60, flush=True)
    print(f"D6: Wider proximity 1km (target +{target:,})", flush=True)

    GRID = 0.01  # ~1km grid cells

    rows = db.execute("""
        SELECT id, lat, lon, entity_type FROM entities
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).fetchall()
    print(f"  Geo entities: {len(rows):,}", flush=True)

    grid = defaultdict(list)
    for eid, lat, lon, etype in rows:
        cell = (round(lat / GRID), round(lon / GRID))
        grid[cell].append((eid, lat, lon, etype))

    total = 0
    batch = 0
    for (cx, cy), entities in grid.items():
        if total >= target:
            break
        neighbors = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                neighbors.extend(grid.get((cx + dx, cy + dy), []))

        for a_id, a_lat, a_lon, a_type in entities:
            if total >= target:
                break
            for b_id, b_lat, b_lon, b_type in neighbors:
                if a_id >= b_id:
                    continue
                dist = haversine_m(a_lat, a_lon, b_lat, b_lon)
                if dist > 1000 or dist < 10:  # skip very close (already in D2)
                    continue
                explanation = f"地理的近接（{dist:.0f}m以内）"
                inserted = insert_conn(
                    db, a_id, b_id, "proximity_wide", 0.5, explanation,
                    "phase12_d6_proximity_wide", pairs, 0.5
                )
                if inserted:
                    total += 1
                    batch += 1
                if batch >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch = 0
                    print(f"    ... {total:,} wide proximity", flush=True)

    if batch > 0:
        db_commit_retry(db)
    print(f"  D6 total: +{total:,}", flush=True)
    return total


def d7_wikidata_extra(db, pairs):
    """Additional Wikidata structural queries."""
    print("\n" + "=" * 60, flush=True)
    print("D7: Additional Wikidata queries", flush=True)

    wd_to_eid = {}
    for eid, wid in db.execute(
        "SELECT id, wikidata_id FROM entities WHERE wikidata_id IS NOT NULL AND wikidata_id != ''"
    ):
        wd_to_eid[wid] = eid
    print(f"  Wikidata entities: {len(wd_to_eid):,}", flush=True)

    queries = [
        ("same_location", "same_setting", """
SELECT ?a ?b WHERE {
  ?a wdt:P840 ?loc .
  ?b wdt:P840 ?loc .
  FILTER(?a != ?b)
  { ?a wdt:P495 wd:Q17 } UNION { ?b wdt:P495 wd:Q17 }
}
LIMIT 50000""", "同じ舞台設定の作品"),

        ("same_publisher", "same_publisher", """
SELECT ?a ?b WHERE {
  ?a wdt:P123 ?pub .
  ?b wdt:P123 ?pub .
  ?a wdt:P495 wd:Q17 .
  FILTER(?a != ?b)
}
LIMIT 50000""", "同じ出版社の作品"),

        ("same_performer", "same_performer", """
SELECT ?a ?b WHERE {
  ?a wdt:P175 ?perf .
  ?b wdt:P175 ?perf .
  ?a wdt:P495 wd:Q17 .
  FILTER(?a != ?b)
}
LIMIT 50000""", "同じアーティスト/パフォーマーの作品"),

        ("series_part", "series_member", """
SELECT ?a ?b WHERE {
  ?a wdt:P179 ?s .
  ?b wdt:P179 ?s .
  FILTER(?a != ?b)
  { ?a wdt:P495 wd:Q17 } UNION { ?b wdt:P495 wd:Q17 }
}
LIMIT 30000""", "同じシリーズの作品"),

        ("same_movement", "same_movement", """
SELECT ?a ?b WHERE {
  ?a wdt:P135 ?mov .
  ?b wdt:P135 ?mov .
  { ?a wdt:P17|wdt:P495 wd:Q17 } UNION { ?b wdt:P17|wdt:P495 wd:Q17 }
  FILTER(?a != ?b)
}
LIMIT 50000""", "同じ芸術運動の作品"),
    ]

    total = 0
    for name, conn_type, sparql, explanation in queries:
        print(f"\n  --- {name} ---", flush=True)
        result = run_sparql(sparql)
        time.sleep(5)

        if not result:
            print(f"  Failed, skipping.", flush=True)
            continue

        bindings = result.get("results", {}).get("bindings", [])
        print(f"  SPARQL returned {len(bindings):,} pairs", flush=True)

        strat_count = 0
        batch = 0
        for row in bindings:
            qa = (row.get("a", {}).get("value", "") or "").rsplit("/", 1)[-1]
            qb = (row.get("b", {}).get("value", "") or "").rsplit("/", 1)[-1]
            ea = wd_to_eid.get(qa)
            eb = wd_to_eid.get(qb)
            if not ea or not eb or ea == eb:
                continue
            if insert_conn(db, ea, eb, conn_type, 0.7, explanation,
                          f"phase12_d7_{name}", pairs, 0.6):
                strat_count += 1
                batch += 1
                total += 1
            if batch >= BATCH_SIZE:
                db_commit_retry(db)
                batch = 0

        if batch > 0:
            db_commit_retry(db)
        print(f"  {name}: +{strat_count:,}", flush=True)

    print(f"\n  D7 total: +{total:,}", flush=True)
    return total


def d8_wider_regional(db, pairs, target=30000):
    """Wider regional clusters (0.2 degree grid ~ 22km)."""
    print("\n" + "=" * 60, flush=True)
    print(f"D8: Wider regional clusters 0.2° (target +{target:,})", flush=True)

    rows = db.execute("""
        SELECT id, source, lat, lon FROM entities
        WHERE lat IS NOT NULL AND lon IS NOT NULL AND source IS NOT NULL
    """).fetchall()

    grid = defaultdict(list)
    for eid, source, lat, lon in rows:
        key = (source, round(lat * 5) / 5, round(lon * 5) / 5)
        grid[key].append(eid)

    grid = {k: v for k, v in grid.items() if 2 <= len(v) <= 200}
    print(f"  Grid groups: {len(grid):,}", flush=True)

    total = 0
    batch = 0
    for (source, glat, glon), eids in grid.items():
        if total >= target:
            break
        max_per = min(len(eids), 30)
        eids = eids[:max_per]
        for i in range(len(eids)):
            if total >= target:
                break
            for j in range(i + 1, min(i + 4, len(eids))):
                if total >= target:
                    break
                if insert_conn(db, eids[i], eids[j], "regional_cluster_wide", 0.4,
                              f"同一出典・広域クラスタ（{source}）",
                              "phase12_d8_regional_wide", pairs, 0.3):
                    total += 1
                    batch += 1
                if batch >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch = 0

    if batch > 0:
        db_commit_retry(db)
    print(f"  D8 total: +{total:,}", flush=True)
    return total


def main():
    print("=" * 60, flush=True)
    print("Phase 12 Stream D supplement: Closing gap to 450K", flush=True)
    print("=" * 60, flush=True)

    db = open_db()
    conn_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"Current connections: {conn_before:,}", flush=True)
    gap = max(0, 450000 - conn_before)
    print(f"Gap to 450K: {gap:,}", flush=True)

    pairs = load_existing_pairs(db)
    print(f"Existing pairs: {len(pairs):,}", flush=True)

    d5 = d5_source_connections(db, pairs, target=min(30000, gap))
    gap -= d5
    d6 = d6_wider_proximity(db, pairs, target=min(30000, max(gap, 0)))
    gap -= d6
    d7 = d7_wikidata_extra(db, pairs)
    gap -= d7
    d8 = d8_wider_regional(db, pairs, target=min(30000, max(gap, 0)))

    conn_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    total_new = d5 + d6 + d7 + d8

    print(f"\n{'='*60}", flush=True)
    print("SUPPLEMENT SUMMARY", flush=True)
    print(f"  D5 (Same source):     +{d5:,}", flush=True)
    print(f"  D6 (Wide proximity):  +{d6:,}", flush=True)
    print(f"  D7 (Wikidata extra):  +{d7:,}", flush=True)
    print(f"  D8 (Wide regional):   +{d8:,}", flush=True)
    print(f"  Total new:            +{total_new:,}", flush=True)
    print(f"  Previous:             {conn_before:,}", flush=True)
    print(f"  New total:            {conn_after:,}", flush=True)

    if conn_after >= 450000:
        print("  TARGET 450K REACHED!", flush=True)
    else:
        print(f"  Gap remaining: {450000 - conn_after:,}", flush=True)

    db.close()
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
