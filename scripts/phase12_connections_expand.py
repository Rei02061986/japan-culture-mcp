"""
Phase 12 Stream D: Connection density expansion from 264K to 450K+.

Sub-strategies:
  D1: Wikidata structural connections (+100K target)
  D2: Coordinate proximity connections (+20K target)
  D3: Same-source regional cluster connections (+30K target)
  D4: Cross-medium label-match connections (+30K target)
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time
import math
import os

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
UA = "japan-culture-mcp/0.8 (teddykmk@gmail.com)"

BATCH_SIZE = 5000
SPARQL_SLEEP = 5  # seconds between SPARQL queries


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def open_db():
    db = sqlite3.connect(DB_PATH)
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
    """Execute a SPARQL query against Wikidata and return parsed JSON."""
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({
                "query": query,
                "format": "json",
            }).encode()
            req = urllib.request.Request(WIKIDATA_ENDPOINT, data=data, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
            })
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  SPARQL error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                time.sleep(10 * (attempt + 1))
    return None


def extract_qid(uri):
    """Extract Wikidata Q-id from a URI like http://www.wikidata.org/entity/Q12345."""
    if not uri:
        return None
    parts = uri.rsplit("/", 1)
    if len(parts) == 2 and parts[1].startswith("Q"):
        return parts[1]
    return None


def haversine_m(lat1, lon1, lat2, lon2):
    """Return distance in metres between two lat/lon points."""
    R = 6_371_000  # Earth radius in metres
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def load_existing_pairs(db):
    """Load all existing connection pairs as a set of (min_id, max_id)."""
    pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(50000)
        if not rows:
            break
        for a, b in rows:
            pairs.add((min(a, b), max(a, b)))
    return pairs


def insert_connection(db, a_id, b_id, conn_type, confidence, explanation, source,
                      existing_pairs, serendipity=0.5):
    """Insert a single connection if the pair does not already exist.
    Returns True if inserted, False if skipped."""
    pair = (min(a_id, b_id), max(a_id, b_id))
    if pair in existing_pairs:
        return False
    try:
        db.execute("""
            INSERT OR IGNORE INTO connections
                (entity_a_id, entity_b_id, connection_type, serendipity_score,
                 explanation, source, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (pair[0], pair[1], conn_type, serendipity, explanation, source, confidence))
        existing_pairs.add(pair)
        return True
    except sqlite3.IntegrityError:
        return False


# ---------------------------------------------------------------------------
# D1: Wikidata structural connections (+100K target)
# ---------------------------------------------------------------------------

SPARQL_STRATEGIES = [
    # (name, connection_type, sparql_query, explanation)
    (
        "same_creator",
        "same_creator",
        """
SELECT ?a ?b WHERE {
  ?a wdt:P50|wdt:P170 ?creator .
  ?b wdt:P50|wdt:P170 ?creator .
  ?a wdt:P17|wdt:P495 wd:Q17 .
  FILTER(?a != ?b)
}
LIMIT 50000
""",
        "同じ作者/クリエイターによる作品",
    ),
    (
        "same_genre",
        "same_genre",
        """
SELECT ?a ?b WHERE {
  ?a wdt:P136 ?genre .
  ?b wdt:P136 ?genre .
  ?a wdt:P17|wdt:P495 wd:Q17 .
  ?b wdt:P17|wdt:P495 wd:Q17 .
  FILTER(?a != ?b)
}
LIMIT 50000
""",
        "同じジャンルに属する作品",
    ),
    (
        "influenced_by",
        "influenced_by",
        """
SELECT ?a ?b WHERE {
  ?a wdt:P737 ?b .
  { ?a wdt:P17|wdt:P495 wd:Q17 } UNION { ?b wdt:P17|wdt:P495 wd:Q17 }
}
LIMIT 50000
""",
        "影響関係にある作品・人物",
    ),
    (
        "adaptation",
        "adaptation",
        """
SELECT ?a ?b WHERE {
  ?a wdt:P144 ?b .
  { ?a wdt:P17|wdt:P495 wd:Q17 } UNION { ?b wdt:P17|wdt:P495 wd:Q17 }
}
LIMIT 50000
""",
        "原作と翻案の関係",
    ),
    (
        "series_member",
        "series_member",
        """
SELECT ?a ?b WHERE {
  ?a wdt:P179 ?series .
  ?b wdt:P179 ?series .
  { ?a wdt:P17|wdt:P495 wd:Q17 } UNION { ?b wdt:P17|wdt:P495 wd:Q17 }
  FILTER(?a != ?b)
}
LIMIT 50000
""",
        "同じシリーズに属する作品",
    ),
]


def run_d1_wikidata_structural(db, existing_pairs):
    """D1: Create connections from Wikidata structural relationships."""
    print("\n" + "=" * 70, flush=True)
    print("D1: Wikidata structural connections (target +100K)", flush=True)
    print("=" * 70, flush=True)

    # Build wikidata_id → entity_id lookup
    print("  Loading wikidata_id → entity_id mapping...", flush=True)
    wd_to_eid = {}
    rows = db.execute(
        "SELECT id, wikidata_id FROM entities WHERE wikidata_id IS NOT NULL AND wikidata_id != ''"
    ).fetchall()
    for eid, wid in rows:
        wd_to_eid[wid] = eid
    print(f"  Loaded {len(wd_to_eid):,} entities with wikidata_id", flush=True)

    if not wd_to_eid:
        print("  No entities with wikidata_id — skipping D1.", flush=True)
        return 0

    total_d1 = 0

    for name, conn_type, sparql, explanation in SPARQL_STRATEGIES:
        print(f"\n  --- {name} ---", flush=True)
        result = run_sparql(sparql)
        time.sleep(SPARQL_SLEEP)

        if result is None:
            print(f"  SPARQL query failed for {name}, skipping.", flush=True)
            continue

        bindings = result.get("results", {}).get("bindings", [])
        print(f"  SPARQL returned {len(bindings):,} pairs", flush=True)

        batch_count = 0
        strategy_count = 0

        for row in bindings:
            qid_a = extract_qid(row.get("a", {}).get("value"))
            qid_b = extract_qid(row.get("b", {}).get("value"))
            if not qid_a or not qid_b:
                continue

            eid_a = wd_to_eid.get(qid_a)
            eid_b = wd_to_eid.get(qid_b)
            if eid_a is None or eid_b is None:
                continue
            if eid_a == eid_b:
                continue

            inserted = insert_connection(
                db, eid_a, eid_b, conn_type, 0.8, explanation,
                f"phase12_d1_{name}", existing_pairs, serendipity=0.6,
            )
            if inserted:
                strategy_count += 1
                batch_count += 1
                total_d1 += 1

            if batch_count >= BATCH_SIZE:
                db_commit_retry(db)
                batch_count = 0

        if batch_count > 0:
            db_commit_retry(db)

        print(f"  {name}: +{strategy_count:,} connections", flush=True)

    print(f"\n  D1 total: +{total_d1:,} connections", flush=True)
    return total_d1


# ---------------------------------------------------------------------------
# D2: Coordinate proximity connections (+20K target)
# ---------------------------------------------------------------------------

def run_d2_proximity(db, existing_pairs):
    """D2: Connect nearby entities of different types within 500m.
    Uses grid-based spatial indexing in Python to avoid slow SQL self-join."""
    print("\n" + "=" * 70, flush=True)
    print("D2: Coordinate proximity connections (target +20K)", flush=True)
    print("=" * 70, flush=True)

    TARGET = 20000
    GRID = 0.005  # ~500m grid cells

    # Load all geo entities into grid
    print("  Loading geo entities into grid...", flush=True)
    rows = db.execute("""
        SELECT id, lat, lon, entity_type FROM entities
        WHERE lat IS NOT NULL AND lon IS NOT NULL
    """).fetchall()
    print(f"  Geo entities: {len(rows):,}", flush=True)

    from collections import defaultdict
    grid = defaultdict(list)
    for eid, lat, lon, etype in rows:
        cell = (round(lat / GRID), round(lon / GRID))
        grid[cell].append((eid, lat, lon, etype))

    print(f"  Grid cells: {len(grid):,}", flush=True)

    total_d2 = 0
    batch_count = 0

    for (cx, cy), entities in grid.items():
        if total_d2 >= TARGET:
            break

        # Check this cell + 8 neighbors
        neighbors = []
        for dx in (-1, 0, 1):
            for dy in (-1, 0, 1):
                neighbors.extend(grid.get((cx + dx, cy + dy), []))

        for i, (a_id, a_lat, a_lon, a_type) in enumerate(entities):
            if total_d2 >= TARGET:
                break
            for b_id, b_lat, b_lon, b_type in neighbors:
                if a_id >= b_id or a_type == b_type:
                    continue
                dist = haversine_m(a_lat, a_lon, b_lat, b_lon)
                if dist > 500:
                    continue

                explanation = f"地理的近接（{dist:.0f}m以内）: {a_type}と{b_type}"
                inserted = insert_connection(
                    db, a_id, b_id, "proximity", 0.6, explanation,
                    "phase12_d2_proximity", existing_pairs, serendipity=0.7,
                )
                if inserted:
                    total_d2 += 1
                    batch_count += 1

                if batch_count >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch_count = 0
                    print(f"    ... {total_d2:,} proximity connections", flush=True)

    if batch_count > 0:
        db_commit_retry(db)

    print(f"  D2 total: +{total_d2:,} connections", flush=True)
    return total_d2


# ---------------------------------------------------------------------------
# D3: Same-source regional cluster connections (+30K target)
# ---------------------------------------------------------------------------

def run_d3_regional_cluster(db, existing_pairs):
    """D3: Connect entities from the same source within the same ~0.1-degree grid cell."""
    print("\n" + "=" * 70, flush=True)
    print("D3: Same-source regional cluster connections (target +30K)", flush=True)
    print("=" * 70, flush=True)

    TARGET = 30000

    # Load geo-entities grouped by source and grid cell
    print("  Loading geo-entities by source and grid cell...", flush=True)
    rows = db.execute("""
        SELECT id, source, lat, lon FROM entities
        WHERE lat IS NOT NULL AND lon IS NOT NULL
          AND source IS NOT NULL AND source != ''
        ORDER BY source
    """).fetchall()
    print(f"  Geo-entities: {len(rows):,}", flush=True)

    # Group by (source, grid_cell)
    grid_groups = {}
    for eid, source, lat, lon in rows:
        # ~0.1 degree grid ≈ ~11km cells
        grid_key = (source, round(lat, 1), round(lon, 1))
        grid_groups.setdefault(grid_key, []).append(eid)

    # Only keep groups with 2+ entities (otherwise no pairs possible)
    grid_groups = {k: v for k, v in grid_groups.items() if len(v) >= 2}
    print(f"  Grid groups with 2+ entities: {len(grid_groups):,}", flush=True)

    total_d3 = 0
    batch_count = 0

    for (source, grid_lat, grid_lon), eids in grid_groups.items():
        if total_d3 >= TARGET:
            break

        # Create connections among entities in this grid cell
        # Cap pairs per cell to avoid quadratic explosion
        max_per_cell = min(len(eids), 50)
        cell_eids = eids[:max_per_cell]

        for i in range(len(cell_eids)):
            if total_d3 >= TARGET:
                break
            for j in range(i + 1, len(cell_eids)):
                if total_d3 >= TARGET:
                    break

                a_id, b_id = cell_eids[i], cell_eids[j]
                explanation = f"同一出典・同一地域のクラスタ（{source}）"
                inserted = insert_connection(
                    db, a_id, b_id, "regional_cluster", 0.5, explanation,
                    "phase12_d3_regional", existing_pairs, serendipity=0.4,
                )
                if inserted:
                    total_d3 += 1
                    batch_count += 1

                if batch_count >= BATCH_SIZE:
                    db_commit_retry(db)
                    batch_count = 0

    if batch_count > 0:
        db_commit_retry(db)

    print(f"  D3 total: +{total_d3:,} connections", flush=True)
    return total_d3


# ---------------------------------------------------------------------------
# D4: Cross-medium connections (+30K target)
# ---------------------------------------------------------------------------

def run_d4_cross_medium(db, existing_pairs):
    """D4: Connect entities of different media types that share the same label."""
    print("\n" + "=" * 70, flush=True)
    print("D4: Cross-medium label-match connections (target +30K)", flush=True)
    print("=" * 70, flush=True)

    TARGET = 30000

    # Exact label matches across different entity types
    print("  Querying exact label matches (different entity_type)...", flush=True)
    exact_matches = db.execute("""
        SELECT a.id, a.label_ja, a.entity_type,
               b.id, b.label_ja, b.entity_type
        FROM entities a, entities b
        WHERE a.label_ja = b.label_ja
          AND a.label_ja IS NOT NULL AND a.label_ja != ''
          AND a.entity_type != b.entity_type
          AND a.id < b.id
    """).fetchall()
    print(f"  Exact label matches: {len(exact_matches):,}", flush=True)

    total_d4 = 0
    batch_count = 0

    for a_id, a_label, a_type, b_id, b_label, b_type in exact_matches:
        if total_d4 >= TARGET:
            break

        explanation = f"同名の異メディア作品: {a_type}↔{b_type}「{a_label}」"
        # Truncate explanation if too long
        if len(explanation) > 200:
            explanation = explanation[:197] + "..."

        inserted = insert_connection(
            db, a_id, b_id, "cross_medium", 0.7, explanation,
            "phase12_d4_cross_medium", existing_pairs, serendipity=0.8,
        )
        if inserted:
            total_d4 += 1
            batch_count += 1

        if batch_count >= BATCH_SIZE:
            db_commit_retry(db)
            batch_count = 0

    # Also try label_en matches for entities without label_ja match
    if total_d4 < TARGET:
        print("  Querying English label matches...", flush=True)
        en_matches = db.execute("""
            SELECT a.id, a.label_en, a.entity_type,
                   b.id, b.label_en, b.entity_type
            FROM entities a, entities b
            WHERE a.label_en = b.label_en
              AND a.label_en IS NOT NULL AND a.label_en != ''
              AND a.entity_type != b.entity_type
              AND a.id < b.id
              AND NOT EXISTS (
                  SELECT 1 FROM connections c
                  WHERE (c.entity_a_id = MIN(a.id, b.id)
                     AND c.entity_b_id = MAX(a.id, b.id))
              )
        """).fetchall()
        print(f"  English label matches: {len(en_matches):,}", flush=True)

        for a_id, a_label, a_type, b_id, b_label, b_type in en_matches:
            if total_d4 >= TARGET:
                break

            explanation = f"同名の異メディア作品(EN): {a_type}↔{b_type}「{a_label}」"
            if len(explanation) > 200:
                explanation = explanation[:197] + "..."

            inserted = insert_connection(
                db, a_id, b_id, "cross_medium", 0.7, explanation,
                "phase12_d4_cross_medium_en", existing_pairs, serendipity=0.8,
            )
            if inserted:
                total_d4 += 1
                batch_count += 1

            if batch_count >= BATCH_SIZE:
                db_commit_retry(db)
                batch_count = 0

    if batch_count > 0:
        db_commit_retry(db)

    print(f"  D4 total: +{total_d4:,} connections", flush=True)
    return total_d4


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70, flush=True)
    print("Phase 12 Stream D: Connection density expansion (264K → 450K+)", flush=True)
    print("=" * 70, flush=True)

    db = open_db()

    # Current stats
    conn_count = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    entity_count = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"Current entities:    {entity_count:,}", flush=True)
    print(f"Current connections: {conn_count:,}", flush=True)
    print(f"Target:              450,000+", flush=True)

    # Load existing pairs to avoid duplicates
    print("\nLoading existing connection pairs...", flush=True)
    existing_pairs = load_existing_pairs(db)
    print(f"Loaded {len(existing_pairs):,} existing pairs", flush=True)

    # Run each sub-strategy
    d1_count = run_d1_wikidata_structural(db, existing_pairs)
    d2_count = run_d2_proximity(db, existing_pairs)
    d3_count = run_d3_regional_cluster(db, existing_pairs)
    d4_count = run_d4_cross_medium(db, existing_pairs)

    # Final summary
    new_total = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    total_new = d1_count + d2_count + d3_count + d4_count

    print("\n" + "=" * 70, flush=True)
    print("SUMMARY", flush=True)
    print("=" * 70, flush=True)
    print(f"  D1 (Wikidata structural): +{d1_count:,}", flush=True)
    print(f"  D2 (Proximity):           +{d2_count:,}", flush=True)
    print(f"  D3 (Regional cluster):    +{d3_count:,}", flush=True)
    print(f"  D4 (Cross-medium):        +{d4_count:,}", flush=True)
    print(f"  ─────────────────────────────────", flush=True)
    print(f"  Total new:                +{total_new:,}", flush=True)
    print(f"  Previous connections:      {conn_count:,}", flush=True)
    print(f"  New total connections:     {new_total:,}", flush=True)
    print(f"  Target:                    450,000+", flush=True)

    if new_total >= 450000:
        print("\n  TARGET REACHED!", flush=True)
    else:
        print(f"\n  Gap to target: {450000 - new_total:,}", flush=True)

    db.close()
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
