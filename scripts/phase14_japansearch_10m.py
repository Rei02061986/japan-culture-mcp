"""
Phase 14 A4: JapanSearch SPARQL expansion to reach 10M entities.
Strategies:
  1. Fine-grained date-range queries (individual months in uncovered years)
  2. Provider-based queries for large uncovered providers
  3. Type-based queries (schema:additionalType filtering)
Target: +2,700,000 entities (total ~10M from JapanSearch)
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
ENDPOINT = "https://jpsearch.go.jp/rdf/sparql/"
UA = "japan-culture-mcp/1.0 (teddykmk@gmail.com)"
PAGE_SIZE = 10000
BATCH_SIZE = 1000
SOURCE = "jps_p14"


def open_db():
    db = sqlite3.connect(WORK_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def sparql_query(query, retries=3):
    for attempt in range(retries):
        try:
            data = urllib.parse.urlencode({
                "query": query,
            }).encode("utf-8")
            req = urllib.request.Request(ENDPOINT, data=data, headers={
                "User-Agent": UA,
                "Accept": "application/sparql-results+json",
                "Content-Type": "application/x-www-form-urlencoded",
            })
            with urllib.request.urlopen(req, timeout=180) as resp:
                return json.loads(resp.read().decode("utf-8")).get("results", {}).get("bindings", [])
        except Exception as e:
            print(f"    SPARQL error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = 15 * (attempt + 1)
                print(f"    Waiting {wait}s...", flush=True)
                time.sleep(wait)
    return []


def load_existing_labels(db):
    """Load all existing label_ja values for dedup."""
    existing = set()
    cursor = db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL")
    count = 0
    while True:
        rows = cursor.fetchmany(200000)
        if not rows:
            break
        for (label,) in rows:
            existing.add(label)
        count += len(rows)
        if count % 2000000 == 0:
            print(f"    Loaded {count:,} labels...", flush=True)
    return existing


def insert_entity(db, label, entity_type, source, existing):
    """Insert an entity if not duplicate. Returns True if inserted."""
    if not label or label in existing:
        return False
    if label.startswith("Q") and label[1:].isdigit():
        return False
    if len(label) <= 1:
        return False
    try:
        db.execute(
            "INSERT OR IGNORE INTO entities (label_ja, entity_type, source) VALUES (?, ?, ?)",
            (label, entity_type, source)
        )
        existing.add(label)
        return True
    except sqlite3.IntegrityError:
        return False


def strategy_fine_date(db, existing, target=1000000):
    """Fine-grained date queries: individual months within years."""
    print("\n--- Strategy 1: Fine-grained date queries ---", flush=True)

    # Generate monthly ranges for years likely to have large uncovered data
    # Phase 13 covered decade-level ranges; now do month-level for dense years
    date_ranges = []

    # Modern era has most items: monthly granularity for 1868-2025
    for year in range(1868, 2026):
        for month in range(1, 13):
            # Format as YYYY-MM
            start = f"{year}-{month:02d}"
            if month == 12:
                end = f"{year+1}-01"
            else:
                end = f"{year}-{month+1:02d}"
            date_ranges.append((start, end))

    # Pre-modern: yearly for less dense periods
    for year in range(1600, 1868):
        date_ranges.append((str(year), str(year + 1)))

    total = 0
    batch_pending = 0
    errors = 0

    for start_date, end_date in date_ranges:
        if total >= target:
            break

        offset = 0
        empty_streak = 0
        range_new = 0

        while offset < 200000 and total < target:
            query = f"""
SELECT ?item ?label WHERE {{
  ?item schema:datePublished ?date .
  ?item rdfs:label ?label .
  FILTER(?date >= "{start_date}" && ?date < "{end_date}")
  FILTER(STRLEN(?label) > 1)
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
            page_new = 0
            for b in bindings:
                label = b.get("label", {}).get("value", "")
                if insert_entity(db, label, "work", f"{SOURCE}_date", existing):
                    page_new += 1
                    batch_pending += 1

            total += page_new
            range_new += page_new

            if batch_pending >= BATCH_SIZE:
                db.commit()
                batch_pending = 0

            offset += PAGE_SIZE

            if len(bindings) < PAGE_SIZE:
                break

            time.sleep(1)

        if range_new > 0 and range_new >= 100:
            print(f"    {start_date}: +{range_new:,}", flush=True)

        if total % 100000 < 1000 and total > 0:
            print(f"    Progress: +{total:,}", flush=True)

    if batch_pending > 0:
        db.commit()
    print(f"  Strategy 1 total: +{total:,}", flush=True)
    return total


def strategy_provider(db, existing, target=1000000):
    """Query by known JapanSearch providers."""
    print("\n--- Strategy 2: Provider-based queries ---", flush=True)

    # Known large providers on JapanSearch
    # Use provider URIs that are commonly found
    providers = [
        ("ndl", "https://ndl.go.jp/"),
        ("colbase", "https://colbase.nich.go.jp/"),
        ("bunka", "https://kunishitei.bunka.go.jp/"),
        ("madb", "https://mediaarts-db.bunka.go.jp/"),
        ("tobunken", "https://www.tobunken.go.jp/"),
        ("nihu", "https://www.nihu.jp/"),
        ("nijl", "https://www.nijl.ac.jp/"),
        ("rekihaku", "https://www.rekihaku.ac.jp/"),
        ("nao", "https://dbrec.nijl.ac.jp/"),
        ("arc", "https://www.arc.ritsumei.ac.jp/"),
        ("adeac", "https://adeac.jp/"),
        ("cultural", "https://bunka.nii.ac.jp/"),
        ("jmapps", "https://jmapps.ne.jp/"),
        ("archive", "https://www.digital.archives.go.jp/"),
    ]

    total = 0
    batch_pending = 0

    for prov_name, prov_uri in providers:
        if total >= target:
            break

        offset = 0
        empty_streak = 0
        prov_new = 0

        while offset < 500000 and total < target:
            query = f"""
SELECT ?item ?label WHERE {{
  ?item schema:provider ?provider .
  ?item rdfs:label ?label .
  FILTER(CONTAINS(STR(?provider), "{prov_uri}"))
  FILTER(STRLEN(?label) > 1)
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
            page_new = 0
            for b in bindings:
                label = b.get("label", {}).get("value", "")
                if insert_entity(db, label, "work", f"{SOURCE}_prov_{prov_name}", existing):
                    page_new += 1
                    batch_pending += 1

            total += page_new
            prov_new += page_new

            if batch_pending >= BATCH_SIZE:
                db.commit()
                batch_pending = 0

            offset += PAGE_SIZE

            if len(bindings) < PAGE_SIZE:
                break

            time.sleep(1)

        if prov_new > 0:
            print(f"    {prov_name}: +{prov_new:,}", flush=True)

    if batch_pending > 0:
        db.commit()
    print(f"  Strategy 2 total: +{total:,}", flush=True)
    return total


def strategy_type(db, existing, target=1000000):
    """Query by schema:additionalType with extended type list."""
    print("\n--- Strategy 3: Type-based queries ---", flush=True)

    type_queries = [
        ("manuscript", 'FILTER(CONTAINS(STR(?type), "Manuscript") || CONTAINS(STR(?type), "manuscript") || CONTAINS(STR(?type), "写本"))'),
        ("woodblock", 'FILTER(CONTAINS(STR(?type), "Woodblock") || CONTAINS(STR(?type), "woodblock") || CONTAINS(STR(?type), "版画") || CONTAINS(STR(?type), "Print"))'),
        ("painting", 'FILTER(CONTAINS(STR(?type), "Painting") || CONTAINS(STR(?type), "painting") || CONTAINS(STR(?type), "絵画"))'),
        ("sculpture", 'FILTER(CONTAINS(STR(?type), "Sculpture") || CONTAINS(STR(?type), "sculpture") || CONTAINS(STR(?type), "彫刻"))'),
        ("ceramic", 'FILTER(CONTAINS(STR(?type), "Ceramic") || CONTAINS(STR(?type), "ceramic") || CONTAINS(STR(?type), "陶器") || CONTAINS(STR(?type), "陶磁"))'),
        ("textile", 'FILTER(CONTAINS(STR(?type), "Textile") || CONTAINS(STR(?type), "textile") || CONTAINS(STR(?type), "織物") || CONTAINS(STR(?type), "染織"))'),
        ("calligraphy", 'FILTER(CONTAINS(STR(?type), "Calligraphy") || CONTAINS(STR(?type), "calligraphy") || CONTAINS(STR(?type), "書"))'),
        ("document", 'FILTER(CONTAINS(STR(?type), "Document") || CONTAINS(STR(?type), "document") || CONTAINS(STR(?type), "文書") || CONTAINS(STR(?type), "古文書"))'),
        ("record", 'FILTER(CONTAINS(STR(?type), "Record") || CONTAINS(STR(?type), "record") || CONTAINS(STR(?type), "記録"))'),
        ("newspaper", 'FILTER(CONTAINS(STR(?type), "Newspaper") || CONTAINS(STR(?type), "newspaper") || CONTAINS(STR(?type), "新聞"))'),
        ("magazine", 'FILTER(CONTAINS(STR(?type), "Magazine") || CONTAINS(STR(?type), "magazine") || CONTAINS(STR(?type), "雑誌"))'),
        ("poster", 'FILTER(CONTAINS(STR(?type), "Poster") || CONTAINS(STR(?type), "poster") || CONTAINS(STR(?type), "ポスター"))'),
        ("artifact", 'FILTER(CONTAINS(STR(?type), "Artifact") || CONTAINS(STR(?type), "artifact") || CONTAINS(STR(?type), "考古"))'),
        ("lacquer", 'FILTER(CONTAINS(STR(?type), "Lacquer") || CONTAINS(STR(?type), "lacquer") || CONTAINS(STR(?type), "漆"))'),
        ("metalwork", 'FILTER(CONTAINS(STR(?type), "Metal") || CONTAINS(STR(?type), "metal") || CONTAINS(STR(?type), "金工"))'),
    ]

    total = 0
    batch_pending = 0

    for type_name, type_filter in type_queries:
        if total >= target:
            break

        offset = 0
        empty_streak = 0
        type_new = 0

        while offset < 500000 and total < target:
            query = f"""
SELECT ?item ?label WHERE {{
  ?item schema:additionalType ?type .
  ?item rdfs:label ?label .
  {type_filter}
  FILTER(STRLEN(?label) > 1)
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
            page_new = 0

            # Map type name to entity_type
            etype = "work"
            if type_name in ("painting", "woodblock", "calligraphy", "ceramic",
                             "textile", "sculpture", "lacquer", "metalwork"):
                etype = "artifact"

            for b in bindings:
                label = b.get("label", {}).get("value", "")
                if insert_entity(db, label, etype, f"{SOURCE}_type_{type_name}", existing):
                    page_new += 1
                    batch_pending += 1

            total += page_new
            type_new += page_new

            if batch_pending >= BATCH_SIZE:
                db.commit()
                batch_pending = 0

            offset += PAGE_SIZE

            if len(bindings) < PAGE_SIZE:
                break

            time.sleep(1)

        if type_new > 0:
            print(f"    {type_name}: +{type_new:,}", flush=True)

    if batch_pending > 0:
        db.commit()
    print(f"  Strategy 3 total: +{total:,}", flush=True)
    return total


def strategy_label_pagination(db, existing, target=700000):
    """Broad label pagination sweep to fill remaining gap."""
    print("\n--- Strategy 4: Label pagination sweep ---", flush=True)

    # Use different starting characters to discover items missed by other strategies
    # Hiragana blocks, katakana blocks, kanji ranges
    start_chars = [
        # Common kanji prefixes for cultural items
        "新", "大", "日本", "東", "西", "南", "北", "古",
        "国", "美", "文", "天", "地", "人", "山", "川",
        "風", "花", "月", "雪", "春", "夏", "秋", "冬",
        "金", "銀", "石", "木", "水", "火", "土", "白",
        "黒", "赤", "青", "明", "和", "平", "昭", "令",
    ]

    total = 0
    batch_pending = 0

    for char in start_chars:
        if total >= target:
            break

        offset = 0
        empty_streak = 0
        char_new = 0

        while offset < 200000 and total < target:
            query = f"""
SELECT ?item ?label WHERE {{
  ?item rdfs:label ?label .
  FILTER(STRSTARTS(?label, "{char}"))
  FILTER(STRLEN(?label) > 2)
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
            page_new = 0
            for b in bindings:
                label = b.get("label", {}).get("value", "")
                if insert_entity(db, label, "work", f"{SOURCE}_label", existing):
                    page_new += 1
                    batch_pending += 1

            total += page_new
            char_new += page_new

            if batch_pending >= BATCH_SIZE:
                db.commit()
                batch_pending = 0

            offset += PAGE_SIZE

            if len(bindings) < PAGE_SIZE:
                break

            time.sleep(1)

        if char_new > 0:
            print(f"    '{char}': +{char_new:,}", flush=True)

    if batch_pending > 0:
        db.commit()
    print(f"  Strategy 4 total: +{total:,}", flush=True)
    return total


def main():
    print("=" * 60, flush=True)
    print("Phase 14 A4: JapanSearch Expansion to 10M", flush=True)
    print("=" * 60, flush=True)
    start = datetime.now()

    # Copy DB to /tmp
    print(f"Copying DB to {WORK_DB}...", flush=True)
    shutil.copy2(ORIG_DB, WORK_DB)
    print("DB copied.", flush=True)

    db = open_db()
    before = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    jps_before = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source LIKE 'jps%'"
    ).fetchone()[0]
    print(f"Entities before: {before:,}", flush=True)
    print(f"JapanSearch entities before: {jps_before:,}", flush=True)
    print(f"Target: reach {before + 2700000:,} entities ({before:,} + 2,700,000)", flush=True)

    print("\nLoading existing labels for dedup...", flush=True)
    existing = load_existing_labels(db)
    print(f"Existing labels: {len(existing):,}", flush=True)

    t1 = strategy_fine_date(db, existing, target=1000000)
    t2 = strategy_provider(db, existing, target=1000000)
    t3 = strategy_type(db, existing, target=1000000)
    t4 = strategy_label_pagination(db, existing, target=700000)

    after = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    jps_after = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source LIKE 'jps%'"
    ).fetchone()[0]

    total_new = t1 + t2 + t3 + t4

    print(f"\n{'='*60}", flush=True)
    print("SUMMARY", flush=True)
    print(f"  Strategy 1 (Fine date):     +{t1:,}", flush=True)
    print(f"  Strategy 2 (Provider):      +{t2:,}", flush=True)
    print(f"  Strategy 3 (Type):          +{t3:,}", flush=True)
    print(f"  Strategy 4 (Label sweep):   +{t4:,}", flush=True)
    print(f"  Total new:                  +{total_new:,}", flush=True)
    print(f"  Entities: {before:,} -> {after:,}", flush=True)
    print(f"  JapanSearch: {jps_before:,} -> {jps_after:,}", flush=True)
    print(f"  Duration: {datetime.now() - start}", flush=True)

    if after >= 10000000:
        print("  TARGET 10M REACHED!", flush=True)
    else:
        print(f"  Gap to 10M: {10000000 - after:,}", flush=True)

    db.close()

    # Copy back
    print(f"Copying DB back to {ORIG_DB}...", flush=True)
    shutil.copy2(WORK_DB, ORIG_DB)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
