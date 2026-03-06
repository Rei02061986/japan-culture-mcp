"""
Phase 13: NDL Search OpenSearch API - Bulk book import by NDC codes.
Target: 50,000+ new book entities.

NDC codes covering Japanese culture domains:
  721 (日本画), 723 (洋画), 730 (版画), 750 (工芸), 760 (音楽)
  770 (演劇・映画), 790 (諸芸・娯楽)
  910 (日本文学), 911 (詩歌), 913 (小説), 914 (評論)
  210 (日本史), 291 (日本地理)
  380 (風俗), 386 (祭礼), 388 (伝説・民話)
"""
import sqlite3
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import time
import re

DB_PATH = "/tmp/culture_ontology_work.db"
NDL_ENDPOINT = "https://ndlsearch.ndl.go.jp/api/opensearch"
BATCH_SIZE = 5000
RATE_LIMIT = 1.0  # seconds between requests
MAX_PER_NDC = 5000
RESULTS_PER_PAGE = 200

# Namespaces used in OpenSearch/RSS/DC responses
NS = {
    'rss': '',  # RSS elements are unnamespaced
    'dc': 'http://purl.org/dc/elements/1.1/',
    'dcterms': 'http://purl.org/dc/terms/',
    'dcndl': 'http://ndl.go.jp/dcndl/terms/',
    'openSearch': 'http://a9.com/-/spec/opensearchrss/1.0/',
    'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
}

NDC_CODES = [
    ("721", "日本画"),
    ("723", "洋画"),
    ("730", "版画"),
    ("750", "工芸"),
    ("760", "音楽"),
    ("770", "演劇・映画"),
    ("790", "諸芸・娯楽"),
    ("910", "日本文学"),
    ("911", "詩歌"),
    ("913", "小説"),
    ("914", "評論"),
    ("210", "日本史"),
    ("291", "日本地理"),
    ("380", "風俗"),
    ("386", "祭礼"),
    ("388", "伝説・民話"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def fetch_opensearch(ndc_code, offset, retries=3):
    """Fetch a page of results from NDL OpenSearch API.

    Returns parsed XML root or None on failure.
    """
    params = urllib.parse.urlencode({
        "ndc": ndc_code,
        "cnt": RESULTS_PER_PAGE,
        "idx": offset + 1,  # NDL uses 1-based indexing
    })
    url = f"{NDL_ENDPOINT}?{params}"

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "japan-culture-mcp/0.9 (teddykmk@gmail.com)",
                "Accept": "application/xml",
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
                # Parse XML
                root = ET.fromstring(raw)
                return root
        except Exception as e:
            print(f"    Fetch error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"    Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
    return None


def parse_items(root):
    """Parse OpenSearch RSS/Atom response and extract items.

    Returns list of dicts with title, creator, link fields.
    """
    items = []

    # NDL OpenSearch returns RSS 2.0 format
    # Structure: <rss><channel><item>...</item></channel></rss>
    # or sometimes Atom-like with namespaces

    # Try RSS items first
    for item in root.iter("item"):
        rec = {"title": None, "creator": None, "link": None}

        # Title: try <title> or <dc:title>
        title_el = item.find("title")
        if title_el is not None and title_el.text:
            rec["title"] = title_el.text.strip()
        else:
            dc_title = item.find(f"{{{NS['dc']}}}title")
            if dc_title is not None and dc_title.text:
                rec["title"] = dc_title.text.strip()

        # Creator: <dc:creator> or <author>
        dc_creator = item.find(f"{{{NS['dc']}}}creator")
        if dc_creator is not None and dc_creator.text:
            rec["creator"] = dc_creator.text.strip()
        else:
            author_el = item.find("author")
            if author_el is not None and author_el.text:
                rec["creator"] = author_el.text.strip()

        # Link/identifier for NDL ID
        link_el = item.find("link")
        if link_el is not None and link_el.text:
            rec["link"] = link_el.text.strip()
        else:
            guid_el = item.find("guid")
            if guid_el is not None and guid_el.text:
                rec["link"] = guid_el.text.strip()

        if rec["title"]:
            items.append(rec)

    return items


def extract_ndl_id(link):
    """Extract NDL ID from a link URL like https://ndlsearch.ndl.go.jp/books/...."""
    if not link:
        return None
    # Try to extract the NDL biblio ID
    m = re.search(r'/(\w+\d+)(?:\?|$)', link)
    if m:
        return m.group(1)
    # Fallback: use the whole link as identifier
    return link


def clean_title(title):
    """Clean up a title string."""
    if not title:
        return None
    # Remove excessive whitespace
    title = re.sub(r'\s+', ' ', title).strip()
    # Skip if too short or too long
    if len(title) < 2 or len(title) > 300:
        return None
    return title


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70, flush=True)
    print("Phase 13: NDL Search OpenSearch API - Bulk Book Import", flush=True)
    print("=" * 70, flush=True)

    db = open_db()

    # Load existing labels for dedup
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    # Also load existing wikidata_ids for completeness
    existing_wdids = set()
    for row in db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL"):
        existing_wdids.add(row[0])
    print(f"Existing wikidata_ids: {len(existing_wdids):,}", flush=True)

    grand_total = 0
    ndc_stats = {}

    for ndc_code, ndc_name in NDC_CODES:
        print(f"\n{'='*60}", flush=True)
        print(f"NDC {ndc_code}: {ndc_name}", flush=True)
        print(f"{'='*60}", flush=True)

        ndc_new = 0
        offset = 0
        batch_count = 0
        empty_pages = 0
        max_empty = 3  # Stop after 3 consecutive empty pages

        while ndc_new < MAX_PER_NDC:
            root = fetch_opensearch(ndc_code, offset)
            if root is None:
                print(f"  Failed to fetch at offset {offset}, stopping NDC {ndc_code}",
                      flush=True)
                break

            items = parse_items(root)

            if not items:
                empty_pages += 1
                if empty_pages >= max_empty:
                    print(f"  {max_empty} consecutive empty pages, stopping NDC {ndc_code}",
                          flush=True)
                    break
                offset += RESULTS_PER_PAGE
                time.sleep(RATE_LIMIT)
                continue

            empty_pages = 0  # Reset on non-empty page
            page_new = 0

            for rec in items:
                if ndc_new >= MAX_PER_NDC:
                    break

                title = clean_title(rec["title"])
                if not title:
                    continue

                # Dedup by label
                if title in existing_labels:
                    continue

                # Extract NDL ID
                ndl_id = extract_ndl_id(rec.get("link"))

                # Insert entity
                try:
                    cur = db.execute("""
                        INSERT OR IGNORE INTO entities
                            (label_ja, entity_type, source, ndl_id)
                        VALUES (?, 'work', 'ndl_phase13', ?)
                    """, (title, ndl_id))

                    if cur.rowcount > 0:
                        existing_labels.add(title)
                        ndc_new += 1
                        page_new += 1
                        batch_count += 1

                        if batch_count >= BATCH_SIZE:
                            db_commit_retry(db)
                            batch_count = 0
                            print(f"    Committed batch. NDC {ndc_code}: "
                                  f"{ndc_new:,} new (total: {grand_total + ndc_new:,})",
                                  flush=True)
                except sqlite3.IntegrityError:
                    continue

            offset += RESULTS_PER_PAGE

            # Progress every 5 pages
            page_num = offset // RESULTS_PER_PAGE
            if page_num % 5 == 0:
                print(f"    Page {page_num}: +{page_new} this page, "
                      f"{ndc_new:,} total for NDC {ndc_code}", flush=True)

            # Rate limit
            time.sleep(RATE_LIMIT)

        # Commit remaining
        if batch_count > 0:
            db_commit_retry(db)

        grand_total += ndc_new
        ndc_stats[f"{ndc_code} ({ndc_name})"] = ndc_new
        print(f"  NDC {ndc_code} ({ndc_name}): +{ndc_new:,} "
              f"(running total: {grand_total:,})", flush=True)

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_works = db.execute(
        "SELECT COUNT(*) FROM entities WHERE entity_type='work'"
    ).fetchone()[0]
    ndl_phase13 = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source='ndl_phase13'"
    ).fetchone()[0]

    print(f"\n{'='*70}", flush=True)
    print("SUMMARY: Phase 13 NDL Books", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  New book entities (this run): +{grand_total:,}", flush=True)
    print(f"  Total entities in DB:          {total_entities:,}", flush=True)
    print(f"  Total 'work' entities:         {total_works:,}", flush=True)
    print(f"  NDL Phase 13 entities:         {ndl_phase13:,}", flush=True)

    print(f"\n  By NDC code:", flush=True)
    for ndc_label, count in ndc_stats.items():
        print(f"    {ndc_label}: {count:,}", flush=True)

    if grand_total >= 50000:
        print(f"\n  TARGET REACHED (50,000+ books)!", flush=True)
    else:
        print(f"\n  Gap to target: {50000 - grand_total:,}", flush=True)

    db.close()
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
