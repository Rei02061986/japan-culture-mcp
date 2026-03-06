"""
Phase 13: MusicBrainz API - Japanese artists and recordings import.
Target: 30,000+ music entities (artists + recordings).

Strategy:
  1. Search for Japanese artists (country:JP) — up to 5,000
  2. For high-score artists, fetch their recordings
  3. Insert artists as entity_type='person'
  4. Insert recordings as entity_type='music'
  5. Create connections between artists and their recordings
"""
import sqlite3
import urllib.request
import urllib.parse
import json
import time

DB_PATH = "/tmp/culture_ontology_work.db"
MB_BASE = "https://musicbrainz.org/ws/2"
UA = "JapanCultureMCP/0.9 (teddykmk@gmail.com)"
BATCH_SIZE = 5000
RATE_LIMIT = 1.0  # 1 request per second (MusicBrainz requirement)
MAX_ARTISTS = 5000
ARTIST_SCORE_THRESHOLD = 50  # Only fetch recordings for artists with score >= this
MAX_RECORDINGS_PER_ARTIST = 500


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


def mb_fetch(path, params=None, retries=3):
    """Fetch from MusicBrainz API and return parsed JSON.

    Respects rate limiting by sleeping RATE_LIMIT seconds after each call.
    """
    if params is None:
        params = {}
    params["fmt"] = "json"

    query_string = urllib.parse.urlencode(params)
    url = f"{MB_BASE}/{path}?{query_string}"

    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": UA,
                "Accept": "application/json",
            })
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                time.sleep(RATE_LIMIT)
                return data
        except Exception as e:
            print(f"    MB fetch error (attempt {attempt+1}): {e}", flush=True)
            if attempt < retries - 1:
                wait = 5 * (attempt + 1)
                print(f"    Waiting {wait}s before retry...", flush=True)
                time.sleep(wait)
            else:
                time.sleep(RATE_LIMIT)
    return None


def insert_connection(db, a_id, b_id, conn_type, confidence, explanation, source,
                      existing_pairs, serendipity=0.5):
    """Insert a single connection if the pair does not already exist."""
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
# Step 1: Search for Japanese artists
# ---------------------------------------------------------------------------

def fetch_japanese_artists():
    """Search MusicBrainz for Japanese artists, returning up to MAX_ARTISTS."""
    print(f"\n{'='*60}", flush=True)
    print("Step 1: Searching for Japanese artists (country:JP)", flush=True)
    print(f"{'='*60}", flush=True)

    artists = []
    offset = 0
    limit = 100

    while len(artists) < MAX_ARTISTS:
        data = mb_fetch("artist", {
            "query": "country:JP",
            "limit": limit,
            "offset": offset,
        })

        if data is None:
            print(f"  Failed at offset {offset}, stopping artist search.", flush=True)
            break

        artist_list = data.get("artists", [])
        if not artist_list:
            print(f"  No more artists at offset {offset}.", flush=True)
            break

        for a in artist_list:
            if len(artists) >= MAX_ARTISTS:
                break
            mbid = a.get("id")
            name = a.get("name", "").strip()
            sort_name = a.get("sort-name", "").strip()
            score = a.get("score", 0)

            if not mbid or not name or len(name) < 2:
                continue

            artists.append({
                "mbid": mbid,
                "name": name,
                "sort_name": sort_name,
                "score": score,
            })

        offset += limit
        print(f"  Fetched {len(artists):,} artists so far (offset={offset})...",
              flush=True)

        # MusicBrainz search API returns at most ~10,000 results
        if len(artist_list) < limit:
            break

    print(f"  Total artists found: {len(artists):,}", flush=True)
    return artists


# ---------------------------------------------------------------------------
# Step 2: Fetch recordings for high-score artists
# ---------------------------------------------------------------------------

def fetch_recordings_for_artist(mbid, artist_name):
    """Fetch recordings for a single artist by MBID.

    Returns list of recording dicts.
    """
    recordings = []
    offset = 0
    limit = 100

    while len(recordings) < MAX_RECORDINGS_PER_ARTIST:
        data = mb_fetch("recording", {
            "artist": mbid,
            "limit": limit,
            "offset": offset,
        })

        if data is None:
            break

        rec_list = data.get("recordings", [])
        if not rec_list:
            break

        for r in rec_list:
            if len(recordings) >= MAX_RECORDINGS_PER_ARTIST:
                break
            title = r.get("title", "").strip()
            rec_id = r.get("id", "")
            if not title or len(title) < 2:
                continue
            recordings.append({
                "mbid": rec_id,
                "title": title,
            })

        offset += limit

        if len(rec_list) < limit:
            break

    return recordings


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("=" * 70, flush=True)
    print("Phase 13: MusicBrainz - Japanese Artists & Recordings Import", flush=True)
    print("=" * 70, flush=True)

    db = open_db()

    # Load existing labels for dedup
    existing_labels = set()
    for row in db.execute("SELECT label_ja FROM entities WHERE label_ja IS NOT NULL"):
        existing_labels.add(row[0])
    print(f"Existing labels: {len(existing_labels):,}", flush=True)

    # Load existing wikidata_ids (not used for MB but for consistency)
    existing_wdids = set()
    for row in db.execute("SELECT wikidata_id FROM entities WHERE wikidata_id IS NOT NULL"):
        existing_wdids.add(row[0])

    # Load existing connection pairs
    existing_pairs = set()
    try:
        cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
        while True:
            rows = cursor.fetchmany(50000)
            if not rows:
                break
            for a, b in rows:
                existing_pairs.add((min(a, b), max(a, b)))
        print(f"Existing connection pairs: {len(existing_pairs):,}", flush=True)
    except sqlite3.OperationalError:
        print("  Connections table not found or empty, starting fresh.", flush=True)

    # -----------------------------------------------------------------------
    # Step 1: Fetch Japanese artists
    # -----------------------------------------------------------------------
    artists = fetch_japanese_artists()

    # -----------------------------------------------------------------------
    # Step 2-5: Insert artists, fetch recordings, insert recordings, connect
    # -----------------------------------------------------------------------
    print(f"\n{'='*60}", flush=True)
    print("Steps 2-5: Insert artists, fetch recordings, create connections", flush=True)
    print(f"{'='*60}", flush=True)

    total_artists_inserted = 0
    total_recordings_inserted = 0
    total_connections = 0
    batch_count = 0

    # Sort artists by score descending so we process the best first
    artists.sort(key=lambda a: a["score"], reverse=True)

    for idx, artist in enumerate(artists):
        mbid = artist["mbid"]
        name = artist["name"]
        score = artist["score"]

        # Insert artist as person entity (dedup by label)
        artist_eid = None
        if name not in existing_labels:
            try:
                cur = db.execute("""
                    INSERT OR IGNORE INTO entities
                        (label_ja, label_en, entity_type, source)
                    VALUES (?, ?, 'person', 'musicbrainz_p13')
                """, (name, artist.get("sort_name") or name))

                if cur.rowcount > 0:
                    artist_eid = cur.lastrowid
                    existing_labels.add(name)
                    total_artists_inserted += 1
                    batch_count += 1
            except sqlite3.IntegrityError:
                pass

        # If already exists, try to look up the entity id
        if artist_eid is None:
            row = db.execute(
                "SELECT id FROM entities WHERE label_ja = ? LIMIT 1", (name,)
            ).fetchone()
            if row:
                artist_eid = row[0]

        # Only fetch recordings for high-score artists
        if score >= ARTIST_SCORE_THRESHOLD and artist_eid is not None:
            recordings = fetch_recordings_for_artist(mbid, name)

            for rec in recordings:
                title = rec["title"]

                # Dedup by label
                if title in existing_labels:
                    # Still try to create connection if entity exists
                    row = db.execute(
                        "SELECT id FROM entities WHERE label_ja = ? LIMIT 1",
                        (title,)
                    ).fetchone()
                    if row:
                        rec_eid = row[0]
                        explanation = f"アーティストと楽曲: {name}"
                        if len(explanation) > 200:
                            explanation = explanation[:197] + "..."
                        inserted = insert_connection(
                            db, artist_eid, rec_eid, "artist_recording", 0.9,
                            explanation, "musicbrainz_p13_conn", existing_pairs,
                            serendipity=0.5,
                        )
                        if inserted:
                            total_connections += 1
                            batch_count += 1
                    continue

                # Insert recording as music entity
                try:
                    cur = db.execute("""
                        INSERT OR IGNORE INTO entities
                            (label_ja, label_en, entity_type, source)
                        VALUES (?, ?, 'music', 'musicbrainz_p13')
                    """, (title, title))

                    if cur.rowcount > 0:
                        rec_eid = cur.lastrowid
                        existing_labels.add(title)
                        total_recordings_inserted += 1
                        batch_count += 1

                        # Create connection: artist -> recording
                        explanation = f"アーティストと楽曲: {name}"
                        if len(explanation) > 200:
                            explanation = explanation[:197] + "..."
                        inserted = insert_connection(
                            db, artist_eid, rec_eid, "artist_recording", 0.9,
                            explanation, "musicbrainz_p13_conn", existing_pairs,
                            serendipity=0.5,
                        )
                        if inserted:
                            total_connections += 1
                            batch_count += 1
                except sqlite3.IntegrityError:
                    continue

            if batch_count >= BATCH_SIZE:
                db_commit_retry(db)
                batch_count = 0
                print(f"    Committed batch. Artists: {total_artists_inserted:,}, "
                      f"Recordings: {total_recordings_inserted:,}, "
                      f"Connections: {total_connections:,}", flush=True)

        # Progress report every 100 artists
        if (idx + 1) % 100 == 0:
            print(f"  Processed {idx+1:,}/{len(artists):,} artists. "
                  f"New artists: {total_artists_inserted:,}, "
                  f"New recordings: {total_recordings_inserted:,}, "
                  f"Connections: {total_connections:,}", flush=True)

    # Final commit
    if batch_count > 0:
        db_commit_retry(db)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_music = db.execute(
        "SELECT COUNT(*) FROM entities WHERE entity_type='music'"
    ).fetchone()[0]
    total_persons_mb = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source='musicbrainz_p13' AND entity_type='person'"
    ).fetchone()[0]
    total_music_mb = db.execute(
        "SELECT COUNT(*) FROM entities WHERE source='musicbrainz_p13' AND entity_type='music'"
    ).fetchone()[0]

    try:
        total_conn_db = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    except sqlite3.OperationalError:
        total_conn_db = 0

    total_new = total_artists_inserted + total_recordings_inserted

    print(f"\n{'='*70}", flush=True)
    print("SUMMARY: Phase 13 MusicBrainz", flush=True)
    print(f"{'='*70}", flush=True)
    print(f"  New artist entities (person):   +{total_artists_inserted:,}", flush=True)
    print(f"  New recording entities (music):  +{total_recordings_inserted:,}", flush=True)
    print(f"  Total new entities:              +{total_new:,}", flush=True)
    print(f"  New artist-recording connections: +{total_connections:,}", flush=True)
    print(f"  ─────────────────────────────────", flush=True)
    print(f"  Total entities in DB:            {total_entities:,}", flush=True)
    print(f"  Total 'music' entities:          {total_music:,}", flush=True)
    print(f"  MB Phase 13 persons:             {total_persons_mb:,}", flush=True)
    print(f"  MB Phase 13 music:               {total_music_mb:,}", flush=True)
    print(f"  Total connections in DB:         {total_conn_db:,}", flush=True)

    if total_new >= 30000:
        print(f"\n  TARGET REACHED (30,000+ music entities)!", flush=True)
    else:
        print(f"\n  Gap to target: {30000 - total_new:,}", flush=True)

    db.close()
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
