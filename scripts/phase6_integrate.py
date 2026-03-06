"""
Phase 6B: Entity integration and deduplication.
Merges data from all sources into SQLite.
"""

import sqlite3
import json
import os
import sys
from typing import Dict, List, Optional, Set
from difflib import SequenceMatcher

DB_PATH = "ontology/culture_ontology.db"

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[Integrate] {msg}\n")

def log_error(msg):
    print(f"ERROR: {msg}", flush=True)
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[Integrate] {msg}\n")

class EntityIndex:
    """Fast deduplication index."""

    def __init__(self):
        self.exact = set()       # exact label matches
        self.normalized = {}     # normalized label -> original label

    def add(self, label):
        self.exact.add(label)
        norm = self._normalize(label)
        self.normalized[norm] = label

    def is_duplicate(self, label):
        if label in self.exact:
            return True
        norm = self._normalize(label)
        if norm in self.normalized:
            return True
        return False

    def _normalize(self, label):
        """Normalize for comparison: lowercase, strip whitespace, remove common suffixes."""
        s = label.strip()
        # Remove common brackets/parentheses content
        for start, end in [('（', '）'), ('(', ')'), ('【', '】')]:
            while start in s and end in s:
                i = s.index(start)
                j = s.index(end, i)
                s = s[:i] + s[j+1:]
        return s.strip()


def load_existing(db):
    """Load existing entities into index."""
    idx = EntityIndex()
    rows = db.execute("SELECT label_ja FROM entities").fetchall()
    for row in rows:
        if row[0]:
            idx.add(row[0])
    return idx


def ensure_columns(db):
    """Add missing columns."""
    cols = {row[1] for row in db.execute("PRAGMA table_info(entities)").fetchall()}
    if 'anilist_id' not in cols:
        db.execute("ALTER TABLE entities ADD COLUMN anilist_id INTEGER")
    if 'source' not in cols:
        db.execute("ALTER TABLE entities ADD COLUMN source TEXT DEFAULT 'phase3'")
    db.commit()


def integrate_madb(db, idx):
    """Integrate MADB data."""
    if not os.path.exists('data/madb'):
        log("  MADB: no data directory, skipping")
        return 0

    CLASS_MAP = {
        'manga_series': ('work', 'manga'),
        'anime_series': ('work', 'anime_tv'),
        'anime_movie': ('work', 'anime_movie'),
        'game_work': ('work', 'game'),
        'media_art': ('work', 'media_art'),
    }

    new_count = 0
    for filename in sorted(os.listdir('data/madb')):
        if not filename.endswith('.json'):
            continue
        filepath = os.path.join('data/madb', filename)
        class_name = filename.replace('.json', '')
        entity_type, medium = CLASS_MAP.get(class_name, ('work', 'unknown'))

        with open(filepath, encoding='utf-8') as f:
            records = json.load(f)

        batch = []
        for rec in records:
            name = rec.get('name', {}).get('value', '')
            if not name or len(name) < 2 or idx.is_duplicate(name):
                continue

            madb_uri = rec.get('item', {}).get('value', '')
            date = rec.get('datePublished', {}).get('value', '')
            creator = rec.get('creator', {}).get('value', '')

            batch.append((name, entity_type, madb_uri, 'madb_phase6'))
            idx.add(name)
            new_count += 1

        # Batch insert
        db.executemany(
            "INSERT INTO entities (label_ja, entity_type, madb_id, source) VALUES (?, ?, ?, ?)",
            batch
        )
        db.commit()
        log(f"  MADB {class_name}: +{len(batch)} (cumulative new: {new_count})")

    return new_count


def integrate_wikidata(db, idx):
    """Integrate Wikidata data."""
    if not os.path.exists('data/wikidata'):
        log("  Wikidata: no data directory, skipping")
        return 0

    CATEGORY_TYPE = {
        'national_treasures': 'place',
        'important_cultural_properties': 'place',
        'world_heritage_japan': 'place',
        'shrines': 'place',
        'temples': 'place',
        'castles': 'place',
        'anime_works': 'work',
        'manga_works': 'work',
        'ukiyoe_artists': 'person',
        'japanese_festivals': 'event',
        'onsen': 'place',
        'japanese_gardens': 'place',
        'japanese_literature': 'work',
        'video_games_japan': 'work',
        'japanese_musicians': 'person',
        'japanese_directors': 'person',
    }

    new_count = 0
    for filename in sorted(os.listdir('data/wikidata')):
        if not filename.endswith('.json'):
            continue
        filepath = os.path.join('data/wikidata', filename)
        category = filename.replace('.json', '')
        entity_type = CATEGORY_TYPE.get(category, 'place')

        with open(filepath, encoding='utf-8') as f:
            records = json.load(f)

        batch = []
        for rec in records:
            name = rec.get('itemLabel', {}).get('value', '')
            if not name or len(name) < 2 or idx.is_duplicate(name):
                continue

            wikidata_uri = rec.get('item', {}).get('value', '')
            wikidata_id = wikidata_uri.split('/')[-1] if wikidata_uri else None

            coord = rec.get('coord', {}).get('value', '')
            lat, lon = None, None
            if coord and coord.startswith('Point('):
                parts = coord.replace('Point(', '').replace(')', '').split()
                if len(parts) == 2:
                    try:
                        lon, lat = float(parts[0]), float(parts[1])
                    except ValueError:
                        pass

            batch.append((name, entity_type, wikidata_id, lat, lon, 'wikidata_phase6'))
            idx.add(name)
            new_count += 1

        db.executemany(
            "INSERT INTO entities (label_ja, entity_type, wikidata_id, lat, lon, source) VALUES (?, ?, ?, ?, ?, ?)",
            batch
        )
        db.commit()
        log(f"  Wikidata {category}: +{len(batch)} (cumulative new: {new_count})")

    return new_count


def integrate_anilist(db, idx):
    """Integrate AniList data."""
    if not os.path.exists('data/anilist'):
        log("  AniList: no data directory, skipping")
        return 0

    new_count = 0
    for media_type in ['anime', 'manga']:
        filepath = os.path.join('data/anilist', f'{media_type}.json')
        if not os.path.exists(filepath):
            continue

        with open(filepath, encoding='utf-8') as f:
            records = json.load(f)

        batch = []
        for rec in records:
            title = rec.get('title', {})
            name = title.get('native') or title.get('romaji') or title.get('english', '')
            if not name or len(name) < 2 or idx.is_duplicate(name):
                continue

            anilist_id = rec.get('id')
            label_en = title.get('english') or title.get('romaji')

            batch.append((name, label_en, 'work', anilist_id, 'anilist_phase6'))
            idx.add(name)
            new_count += 1

        db.executemany(
            "INSERT INTO entities (label_ja, label_en, entity_type, anilist_id, source) VALUES (?, ?, ?, ?, ?)",
            batch
        )
        db.commit()
        log(f"  AniList {media_type}: +{len(batch)} (cumulative new: {new_count})")

    return new_count


def integrate_ndl(db, idx):
    """Integrate NDL data."""
    if not os.path.exists('data/ndl'):
        log("  NDL: no data directory, skipping")
        return 0

    new_count = 0
    for filename in sorted(os.listdir('data/ndl')):
        if not filename.endswith('.json'):
            continue
        filepath = os.path.join('data/ndl', filename)

        with open(filepath, encoding='utf-8') as f:
            records = json.load(f)

        batch = []
        for rec in records:
            name = rec.get('title', '')
            if not name or len(name) < 2 or idx.is_duplicate(name):
                continue

            ndl_id = rec.get('pid')

            batch.append((name, 'work', ndl_id, 'ndl_phase6'))
            idx.add(name)
            new_count += 1

        db.executemany(
            "INSERT INTO entities (label_ja, entity_type, ndl_id, source) VALUES (?, ?, ?, ?)",
            batch
        )
        db.commit()
        log(f"  NDL {filename}: +{len(batch)} (cumulative new: {new_count})")

    return new_count


def integrate_jpsearch(db, idx):
    """Integrate JapanSearch data."""
    filepath = 'data/jpsearch/all_themes.json'
    if not os.path.exists(filepath):
        log("  JapanSearch: no data file, skipping")
        return 0

    with open(filepath, encoding='utf-8') as f:
        all_data = json.load(f)

    new_count = 0
    batch = []
    for theme, records in all_data.items():
        for rec in records:
            name = rec.get('label', {}).get('value', '')
            if not name or len(name) < 2 or len(name) > 100 or idx.is_duplicate(name):
                continue

            batch.append((name, 'work', 'jpsearch_phase6'))
            idx.add(name)
            new_count += 1

    db.executemany(
        "INSERT INTO entities (label_ja, entity_type, source) VALUES (?, ?, ?)",
        batch
    )
    db.commit()
    log(f"  JapanSearch: +{len(batch)}")

    return new_count


def main():
    db = sqlite3.connect(DB_PATH)
    ensure_columns(db)

    idx = load_existing(db)
    baseline = len(idx.exact)
    log(f"Existing entities: {baseline:,}")

    madb_count = integrate_madb(db, idx)
    log(f"\nMADB total new: +{madb_count:,}")

    wikidata_count = integrate_wikidata(db, idx)
    log(f"Wikidata total new: +{wikidata_count:,}")

    anilist_count = integrate_anilist(db, idx)
    log(f"AniList total new: +{anilist_count:,}")

    ndl_count = integrate_ndl(db, idx)
    log(f"NDL total new: +{ndl_count:,}")

    jpsearch_count = integrate_jpsearch(db, idx)
    log(f"JapanSearch total new: +{jpsearch_count:,}")

    total_new = madb_count + wikidata_count + anilist_count + ndl_count + jpsearch_count
    total = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    log(f"\n=== Integration Complete ===")
    log(f"New entities: {total_new:,}")
    log(f"Total entities: {total:,}")

    db.close()

if __name__ == "__main__":
    main()
