"""
Phase 14 Master Runner — runs all data expansion scripts sequentially.
Uses a single /tmp/ DB copy to avoid repeated 3GB copy operations.

Usage: python3 scripts/phase14_run_all.py
"""
import os
import sys
import shutil
import sqlite3
import time
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ORIG_DB = os.path.join(BASE_DIR, "..", "ontology", "culture_ontology.db")
WORK_DB = "/tmp/culture_ontology_p14.db"

SCRIPTS = [
    "phase14_music.py",
    "phase14_wikidata_sweep.py",
    "phase14_cuisine_expanded.py",
    "phase14_osm_remaining.py",
    "phase14_images.py",
    # phase14_japansearch_10m.py is run separately (very long)
]


def get_counts(db_path):
    db = sqlite3.connect(db_path)
    entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    sources = db.execute("SELECT COUNT(DISTINCT source) FROM entities").fetchone()[0]
    db.close()
    return entities, conns, sources


def main():
    start = datetime.now()
    print("=" * 70)
    print("Phase 14 Master Runner — Data Expansion")
    print("=" * 70)

    # Step 1: Copy DB to /tmp/ if not already there or if original is newer
    if os.path.exists(WORK_DB):
        orig_mtime = os.path.getmtime(ORIG_DB)
        work_mtime = os.path.getmtime(WORK_DB)
        if orig_mtime > work_mtime:
            print("Original DB is newer, copying to /tmp/...")
            shutil.copy2(ORIG_DB, WORK_DB)
        else:
            print(f"Using existing /tmp/ copy ({WORK_DB})")
    else:
        print(f"Copying DB to {WORK_DB}...")
        shutil.copy2(ORIG_DB, WORK_DB)

    e0, c0, s0 = get_counts(WORK_DB)
    print(f"Starting counts: {e0:,} entities, {c0:,} connections, {s0} sources")
    print()

    # Step 2: Patch all scripts to skip DB copy (use existing /tmp/ copy)
    # We do this by setting environment variable
    os.environ["PHASE14_SKIP_COPY"] = "1"
    os.environ["PHASE14_WORK_DB"] = WORK_DB

    # Step 3: Run each script
    for script in SCRIPTS:
        script_path = os.path.join(BASE_DIR, script)
        if not os.path.exists(script_path):
            print(f"SKIP: {script} not found")
            continue

        print(f"\n{'='*70}")
        print(f"Running: {script}")
        print(f"{'='*70}")
        script_start = datetime.now()

        ret = os.system(f'{sys.executable} "{script_path}"')

        elapsed = datetime.now() - script_start
        if ret == 0:
            e, c, s = get_counts(WORK_DB)
            print(f"  Completed in {elapsed}. Counts: {e:,} entities, {c:,} connections, {s} sources")
        else:
            print(f"  FAILED with return code {ret} after {elapsed}")

    # Step 4: Copy back to Google Drive
    print(f"\n{'='*70}")
    print(f"Copying DB back to Google Drive...")
    try:
        shutil.copy2(WORK_DB, ORIG_DB)
        print("Copy successful.")
    except Exception as e:
        print(f"Copy failed: {e}")
        print(f"DB remains at {WORK_DB}")

    # Step 5: Final summary
    ef, cf, sf = get_counts(ORIG_DB)
    total_elapsed = datetime.now() - start
    print(f"\n{'='*70}")
    print("FINAL SUMMARY")
    print(f"{'='*70}")
    print(f"  Entities:    {e0:,} -> {ef:,} (+{ef-e0:,})")
    print(f"  Connections: {c0:,} -> {cf:,} (+{cf-c0:,})")
    print(f"  Sources:     {s0} -> {sf} (+{sf-s0})")
    print(f"  Total time:  {total_elapsed}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
