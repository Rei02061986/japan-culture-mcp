"""
Phase 6A1: MADB SPARQL bulk fetch.
LIMIT/OFFSET paging, 10000 per query, 5s interval.
"""

MADB_ENDPOINT = "https://mediaarts-db.artmuseums.go.jp/sparql"

QUERIES = {
    "manga_series": {
        "query": """
SELECT ?item ?name ?creator ?datePublished ?genre WHERE {{
  ?item a <https://mediaarts-db.artmuseums.go.jp/data/class#MangaBookSeries> .
  OPTIONAL {{ ?item <http://schema.org/name> ?name . }}
  OPTIONAL {{ ?item <http://schema.org/creator> ?creator . }}
  OPTIONAL {{ ?item <http://schema.org/datePublished> ?datePublished . }}
  OPTIONAL {{ ?item <http://schema.org/genre> ?genre . }}
}}
LIMIT 10000
OFFSET {offset}
""",
        "expected_total": 139000,
    },
    "anime_series": {
        "query": """
SELECT ?item ?name ?creator ?datePublished WHERE {{
  ?item a <https://mediaarts-db.artmuseums.go.jp/data/class#AnimationTVRegularSeries> .
  OPTIONAL {{ ?item <http://schema.org/name> ?name . }}
  OPTIONAL {{ ?item <http://schema.org/creator> ?creator . }}
  OPTIONAL {{ ?item <http://schema.org/datePublished> ?datePublished . }}
}}
LIMIT 10000
OFFSET {offset}
""",
        "expected_total": 6000,
    },
    "anime_movie": {
        "query": """
SELECT ?item ?name ?creator ?datePublished WHERE {{
  ?item a <https://mediaarts-db.artmuseums.go.jp/data/class#AnimationMovie> .
  OPTIONAL {{ ?item <http://schema.org/name> ?name . }}
  OPTIONAL {{ ?item <http://schema.org/creator> ?creator . }}
  OPTIONAL {{ ?item <http://schema.org/datePublished> ?datePublished . }}
}}
LIMIT 10000
OFFSET {offset}
""",
        "expected_total": 3000,
    },
    "game_work": {
        "query": """
SELECT ?item ?name ?creator ?datePublished ?platform WHERE {{
  ?item a <https://mediaarts-db.artmuseums.go.jp/data/class#GameWork> .
  OPTIONAL {{ ?item <http://schema.org/name> ?name . }}
  OPTIONAL {{ ?item <http://schema.org/creator> ?creator . }}
  OPTIONAL {{ ?item <http://schema.org/datePublished> ?datePublished . }}
  OPTIONAL {{ ?item <http://schema.org/gamePlatform> ?platform . }}
}}
LIMIT 10000
OFFSET {offset}
""",
        "expected_total": 5000,
    },
    "media_art": {
        "query": """
SELECT ?item ?name ?creator ?startDate ?location WHERE {{
  ?item a <https://mediaarts-db.artmuseums.go.jp/data/class#MediaArtEvent> .
  OPTIONAL {{ ?item <http://schema.org/name> ?name . }}
  OPTIONAL {{ ?item <http://schema.org/creator> ?creator . }}
  OPTIONAL {{ ?item <http://schema.org/startDate> ?startDate . }}
  OPTIONAL {{ ?item <http://schema.org/location> ?location . }}
}}
LIMIT 10000
OFFSET {offset}
""",
        "expected_total": 10000,
    },
}

import requests
import json
import time
import os
import sys

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[MADB] {msg}\n")

def log_error(msg):
    print(f"ERROR: {msg}", flush=True)
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[MADB] {msg}\n")

def fetch_all(class_name, query_template, expected_total):
    all_results = []
    offset = 0

    while True:
        query = query_template.format(offset=offset)
        log(f"  [{class_name}] offset={offset}...")

        try:
            resp = requests.get(
                MADB_ENDPOINT,
                params={'query': query},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=120
            )

            if resp.status_code != 200:
                log(f"    HTTP {resp.status_code}, retrying in 30s...")
                time.sleep(30)
                # retry once
                resp = requests.get(
                    MADB_ENDPOINT,
                    params={'query': query},
                    headers={'Accept': 'application/sparql-results+json'},
                    timeout=120
                )
                if resp.status_code != 200:
                    log_error(f"{class_name} offset={offset}: HTTP {resp.status_code}")
                    break

            data = resp.json()
            bindings = data.get('results', {}).get('bindings', [])

            if not bindings:
                log(f"    No more results. Total: {len(all_results)}")
                break

            all_results.extend(bindings)
            log(f"    Got {len(bindings)}, total so far: {len(all_results)}")

            if len(bindings) < 10000:
                break

            offset += 10000
            time.sleep(5)

        except Exception as e:
            log_error(f"{class_name} offset={offset}: {e}")
            time.sleep(60)
            # retry
            try:
                resp = requests.get(
                    MADB_ENDPOINT,
                    params={'query': query},
                    headers={'Accept': 'application/sparql-results+json'},
                    timeout=120
                )
                if resp.status_code == 200:
                    data = resp.json()
                    bindings = data.get('results', {}).get('bindings', [])
                    if bindings:
                        all_results.extend(bindings)
                        offset += 10000
                        continue
            except:
                pass
            log_error(f"{class_name}: giving up at offset={offset}")
            break

    return all_results

def main():
    os.makedirs('data/madb', exist_ok=True)

    for class_name, config in QUERIES.items():
        log(f"\n=== {class_name} (expected ~{config['expected_total']:,}) ===")
        results = fetch_all(class_name, config['query'], config['expected_total'])

        output_path = f'data/madb/{class_name}.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False)
        log(f"  Saved: {output_path} ({len(results):,} records)")

    log("\n=== MADB Bulk Complete ===")

if __name__ == "__main__":
    main()
