"""
Phase 6A2: Wikidata SPARQL bulk fetch.
Uses SERVICE wikibase:label for reliable label resolution.
500 per page, 10s interval.
"""

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.3 (contact@example.com)'
}

CATEGORIES = {
    "shrines": """
SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31 wd:Q845945 ;
        wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "temples": """
SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31 wd:Q160742 ;
        wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "castles": """
SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q744913 ;
        wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "world_heritage_japan": """
SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P1435 wd:Q9259 ;
        wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "anime_tv_series": """
SELECT ?item ?itemLabel ?date WHERE {{
  ?item wdt:P31 wd:Q63952888 ;
        wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P580 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "anime_films": """
SELECT ?item ?itemLabel ?date WHERE {{
  ?item wdt:P31 wd:Q20650540 ;
        wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "manga_series": """
SELECT ?item ?itemLabel ?date WHERE {{
  ?item wdt:P31 wd:Q21198342 ;
        wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "light_novels": """
SELECT ?item ?itemLabel ?date WHERE {{
  ?item wdt:P31 wd:Q747381 ;
        wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "ukiyoe_artists": """
SELECT ?item ?itemLabel ?birth ?death WHERE {{
  ?item wdt:P106 wd:Q1028181 .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  OPTIONAL {{ ?item wdt:P570 ?death . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "japanese_festivals": """
SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q132241 ;
        wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "onsen": """
SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q27185 ;
        wdt:P17 wd:Q17 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "japanese_gardens": """
SELECT ?item ?itemLabel ?coord WHERE {{
  ?item wdt:P31/wdt:P279* wd:Q15107753 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "video_games_japan": """
SELECT ?item ?itemLabel ?date WHERE {{
  ?item wdt:P31 wd:Q7889 ;
        wdt:P495 wd:Q17 .
  OPTIONAL {{ ?item wdt:P577 ?date . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "japanese_directors": """
SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q2526255 ;
        wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "japanese_voice_actors": """
SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q214917 ;
        wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "japanese_writers": """
SELECT ?item ?itemLabel ?birth WHERE {{
  ?item wdt:P106 wd:Q36180 ;
        wdt:P27 wd:Q17 .
  OPTIONAL {{ ?item wdt:P569 ?birth . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "prefectures": """
SELECT ?item ?itemLabel ?coord ?population WHERE {{
  ?item wdt:P31 wd:Q50337 .
  OPTIONAL {{ ?item wdt:P625 ?coord . }}
  OPTIONAL {{ ?item wdt:P1082 ?population . }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
    "traditional_performing_arts": """
SELECT ?item ?itemLabel WHERE {{
  VALUES ?type {{ wd:Q191159 wd:Q267285 wd:Q233838 wd:Q193355 }}
  ?item wdt:P31 ?type .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "ja,en" . }}
}}
LIMIT 500
OFFSET {offset}
""",
}

import requests
import json
import time
import os

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[Wikidata] {msg}\n")

def log_error(msg):
    print(f"ERROR: {msg}", flush=True)
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[Wikidata] {msg}\n")

def sparql_fetch(query, offset=0):
    q = query.format(offset=offset)
    for attempt in range(3):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={'query': q},
                headers=HEADERS,
                timeout=90
            )
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                log(f"    429 rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            elif resp.status_code == 500:
                log(f"    500 server error (query too heavy?), attempt {attempt+1}")
                time.sleep(30)
            else:
                log(f"    HTTP {resp.status_code}, attempt {attempt+1}")
                time.sleep(30)
        except requests.exceptions.Timeout:
            log(f"    Timeout, attempt {attempt+1}")
            time.sleep(30)
        except Exception as e:
            log_error(f"sparql_fetch offset={offset}: {e}")
            time.sleep(30)
    return []

def fetch_category(name, query):
    all_results = []
    offset = 0
    max_offset = 10000  # Safety limit

    while offset < max_offset:
        log(f"  [{name}] offset={offset}...")
        bindings = sparql_fetch(query, offset)

        if not bindings:
            break

        all_results.extend(bindings)
        log(f"    Got {len(bindings)}, total: {len(all_results)}")

        if len(bindings) < 500:
            break

        offset += 500
        time.sleep(10)

    return all_results

def main():
    os.makedirs('data/wikidata', exist_ok=True)

    total = 0
    for name, query in CATEGORIES.items():
        log(f"\n=== {name} ===")
        results = fetch_category(name, query)

        output_path = f'data/wikidata/{name}.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False)
        log(f"  Saved: {output_path} ({len(results):,} records)")
        total += len(results)

    log(f"\n=== Wikidata Bulk Complete: {total:,} total records ===")

if __name__ == "__main__":
    main()
