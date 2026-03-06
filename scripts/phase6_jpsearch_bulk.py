"""
Phase 6A5: JapanSearch SPARQL bulk fetch.
500 per page, 5s interval.
"""

JPS_SPARQL = "https://jpsearch.go.jp/rdf/sparql"

THEMES = [
    "浮世絵", "錦絵", "妖怪", "忍者", "侍", "武士",
    "茶道", "華道", "書道", "能", "歌舞伎", "文楽",
    "祭り", "神社", "仏像", "城", "庭園", "温泉",
    "桜", "紅葉", "富士山", "東海道",
    "源氏物語", "枕草子", "百人一首",
    "俳句", "和歌", "落語", "相撲", "柔道", "剣道",
    "着物", "陶芸", "漆器", "刀剣",
]

QUERY_TEMPLATE = """
SELECT ?item ?label ?type ?provider ?thumbnail WHERE {{
  ?item rdfs:label ?label .
  FILTER(CONTAINS(?label, "{keyword}"))
  OPTIONAL {{ ?item schema:additionalType ?type . }}
  OPTIONAL {{ ?item schema:provider ?provider . }}
  OPTIONAL {{ ?item schema:thumbnail ?thumbnail . }}
}}
LIMIT 500
OFFSET {offset}
"""

import requests
import json
import time
import os

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[JPS] {msg}\n")

def log_error(msg):
    print(f"ERROR: {msg}", flush=True)
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[JPS] {msg}\n")

def fetch_theme(keyword):
    all_results = []
    offset = 0

    while True:
        query = QUERY_TEMPLATE.format(keyword=keyword, offset=offset)

        for attempt in range(3):
            try:
                resp = requests.get(
                    JPS_SPARQL,
                    params={'query': query},
                    headers={'Accept': 'application/sparql-results+json'},
                    timeout=120
                )

                if resp.status_code == 200:
                    data = resp.json()
                    bindings = data.get('results', {}).get('bindings', [])

                    if not bindings:
                        return all_results

                    all_results.extend(bindings)
                    log(f"    [{keyword}] offset={offset}, got {len(bindings)}, total: {len(all_results)}")

                    if len(bindings) < 500:
                        return all_results

                    offset += 500
                    time.sleep(5)
                    break
                else:
                    log(f"    HTTP {resp.status_code}, attempt {attempt+1}")
                    time.sleep(30)
            except Exception as e:
                log_error(f"{keyword} offset={offset}: {e}")
                time.sleep(30)
        else:
            # All 3 attempts failed
            log_error(f"{keyword}: giving up at offset={offset}")
            return all_results

    return all_results

def main():
    os.makedirs('data/jpsearch', exist_ok=True)

    total = 0
    all_results = {}

    for keyword in THEMES:
        log(f"\n=== JapanSearch: {keyword} ===")
        results = fetch_theme(keyword)
        all_results[keyword] = results
        total += len(results)
        log(f"  {keyword}: {len(results):,} records")

    with open('data/jpsearch/all_themes.json', 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False)

    log(f"\n=== JapanSearch Bulk Complete: {total:,} total records ===")

if __name__ == "__main__":
    main()
