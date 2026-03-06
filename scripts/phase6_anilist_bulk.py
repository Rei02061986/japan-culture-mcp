"""
Phase 6A3: AniList GraphQL bulk fetch.
50 per page, 0.7s interval. Rate limit: 90 req/min.
"""

ANILIST_URL = "https://graphql.anilist.co"

QUERY = """
query ($page: Int, $perPage: Int, $type: MediaType) {
  Page(page: $page, perPage: $perPage) {
    pageInfo {
      total
      currentPage
      lastPage
      hasNextPage
    }
    media(type: $type, countryOfOrigin: JP, sort: POPULARITY_DESC) {
      id
      title {
        romaji
        english
        native
      }
      format
      status
      seasonYear
      season
      genres
      tags {
        name
        category
        rank
      }
      averageScore
      popularity
      studios(isMain: true) {
        nodes {
          name
        }
      }
      source
    }
  }
}
"""

import requests
import json
import time
import os

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[AniList] {msg}\n")

def log_error(msg):
    print(f"ERROR: {msg}", flush=True)
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[AniList] {msg}\n")

def fetch_page(page, media_type="ANIME", per_page=50):
    for attempt in range(3):
        try:
            resp = requests.post(
                ANILIST_URL,
                json={
                    'query': QUERY,
                    'variables': {
                        'page': page,
                        'perPage': per_page,
                        'type': media_type
                    }
                },
                timeout=30
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get('Retry-After', 60))
                log(f"    429, waiting {retry_after}s...")
                time.sleep(retry_after)
                continue

            if resp.status_code == 200:
                return resp.json()
            else:
                log(f"    HTTP {resp.status_code}, attempt {attempt+1}")
                time.sleep(10)
        except Exception as e:
            log_error(f"page {page}: {e}")
            time.sleep(10)
    return None

def fetch_all(media_type="ANIME"):
    all_media = []
    page = 1

    while True:
        if page % 50 == 1:
            log(f"  [{media_type}] page={page}, total so far: {len(all_media)}...")
        data = fetch_page(page, media_type)

        if not data:
            break

        page_data = data.get('data', {}).get('Page', {})
        page_info = page_data.get('pageInfo', {})
        media_list = page_data.get('media', [])

        if not media_list:
            break

        all_media.extend(media_list)

        if page % 100 == 0:
            log(f"    Progress: {len(all_media)}/{page_info.get('total', '?')}")

        if not page_info.get('hasNextPage', False):
            break

        page += 1
        time.sleep(0.7)

    return all_media

def main():
    os.makedirs('data/anilist', exist_ok=True)

    for media_type in ["ANIME", "MANGA"]:
        log(f"\n=== AniList {media_type} ===")
        results = fetch_all(media_type)

        output_path = f'data/anilist/{media_type.lower()}.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False)
        log(f"  Saved: {output_path} ({len(results):,} records)")

    log("\n=== AniList Bulk Complete ===")

if __name__ == "__main__":
    main()
