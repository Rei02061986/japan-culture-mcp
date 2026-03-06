"""Phase 3 Task B: Wikidata ID Mapping — Anime, Manga, People, Places
Uses REST API (wbsearchentities + EntityData) to avoid SPARQL timeouts.
Falls back to SPARQL with aggressive pagination for bulk queries.
Python 3.8 compatible.
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

BASE_DIR = Path(__file__).parent.parent
ONTOLOGY_DIR = BASE_DIR / "ontology"
ONTOLOGY_DIR.mkdir(parents=True, exist_ok=True)

UA = "japan-culture-mcp/0.2 (teddykmk@gmail.com)"
TIMEOUT = 60
MAX_RETRIES = 3
SLEEP_BETWEEN = 5


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {path.name}")


def sparql_query_with_retry(query, retries=MAX_RETRIES):
    """Execute SPARQL query with retries and timeout handling"""
    for attempt in range(retries):
        try:
            resp = requests.get(
                "https://query.wikidata.org/sparql",
                params={"query": query},
                headers={
                    "Accept": "application/sparql-results+json",
                    "User-Agent": UA,
                },
                timeout=TIMEOUT,
            )
            if resp.status_code == 429:
                wait = min(30, SLEEP_BETWEEN * (attempt + 1) * 2)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.Timeout:
            print(f"    Timeout (attempt {attempt + 1}/{retries})")
            time.sleep(SLEEP_BETWEEN * (attempt + 1))
        except Exception as e:
            print(f"    Error (attempt {attempt + 1}/{retries}): {e}")
            time.sleep(SLEEP_BETWEEN)
    return None


def rest_search(keyword, language="ja", limit=50):
    """Wikidata REST API wbsearchentities"""
    try:
        resp = requests.get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbsearchentities",
                "search": keyword,
                "language": language,
                "format": "json",
                "limit": limit,
                "type": "item",
            },
            headers={"User-Agent": UA},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("search", [])
    except Exception as e:
        print(f"    REST search error: {e}")
        return []


def get_entity_data(qid):
    """Fetch entity data by QID"""
    try:
        resp = requests.get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            headers={"User-Agent": UA},
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("entities", {}).get(qid, {})
    except Exception as e:
        print(f"    Entity fetch error for {qid}: {e}")
        return {}


def extract_claim_value(claims, prop):
    """Extract first claim value for a property"""
    cl = claims.get(prop, [])
    if not cl:
        return None
    snak = cl[0].get("mainsnak", {})
    dv = snak.get("datavalue", {})
    if dv.get("type") == "string":
        return dv.get("value")
    if dv.get("type") == "wikibase-entityid":
        return dv.get("value", {}).get("id")
    if dv.get("type") == "globecoordinate":
        v = dv.get("value", {})
        return {"lat": v.get("latitude"), "lon": v.get("longitude")}
    return None


def extract_entity_ids(entity):
    """Extract external IDs from entity data"""
    claims = entity.get("claims", {})
    labels = entity.get("labels", {})

    result = {
        "label_ja": labels.get("ja", {}).get("value", ""),
        "label_en": labels.get("en", {}).get("value", ""),
        "madb_id": extract_claim_value(claims, "P4082"),
        "ndl_id": extract_claim_value(claims, "P349"),
        "viaf_id": extract_claim_value(claims, "P214"),
    }

    # DBpedia sitelinks
    sitelinks = entity.get("sitelinks", {})
    jawiki = sitelinks.get("jawiki", {})
    if jawiki:
        title = jawiki.get("title", "")
        result["dbpedia_uri"] = f"http://ja.dbpedia.org/resource/{title}"

    # Coordinates
    coord = extract_claim_value(claims, "P625")
    if isinstance(coord, dict):
        result["lat"] = coord.get("lat")
        result["lon"] = coord.get("lon")

    return result


# ================================================================
# Task B1: Anime works with MADB or NDL IDs
# ================================================================

def query_anime_mappings():
    print("\n" + "=" * 60)
    print("Task B1: Anime works with MADB/NDL IDs")
    print("=" * 60)

    all_mappings = []
    offset = 0

    while True:
        print(f"\n  Querying offset={offset}...")
        query = f"""
        SELECT ?item ?itemLabel_ja ?madb ?ndl WHERE {{
          ?item wdt:P31/wdt:P279* wd:Q1107 .
          ?item rdfs:label ?itemLabel_ja . FILTER(LANG(?itemLabel_ja) = "ja")
          OPTIONAL {{ ?item wdt:P4082 ?madb. }}
          OPTIONAL {{ ?item wdt:P349 ?ndl. }}
          FILTER(BOUND(?madb) || BOUND(?ndl))
        }}
        LIMIT 100
        OFFSET {offset}
        """
        data = sparql_query_with_retry(query)
        if data is None:
            print(f"    Failed at offset={offset}, stopping")
            break

        bindings = data.get("results", {}).get("bindings", [])
        if not bindings:
            print(f"    No more results at offset={offset}")
            break

        for b in bindings:
            qid = b.get("item", {}).get("value", "").rsplit("/", 1)[-1]
            all_mappings.append({
                "wikidata_id": qid,
                "label_ja": b.get("itemLabel_ja", {}).get("value", ""),
                "madb_id": b.get("madb", {}).get("value") if "madb" in b else None,
                "ndl_id": b.get("ndl", {}).get("value") if "ndl" in b else None,
                "entity_type": "work",
                "work_type": "anime",
            })

        print(f"    Got {len(bindings)} results (total: {len(all_mappings)})")
        offset += 100
        time.sleep(SLEEP_BETWEEN)

        if len(bindings) < 100:
            break

    print(f"\n  Total anime mappings: {len(all_mappings)}")
    return all_mappings


# ================================================================
# Task B2: Manga works with MADB or NDL IDs
# ================================================================

def query_manga_mappings():
    print("\n" + "=" * 60)
    print("Task B2: Manga works with MADB/NDL IDs")
    print("=" * 60)

    all_mappings = []
    offset = 0

    while True:
        print(f"\n  Querying offset={offset}...")
        query = f"""
        SELECT ?item ?itemLabel_ja ?madb ?ndl WHERE {{
          ?item wdt:P31/wdt:P279* wd:Q21198342 .
          ?item rdfs:label ?itemLabel_ja . FILTER(LANG(?itemLabel_ja) = "ja")
          OPTIONAL {{ ?item wdt:P4082 ?madb. }}
          OPTIONAL {{ ?item wdt:P349 ?ndl. }}
          FILTER(BOUND(?madb) || BOUND(?ndl))
        }}
        LIMIT 100
        OFFSET {offset}
        """
        data = sparql_query_with_retry(query)
        if data is None:
            print(f"    Failed at offset={offset}, stopping")
            break

        bindings = data.get("results", {}).get("bindings", [])
        if not bindings:
            break

        for b in bindings:
            qid = b.get("item", {}).get("value", "").rsplit("/", 1)[-1]
            all_mappings.append({
                "wikidata_id": qid,
                "label_ja": b.get("itemLabel_ja", {}).get("value", ""),
                "madb_id": b.get("madb", {}).get("value") if "madb" in b else None,
                "ndl_id": b.get("ndl", {}).get("value") if "ndl" in b else None,
                "entity_type": "work",
                "work_type": "manga",
            })

        print(f"    Got {len(bindings)} results (total: {len(all_mappings)})")
        offset += 100
        time.sleep(SLEEP_BETWEEN)

        if len(bindings) < 100:
            break

    print(f"\n  Total manga mappings: {len(all_mappings)}")
    return all_mappings


# ================================================================
# Task B3: Historical persons (ukiyo-e artists, mangaka, etc.)
# ================================================================

def query_person_mappings():
    print("\n" + "=" * 60)
    print("Task B3: Historical persons with NDL IDs")
    print("=" * 60)

    occupations = [
        ("wd:Q1028181", "浮世絵師"),
        ("wd:Q483501", "日本画家"),
        ("wd:Q6625963", "漫画家"),
        ("wd:Q482980", "著作家"),
        ("wd:Q36834", "作曲家"),
    ]

    all_mappings = []

    for occ_qid, occ_label in occupations:
        print(f"\n  --- {occ_label} ({occ_qid}) ---")
        query = f"""
        SELECT ?item ?itemLabel_ja ?ndl WHERE {{
          ?item wdt:P31 wd:Q5 ;
                wdt:P27 wd:Q17 ;
                wdt:P106 {occ_qid} .
          ?item rdfs:label ?itemLabel_ja . FILTER(LANG(?itemLabel_ja) = "ja")
          OPTIONAL {{ ?item wdt:P349 ?ndl. }}
          FILTER(BOUND(?ndl))
        }}
        LIMIT 200
        """
        data = sparql_query_with_retry(query)
        if data is None:
            print(f"    Failed for {occ_label}")
            time.sleep(SLEEP_BETWEEN)
            continue

        bindings = data.get("results", {}).get("bindings", [])
        for b in bindings:
            qid = b.get("item", {}).get("value", "").rsplit("/", 1)[-1]
            all_mappings.append({
                "wikidata_id": qid,
                "label_ja": b.get("itemLabel_ja", {}).get("value", ""),
                "ndl_id": b.get("ndl", {}).get("value") if "ndl" in b else None,
                "entity_type": "person",
                "occupation": occ_label,
            })

        print(f"    Got {len(bindings)} {occ_label}")
        time.sleep(SLEEP_BETWEEN)

    print(f"\n  Total person mappings: {len(all_mappings)}")
    return all_mappings


# ================================================================
# Task B4: Cultural heritage and landmarks with coordinates
# ================================================================

def query_place_mappings():
    print("\n" + "=" * 60)
    print("Task B4: Cultural heritage with coordinates")
    print("=" * 60)

    query = """
    SELECT ?item ?itemLabel_ja ?coord ?heritage ?ndl WHERE {
      ?item wdt:P17 wd:Q17 ;
            wdt:P625 ?coord .
      ?item rdfs:label ?itemLabel_ja . FILTER(LANG(?itemLabel_ja) = "ja")
      OPTIONAL { ?item wdt:P1435 ?heritage. }
      OPTIONAL { ?item wdt:P349 ?ndl. }
      FILTER(BOUND(?heritage))
    }
    LIMIT 200
    """
    all_mappings = []

    data = sparql_query_with_retry(query)
    if data is None:
        print("  Failed to query places")
        return all_mappings

    bindings = data.get("results", {}).get("bindings", [])
    for b in bindings:
        qid = b.get("item", {}).get("value", "").rsplit("/", 1)[-1]
        coord_str = b.get("coord", {}).get("value", "")
        lat, lon = None, None
        if coord_str.startswith("Point("):
            parts = coord_str.replace("Point(", "").replace(")", "").split()
            if len(parts) == 2:
                try:
                    lon = float(parts[0])
                    lat = float(parts[1])
                except ValueError:
                    pass

        all_mappings.append({
            "wikidata_id": qid,
            "label_ja": b.get("itemLabel_ja", {}).get("value", ""),
            "ndl_id": b.get("ndl", {}).get("value") if "ndl" in b else None,
            "heritage_status": b.get("heritage", {}).get("value", "").rsplit("/", 1)[-1] if "heritage" in b else None,
            "entity_type": "place",
            "lat": lat,
            "lon": lon,
        })

    print(f"  Total place mappings: {len(all_mappings)}")
    return all_mappings


# ================================================================
# Main
# ================================================================

def main():
    print("=" * 60)
    print("Phase 3 Task B: Wikidata ID Mapping")
    print(f"Started: {now_iso()}")
    print("=" * 60)

    all_mappings = []

    # B1: Anime
    anime = query_anime_mappings()
    all_mappings.extend(anime)

    # B2: Manga
    manga = query_manga_mappings()
    all_mappings.extend(manga)

    # B3: Persons
    persons = query_person_mappings()
    all_mappings.extend(persons)

    # B4: Places
    places = query_place_mappings()
    all_mappings.extend(places)

    # Stats
    stats = {
        "total": len(all_mappings),
        "anime": len(anime),
        "manga": len(manga),
        "persons": len(persons),
        "places": len(places),
        "with_madb": sum(1 for m in all_mappings if m.get("madb_id")),
        "with_ndl": sum(1 for m in all_mappings if m.get("ndl_id")),
        "with_both": sum(1 for m in all_mappings if m.get("madb_id") and m.get("ndl_id")),
        "with_coords": sum(1 for m in all_mappings if m.get("lat") is not None),
    }

    output = {
        "mappings": all_mappings,
        "stats": stats,
        "ts": now_iso(),
    }

    save_json(ONTOLOGY_DIR / "wikidata_id_mapping.json", output)

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    for k, v in stats.items():
        print(f"  {k}: {v}")
    print(f"\nCompleted: {now_iso()}")


if __name__ == "__main__":
    main()
