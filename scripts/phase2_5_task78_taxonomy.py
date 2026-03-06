"""Phase 2.5 Task 7+8: AniListタグ・MADBオントロジー完全取得
Python 3.8 compatible.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests

BASE_DIR = Path(__file__).parent.parent
RESP_DIR = BASE_DIR / "responses" / "phase2_5"
RESP_DIR.mkdir(parents=True, exist_ok=True)

UA = "japan-culture-mcp/0.2 (research-project)"
TIMEOUT = 30


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def save_json(name, data):
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  Saved: {name}")


def save_text(name, text):
    path = RESP_DIR / name
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"  Saved: {name}")


# ================================================================
# Task 7A: AniList全ジャンル・全タグ取得
# ================================================================

def get_anilist_genres():
    print("\n" + "=" * 60)
    print("Task 7A-1: AniList全ジャンル取得")
    print("=" * 60)
    try:
        resp = requests.post(
            "https://graphql.anilist.co",
            json={"query": "query { GenreCollection }"},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        genres = data.get("data", {}).get("GenreCollection", [])
        save_json("anilist_genres_full.json", data)
        print(f"  [OK] {len(genres)} genres")
        for g in genres:
            print(f"    - {g}")
        return genres
    except Exception as e:
        print(f"  [FAIL] {e}")
        return []


def get_anilist_tags():
    print("\n" + "=" * 60)
    print("Task 7A-2: AniList全タグ取得")
    print("=" * 60)
    query = """
    query {
      MediaTagCollection {
        id
        name
        category
        description
        isAdult
      }
    }
    """
    try:
        resp = requests.post(
            "https://graphql.anilist.co",
            json={"query": query},
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        tags = data.get("data", {}).get("MediaTagCollection", [])
        save_json("anilist_tags_full.json", data)
        print(f"  [OK] {len(tags)} tags")

        # Category breakdown
        categories = {}
        for tag in tags:
            cat = tag.get("category", "Unknown")
            if cat not in categories:
                categories[cat] = []
            categories[cat].append({
                "name": tag["name"],
                "description": tag.get("description", ""),
                "isAdult": tag.get("isAdult", False),
            })

        print(f"\n  === カテゴリ別集計 ({len(categories)} categories) ===")
        for cat in sorted(categories.keys()):
            tag_list = categories[cat]
            print(f"\n  [{cat}] ({len(tag_list)} tags)")
            for t in sorted(tag_list, key=lambda x: x["name"]):
                adult = " [Adult]" if t["isAdult"] else ""
                print(f"    - {t['name']}{adult}")

        # Save category summary
        save_json("anilist_tags_by_category.json", {
            "total_tags": len(tags),
            "total_categories": len(categories),
            "categories": {cat: [t["name"] for t in tags_list] for cat, tags_list in categories.items()},
            "ts": now_iso(),
        })

        return tags, categories
    except Exception as e:
        print(f"  [FAIL] {e}")
        return [], {}


# ================================================================
# Task 8: MADBオントロジー完全マッピング
# ================================================================

MADB_ENDPOINT = "https://mediaarts-db.artmuseums.go.jp/sparql"


def sparql_query(query):
    resp = requests.get(
        MADB_ENDPOINT,
        params={"query": query},
        headers={"Accept": "application/sparql-results+json"},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()


def get_madb_classes():
    print("\n" + "=" * 60)
    print("Task 8A: MADB全クラス一覧")
    print("=" * 60)

    query = """
    SELECT DISTINCT ?class (COUNT(?s) AS ?count) WHERE {
      ?s a ?class .
    }
    GROUP BY ?class
    ORDER BY DESC(?count)
    LIMIT 100
    """
    try:
        data = sparql_query(query)
        bindings = data.get("results", {}).get("bindings", [])
        classes = []
        for b in bindings:
            cls_uri = b.get("class", {}).get("value", "")
            count = int(b.get("count", {}).get("value", "0"))
            cls_name = cls_uri.rsplit("#", 1)[-1] if "#" in cls_uri else cls_uri.rsplit("/", 1)[-1]
            classes.append({
                "uri": cls_uri,
                "name": cls_name,
                "count": count,
            })
            print(f"    {cls_name}: {count:,} instances ({cls_uri})")

        save_json("madb_classes_full.json", {"classes": classes, "ts": now_iso()})
        print(f"\n  [OK] {len(classes)} classes found")
        return classes
    except Exception as e:
        print(f"  [FAIL] {e}")
        return []


def get_madb_properties(class_uri, class_name):
    """特定クラスのプロパティ一覧を取得"""
    query = f"""
    SELECT DISTINCT ?prop (COUNT(?o) AS ?count) (SAMPLE(?o) AS ?sample) WHERE {{
      ?s a <{class_uri}> ;
         ?prop ?o .
    }}
    GROUP BY ?prop
    ORDER BY DESC(?count)
    LIMIT 100
    """
    try:
        data = sparql_query(query)
        bindings = data.get("results", {}).get("bindings", [])
        props = []
        for b in bindings:
            prop_uri = b.get("prop", {}).get("value", "")
            count = int(b.get("count", {}).get("value", "0"))
            sample = b.get("sample", {}).get("value", "")[:200]
            prop_name = prop_uri.rsplit("#", 1)[-1] if "#" in prop_uri else prop_uri.rsplit("/", 1)[-1]
            props.append({
                "uri": prop_uri,
                "name": prop_name,
                "count": count,
                "sample_value": sample,
            })
        return props
    except Exception as e:
        print(f"    [FAIL] Properties for {class_name}: {e}")
        return []


def get_madb_full_ontology(classes):
    print("\n" + "=" * 60)
    print("Task 8B: MADB主要クラスのプロパティ一覧")
    print("=" * 60)

    # Filter to MADB-specific classes (not generic RDF/OWL classes)
    madb_classes = [c for c in classes if "mediaarts-db" in c["uri"] or "schema.org" in c["uri"]]
    if not madb_classes:
        madb_classes = classes[:10]  # Fallback: top 10

    ontology = {}
    for cls in madb_classes[:15]:  # Top 15
        print(f"\n  --- {cls['name']} ({cls['count']:,} instances) ---")
        props = get_madb_properties(cls["uri"], cls["name"])
        ontology[cls["name"]] = {
            "uri": cls["uri"],
            "instance_count": cls["count"],
            "properties": props,
        }
        for p in props[:10]:
            print(f"    {p['name']}: {p['count']:,}x  (e.g. {p['sample_value'][:80]})")
        time.sleep(1)

    save_json("madb_ontology_full.json", {"ontology": ontology, "ts": now_iso()})
    print(f"\n  [OK] Ontology mapped for {len(ontology)} classes")
    return ontology


# ================================================================
# Main
# ================================================================

def main():
    print("=" * 60)
    print("Phase 2.5 Tasks 7+8: AniListタグ・MADBオントロジー")
    print(f"Started: {now_iso()}")
    print(f"Output: {RESP_DIR}")
    print("=" * 60)

    # Task 7
    genres = get_anilist_genres()
    time.sleep(1)
    tags, categories = get_anilist_tags()
    time.sleep(1)

    # Task 8
    classes = get_madb_classes()
    time.sleep(1)
    ontology = get_madb_full_ontology(classes)

    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"  AniList genres: {len(genres)}")
    print(f"  AniList tags: {len(tags)}")
    print(f"  AniList tag categories: {len(categories)}")
    print(f"  MADB classes: {len(classes)}")
    print(f"  MADB ontology entries: {len(ontology)}")
    print(f"Completed: {now_iso()}")


if __name__ == "__main__":
    main()
