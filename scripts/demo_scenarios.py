"""
Phase 7 B6: Demo scenarios — test all core tools and save outputs.
"""
import sqlite3
import json
import os
import math
from pathlib import Path

DB_PATH = "ontology/culture_ontology.db"

def get_db():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn

def demo_find_serendipity(keyword, max_results=10, min_score=0.3):
    """Simulate find_serendipity tool."""
    db = get_db()
    entities = db.execute(
        "SELECT id, wikidata_id, label_ja, label_en, entity_type FROM entities WHERE label_ja LIKE ?",
        (f"%{keyword}%",),
    ).fetchall()

    if not entities:
        db.close()
        return {"query": keyword, "error": "Not found"}

    # Pick entity with most connections
    best = entities[0]
    best_cnt = 0
    for e in entities[:10]:
        cnt = db.execute("SELECT COUNT(*) FROM connections WHERE (entity_a_id=? OR entity_b_id=?) AND llm_verdict='keep'",
                        (e["id"], e["id"])).fetchone()[0]
        if cnt > best_cnt:
            best_cnt = cnt
            best = e

    eid = best["id"]

    # Tags
    tags = {}
    for row in db.execute("SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (eid,)):
        tags.setdefault(row["axis"], []).append(row["value_code"])

    # Connections
    connections = db.execute("""
        SELECT c.connection_type, c.serendipity_score, c.llm_explanation,
               c.llm_cultural_relevance, c.llm_serendipity_quality,
               ea.label_ja as a_label, eb.label_ja as b_label
        FROM connections c
        JOIN entities ea ON c.entity_a_id = ea.id
        JOIN entities eb ON c.entity_b_id = eb.id
        WHERE (c.entity_a_id = ? OR c.entity_b_id = ?) AND c.llm_verdict = 'keep'
        ORDER BY c.serendipity_score DESC LIMIT ?
    """, (eid, eid, max_results)).fetchall()

    results = []
    for c in connections:
        other = c["b_label"] if c["a_label"] == best["label_ja"] else c["a_label"]
        results.append({
            "connected_entity": other,
            "connection_type": c["connection_type"],
            "serendipity_score": round(c["serendipity_score"], 3) if c["serendipity_score"] else None,
            "explanation": c["llm_explanation"],
        })

    db.close()
    return {
        "query": keyword,
        "matched_entity": {"label_ja": best["label_ja"], "entity_type": best["entity_type"]},
        "tags": tags,
        "connections_found": len(results),
        "results": results,
    }

def demo_explore_axis(axis, value):
    """Simulate explore_axis tool."""
    db = get_db()
    entities = db.execute("""
        SELECT DISTINCT e.label_ja, e.entity_type, e.wikidata_id
        FROM entities e
        JOIN entity_tags et ON e.id = et.entity_id
        WHERE et.axis = ? AND et.value_code = ?
        LIMIT 20
    """, (axis, value)).fetchall()

    total = db.execute("""
        SELECT COUNT(DISTINCT entity_id) FROM entity_tags WHERE axis = ? AND value_code = ?
    """, (axis, value)).fetchone()[0]

    db.close()
    return {
        "axis": axis,
        "value": value,
        "total_entities": total,
        "sample": [{"label_ja": e["label_ja"], "entity_type": e["entity_type"]} for e in entities],
    }

def demo_cultural_route(theme, region, limit=10):
    """Simulate get_cultural_route tool."""
    db = get_db()
    spots = db.execute("""
        SELECT DISTINCT e.id, e.label_ja, e.lat, e.lon, e.wikidata_id
        FROM entities e
        JOIN entity_tags et_theme ON e.id = et_theme.entity_id AND et_theme.axis = 'theme'
        LEFT JOIN entity_tags et_geo ON e.id = et_geo.entity_id AND et_geo.axis = 'geography'
        WHERE e.lat IS NOT NULL AND e.lon IS NOT NULL
        AND et_theme.value_code = ? AND et_geo.value_code = ?
        LIMIT ?
    """, (theme, region, limit)).fetchall()

    enriched = []
    for s in spots:
        # GSI tile
        zoom = 15
        n = 2 ** zoom
        x = int((s["lon"] + 180.0) / 360.0 * n)
        lat_rad = math.radians(s["lat"])
        y = int((1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0 * n)

        enriched.append({
            "name": s["label_ja"],
            "lat": s["lat"],
            "lon": s["lon"],
            "map_tile": f"https://cyberjapandata.gsi.go.jp/xyz/std/{zoom}/{x}/{y}.png",
        })

    db.close()
    return {"theme": theme, "region": region, "spots": enriched, "total": len(enriched)}


SCENARIOS = [
    {
        "name": "yokai_serendipity",
        "title": "妖怪セレンディピティ",
        "description": "「妖怪」で検索し、古典から現代までの妖怪文化の繋がりを発見",
        "calls": [
            ("find_serendipity", {"keyword": "妖怪"}),
            ("explore_axis", {"axis": "theme", "value": "yokai"}),
        ],
    },
    {
        "name": "hokusai_network",
        "title": "北斎の文化圏",
        "description": "葛飾北斎を起点に、浮世絵→ジャポニスム→現代アニメの接続を探索",
        "calls": [
            ("find_serendipity", {"keyword": "北斎"}),
            ("explore_axis", {"axis": "theme", "value": "ukiyoe_craft"}),
        ],
    },
    {
        "name": "kyoto_sacred_route",
        "title": "京都・聖と俗ルート",
        "description": "京都の文化財を「神社仏閣」テーマでルート生成",
        "calls": [
            ("cultural_route", {"theme": "shrine_temple", "region": "kinki"}),
            ("find_serendipity", {"keyword": "金閣寺"}),
        ],
    },
]


def main():
    os.makedirs("demo_output", exist_ok=True)

    for scenario in SCENARIOS:
        print(f"\n=== {scenario['title']} ===", flush=True)
        print(f"  {scenario['description']}", flush=True)

        results = []
        for call_type, params in scenario["calls"]:
            if call_type == "find_serendipity":
                result = demo_find_serendipity(**params)
            elif call_type == "explore_axis":
                result = demo_explore_axis(**params)
            elif call_type == "cultural_route":
                result = demo_cultural_route(**params)
            else:
                result = {"error": f"Unknown call type: {call_type}"}

            results.append({"tool": call_type, "params": params, "result": result})
            print(f"  {call_type}: {len(json.dumps(result))} chars", flush=True)

        # Save
        output_path = f"demo_output/{scenario['name']}.json"
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({
                "scenario": scenario["title"],
                "description": scenario["description"],
                "results": results,
            }, f, ensure_ascii=False, indent=2)
        print(f"  Saved: {output_path}", flush=True)

    print("\n=== All Demo Scenarios Complete ===", flush=True)

if __name__ == "__main__":
    main()
