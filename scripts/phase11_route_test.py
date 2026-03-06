"""
Phase 11 Stream C: generate_pilgrimage_route 実動テスト
Tests search_pilgrimage, generate_pilgrimage_route, get_nearby_culture
"""
import sqlite3
import json
import math

DB_PATH = "ontology/culture_ontology.db"


def get_db():
    conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def test_search_pilgrimage_by_work(work_title, expected_min=1):
    """Test search_pilgrimage by work title."""
    db = get_db()
    rows = db.execute("""
        SELECT DISTINCT
            e_work.label_ja AS work_name,
            e_loc.label_ja AS location_name,
            e_loc.lat, e_loc.lon,
            c.connection_type, c.explanation
        FROM connections c
        JOIN entities e_work ON (
            (c.entity_a_id = e_work.id AND c.entity_b_id IN (SELECT id FROM entities WHERE lat IS NOT NULL))
            OR (c.entity_b_id = e_work.id AND c.entity_a_id IN (SELECT id FROM entities WHERE lat IS NOT NULL))
        )
        JOIN entities e_loc ON (
            (c.entity_a_id = e_loc.id AND e_loc.id != e_work.id)
            OR (c.entity_b_id = e_loc.id AND e_loc.id != e_work.id)
        )
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND (e_work.label_ja LIKE ? OR e_work.label_en LIKE ? OR c.explanation LIKE ?)
        AND e_loc.lat IS NOT NULL
        LIMIT 20
    """, (f"%{work_title}%", f"%{work_title}%", f"%{work_title}%")).fetchall()
    db.close()

    results = [dict(r) for r in rows]
    passed = len(results) >= expected_min
    return {
        "test": f"search_pilgrimage(work_title='{work_title}')",
        "passed": passed,
        "results_count": len(results),
        "expected_min": expected_min,
        "sample_results": [
            {"work": r["work_name"], "location": r["location_name"],
             "lat": r["lat"], "lon": r["lon"]}
            for r in results[:5]
        ],
    }


def test_search_pilgrimage_by_coords(lat, lon, radius_km=10, expected_min=1):
    """Test search_pilgrimage by coordinates."""
    db = get_db()
    lat_off = radius_km / 111.0
    lon_off = radius_km / (111.0 * math.cos(math.radians(lat)))

    rows = db.execute("""
        SELECT DISTINCT
            e_loc.label_ja AS location_name,
            e_loc.lat, e_loc.lon,
            e_work.label_ja AS work_name,
            c.connection_type
        FROM entities e_loc
        JOIN connections c ON (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
        JOIN entities e_work ON (
            (c.entity_a_id = e_work.id AND e_work.id != e_loc.id)
            OR (c.entity_b_id = e_work.id AND e_work.id != e_loc.id)
        )
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e_loc.lat BETWEEN ? AND ?
        AND e_loc.lon BETWEEN ? AND ?
        LIMIT 20
    """, (lat - lat_off, lat + lat_off, lon - lon_off, lon + lon_off)).fetchall()
    db.close()

    results = [dict(r) for r in rows]
    passed = len(results) >= expected_min
    return {
        "test": f"search_pilgrimage(lat={lat}, lon={lon}, radius={radius_km}km)",
        "passed": passed,
        "results_count": len(results),
        "sample_results": [
            {"location": r["location_name"], "work": r["work_name"]}
            for r in results[:5]
        ],
    }


def test_generate_pilgrimage_route(work_title=None, region=None, max_spots=8):
    """Test generate_pilgrimage_route logic."""
    db = get_db()

    # Step 1: Get pilgrimage spots
    params = []
    where_parts = ["c.connection_type LIKE 'pilgrimage%'", "e_loc.lat IS NOT NULL"]

    if work_title:
        where_parts.append("(e_work.label_ja LIKE ? OR e_work.label_en LIKE ?)")
        params.append(f"%{work_title}%")
        params.append(f"%{work_title}%")

    where_clause = " AND ".join(where_parts)
    params.append(max_spots)

    spots = db.execute(f"""
        SELECT DISTINCT
            e_work.label_ja AS work_name,
            e_loc.label_ja AS location_name,
            e_loc.lat, e_loc.lon,
            c.explanation, c.connection_type,
            e_loc.id AS loc_id
        FROM connections c
        JOIN entities e_work ON (c.entity_a_id = e_work.id OR c.entity_b_id = e_work.id)
        JOIN entities e_loc ON (
            (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
            AND e_loc.id != e_work.id
        )
        WHERE {where_clause}
        ORDER BY RANDOM()
        LIMIT ?
    """, params).fetchall()

    pilgrimage_spots = [dict(s) for s in spots]

    # Step 2: Find nearby cultural spots
    cultural_spots = []
    if pilgrimage_spots:
        center_lat = sum(s["lat"] for s in pilgrimage_spots) / len(pilgrimage_spots)
        center_lon = sum(s["lon"] for s in pilgrimage_spots) / len(pilgrimage_spots)
        remaining = max_spots - len(pilgrimage_spots)

        if remaining > 0:
            loc_ids = [s["loc_id"] for s in pilgrimage_spots]
            placeholders = ",".join("?" * len(loc_ids))
            cultural = db.execute(f"""
                SELECT e.label_ja, e.lat, e.lon, e.entity_type
                FROM entities e
                WHERE e.lat IS NOT NULL AND e.lon IS NOT NULL
                AND e.lat BETWEEN ? AND ?
                AND e.lon BETWEEN ? AND ?
                AND e.id NOT IN ({placeholders})
                AND e.entity_type IN ('shrine', 'temple', 'cultural_property', 'museum', 'place', 'building', 'craft')
                ORDER BY ABS(e.lat - ?) + ABS(e.lon - ?)
                LIMIT ?
            """, (center_lat - 0.5, center_lat + 0.5,
                  center_lon - 0.5, center_lon + 0.5,
                  *loc_ids,
                  center_lat, center_lon, remaining)).fetchall()
            cultural_spots = [dict(c) for c in cultural]

    db.close()

    all_spots = pilgrimage_spots + cultural_spots
    has_pilgrimage = len(pilgrimage_spots) > 0
    has_cultural = len(cultural_spots) > 0
    all_have_coords = all(s.get("lat") and s.get("lon") for s in all_spots)

    # Check distances
    max_dist = 0
    if len(all_spots) >= 2:
        for i in range(len(all_spots)):
            for j in range(i + 1, len(all_spots)):
                s1, s2 = all_spots[i], all_spots[j]
                dist = math.sqrt(
                    ((s1.get("lat", 0) - s2.get("lat", 0)) * 111) ** 2 +
                    ((s1.get("lon", 0) - s2.get("lon", 0)) * 111 * math.cos(math.radians(s1.get("lat", 35)))) ** 2
                )
                max_dist = max(max_dist, dist)

    passed = has_pilgrimage and all_have_coords and len(all_spots) >= 2
    return {
        "test": f"generate_pilgrimage_route(work='{work_title}')",
        "passed": passed,
        "pilgrimage_spots": len(pilgrimage_spots),
        "cultural_spots": len(cultural_spots),
        "total_spots": len(all_spots),
        "all_have_coords": all_have_coords,
        "max_distance_km": round(max_dist, 1),
        "distance_realistic": max_dist < 200,
        "sample_pilgrimage": [
            {"work": s["work_name"], "location": s["location_name"]}
            for s in pilgrimage_spots[:3]
        ],
        "sample_cultural": [
            {"name": s["label_ja"], "type": s["entity_type"]}
            for s in cultural_spots[:3]
        ],
    }


def test_get_nearby_culture(lat, lon, radius_km=5, expected_min=1):
    """Test get_nearby_culture."""
    db = get_db()
    lat_off = radius_km / 111.0
    lon_off = radius_km / (111.0 * math.cos(math.radians(lat)))

    rows = db.execute("""
        SELECT e.label_ja, e.entity_type, e.lat, e.lon, e.source
        FROM entities e
        WHERE e.lat IS NOT NULL AND e.lon IS NOT NULL
        AND e.lat BETWEEN ? AND ?
        AND e.lon BETWEEN ? AND ?
        ORDER BY ABS(e.lat - ?) + ABS(e.lon - ?)
        LIMIT 20
    """, (lat - lat_off, lat + lat_off, lon - lon_off, lon + lon_off, lat, lon)).fetchall()
    db.close()

    results = [dict(r) for r in rows]
    # Calculate distances
    for r in results:
        r["distance_km"] = round(math.sqrt(
            ((r["lat"] - lat) * 111) ** 2 +
            ((r["lon"] - lon) * 111 * math.cos(math.radians(lat))) ** 2
        ), 2)

    passed = len(results) >= expected_min
    entity_types = list(set(r["entity_type"] for r in results))
    return {
        "test": f"get_nearby_culture(lat={lat}, lon={lon}, radius={radius_km}km)",
        "passed": passed,
        "results_count": len(results),
        "entity_types": entity_types,
        "sample_results": [
            {"name": r["label_ja"], "type": r["entity_type"], "distance_km": r["distance_km"]}
            for r in results[:5]
        ],
    }


def main():
    print("=" * 60, flush=True)
    print("Phase 11 Stream C: MCP Tool Tests", flush=True)
    print("=" * 60, flush=True)

    results = []

    # ── Test 1: search_pilgrimage by popular works ──
    test_works = [
        ("君の名は", 1),
        ("スラムダンク", 1),
        ("鬼滅の刃", 1),
        ("もののけ姫", 1),
        ("ワンピース", 1),
    ]

    for work, min_r in test_works:
        r = test_search_pilgrimage_by_work(work, min_r)
        results.append(r)
        status = "PASS" if r["passed"] else "FAIL"
        print(f"\n[{status}] {r['test']}", flush=True)
        print(f"  Results: {r['results_count']}", flush=True)
        for s in r["sample_results"][:3]:
            print(f"    → {s['work']} @ {s['location']} ({s.get('lat', '?')}, {s.get('lon', '?')})", flush=True)

    # ── Test 2: search by coordinates ──
    coord_tests = [
        (35.31, 139.55, 10, 1),   # 鎌倉
        (35.68, 139.77, 5, 1),    # 東京
        (35.01, 135.77, 10, 1),   # 京都
        (36.23, 137.25, 20, 1),   # 飛騨
    ]

    for lat, lon, rad, min_r in coord_tests:
        r = test_search_pilgrimage_by_coords(lat, lon, rad, min_r)
        results.append(r)
        status = "PASS" if r["passed"] else "FAIL"
        print(f"\n[{status}] {r['test']}", flush=True)
        print(f"  Results: {r['results_count']}", flush=True)
        for s in r["sample_results"][:3]:
            print(f"    → {s['location']} ({s['work']})", flush=True)

    # ── Test 3: generate_pilgrimage_route ──
    route_tests = [
        "君の名は",
        "鬼滅の刃",
        "スラムダンク",
    ]

    for work in route_tests:
        r = test_generate_pilgrimage_route(work_title=work, max_spots=8)
        results.append(r)
        status = "PASS" if r["passed"] else "FAIL"
        print(f"\n[{status}] {r['test']}", flush=True)
        print(f"  Pilgrimage: {r['pilgrimage_spots']}, Cultural: {r['cultural_spots']}, Max dist: {r['max_distance_km']}km", flush=True)
        for s in r["sample_pilgrimage"][:2]:
            print(f"    聖地: {s['work']} → {s['location']}", flush=True)
        for s in r["sample_cultural"][:2]:
            print(f"    文化: {s['name']} ({s['type']})", flush=True)

    # ── Test 4: get_nearby_culture ──
    nearby_tests = [
        (35.3197, 139.5465, 3, 3),    # 鎌倉
        (34.9671, 135.7727, 5, 3),     # 京都
        (35.6762, 139.6503, 3, 3),     # 東京
    ]

    for lat, lon, rad, min_r in nearby_tests:
        r = test_get_nearby_culture(lat, lon, rad, min_r)
        results.append(r)
        status = "PASS" if r["passed"] else "FAIL"
        print(f"\n[{status}] {r['test']}", flush=True)
        print(f"  Results: {r['results_count']}, Types: {r['entity_types']}", flush=True)
        for s in r["sample_results"][:3]:
            print(f"    → {s['name']} ({s['type']}) {s['distance_km']}km", flush=True)

    # ── Summary ──
    total = len(results)
    passed = sum(1 for r in results if r["passed"])
    print(f"\n{'='*60}", flush=True)
    print(f"=== Test Summary: {passed}/{total} passed ===", flush=True)
    for r in results:
        status = "PASS" if r["passed"] else "FAIL"
        print(f"  [{status}] {r['test']}", flush=True)

    if passed < total:
        print(f"\n{total - passed} tests failed — investigation needed", flush=True)


if __name__ == "__main__":
    main()
