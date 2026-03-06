"""
Phase 10 Stream D-2: 聖地巡礼クロス接続生成
Target: 8,000+ total pilgrimage connections
Strategies:
1. 聖地×文化財 proximity (same location)
2. Same-location cross-work connections
3. 聖地×伝統工芸 (regional)
4. Anime/manga works → related cultural entities
5. Location-based serendipity (pilgrimage spot → nearby cultural entity)
"""
import sqlite3
import random
import math

DB_PATH = "ontology/culture_ontology.db"


def main():
    db = sqlite3.connect(DB_PATH)

    # Current state
    existing = db.execute("SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'").fetchone()[0]
    total_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"Existing pilgrimage connections: {existing:,}", flush=True)
    print(f"Total connections: {total_conns:,}", flush=True)

    # Build existing connection set
    existing_pairs = set()
    for row in db.execute("SELECT entity_a_id, entity_b_id FROM connections"):
        existing_pairs.add((row[0], row[1]))
        existing_pairs.add((row[1], row[0]))

    new_total = 0

    # ── Strategy 1: Pilgrimage location → nearby cultural properties ──
    print("\n=== Strategy 1: Pilgrimage spots → nearby cultural properties ===", flush=True)

    # Get all pilgrimage locations with coordinates
    pilgrimage_locs = db.execute("""
        SELECT DISTINCT e_loc.id, e_loc.label_ja, e_loc.lat, e_loc.lon
        FROM connections c
        JOIN entities e_loc ON (
            (c.entity_a_id = e_loc.id AND e_loc.lat IS NOT NULL)
            OR (c.entity_b_id = e_loc.id AND e_loc.lat IS NOT NULL)
        )
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e_loc.entity_type = 'place'
    """).fetchall()
    print(f"  Pilgrimage locations with coords: {len(pilgrimage_locs)}", flush=True)

    strategy1_new = 0
    for loc in pilgrimage_locs:
        if loc[2] is None or loc[3] is None:
            continue

        lat, lon = loc[2], loc[3]
        lat_off = 0.15  # ~16km
        lon_off = 0.15

        nearby = db.execute("""
            SELECT id, label_ja, entity_type FROM entities
            WHERE lat BETWEEN ? AND ?
            AND lon BETWEEN ? AND ?
            AND id != ?
            AND entity_type IN ('shrine', 'temple', 'cultural_property', 'museum', 'place', 'artwork', 'building')
            LIMIT 10
        """, (lat - lat_off, lat + lat_off, lon - lon_off, lon + lon_off, loc[0])).fetchall()

        for nb in nearby:
            if (loc[0], nb[0]) in existing_pairs:
                continue
            db.execute("""
                INSERT INTO connections
                (entity_a_id, entity_b_id, connection_type, serendipity_score,
                 explanation, source, confidence, llm_verdict)
                VALUES (?, ?, 'pilgrimage_proximity', 0.7, ?, 'pilgrimage_cross', 0.8, 'keep')
            """, (loc[0], nb[0], f"聖地巡礼スポット「{loc[1]}」の近くにある文化スポット「{nb[1]}」"))
            existing_pairs.add((loc[0], nb[0]))
            existing_pairs.add((nb[0], loc[0]))
            strategy1_new += 1

    db.commit()
    new_total += strategy1_new
    print(f"  Strategy 1: {strategy1_new:,} new", flush=True)

    # ── Strategy 2: Same-location cross-work ──
    print("\n=== Strategy 2: Same-location cross-work connections ===", flush=True)

    # Find locations that appear in multiple pilgrimage connections (multiple works → same location)
    multi_work_locs = db.execute("""
        SELECT e_loc.id, e_loc.label_ja, COUNT(DISTINCT
            CASE WHEN c.entity_a_id = e_loc.id THEN c.entity_b_id ELSE c.entity_a_id END
        ) as work_count
        FROM connections c
        JOIN entities e_loc ON (c.entity_a_id = e_loc.id OR c.entity_b_id = e_loc.id)
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e_loc.entity_type = 'place'
        GROUP BY e_loc.id
        HAVING work_count >= 2
    """).fetchall()
    print(f"  Locations with multiple works: {len(multi_work_locs)}", flush=True)

    strategy2_new = 0
    for loc in multi_work_locs:
        # Get all works connected to this location
        works = db.execute("""
            SELECT DISTINCT
                CASE WHEN c.entity_a_id = ? THEN c.entity_b_id ELSE c.entity_a_id END as work_id,
                CASE WHEN c.entity_a_id = ? THEN eb.label_ja ELSE ea.label_ja END as work_label
            FROM connections c
            JOIN entities ea ON c.entity_a_id = ea.id
            JOIN entities eb ON c.entity_b_id = eb.id
            WHERE (c.entity_a_id = ? OR c.entity_b_id = ?)
            AND c.connection_type LIKE 'pilgrimage%'
        """, (loc[0], loc[0], loc[0], loc[0])).fetchall()

        # Create cross-work connections
        for i in range(len(works)):
            for j in range(i + 1, len(works)):
                w1, w2 = works[i], works[j]
                if (w1[0], w2[0]) in existing_pairs:
                    continue
                db.execute("""
                    INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, 'pilgrimage_same_location', 0.75, ?, 'pilgrimage_cross', 0.85, 'keep')
                """, (w1[0], w2[0],
                      f"聖地巡礼: 「{w1[1]}」と「{w2[1]}」は同じ場所「{loc[1]}」を舞台としている"))
                existing_pairs.add((w1[0], w2[0]))
                existing_pairs.add((w2[0], w1[0]))
                strategy2_new += 1

    db.commit()
    new_total += strategy2_new
    print(f"  Strategy 2: {strategy2_new:,} new", flush=True)

    # ── Strategy 3: Pilgrimage works → related anime/manga entities ──
    print("\n=== Strategy 3: Pilgrimage works → related anime/manga entities ===", flush=True)

    pilgrimage_works = db.execute("""
        SELECT DISTINCT
            CASE WHEN e.entity_type != 'place' THEN e.id END as work_id,
            e.label_ja, e.entity_type
        FROM connections c
        JOIN entities e ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e.entity_type != 'place'
    """).fetchall()
    # Filter None
    pilgrimage_works = [w for w in pilgrimage_works if w[0] is not None]
    print(f"  Pilgrimage works: {len(pilgrimage_works)}", flush=True)

    # Get anime/manga entities not yet connected to pilgrimage works
    anime_manga = db.execute("""
        SELECT id, label_ja, entity_type FROM entities
        WHERE entity_type IN ('anime', 'manga', 'game', 'film', 'tv', 'light_novel')
        AND source NOT LIKE 'wd_pilgrimage%'
        ORDER BY RANDOM()
        LIMIT 5000
    """).fetchall()

    strategy3_new = 0
    # For each pilgrimage work, find similar entities by shared tags
    for pw in pilgrimage_works:
        pw_tags = set()
        for row in db.execute("SELECT axis || ':' || value_code FROM entity_tags WHERE entity_id=?", (pw[0],)):
            pw_tags.add(row[0])

        if not pw_tags:
            continue

        for am in random.sample(anime_manga, min(50, len(anime_manga))):
            if (pw[0], am[0]) in existing_pairs:
                continue

            am_tags = set()
            for row in db.execute("SELECT axis || ':' || value_code FROM entity_tags WHERE entity_id=?", (am[0],)):
                am_tags.add(row[0])

            shared = pw_tags & am_tags
            if len(shared) >= 2:
                db.execute("""
                    INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, 'pilgrimage_related', 0.65, ?, 'pilgrimage_cross', 0.7, 'keep')
                """, (pw[0], am[0],
                      f"聖地巡礼作品「{pw[1]}」と「{am[1]}」は共通の文化タグを持つ ({', '.join(list(shared)[:3])})"))
                existing_pairs.add((pw[0], am[0]))
                existing_pairs.add((am[0], pw[0]))
                strategy3_new += 1

        if strategy3_new >= 3000:
            break

    db.commit()
    new_total += strategy3_new
    print(f"  Strategy 3: {strategy3_new:,} new", flush=True)

    # ── Strategy 4: Regional pilgrimage → traditional crafts/festivals ──
    print("\n=== Strategy 4: Regional pilgrimage → crafts/festivals ===", flush=True)

    # Get pilgrimage locations by region
    regions = ['hokkaido', 'tohoku', 'kanto', 'chubu', 'kinki', 'chugoku', 'shikoku', 'kyushu']
    strategy4_new = 0

    for region in regions:
        # Get pilgrimage entities in this region
        pilgrim_in_region = db.execute("""
            SELECT DISTINCT e.id, e.label_ja
            FROM entities e
            JOIN entity_tags et ON e.id = et.entity_id
            JOIN connections c ON (c.entity_a_id = e.id OR c.entity_b_id = e.id)
            WHERE et.axis = 'geography' AND et.value_code = ?
            AND c.connection_type LIKE 'pilgrimage%'
            LIMIT 50
        """, (region,)).fetchall()

        if not pilgrim_in_region:
            continue

        # Get crafts/festivals in same region
        crafts = db.execute("""
            SELECT DISTINCT e.id, e.label_ja
            FROM entities e
            JOIN entity_tags et ON e.id = et.entity_id
            WHERE et.axis = 'geography' AND et.value_code = ?
            AND e.entity_type IN ('craft', 'festival', 'cultural_property', 'tradition')
            LIMIT 50
        """, (region,)).fetchall()

        for pe in pilgrim_in_region:
            for cr in crafts:
                if (pe[0], cr[0]) in existing_pairs:
                    continue
                db.execute("""
                    INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, 'pilgrimage_regional', 0.6, ?, 'pilgrimage_cross', 0.7, 'keep')
                """, (pe[0], cr[0],
                      f"聖地巡礼で{region}を訪れたら「{cr[1]}」も体験できる"))
                existing_pairs.add((pe[0], cr[0]))
                existing_pairs.add((cr[0], pe[0]))
                strategy4_new += 1

                if strategy4_new >= 2000:
                    break
            if strategy4_new >= 2000:
                break
        if strategy4_new >= 2000:
            break

    db.commit()
    new_total += strategy4_new
    print(f"  Strategy 4: {strategy4_new:,} new", flush=True)

    # ── Strategy 5: Pilgrimage works → cultural landmarks (coordinate-based) ──
    print("\n=== Strategy 5: Coordinate-based pilgrimage → landmark connections ===", flush=True)

    # Get all pilgrimage work → location pairs with coordinates
    pairs_with_coords = db.execute("""
        SELECT e_work.id AS work_id, e_work.label_ja AS work_name,
               e_loc.lat, e_loc.lon
        FROM connections c
        JOIN entities e_work ON (
            (c.entity_a_id = e_work.id AND e_work.entity_type != 'place')
            OR (c.entity_b_id = e_work.id AND e_work.entity_type != 'place')
        )
        JOIN entities e_loc ON (
            (c.entity_a_id = e_loc.id AND e_loc.entity_type = 'place')
            OR (c.entity_b_id = e_loc.id AND e_loc.entity_type = 'place')
        )
        WHERE c.connection_type LIKE 'pilgrimage%'
        AND e_loc.lat IS NOT NULL
    """).fetchall()
    print(f"  Work-location pairs: {len(pairs_with_coords)}", flush=True)

    strategy5_new = 0
    for pair in pairs_with_coords:
        work_id = pair[0]
        lat, lon = pair[2], pair[3]
        if lat is None or lon is None:
            continue

        # Find landmarks near this location
        lat_off = 0.3  # ~33km
        lon_off = 0.3
        nearby_landmarks = db.execute("""
            SELECT id, label_ja, entity_type FROM entities
            WHERE lat BETWEEN ? AND ?
            AND lon BETWEEN ? AND ?
            AND entity_type IN ('shrine', 'temple', 'museum', 'cultural_property', 'place', 'building')
            AND id != ?
            ORDER BY RANDOM()
            LIMIT 5
        """, (lat - lat_off, lat + lat_off, lon - lon_off, lon + lon_off, work_id)).fetchall()

        for lm in nearby_landmarks:
            if (work_id, lm[0]) in existing_pairs:
                continue
            db.execute("""
                INSERT INTO connections
                (entity_a_id, entity_b_id, connection_type, serendipity_score,
                 explanation, source, confidence, llm_verdict)
                VALUES (?, ?, 'pilgrimage_landmark', 0.65, ?, 'pilgrimage_cross', 0.75, 'keep')
            """, (work_id, lm[0],
                  f"「{pair[1]}」の聖地巡礼で「{lm[1]}」（{lm[2]}）も訪問推奨"))
            existing_pairs.add((work_id, lm[0]))
            existing_pairs.add((lm[0], work_id))
            strategy5_new += 1

        if strategy5_new >= 3000:
            break

    db.commit()
    new_total += strategy5_new
    print(f"  Strategy 5: {strategy5_new:,} new", flush=True)

    # ── Summary ──
    total_pilgrim = db.execute("SELECT COUNT(*) FROM connections WHERE connection_type LIKE 'pilgrimage%'").fetchone()[0]
    total_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== Pilgrimage Cross-Connection Complete ===", flush=True)
    print(f"New connections: {new_total:,}", flush=True)
    print(f"Total pilgrimage connections: {total_pilgrim:,}", flush=True)
    print(f"Total connections: {total_conns:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
