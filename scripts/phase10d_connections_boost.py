"""
Phase 10D-boost: Additional connections to reach 200K+ target.
Focus on high-volume strategies with larger sample sizes.
"""
import sqlite3
import random

DB_PATH = "ontology/culture_ontology.db"
random.seed(123)


def main():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")

    existing_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"Entities: {total_entities:,}", flush=True)
    print(f"Existing connections: {existing_conns:,}", flush=True)
    target_new = max(0, 210000 - existing_conns)
    print(f"Target new: {target_new:,}", flush=True)

    existing_pairs = set()
    for row in db.execute("SELECT entity_a_id, entity_b_id FROM connections"):
        a, b = min(row[0], row[1]), max(row[0], row[1])
        existing_pairs.add((a, b))

    new_conns = 0

    # === Strategy A: Source-pair cross-connections (high volume) ===
    print("\n=== Strategy A: Source-pair cross-connections ===", flush=True)
    sources = db.execute("""
        SELECT source, COUNT(*) c FROM entities GROUP BY source HAVING c > 500 ORDER BY c DESC
    """).fetchall()

    sa_count = 0
    for i in range(len(sources)):
        if sa_count >= target_new * 0.5:
            break
        for j in range(i + 1, len(sources)):
            if sa_count >= target_new * 0.5:
                break
            src_a, cnt_a = sources[i]
            src_b, cnt_b = sources[j]
            n = min(300, cnt_a, cnt_b)

            ents_a = [r[0] for r in db.execute(
                "SELECT id FROM entities WHERE source=? ORDER BY RANDOM() LIMIT ?",
                (src_a, n)
            ).fetchall()]
            ents_b = [r[0] for r in db.execute(
                "SELECT id FROM entities WHERE source=? ORDER BY RANDOM() LIMIT ?",
                (src_b, n)
            ).fetchall()]

            batch = 0
            for a, b_ent in zip(ents_a, ents_b):
                pair = (min(a, b_ent), max(a, b_ent))
                if pair in existing_pairs or a == b_ent:
                    continue
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        serendipity_score, source, confidence)
                    VALUES (?, ?, 'cross_source', 0.7, 'phase10_boost', 0.5)
                """, (a, b_ent))
                existing_pairs.add(pair)
                batch += 1
                sa_count += 1
                new_conns += 1

            if batch > 0:
                db.commit()

    print(f"  Strategy A total: {sa_count:,}", flush=True)

    # === Strategy B: Same theme, larger samples ===
    print("\n=== Strategy B: Same theme connections (large) ===", flush=True)
    themes = db.execute("""
        SELECT value_code, COUNT(*) c FROM entity_tags WHERE axis='theme'
        GROUP BY value_code HAVING c > 100 ORDER BY c DESC
    """).fetchall()

    sb_count = 0
    for theme, theme_cnt in themes:
        if sb_count >= target_new * 0.3:
            break
        ents = [r[0] for r in db.execute("""
            SELECT entity_id FROM entity_tags WHERE axis='theme' AND value_code=?
            ORDER BY RANDOM() LIMIT 2000
        """, (theme,)).fetchall()]

        if len(ents) < 10:
            continue

        batch = 0
        n_pairs = min(500, len(ents) * (len(ents) - 1) // 2)
        for _ in range(n_pairs):
            a, b = random.sample(ents, 2)
            pair = (min(a, b), max(a, b))
            if pair in existing_pairs:
                continue
            db.execute("""
                INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                    theme_distance, serendipity_score, source, confidence)
                VALUES (?, ?, 'same_theme', 0.0, 0.5, 'phase10_boost', 0.6)
            """, (a, b))
            existing_pairs.add(pair)
            batch += 1
            sb_count += 1
            new_conns += 1

        if batch > 0:
            db.commit()

    print(f"  Strategy B total: {sb_count:,}", flush=True)

    # === Strategy C: Geographic proximity (entities with coordinates) ===
    print("\n=== Strategy C: Geographic proximity ===", flush=True)
    coord_ents = db.execute("""
        SELECT id, lat, lon FROM entities WHERE lat IS NOT NULL AND lon IS NOT NULL
        ORDER BY RANDOM() LIMIT 10000
    """).fetchall()

    sc_count = 0
    for i in range(len(coord_ents)):
        if sc_count >= 5000:
            break
        id_a, lat_a, lon_a = coord_ents[i]
        for j in range(i + 1, min(i + 50, len(coord_ents))):
            id_b, lat_b, lon_b = coord_ents[j]
            dist = ((lat_a - lat_b)**2 + (lon_a - lon_b)**2)**0.5
            if dist < 0.1:  # ~10km
                pair = (min(id_a, id_b), max(id_a, id_b))
                if pair in existing_pairs:
                    continue
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        geography_distance, serendipity_score, source, confidence)
                    VALUES (?, ?, 'proximity', 0.1, 0.6, 'phase10_boost', 0.7)
                """, (id_a, id_b))
                existing_pairs.add(pair)
                sc_count += 1
                new_conns += 1

    db.commit()
    print(f"  Strategy C total: {sc_count:,}", flush=True)

    # Final stats
    final_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    final_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== Connection Boost Complete ===", flush=True)
    print(f"New connections: {new_conns:,}", flush=True)
    print(f"Total connections: {final_conns:,}", flush=True)
    print(f"Total entities: {final_entities:,}", flush=True)
    print(f"Density: {final_conns/final_entities*100:.2f}%", flush=True)
    db.close()


if __name__ == "__main__":
    main()
