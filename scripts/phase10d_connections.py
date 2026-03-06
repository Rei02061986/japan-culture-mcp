"""
Phase 10D: Massive connection generation for 1M+ entities.
Target: 200,000+ connections with high cultural relevance.
Strategy: Tag-based matching across all axes with source diversity bonus.
"""
import sqlite3
import random
import time

DB_PATH = "ontology/culture_ontology.db"
random.seed(42)


def main():
    db = sqlite3.connect(DB_PATH)
    db.execute("PRAGMA journal_mode=WAL")

    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    existing_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"Entities: {total_entities:,}", flush=True)
    print(f"Existing connections: {existing_conns:,}", flush=True)

    # Get existing connection pairs for dedup
    existing_pairs = set()
    for row in db.execute("SELECT entity_a_id, entity_b_id FROM connections"):
        a, b = min(row[0], row[1]), max(row[0], row[1])
        existing_pairs.add((a, b))
    print(f"Existing pairs: {len(existing_pairs):,}", flush=True)

    new_conns = 0
    target = 200000 - existing_conns  # How many more we need

    # === Strategy 1: Same theme, different medium ===
    print("\n=== Strategy 1: Same theme, different medium (cross-medium discovery) ===", flush=True)
    themes = db.execute("""
        SELECT DISTINCT value_code FROM entity_tags WHERE axis='theme'
    """).fetchall()

    s1_count = 0
    for (theme,) in themes:
        # Get entities with this theme, grouped by medium
        entities_by_medium = {}
        rows = db.execute("""
            SELECT DISTINCT et_t.entity_id, et_m.value_code as medium
            FROM entity_tags et_t
            JOIN entity_tags et_m ON et_t.entity_id = et_m.entity_id AND et_m.axis='medium'
            WHERE et_t.axis='theme' AND et_t.value_code=?
            ORDER BY RANDOM()
            LIMIT 5000
        """, (theme,)).fetchall()

        for eid, medium in rows:
            if medium not in entities_by_medium:
                entities_by_medium[medium] = []
            entities_by_medium[medium].append(eid)

        mediums = list(entities_by_medium.keys())
        if len(mediums) < 2:
            continue

        # Create connections between different mediums
        batch = 0
        for i in range(len(mediums)):
            for j in range(i + 1, len(mediums)):
                m1_ents = entities_by_medium[mediums[i]]
                m2_ents = entities_by_medium[mediums[j]]
                # Sample pairs
                n_pairs = min(20, len(m1_ents), len(m2_ents))
                if n_pairs == 0:
                    continue
                sample1 = random.sample(m1_ents, n_pairs)
                sample2 = random.sample(m2_ents, n_pairs)
                for a, b in zip(sample1, sample2):
                    pair = (min(a, b), max(a, b))
                    if pair in existing_pairs or a == b:
                        continue
                    db.execute("""
                        INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                            theme_distance, medium_distance, serendipity_score, source, confidence)
                        VALUES (?, ?, 'cross_medium', 0.1, 0.8, 0.7, 'phase10_conn', 0.7)
                    """, (a, b))
                    existing_pairs.add(pair)
                    batch += 1
                    s1_count += 1
                    new_conns += 1

        if batch > 0 and s1_count % 5000 == 0:
            db.commit()
            print(f"  Theme '{theme}': {batch} new (total S1: {s1_count:,})", flush=True)

    db.commit()
    print(f"  Strategy 1 total: {s1_count:,}", flush=True)

    # === Strategy 2: Same era, different source ===
    print("\n=== Strategy 2: Same era, different source (temporal connections) ===", flush=True)
    eras = db.execute("SELECT DISTINCT value_code FROM entity_tags WHERE axis='era'").fetchall()

    s2_count = 0
    for (era,) in eras:
        entities_by_source = {}
        rows = db.execute("""
            SELECT DISTINCT et.entity_id, e.source
            FROM entity_tags et
            JOIN entities e ON et.entity_id = e.id
            WHERE et.axis='era' AND et.value_code=?
            ORDER BY RANDOM()
            LIMIT 5000
        """, (era,)).fetchall()

        for eid, source in rows:
            if source not in entities_by_source:
                entities_by_source[source] = []
            entities_by_source[source].append(eid)

        sources = list(entities_by_source.keys())
        if len(sources) < 2:
            continue

        batch = 0
        for i in range(len(sources)):
            for j in range(i + 1, len(sources)):
                s1_ents = entities_by_source[sources[i]]
                s2_ents = entities_by_source[sources[j]]
                n_pairs = min(15, len(s1_ents), len(s2_ents))
                if n_pairs == 0:
                    continue
                sample1 = random.sample(s1_ents, n_pairs)
                sample2 = random.sample(s2_ents, n_pairs)
                for a, b in zip(sample1, sample2):
                    pair = (min(a, b), max(a, b))
                    if pair in existing_pairs or a == b:
                        continue
                    db.execute("""
                        INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                            era_distance, serendipity_score, source, confidence)
                        VALUES (?, ?, 'same_era', 0.0, 0.5, 'phase10_conn', 0.6)
                    """, (a, b))
                    existing_pairs.add(pair)
                    batch += 1
                    s2_count += 1
                    new_conns += 1

        if batch > 0:
            db.commit()
            print(f"  Era '{era}': {batch} new (total S2: {s2_count:,})", flush=True)

    db.commit()
    print(f"  Strategy 2 total: {s2_count:,}", flush=True)

    # === Strategy 3: Same geography, different theme ===
    print("\n=== Strategy 3: Same geography, different theme (regional discovery) ===", flush=True)
    geos = db.execute("SELECT DISTINCT value_code FROM entity_tags WHERE axis='geography'").fetchall()

    s3_count = 0
    for (geo,) in geos:
        entities_by_theme = {}
        rows = db.execute("""
            SELECT DISTINCT et_g.entity_id, et_t.value_code as theme
            FROM entity_tags et_g
            JOIN entity_tags et_t ON et_g.entity_id = et_t.entity_id AND et_t.axis='theme'
            WHERE et_g.axis='geography' AND et_g.value_code=?
            ORDER BY RANDOM()
            LIMIT 5000
        """, (geo,)).fetchall()

        for eid, theme in rows:
            if theme not in entities_by_theme:
                entities_by_theme[theme] = []
            entities_by_theme[theme].append(eid)

        theme_keys = list(entities_by_theme.keys())
        if len(theme_keys) < 2:
            continue

        batch = 0
        for i in range(len(theme_keys)):
            for j in range(i + 1, len(theme_keys)):
                t1_ents = entities_by_theme[theme_keys[i]]
                t2_ents = entities_by_theme[theme_keys[j]]
                n_pairs = min(10, len(t1_ents), len(t2_ents))
                if n_pairs == 0:
                    continue
                sample1 = random.sample(t1_ents, n_pairs)
                sample2 = random.sample(t2_ents, n_pairs)
                for a, b in zip(sample1, sample2):
                    pair = (min(a, b), max(a, b))
                    if pair in existing_pairs or a == b:
                        continue
                    db.execute("""
                        INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                            geography_distance, theme_distance, serendipity_score, source, confidence)
                        VALUES (?, ?, 'regional_discovery', 0.0, 0.7, 0.6, 'phase10_conn', 0.6)
                    """, (a, b))
                    existing_pairs.add(pair)
                    batch += 1
                    s3_count += 1
                    new_conns += 1

        if batch > 0:
            db.commit()
            print(f"  Geo '{geo}': {batch} new (total S3: {s3_count:,})", flush=True)

    db.commit()
    print(f"  Strategy 3 total: {s3_count:,}", flush=True)

    # === Strategy 4: Experience mode connections (aesthetic ↔ intellectual) ===
    print("\n=== Strategy 4: Experience mode cross-connections ===", flush=True)
    exp_pairs = [
        ('aesthetic', 'intellectual'),
        ('aesthetic', 'reflective'),
        ('intellectual', 'social'),
        ('physical', 'aesthetic'),
    ]

    s4_count = 0
    for exp_a, exp_b in exp_pairs:
        ents_a = [r[0] for r in db.execute("""
            SELECT entity_id FROM entity_tags WHERE axis='experience' AND value_code=?
            ORDER BY RANDOM() LIMIT 3000
        """, (exp_a,)).fetchall()]
        ents_b = [r[0] for r in db.execute("""
            SELECT entity_id FROM entity_tags WHERE axis='experience' AND value_code=?
            ORDER BY RANDOM() LIMIT 3000
        """, (exp_b,)).fetchall()]

        n_pairs = min(2000, len(ents_a), len(ents_b))
        if n_pairs == 0:
            continue

        sample_a = random.sample(ents_a, n_pairs)
        sample_b = random.sample(ents_b, n_pairs)

        batch = 0
        for a, b in zip(sample_a, sample_b):
            pair = (min(a, b), max(a, b))
            if pair in existing_pairs or a == b:
                continue
            db.execute("""
                INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                    experience_distance, serendipity_score, source, confidence)
                VALUES (?, ?, 'experience_cross', 0.5, 0.6, 'phase10_conn', 0.5)
            """, (a, b))
            existing_pairs.add(pair)
            batch += 1
            s4_count += 1
            new_conns += 1

        db.commit()
        print(f"  {exp_a} ↔ {exp_b}: {batch} new", flush=True)

    print(f"  Strategy 4 total: {s4_count:,}", flush=True)

    # === Strategy 5: Random serendipity (different everything) ===
    print("\n=== Strategy 5: Random serendipity connections ===", flush=True)
    # Pick random pairs from different sources
    sources = [r[0] for r in db.execute(
        "SELECT source FROM entities GROUP BY source HAVING COUNT(*) > 100 ORDER BY COUNT(*) DESC"
    ).fetchall()]

    s5_count = 0
    for i in range(len(sources)):
        for j in range(i + 1, len(sources)):
            ents_a = [r[0] for r in db.execute(
                "SELECT id FROM entities WHERE source=? ORDER BY RANDOM() LIMIT 500",
                (sources[i],)
            ).fetchall()]
            ents_b = [r[0] for r in db.execute(
                "SELECT id FROM entities WHERE source=? ORDER BY RANDOM() LIMIT 500",
                (sources[j],)
            ).fetchall()]

            n_pairs = min(100, len(ents_a), len(ents_b))
            if n_pairs == 0:
                continue

            sample_a = random.sample(ents_a, n_pairs)
            sample_b = random.sample(ents_b, n_pairs)

            batch = 0
            for a, b in zip(sample_a, sample_b):
                pair = (min(a, b), max(a, b))
                if pair in existing_pairs or a == b:
                    continue
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        serendipity_score, source, confidence)
                    VALUES (?, ?, 'serendipity', 0.8, 'phase10_conn', 0.4)
                """, (a, b))
                existing_pairs.add(pair)
                batch += 1
                s5_count += 1
                new_conns += 1

            if batch > 0:
                db.commit()

    print(f"  Strategy 5 total: {s5_count:,}", flush=True)

    # Final stats
    db.commit()
    final_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    final_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    print(f"\n{'='*60}", flush=True)
    print(f"=== Connection Generation Complete ===", flush=True)
    print(f"New connections: {new_conns:,}", flush=True)
    print(f"Total connections: {final_conns:,}", flush=True)
    print(f"Total entities: {final_entities:,}", flush=True)
    print(f"Density: {final_conns/final_entities*100:.2f}%", flush=True)
    db.close()


if __name__ == "__main__":
    main()
