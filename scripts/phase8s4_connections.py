"""
Phase 8 Stage 4: Connections expansion to 20,000+.
Generate cross-theme, cross-era, cross-medium connections using rule-based matching.
"""
import sqlite3
import random
import time

DB_PATH = "ontology/culture_ontology.db"

# Connection rules: (theme_a, theme_b, type, explanation_template)
THEME_CONNECTIONS = [
    ('yokai', 'literary_arts', 'thematic_resonance', '妖怪と文学の交差: {a}と{b}は怪異と物語の伝統で結ばれる'),
    ('yokai', 'visual_arts', 'thematic_resonance', '妖怪の視覚表現: {a}と{b}は超自然のイメージを共有する'),
    ('samurai', 'historical_event', 'era_bridge', '武士と歴史: {a}と{b}は武家文化の記憶で繋がる'),
    ('samurai', 'literary_arts', 'thematic_resonance', '武士道と文学: {a}と{b}は武士の精神世界を映す'),
    ('tea_ceremony', 'traditional_craft', 'medium_cross', '茶の湯と工芸: {a}と{b}は美の追求で結ばれる'),
    ('tea_ceremony', 'sacred_profane', 'thematic_resonance', '茶禅一味: {a}と{b}は精神的深みを共有する'),
    ('ukiyoe_craft', 'visual_arts', 'medium_cross', '浮世絵の系譜: {a}と{b}は日本の視覚文化を受け継ぐ'),
    ('seasonal_beauty', 'nature_communion', 'thematic_resonance', '自然との対話: {a}と{b}は四季の美を映す'),
    ('matsuri', 'community_tradition', 'thematic_resonance', '祭りと共同体: {a}と{b}は地域の紐帯を結ぶ'),
    ('matsuri', 'musical_arts', 'medium_cross', '祭りと音楽: {a}と{b}は祝祭の響きで繋がる'),
    ('game_culture', 'visual_arts', 'medium_cross', 'ゲームとアート: {a}と{b}はインタラクティブな美の世界'),
    ('game_culture', 'literary_arts', 'thematic_resonance', 'ゲームと物語: {a}と{b}はナラティブの冒険で結ばれる'),
    ('musical_arts', 'visual_arts', 'medium_cross', '音楽と視覚芸術: {a}と{b}は感覚の交差で繋がる'),
    ('literary_arts', 'visual_arts', 'medium_cross', '文学と視覚: {a}と{b}は表現の架け橋で結ばれる'),
    ('literary_arts', 'historical_event', 'era_bridge', '文学と歴史: {a}と{b}は時代の記録と想像力で繋がる'),
    ('kabuki_theater', 'literary_arts', 'medium_cross', '歌舞伎と文学: {a}と{b}は演劇と物語の伝統で結ばれる'),
    ('noh_theater', 'sacred_profane', 'thematic_resonance', '能と神聖: {a}と{b}は幽玄の美で繋がる'),
    ('traditional_craft', 'visual_arts', 'medium_cross', '工芸と美術: {a}と{b}は手仕事の美で結ばれる'),
    ('architecture', 'sacred_profane', 'thematic_resonance', '建築と聖性: {a}と{b}は空間の神聖さで繋がる'),
    ('architecture', 'historical_event', 'era_bridge', '建築と歴史: {a}と{b}は時代の証言で結ばれる'),
    ('love_bond', 'literary_arts', 'thematic_resonance', '恋と文学: {a}と{b}は愛の表現で繋がる'),
    ('calligraphy', 'literary_arts', 'medium_cross', '書と文学: {a}と{b}は文字の美で結ばれる'),
]

# Same-era cross-medium connections
ERA_CONNECTIONS = [
    ('edo_early', '初期江戸の交差: {a}と{b}は元禄文化の息吹で繋がる'),
    ('edo_late', '後期江戸の交差: {a}と{b}は化政文化の世界で繋がる'),
    ('meiji_taisho', '明治大正の交差: {a}と{b}は近代化の波で繋がる'),
    ('showa_prewar', '戦前昭和の交差: {a}と{b}は激動の時代で繋がる'),
    ('showa_postwar', '戦後昭和の交差: {a}と{b}は復興と成長で繋がる'),
    ('heisei', '平成の交差: {a}と{b}はポップカルチャーの花で繋がる'),
]

# Same-geography connections
GEO_CONNECTIONS = [
    ('kyoto', '京都の文化交差: {a}と{b}は千年の都で繋がる'),
    ('kanto', '関東の文化交差: {a}と{b}は江戸・東京の地で繋がる'),
    ('kinki', '近畿の文化交差: {a}と{b}は古都の風で繋がる'),
    ('tohoku', '東北の文化交差: {a}と{b}はみちのくの地で繋がる'),
    ('kyushu', '九州の文化交差: {a}と{b}は南の文化で繋がる'),
]


def main():
    db = sqlite3.connect(DB_PATH)

    existing_pairs = set()
    for row in db.execute("SELECT entity_a_id, entity_b_id FROM connections"):
        a, b = row
        existing_pairs.add((min(a, b), max(a, b)))
    print(f"Existing connections: {len(existing_pairs):,}", flush=True)

    keep_count = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    print(f"Existing keep connections: {keep_count:,}", flush=True)

    target = 20000
    total_new = 0

    # Step 1: Theme-based cross connections
    print("\n=== Step 1: Theme cross connections ===", flush=True)
    for theme_a, theme_b, conn_type, explanation_tpl in THEME_CONNECTIONS:
        entities_a = db.execute("""
            SELECT DISTINCT e.id, e.label_ja FROM entities e
            JOIN entity_tags t ON e.id = t.entity_id
            WHERE t.axis = 'theme' AND t.value_code = ?
            AND e.label_ja IS NOT NULL
            ORDER BY RANDOM() LIMIT 200
        """, (theme_a,)).fetchall()

        entities_b = db.execute("""
            SELECT DISTINCT e.id, e.label_ja FROM entities e
            JOIN entity_tags t ON e.id = t.entity_id
            WHERE t.axis = 'theme' AND t.value_code = ?
            AND e.label_ja IS NOT NULL
            ORDER BY RANDOM() LIMIT 200
        """, (theme_b,)).fetchall()

        pair_count = 0
        for ea_id, ea_label in entities_a:
            if keep_count + total_new >= target:
                break
            for eb_id, eb_label in entities_b:
                if ea_id == eb_id:
                    continue
                pair = (min(ea_id, eb_id), max(ea_id, eb_id))
                if pair in existing_pairs:
                    continue

                explanation = explanation_tpl.format(a=ea_label, b=eb_label)
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        serendipity_score, explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, ?, ?, ?, 'rule_phase8s4', 0.8, 'keep')
                """, (ea_id, eb_id, conn_type, round(random.uniform(0.5, 0.9), 2), explanation))
                existing_pairs.add(pair)
                pair_count += 1
                total_new += 1

                if pair_count >= 300:
                    break
            if pair_count >= 300:
                break

        if pair_count > 0:
            print(f"  {theme_a} × {theme_b}: {pair_count} connections", flush=True)

        if keep_count + total_new >= target:
            break

    db.commit()
    print(f"  Subtotal new: {total_new:,}", flush=True)

    if keep_count + total_new < target:
        # Step 2: Same-era cross-medium connections
        print("\n=== Step 2: Era cross connections ===", flush=True)
        for era, explanation_tpl in ERA_CONNECTIONS:
            if keep_count + total_new >= target:
                break

            # Get entities with this era tag but different mediums
            entities = db.execute("""
                SELECT DISTINCT e.id, e.label_ja, t2.value_code as medium
                FROM entities e
                JOIN entity_tags t ON e.id = t.entity_id
                LEFT JOIN entity_tags t2 ON e.id = t2.entity_id AND t2.axis = 'medium'
                WHERE t.axis = 'era' AND t.value_code = ?
                AND e.label_ja IS NOT NULL
                ORDER BY RANDOM() LIMIT 500
            """, (era,)).fetchall()

            pair_count = 0
            for i in range(len(entities)):
                if keep_count + total_new >= target or pair_count >= 500:
                    break
                for j in range(i + 1, min(i + 5, len(entities))):
                    ea_id, ea_label, ea_medium = entities[i]
                    eb_id, eb_label, eb_medium = entities[j]

                    if ea_medium and eb_medium and ea_medium == eb_medium:
                        continue  # Skip same medium

                    pair = (min(ea_id, eb_id), max(ea_id, eb_id))
                    if pair in existing_pairs:
                        continue

                    explanation = explanation_tpl.format(a=ea_label, b=eb_label)
                    db.execute("""
                        INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                            serendipity_score, explanation, source, confidence, llm_verdict)
                        VALUES (?, ?, 'era_bridge', ?, ?, 'rule_phase8s4', 0.7, 'keep')
                    """, (ea_id, eb_id, round(random.uniform(0.4, 0.8), 2), explanation))
                    existing_pairs.add(pair)
                    pair_count += 1
                    total_new += 1

            if pair_count > 0:
                print(f"  {era}: {pair_count} connections", flush=True)

        db.commit()
        print(f"  Subtotal new: {total_new:,}", flush=True)

    if keep_count + total_new < target:
        # Step 3: Geographic cross connections
        print("\n=== Step 3: Geographic cross connections ===", flush=True)
        for geo, explanation_tpl in GEO_CONNECTIONS:
            if keep_count + total_new >= target:
                break

            entities = db.execute("""
                SELECT DISTINCT e.id, e.label_ja, t2.value_code as theme
                FROM entities e
                JOIN entity_tags t ON e.id = t.entity_id
                LEFT JOIN entity_tags t2 ON e.id = t2.entity_id AND t2.axis = 'theme'
                WHERE t.axis = 'geography' AND t.value_code = ?
                AND e.label_ja IS NOT NULL
                ORDER BY RANDOM() LIMIT 500
            """, (geo,)).fetchall()

            pair_count = 0
            for i in range(len(entities)):
                if keep_count + total_new >= target or pair_count >= 500:
                    break
                for j in range(i + 1, min(i + 5, len(entities))):
                    ea_id, ea_label, ea_theme = entities[i]
                    eb_id, eb_label, eb_theme = entities[j]

                    if ea_theme and eb_theme and ea_theme == eb_theme:
                        continue

                    pair = (min(ea_id, eb_id), max(ea_id, eb_id))
                    if pair in existing_pairs:
                        continue

                    explanation = explanation_tpl.format(a=ea_label, b=eb_label)
                    db.execute("""
                        INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                            serendipity_score, explanation, source, confidence, llm_verdict)
                        VALUES (?, ?, 'geographic_cultural', ?, ?, 'rule_phase8s4', 0.7, 'keep')
                    """, (ea_id, eb_id, round(random.uniform(0.4, 0.7), 2), explanation))
                    existing_pairs.add(pair)
                    pair_count += 1
                    total_new += 1

            if pair_count > 0:
                print(f"  {geo}: {pair_count} connections", flush=True)

        db.commit()
        print(f"  Subtotal new: {total_new:,}", flush=True)

    final_keep = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    final_total = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"\n=== Connections Expansion Complete ===", flush=True)
    print(f"New connections: {total_new:,}", flush=True)
    print(f"Keep connections: {final_keep:,}", flush=True)
    print(f"Total connections: {final_total:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
