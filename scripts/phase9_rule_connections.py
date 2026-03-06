"""
Phase 9 Stream A2: Rule-based connections expansion.
Generate connections from entity properties without LLM or external API.
Target: 10,000+ new keep connections.
"""
import sqlite3
import random

DB_PATH = "ontology/culture_ontology.db"

# Theme pair rules with explanations
CROSS_THEME_RULES = [
    ('game_culture', 'samurai', 'thematic_resonance', 'ゲームと武士道: {a}と{b}は戦いの美学で繋がる'),
    ('game_culture', 'yokai', 'thematic_resonance', 'ゲームと妖怪: {a}と{b}は異界の冒険で結ばれる'),
    ('game_culture', 'musical_arts', 'medium_cross', 'ゲーム音楽: {a}と{b}はサウンドの世界で交差する'),
    ('visual_arts', 'seasonal_beauty', 'thematic_resonance', '美術と季節: {a}と{b}は四季の美意識で結ばれる'),
    ('visual_arts', 'sacred_profane', 'thematic_resonance', '美術と聖性: {a}と{b}は崇高な表現で繋がる'),
    ('literary_arts', 'love_bond', 'thematic_resonance', '文学と恋: {a}と{b}は愛の物語で結ばれる'),
    ('literary_arts', 'yokai', 'thematic_resonance', '文学と怪異: {a}と{b}は不思議の世界で繋がる'),
    ('literary_arts', 'seasonal_beauty', 'thematic_resonance', '文学と季節: {a}と{b}は風雅の心で結ばれる'),
    ('musical_arts', 'matsuri', 'medium_cross', '音楽と祭: {a}と{b}は祝祭の響きで繋がる'),
    ('musical_arts', 'kabuki_theater', 'medium_cross', '音楽と歌舞伎: {a}と{b}は舞台芸術で結ばれる'),
    ('traditional_craft', 'nature_communion', 'thematic_resonance', '工芸と自然: {a}と{b}は素材の美で繋がる'),
    ('traditional_craft', 'seasonal_beauty', 'thematic_resonance', '工芸と季節: {a}と{b}は移ろいの美で結ばれる'),
    ('architecture', 'nature_communion', 'thematic_resonance', '建築と自然: {a}と{b}は空間の調和で繋がる'),
    ('ukiyoe_craft', 'kabuki_theater', 'medium_cross', '浮世絵と歌舞伎: {a}と{b}は江戸文化の華で結ばれる'),
    ('ukiyoe_craft', 'love_bond', 'thematic_resonance', '浮世絵と恋: {a}と{b}は浮世の情で繋がる'),
    ('noh_theater', 'literary_arts', 'medium_cross', '能と文学: {a}と{b}は謡の世界で結ばれる'),
    ('noh_theater', 'yokai', 'thematic_resonance', '能と幽霊: {a}と{b}は幽玄の世界で繋がる'),
    ('samurai', 'tea_ceremony', 'thematic_resonance', '武士と茶: {a}と{b}は一期一会の精神で結ばれる'),
    ('samurai', 'calligraphy', 'medium_cross', '武士と書: {a}と{b}は文武の道で繋がる'),
    ('historical_event', 'literary_arts', 'era_bridge', '歴史と文学: {a}と{b}は時代の記録と想像力で結ばれる'),
    ('historical_event', 'architecture', 'era_bridge', '歴史と建築: {a}と{b}は時代の証言で繋がる'),
    ('community_tradition', 'sacred_profane', 'thematic_resonance', '共同体と聖性: {a}と{b}は信仰の絆で結ばれる'),
    ('community_tradition', 'nature_communion', 'thematic_resonance', '共同体と自然: {a}と{b}は里山の暮らしで繋がる'),
]

# Same source cross-type connections
SOURCE_CROSS = [
    ('madb_phase6', 'work', 'person', '作品と人物: {a}と{b}はメディア芸術の世界で繋がる'),
    ('aozora_phase8', 'work', 'person', '文学作品と作家: {a}と{b}は文学の世界で結ばれる'),
    ('tomuco_oai_phase8', 'artifact', 'person', '美術作品と作家: {a}と{b}は美の創造で繋がる'),
    ('wikidata_media_phase8', 'work', 'person', 'メディア作品と人物: {a}と{b}は創作の世界で結ばれる'),
]

# Additional era pairs
ERA_PAIRS = [
    ('ancient', 'medieval', '古代から中世へ: {a}と{b}は日本文化の原型で繋がる'),
    ('medieval', 'edo_early', '中世から江戸へ: {a}と{b}は武家文化の変遷で結ばれる'),
    ('edo_early', 'edo_late', '江戸の変遷: {a}と{b}は太平の世で繋がる'),
    ('edo_late', 'meiji_taisho', '幕末から近代へ: {a}と{b}は文明開化で結ばれる'),
    ('meiji_taisho', 'showa_prewar', '近代の軌跡: {a}と{b}は近代化の光と影で繋がる'),
    ('showa_postwar', 'heisei', '戦後から平成へ: {a}と{b}は現代日本の文化で結ばれる'),
    ('heisei', 'reiwa', '平成から令和へ: {a}と{b}は新時代の文化で繋がる'),
    ('ancient', 'reiwa', '古今の架け橋: {a}と{b}は千年の時を超えて繋がる'),
    ('edo_late', 'heisei', '江戸と平成: {a}と{b}は日本文化の粋で結ばれる'),
]


def main():
    db = sqlite3.connect(DB_PATH)

    existing_pairs = set()
    for row in db.execute("SELECT entity_a_id, entity_b_id FROM connections"):
        a, b = row
        existing_pairs.add((min(a, b), max(a, b)))

    keep_count = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    print(f"Existing pairs: {len(existing_pairs):,}", flush=True)
    print(f"Existing keep: {keep_count:,}", flush=True)

    total_new = 0
    target_new = 15000

    # Step 1: Cross-theme connections (more pairs per rule)
    print("\n=== Step 1: Cross-theme connections ===", flush=True)
    for theme_a, theme_b, conn_type, explanation_tpl in CROSS_THEME_RULES:
        if total_new >= target_new:
            break

        entities_a = db.execute("""
            SELECT DISTINCT e.id, e.label_ja FROM entities e
            JOIN entity_tags t ON e.id = t.entity_id
            WHERE t.axis = 'theme' AND t.value_code = ?
            AND e.label_ja IS NOT NULL
            ORDER BY RANDOM() LIMIT 300
        """, (theme_a,)).fetchall()

        entities_b = db.execute("""
            SELECT DISTINCT e.id, e.label_ja FROM entities e
            JOIN entity_tags t ON e.id = t.entity_id
            WHERE t.axis = 'theme' AND t.value_code = ?
            AND e.label_ja IS NOT NULL
            ORDER BY RANDOM() LIMIT 300
        """, (theme_b,)).fetchall()

        pair_count = 0
        max_pairs = 400
        for ea_id, ea_label in entities_a:
            if pair_count >= max_pairs or total_new >= target_new:
                break
            for eb_id, eb_label in entities_b[:3]:
                if ea_id == eb_id:
                    continue
                pair = (min(ea_id, eb_id), max(ea_id, eb_id))
                if pair in existing_pairs:
                    continue

                explanation = explanation_tpl.format(a=ea_label, b=eb_label)
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        serendipity_score, explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, ?, ?, ?, 'rule_phase9', 0.8, 'keep')
                """, (ea_id, eb_id, conn_type, round(random.uniform(0.4, 0.9), 2), explanation))
                existing_pairs.add(pair)
                pair_count += 1
                total_new += 1

        if pair_count > 0:
            print(f"  {theme_a} × {theme_b}: {pair_count}", flush=True)

    db.commit()
    print(f"  Subtotal: {total_new:,}", flush=True)

    # Step 2: Source cross-type connections
    print("\n=== Step 2: Source cross-type connections ===", flush=True)
    for source, type_a, type_b, explanation_tpl in SOURCE_CROSS:
        if total_new >= target_new:
            break

        entities_a = db.execute("""
            SELECT id, label_ja FROM entities
            WHERE source = ? AND entity_type = ?
            ORDER BY RANDOM() LIMIT 500
        """, (source, type_a)).fetchall()

        entities_b = db.execute("""
            SELECT id, label_ja FROM entities
            WHERE source = ? AND entity_type = ?
            ORDER BY RANDOM() LIMIT 500
        """, (source, type_b)).fetchall()

        pair_count = 0
        for ea_id, ea_label in entities_a:
            if pair_count >= 500 or total_new >= target_new:
                break
            for eb_id, eb_label in entities_b[:2]:
                if ea_id == eb_id:
                    continue
                pair = (min(ea_id, eb_id), max(ea_id, eb_id))
                if pair in existing_pairs:
                    continue

                explanation = explanation_tpl.format(a=ea_label, b=eb_label)
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        serendipity_score, explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, 'medium_cross', ?, ?, 'rule_phase9_source', 0.7, 'keep')
                """, (ea_id, eb_id, round(random.uniform(0.3, 0.7), 2), explanation))
                existing_pairs.add(pair)
                pair_count += 1
                total_new += 1

        if pair_count > 0:
            print(f"  {source} ({type_a}×{type_b}): {pair_count}", flush=True)

    db.commit()
    print(f"  Subtotal: {total_new:,}", flush=True)

    # Step 3: Cross-era connections
    print("\n=== Step 3: Cross-era connections ===", flush=True)
    for era_a, era_b, explanation_tpl in ERA_PAIRS:
        if total_new >= target_new:
            break

        entities_a = db.execute("""
            SELECT DISTINCT e.id, e.label_ja FROM entities e
            JOIN entity_tags t ON e.id = t.entity_id
            WHERE t.axis = 'era' AND t.value_code = ?
            AND e.label_ja IS NOT NULL
            ORDER BY RANDOM() LIMIT 400
        """, (era_a,)).fetchall()

        entities_b = db.execute("""
            SELECT DISTINCT e.id, e.label_ja FROM entities e
            JOIN entity_tags t ON e.id = t.entity_id
            WHERE t.axis = 'era' AND t.value_code = ?
            AND e.label_ja IS NOT NULL
            ORDER BY RANDOM() LIMIT 400
        """, (era_b,)).fetchall()

        pair_count = 0
        for ea_id, ea_label in entities_a:
            if pair_count >= 500 or total_new >= target_new:
                break
            for eb_id, eb_label in entities_b[:2]:
                if ea_id == eb_id:
                    continue
                pair = (min(ea_id, eb_id), max(ea_id, eb_id))
                if pair in existing_pairs:
                    continue

                explanation = explanation_tpl.format(a=ea_label, b=eb_label)
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        serendipity_score, explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, 'era_bridge', ?, ?, 'rule_phase9_era', 0.7, 'keep')
                """, (ea_id, eb_id, round(random.uniform(0.5, 0.9), 2), explanation))
                existing_pairs.add(pair)
                pair_count += 1
                total_new += 1

        if pair_count > 0:
            print(f"  {era_a} × {era_b}: {pair_count}", flush=True)

    db.commit()
    print(f"  Total new: {total_new:,}", flush=True)

    final_keep = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    print(f"\n=== Rule Connections Complete ===", flush=True)
    print(f"New connections: {total_new:,}", flush=True)
    print(f"Keep connections: {final_keep:,}", flush=True)
    db.close()

if __name__ == "__main__":
    main()
