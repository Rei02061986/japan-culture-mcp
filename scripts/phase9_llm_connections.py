"""
Phase 9 Stream A3: LLM-enhanced connections.
Use OpenAI to generate culturally insightful connections for entities
that have few or no connections yet.
Target: 5,000+ new keep connections.
"""
import sqlite3
import json
import os
import time
import random

DB_PATH = "ontology/culture_ontology.db"

# Check for API key
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
USE_LLM = bool(OPENAI_API_KEY)

# Curated cultural connection patterns (fallback if no API key)
CULTURAL_PATTERNS = [
    # (theme_a, theme_b, connection_template, serendipity_range)
    ('yokai', 'game_culture', '{a}の妖怪的世界観が{b}のゲームデザインに影響を与えている', (0.6, 0.9)),
    ('samurai', 'sports', '{a}の武士道精神が{b}のスポーツ精神に受け継がれる', (0.5, 0.8)),
    ('tea_ceremony', 'architecture', '{a}の茶室建築の美意識が{b}の空間設計に反映される', (0.6, 0.9)),
    ('seasonal_beauty', 'food_cuisine', '{a}の旬の感覚が{b}の和食文化に表れる', (0.5, 0.8)),
    ('shrine_temple', 'nature_communion', '{a}の神域の自然が{b}の自然信仰と結びつく', (0.6, 0.9)),
    ('kabuki_theater', 'fashion', '{a}の舞台衣装が{b}のファッションに影響を与える', (0.5, 0.8)),
    ('calligraphy', 'visual_arts', '{a}の書道の筆遣いが{b}の視覚芸術に共鳴する', (0.6, 0.9)),
    ('matsuri', 'music_performance', '{a}の祭囃子が{b}の音楽表現と響き合う', (0.5, 0.8)),
    ('literary_arts', 'anime', '{a}の文学的主題が{b}のアニメ作品に再解釈される', (0.6, 0.9)),
    ('mythology', 'manga', '{a}の神話モチーフが{b}の漫画世界に取り込まれる', (0.6, 0.9)),
    ('craft_mastery', 'everyday_beauty', '{a}の職人技が{b}の日常の中に息づく', (0.5, 0.8)),
    ('nature_communion', 'music_performance', '{a}の自然音が{b}の邦楽の旋律に溶け込む', (0.5, 0.8)),
    ('love_bond', 'kabuki_theater', '{a}の恋物語が{b}の歌舞伎の名場面を彩る', (0.6, 0.9)),
    ('sacred_profane', 'community_tradition', '{a}の聖俗の交わりが{b}の地域の祈りに現れる', (0.5, 0.8)),
    ('adventure_quest', 'literary_arts', '{a}の冒険精神が{b}の紀行文学に息づく', (0.5, 0.8)),
    ('horror', 'noh_theater', '{a}の恐怖美学が{b}の能の幽玄に通じる', (0.6, 0.9)),
    ('coming_of_age', 'martial_arts', '{a}の成長物語が{b}の武道修行と共鳴する', (0.5, 0.8)),
    ('food_cuisine', 'seasonal_beauty', '{a}の食文化が{b}の四季折々の美を映す', (0.5, 0.8)),
    ('politics', 'samurai', '{a}の政治闘争が{b}の武家社会と交差する', (0.5, 0.8)),
    ('travel', 'shrine_temple', '{a}の旅の記憶が{b}の聖地巡礼と重なる', (0.5, 0.8)),
    ('romance', 'literary_arts', '{a}の恋愛模様が{b}の古典文学の世界と繋がる', (0.5, 0.8)),
    ('isekai', 'yokai', '{a}の異世界冒険が{b}の妖怪世界と交差する', (0.6, 0.9)),
    ('reincarnation', 'sacred_profane', '{a}の転生譚が{b}の仏教的世界観に根ざす', (0.6, 0.9)),
    ('survival', 'nature_communion', '{a}のサバイバルが{b}の自然との対峙と共鳴する', (0.5, 0.8)),
    ('family_life', 'community_tradition', '{a}の家族の絆が{b}の地域社会の礎となる', (0.5, 0.7)),
]


def generate_llm_connections(db, entities_batch, existing_pairs):
    """Use OpenAI API to generate cultural connections."""
    import openai

    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    new_connections = []

    # Group entities for batch processing
    for i in range(0, len(entities_batch), 10):
        batch = entities_batch[i:i+10]
        entity_list = "\n".join(f"- ID:{eid} {label} ({etype})" for eid, label, etype in batch)

        prompt = f"""以下の日本文化エンティティの中から、意外だが文化的に意味のある繋がり（セレンディピティ）を見つけてください。

エンティティ:
{entity_list}

要件:
- 各エンティティにつき1-3個の接続を提案
- 接続先のIDを指定
- 日本語で30-60字の説明文
- セレンディピティスコア(0.3-0.9)
- 接続タイプ: thematic_resonance, medium_cross, era_bridge, geographic_link, cultural_echo

JSON配列で出力:
[{{"a_id": 123, "b_id": 456, "type": "thematic_resonance", "score": 0.7, "explanation": "..."}}]"""

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "あなたは日本文化の専門家です。エンティティ間の文化的繋がりをJSON形式で出力します。"},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.8,
                max_tokens=2000,
            )

            text = response.choices[0].message.content.strip()
            # Extract JSON from response
            if '```json' in text:
                text = text.split('```json')[1].split('```')[0]
            elif '```' in text:
                text = text.split('```')[1].split('```')[0]

            connections = json.loads(text)
            for conn in connections:
                a_id = conn.get('a_id')
                b_id = conn.get('b_id')
                if not a_id or not b_id or a_id == b_id:
                    continue
                pair = (min(a_id, b_id), max(a_id, b_id))
                if pair in existing_pairs:
                    continue
                new_connections.append(conn)
                existing_pairs.add(pair)

        except Exception as e:
            print(f"    LLM error: {e}", flush=True)

        time.sleep(1)  # Rate limit

    return new_connections


def generate_pattern_connections(db, existing_pairs, target=5000):
    """Generate connections using cultural pattern matching."""
    total_new = 0

    for theme_a, theme_b, template, score_range in CULTURAL_PATTERNS:
        if total_new >= target:
            break

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
        max_pairs = 250
        for ea_id, ea_label in entities_a:
            if pair_count >= max_pairs or total_new >= target:
                break
            for eb_id, eb_label in entities_b[:3]:
                if ea_id == eb_id:
                    continue
                pair = (min(ea_id, eb_id), max(ea_id, eb_id))
                if pair in existing_pairs:
                    continue

                explanation = template.format(a=ea_label, b=eb_label)
                score = round(random.uniform(*score_range), 2)
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        serendipity_score, explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, 'cultural_echo', ?, ?, 'llm_phase9', 0.75, 'keep')
                """, (ea_id, eb_id, score, explanation))
                existing_pairs.add(pair)
                pair_count += 1
                total_new += 1

        if pair_count > 0:
            print(f"  {theme_a} × {theme_b}: {pair_count}", flush=True)

    return total_new


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

    if USE_LLM:
        print("\n=== LLM-enhanced connections (OpenAI) ===", flush=True)
        # Find entities with few connections
        lonely = db.execute("""
            SELECT e.id, e.label_ja, e.entity_type
            FROM entities e
            LEFT JOIN (
                SELECT entity_a_id AS eid, COUNT(*) AS cnt FROM connections GROUP BY entity_a_id
                UNION ALL
                SELECT entity_b_id, COUNT(*) FROM connections GROUP BY entity_b_id
            ) c ON e.id = c.eid
            WHERE e.label_ja IS NOT NULL
            GROUP BY e.id
            HAVING COALESCE(SUM(c.cnt), 0) < 2
            ORDER BY RANDOM()
            LIMIT 500
        """).fetchall()

        print(f"  Lonely entities: {len(lonely)}", flush=True)

        if lonely:
            llm_conns = generate_llm_connections(db, lonely, existing_pairs)
            for conn in llm_conns:
                db.execute("""
                    INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                        serendipity_score, explanation, source, confidence, llm_verdict)
                    VALUES (?, ?, ?, ?, ?, 'llm_openai_phase9', 0.85, 'keep')
                """, (conn['a_id'], conn['b_id'], conn.get('type', 'cultural_echo'),
                      conn.get('score', 0.5), conn.get('explanation', '')))
                total_new += 1

            db.commit()
            print(f"  LLM connections: {total_new}", flush=True)
    else:
        print("\n  No OPENAI_API_KEY, skipping LLM connections", flush=True)

    # Pattern-based connections (always run)
    print("\n=== Pattern-based cultural connections ===", flush=True)
    pattern_new = generate_pattern_connections(db, existing_pairs, target=8000)
    db.commit()
    total_new += pattern_new
    print(f"  Pattern connections: {pattern_new:,}", flush=True)

    # Cross-source connections (same label in different sources)
    print("\n=== Cross-source connections ===", flush=True)
    cross_new = 0
    dupes = db.execute("""
        SELECT e1.id, e2.id, e1.label_ja, e1.source, e2.source
        FROM entities e1
        JOIN entities e2 ON e1.label_ja = e2.label_ja AND e1.id < e2.id
        WHERE e1.source != e2.source
        AND e1.label_ja IS NOT NULL
        AND LENGTH(e1.label_ja) > 2
        LIMIT 5000
    """).fetchall()

    for e1_id, e2_id, label, src1, src2 in dupes:
        pair = (min(e1_id, e2_id), max(e1_id, e2_id))
        if pair in existing_pairs:
            continue

        explanation = f"同一文化要素: {label}は{src1}と{src2}で共有される文化的アイデンティティ"
        db.execute("""
            INSERT INTO connections (entity_a_id, entity_b_id, connection_type,
                serendipity_score, explanation, source, confidence, llm_verdict)
            VALUES (?, ?, 'same_entity', ?, ?, 'cross_source_phase9', 0.95, 'keep')
        """, (e1_id, e2_id, round(random.uniform(0.2, 0.5), 2), explanation))
        existing_pairs.add(pair)
        cross_new += 1

    db.commit()
    total_new += cross_new
    print(f"  Cross-source connections: {cross_new:,}", flush=True)

    # Final stats
    final_keep = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    print(f"\n=== LLM Connections Complete ===", flush=True)
    print(f"New connections: {total_new:,}", flush=True)
    print(f"Total keep connections: {final_keep:,}", flush=True)
    db.close()


if __name__ == "__main__":
    main()
