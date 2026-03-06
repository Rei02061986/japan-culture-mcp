"""
Phase 9 Stream D: Serendipity engine testing.
10 diverse scenarios, 5 route tests.
Target: avg quality >= 3.5/5.0, route completion >= 90%.
"""
import sqlite3
import json
import random

DB_PATH = "ontology/culture_ontology.db"

# 10 test scenarios covering diverse cultural dimensions
TEST_SCENARIOS = [
    {
        'name': '北斎から現代アートへ',
        'query': '北斎',
        'expected_themes': ['ukiyoe_craft', 'visual_arts', 'everyday_beauty'],
        'expected_cross': True,  # Should cross media boundaries
        'description': '浮世絵の巨匠から現代文化への意外な繋がりを発見',
    },
    {
        'name': '妖怪とポップカルチャー',
        'query': '妖怪',
        'expected_themes': ['yokai', 'supernatural', 'otherworld'],
        'expected_cross': True,
        'description': '古来の妖怪文化からアニメ・ゲームへの展開',
    },
    {
        'name': '侍の精神',
        'query': '武士道',
        'expected_themes': ['samurai', 'martial_arts', 'power_rebellion'],
        'expected_cross': True,
        'description': '武士の文化が現代エンタメにどう継承されるか',
    },
    {
        'name': '茶の湯の美学',
        'query': '茶道',
        'expected_themes': ['tea_ceremony', 'craft_mastery', 'sacred_profane'],
        'expected_cross': True,
        'description': '茶の湯文化の広がりと現代への影響',
    },
    {
        'name': '京都の文化圏',
        'query': '京都',
        'expected_themes': ['community_tradition', 'shrine_temple', 'seasonal_beauty'],
        'expected_cross': True,
        'description': '千年の都から広がる文化ネットワーク',
    },
    {
        'name': '俳句と自然',
        'query': '俳句',
        'expected_themes': ['literary_arts', 'seasonal_beauty', 'nature_communion'],
        'expected_cross': True,
        'description': '短詩型文学が映し出す自然観と現代の共鳴',
    },
    {
        'name': 'ゲーム文化の源流',
        'query': 'ファイナルファンタジー',
        'expected_themes': ['game_culture', 'adventure_quest', 'battle'],
        'expected_cross': True,
        'description': 'RPGゲームから古典文化へのリンク',
    },
    {
        'name': '歌舞伎の世界',
        'query': '歌舞伎',
        'expected_themes': ['kabuki_theater', 'love_bond', 'everyday_beauty'],
        'expected_cross': True,
        'description': '江戸の大衆芸能から現代への繋がり',
    },
    {
        'name': '祭りと共同体',
        'query': '祭り',
        'expected_themes': ['matsuri', 'community_tradition', 'sacred_profane'],
        'expected_cross': True,
        'description': '日本の祭り文化と地域性の繋がり',
    },
    {
        'name': '建築と自然の調和',
        'query': '建築',
        'expected_themes': ['craft_mastery', 'nature_communion'],
        'expected_cross': True,
        'description': '日本建築の美意識と自然との調和',
    },
]

ROUTE_TESTS = [
    {'theme': 'yokai', 'region': 'kanto', 'name': '関東妖怪ルート'},
    {'theme': 'samurai', 'region': 'kinki', 'name': '近畿武士ルート'},
    {'theme': 'shrine_temple', 'region': 'kyoto', 'name': '京都社寺ルート'},
    {'theme': 'matsuri', 'region': 'kyushu', 'name': '九州祭りルート'},
    {'theme': 'craft_mastery', 'region': 'chubu', 'name': '中部工芸ルート'},
]


def test_find_serendipity(db, scenario):
    """Simulate find_serendipity tool logic and score the results."""
    query = scenario['query']
    results = {'score': 0, 'details': {}, 'connections': []}

    # Find matching entities - prefer exact match, then short labels (more relevant)
    matches = db.execute("""
        SELECT id, label_ja, entity_type FROM entities
        WHERE label_ja = ?
        LIMIT 3
    """, (query,)).fetchall()

    if len(matches) < 3:
        more = db.execute("""
            SELECT id, label_ja, entity_type FROM entities
            WHERE label_ja LIKE ? AND label_ja != ?
            ORDER BY LENGTH(label_ja) ASC
            LIMIT ?
        """, (f'%{query}%', query, 5 - len(matches))).fetchall()
        matches.extend(more)

    if not matches:
        results['details']['entity_found'] = False
        results['score'] = 1.0
        return results

    results['details']['entity_found'] = True
    results['details']['matches'] = len(matches)

    # Check connections for each match
    all_connections = []
    for eid, label, etype in matches:
        connections = db.execute("""
            SELECT c.entity_b_id, c.connection_type, c.serendipity_score, c.explanation,
                   e.label_ja, e.entity_type
            FROM connections c
            JOIN entities e ON e.id = c.entity_b_id
            WHERE c.entity_a_id = ? AND c.llm_verdict = 'keep'
            UNION
            SELECT c.entity_a_id, c.connection_type, c.serendipity_score, c.explanation,
                   e.label_ja, e.entity_type
            FROM connections c
            JOIN entities e ON e.id = c.entity_a_id
            WHERE c.entity_b_id = ? AND c.llm_verdict = 'keep'
        """, (eid, eid)).fetchall()
        all_connections.extend(connections)

    results['details']['total_connections'] = len(all_connections)

    if not all_connections:
        results['score'] = 1.5
        return results

    # Score criteria
    score = 2.0  # Base: has entity and connections

    # Diversity of connection types
    conn_types = set(c[1] for c in all_connections)
    results['details']['connection_types'] = list(conn_types)
    if len(conn_types) >= 3:
        score += 0.5
    elif len(conn_types) >= 2:
        score += 0.3

    # Cross-type connections (different entity_types in connected entities)
    connected_types = set(c[5] for c in all_connections)
    results['details']['connected_entity_types'] = list(connected_types)
    if len(connected_types) >= 2:
        score += 0.5

    # Theme coverage (check if connected entities have expected themes)
    connected_eids = [c[0] for c in all_connections[:20]]
    if connected_eids:
        placeholders = ','.join('?' * len(connected_eids))
        theme_tags = db.execute(f"""
            SELECT DISTINCT value_code FROM entity_tags
            WHERE entity_id IN ({placeholders}) AND axis = 'theme'
        """, connected_eids).fetchall()
        themes = set(t[0] for t in theme_tags)
        results['details']['themes_found'] = list(themes)[:10]

        expected_hit = sum(1 for t in scenario['expected_themes'] if t in themes)
        theme_ratio = expected_hit / len(scenario['expected_themes']) if scenario['expected_themes'] else 0
        score += theme_ratio * 0.5

    # Serendipity score quality
    avg_serendipity = sum(c[2] for c in all_connections) / len(all_connections) if all_connections else 0
    results['details']['avg_serendipity'] = round(avg_serendipity, 2)
    if avg_serendipity > 0.5:
        score += 0.3
    elif avg_serendipity > 0.3:
        score += 0.1

    # Explanation quality
    explanations_with_content = sum(1 for c in all_connections if c[3] and len(c[3]) > 10)
    explanation_ratio = explanations_with_content / len(all_connections)
    results['details']['explanation_quality'] = round(explanation_ratio, 2)
    if explanation_ratio > 0.8:
        score += 0.4
    elif explanation_ratio > 0.5:
        score += 0.2

    # Connection count bonus
    if len(all_connections) >= 10:
        score += 0.3
    elif len(all_connections) >= 5:
        score += 0.1

    # Connections list (top 5 by serendipity)
    sorted_conns = sorted(all_connections, key=lambda c: c[2], reverse=True)[:5]
    results['connections'] = [
        {'label': c[4], 'type': c[1], 'serendipity': c[2], 'explanation': c[3][:80] if c[3] else ''}
        for c in sorted_conns
    ]

    results['score'] = min(round(score, 1), 5.0)
    return results


def test_cultural_route(db, route_test):
    """Simulate get_cultural_route tool and test route generation."""
    result = {'name': route_test['name'], 'completed': False, 'stops': 0, 'details': {}}

    theme = route_test['theme']
    region = route_test['region']

    # Find entities with matching theme and geography that have coordinates
    entities = db.execute("""
        SELECT DISTINCT e.id, e.label_ja, e.lat, e.lon
        FROM entities e
        JOIN entity_tags t1 ON e.id = t1.entity_id
        JOIN entity_tags t2 ON e.id = t2.entity_id
        WHERE t1.axis = 'theme' AND t1.value_code = ?
        AND t2.axis = 'geography' AND t2.value_code = ?
        AND e.lat IS NOT NULL AND e.lon IS NOT NULL
        LIMIT 20
    """, (theme, region)).fetchall()

    result['details']['entities_with_coords'] = len(entities)

    if not entities:
        # Try without geography constraint
        entities = db.execute("""
            SELECT DISTINCT e.id, e.label_ja, e.lat, e.lon
            FROM entities e
            JOIN entity_tags t ON e.id = t.entity_id
            WHERE t.axis = 'theme' AND t.value_code = ?
            AND e.lat IS NOT NULL AND e.lon IS NOT NULL
            LIMIT 20
        """, (theme,)).fetchall()
        result['details']['fallback_entities'] = len(entities)

    if len(entities) >= 3:
        result['completed'] = True
        result['stops'] = len(entities)

        # Check connections between route stops
        stop_ids = [e[0] for e in entities]
        connected = 0
        for i in range(len(stop_ids) - 1):
            conn = db.execute("""
                SELECT COUNT(*) FROM connections
                WHERE (entity_a_id = ? AND entity_b_id = ?)
                OR (entity_a_id = ? AND entity_b_id = ?)
            """, (stop_ids[i], stop_ids[i+1], stop_ids[i+1], stop_ids[i])).fetchone()[0]
            if conn > 0:
                connected += 1

        result['details']['connected_stops'] = connected
        result['details']['stop_labels'] = [e[1] for e in entities[:5]]

    return result


def main():
    db = sqlite3.connect(DB_PATH)
    print("=" * 60, flush=True)
    print("Phase 9 Stream D: Serendipity Engine Testing", flush=True)
    print("=" * 60, flush=True)

    # Test 1: Serendipity scenarios
    print("\n--- Serendipity Discovery Tests ---", flush=True)
    total_score = 0
    scenario_results = []

    for i, scenario in enumerate(TEST_SCENARIOS, 1):
        result = test_find_serendipity(db, scenario)
        total_score += result['score']
        scenario_results.append({
            'name': scenario['name'],
            'query': scenario['query'],
            'score': result['score'],
            'details': result['details'],
            'top_connections': result['connections'],
        })
        status = "PASS" if result['score'] >= 3.0 else "WARN" if result['score'] >= 2.0 else "FAIL"
        print(f"  [{status}] {i}. {scenario['name']}: {result['score']}/5.0", flush=True)
        print(f"       Connections: {result['details'].get('total_connections', 0)}, "
              f"Types: {len(result['details'].get('connection_types', []))}", flush=True)
        if result['connections']:
            top = result['connections'][0]
            print(f"       Top: {top['label']} ({top['type']}, {top['serendipity']})", flush=True)

    avg_score = total_score / len(TEST_SCENARIOS)
    print(f"\n  Average quality: {avg_score:.1f}/5.0 "
          f"{'✓ PASS' if avg_score >= 3.5 else '✗ FAIL (target: 3.5)'}", flush=True)

    # Test 2: Route generation tests
    print("\n--- Cultural Route Tests ---", flush=True)
    completed_routes = 0
    route_results = []

    for route_test in ROUTE_TESTS:
        result = test_cultural_route(db, route_test)
        route_results.append(result)
        if result['completed']:
            completed_routes += 1
        status = "PASS" if result['completed'] else "FAIL"
        print(f"  [{status}] {result['name']}: "
              f"{result['stops']} stops, "
              f"{result['details'].get('entities_with_coords', 0)} entities with coords", flush=True)
        if result['details'].get('stop_labels'):
            print(f"       Stops: {', '.join(result['details']['stop_labels'][:3])}", flush=True)

    route_completion = completed_routes / len(ROUTE_TESTS) * 100
    print(f"\n  Route completion: {route_completion:.0f}% "
          f"{'✓ PASS' if route_completion >= 90 else '✗ FAIL (target: 90%)'}", flush=True)

    # Test 3: DB health checks
    print("\n--- DB Health Checks ---", flush=True)
    entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    keep_conns = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    total_conns = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    tags = db.execute("SELECT COUNT(*) FROM entity_tags").fetchone()[0]
    en_labels = db.execute("SELECT COUNT(*) FROM entities WHERE label_en IS NOT NULL").fetchone()[0]

    density = keep_conns / entities * 100 if entities else 0
    en_ratio = en_labels / entities * 100 if entities else 0

    print(f"  Entities: {entities:,}", flush=True)
    print(f"  Keep connections: {keep_conns:,}", flush=True)
    print(f"  Connection density: {density:.1f}% {'✓' if density >= 15 else '✗ (target: 15%)'}", flush=True)
    print(f"  Tags: {tags:,}", flush=True)
    print(f"  EN labels: {en_ratio:.1f}%", flush=True)

    # Tag coverage
    for axis in ['theme', 'era', 'medium', 'geography', 'experience']:
        count = db.execute("SELECT COUNT(DISTINCT entity_id) FROM entity_tags WHERE axis = ?",
                           (axis,)).fetchone()[0]
        coverage = count / entities * 100 if entities else 0
        print(f"  {axis} coverage: {coverage:.1f}% ({count:,})", flush=True)

    # Source distribution
    print("\n  Source distribution:", flush=True)
    sources = db.execute("""
        SELECT source, COUNT(*) FROM entities GROUP BY source ORDER BY COUNT(*) DESC LIMIT 10
    """).fetchall()
    for src, cnt in sources:
        print(f"    {src}: {cnt:,}", flush=True)

    # Summary
    print("\n" + "=" * 60, flush=True)
    print("SUMMARY", flush=True)
    print("=" * 60, flush=True)
    print(f"  Serendipity avg quality: {avg_score:.1f}/5.0 "
          f"{'PASS' if avg_score >= 3.5 else 'FAIL'}", flush=True)
    print(f"  Route completion: {route_completion:.0f}% "
          f"{'PASS' if route_completion >= 90 else 'FAIL'}", flush=True)
    print(f"  Entities: {entities:,} "
          f"{'PASS' if entities >= 350000 else 'NEED MORE' if entities >= 277000 else 'FAIL'}", flush=True)
    print(f"  Keep connections: {keep_conns:,} "
          f"{'PASS' if keep_conns >= 45000 else 'NEED MORE'}", flush=True)
    print(f"  Connection density: {density:.1f}% "
          f"{'PASS' if density >= 15 else 'NEED MORE'}", flush=True)

    # Write detailed report
    report = {
        'serendipity_scenarios': scenario_results,
        'avg_quality': round(avg_score, 2),
        'route_tests': route_results,
        'route_completion': route_completion,
        'db_stats': {
            'entities': entities,
            'keep_connections': keep_conns,
            'total_connections': total_conns,
            'tags': tags,
            'en_labels_pct': round(en_ratio, 1),
            'connection_density_pct': round(density, 1),
        }
    }

    with open('ontology/phase9_serendipity_report.json', 'w') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\nDetailed report: ontology/phase9_serendipity_report.json", flush=True)

    db.close()


if __name__ == "__main__":
    main()
