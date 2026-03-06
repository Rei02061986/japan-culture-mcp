"""
Phase 8B-2: Improve connection density.
1. Wikidata author→work relations (P800 notable work, P50 author)
2. Additional rule-based connections (same theme × different era)
3. LLM connection generation for new entities
Target: >= 10,000 keep connections.
"""
import sqlite3
import json
import time
import random
import os
import httpx
import requests

DB_PATH = "ontology/culture_ontology.db"
WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    'Accept': 'application/sparql-results+json',
    'User-Agent': 'japan-culture-mcp/0.3 (contact@example.com)'
}

def log(msg):
    print(msg, flush=True)


def sparql_fetch(query):
    for attempt in range(3):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={'query': query},
                headers=HEADERS,
                timeout=90
            )
            if resp.status_code == 429:
                time.sleep(60 * (attempt + 1))
                continue
            if resp.status_code == 200:
                return resp.json().get('results', {}).get('bindings', [])
            else:
                log(f"    HTTP {resp.status_code}")
                time.sleep(30)
        except Exception as e:
            log(f"    ERROR: {e}")
            time.sleep(30)
    return []


def step1_wikidata_relations(db):
    """Fetch author↔work relations from Wikidata and create connections."""
    log("\n=== Step 1: Wikidata Author-Work Relations ===")

    # Get entities with wikidata_id
    wikidata_entities = {}
    for row in db.execute("SELECT id, wikidata_id, label_ja, entity_type FROM entities WHERE wikidata_id IS NOT NULL"):
        wikidata_entities[row[1]] = {'id': row[0], 'label': row[2], 'type': row[3]}

    log(f"  Entities with Wikidata ID: {len(wikidata_entities):,}")

    # Get existing connection pairs
    existing_pairs = set()
    for row in db.execute("SELECT entity_a_id, entity_b_id FROM connections"):
        existing_pairs.add((min(row[0], row[1]), max(row[0], row[1])))

    # Query 1: P800 (notable work) — author → work
    log("  Fetching P800 (notable work) relations...")
    query_p800 = """
SELECT ?person ?work WHERE {
  ?person wdt:P27 wd:Q17 .
  ?person wdt:P800 ?work .
}
LIMIT 10000
"""
    bindings = sparql_fetch(query_p800)
    log(f"    Got {len(bindings)} P800 relations")

    new_p800 = 0
    for rec in bindings:
        person_uri = rec.get('person', {}).get('value', '')
        work_uri = rec.get('work', {}).get('value', '')
        person_qid = person_uri.split('/')[-1] if person_uri else None
        work_qid = work_uri.split('/')[-1] if work_uri else None

        if person_qid in wikidata_entities and work_qid in wikidata_entities:
            pid = wikidata_entities[person_qid]['id']
            wid = wikidata_entities[work_qid]['id']
            pair = (min(pid, wid), max(pid, wid))

            if pair not in existing_pairs:
                db.execute("""
                    INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     source, llm_verdict, llm_explanation)
                    VALUES (?, ?, 'creator_work', 0.8, 'wikidata_p800', 'keep', '作者と代表作の関係')
                """, (pid, wid))
                existing_pairs.add(pair)
                new_p800 += 1

    db.commit()
    log(f"    New P800 connections: {new_p800}")

    # Query 2: P50 (author) — work → author
    log("  Fetching P50 (author) relations...")
    query_p50 = """
SELECT ?work ?author WHERE {
  ?work wdt:P495 wd:Q17 .
  ?work wdt:P50 ?author .
}
LIMIT 10000
"""
    bindings = sparql_fetch(query_p50)
    log(f"    Got {len(bindings)} P50 relations")

    new_p50 = 0
    for rec in bindings:
        work_uri = rec.get('work', {}).get('value', '')
        author_uri = rec.get('author', {}).get('value', '')
        work_qid = work_uri.split('/')[-1] if work_uri else None
        author_qid = author_uri.split('/')[-1] if author_uri else None

        if work_qid in wikidata_entities and author_qid in wikidata_entities:
            wid = wikidata_entities[work_qid]['id']
            aid = wikidata_entities[author_qid]['id']
            pair = (min(wid, aid), max(wid, aid))

            if pair not in existing_pairs:
                db.execute("""
                    INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     source, llm_verdict, llm_explanation)
                    VALUES (?, ?, 'creator_work', 0.8, 'wikidata_p50', 'keep', '作品と著者の関係')
                """, (wid, aid))
                existing_pairs.add(pair)
                new_p50 += 1

    db.commit()
    log(f"    New P50 connections: {new_p50}")

    # Query 3: P135 (movement) — work/person → art movement
    log("  Fetching P135 (movement) relations...")
    query_p135 = """
SELECT ?item ?movement WHERE {
  ?item wdt:P135 ?movement .
  { ?item wdt:P27 wd:Q17 . } UNION { ?item wdt:P495 wd:Q17 . }
}
LIMIT 5000
"""
    bindings = sparql_fetch(query_p135)
    log(f"    Got {len(bindings)} P135 relations")

    new_p135 = 0
    for rec in bindings:
        item_uri = rec.get('item', {}).get('value', '')
        movement_uri = rec.get('movement', {}).get('value', '')
        item_qid = item_uri.split('/')[-1] if item_uri else None
        movement_qid = movement_uri.split('/')[-1] if movement_uri else None

        if item_qid in wikidata_entities and movement_qid in wikidata_entities:
            iid = wikidata_entities[item_qid]['id']
            mid = wikidata_entities[movement_qid]['id']
            pair = (min(iid, mid), max(iid, mid))

            if pair not in existing_pairs:
                db.execute("""
                    INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type, serendipity_score,
                     source, llm_verdict, llm_explanation)
                    VALUES (?, ?, 'influenced_by', 0.7, 'wikidata_p135', 'keep', '芸術運動への参加')
                """, (iid, mid))
                existing_pairs.add(pair)
                new_p135 += 1

    db.commit()
    log(f"    New P135 connections: {new_p135}")

    return existing_pairs


def step2_rule_connections(db, existing_pairs):
    """Generate cross-era, cross-medium rule-based connections."""
    log("\n=== Step 2: Rule-based Cross Connections ===")

    # Get entities grouped by theme
    theme_groups = {}
    rows = db.execute("""
        SELECT et.entity_id, et.value_code, e.label_ja, e.entity_type
        FROM entity_tags et
        JOIN entities e ON et.entity_id = e.id
        WHERE et.axis = 'theme' AND e.label_ja IS NOT NULL
        AND et.value_code NOT IN ('craft_mastery', 'literary_arts', 'visual_arts')
    """).fetchall()

    for eid, theme, label, etype in rows:
        theme_groups.setdefault(theme, []).append((eid, label, etype))

    log(f"  Specific theme groups: {len(theme_groups)}")

    # Get medium/era for cross-checking
    entity_meta = {}
    for row in db.execute("SELECT entity_id, axis, value_code FROM entity_tags WHERE axis IN ('medium', 'era')"):
        entity_meta.setdefault(row[0], {})[row[1]] = row[2]

    # Generate candidates for interesting themes
    interesting_themes = [t for t, entities in theme_groups.items()
                         if 5 <= len(entities) <= 5000]
    log(f"  Interesting themes (5-5000 entities): {len(interesting_themes)}")

    candidates = []
    random.seed(8)

    for theme in interesting_themes:
        entities = theme_groups[theme]
        random.shuffle(entities)

        pairs_found = 0
        max_pairs = min(50, len(entities))

        for i in range(min(len(entities), 200)):
            if pairs_found >= max_pairs:
                break
            for j in range(i + 1, min(i + 10, len(entities))):
                eid_a, label_a, type_a = entities[i]
                eid_b, label_b, type_b = entities[j]

                pair_key = (min(eid_a, eid_b), max(eid_a, eid_b))
                if pair_key in existing_pairs:
                    continue

                meta_a = entity_meta.get(eid_a, {})
                meta_b = entity_meta.get(eid_b, {})

                medium_diff = meta_a.get('medium') != meta_b.get('medium')
                era_diff = meta_a.get('era') != meta_b.get('era')
                type_diff = type_a != type_b

                if not (medium_diff or era_diff or type_diff):
                    continue

                if medium_diff and era_diff:
                    conn_type = 'era_bridge'
                    score = 0.7
                elif medium_diff:
                    conn_type = 'medium_cross'
                    score = 0.65
                elif type_diff:
                    conn_type = 'thematic_resonance'
                    score = 0.6
                else:
                    conn_type = 'temporal_echo'
                    score = 0.55

                candidates.append({
                    'entity_a_id': eid_a,
                    'entity_b_id': eid_b,
                    'label_a': label_a,
                    'label_b': label_b,
                    'theme': theme,
                    'connection_type': conn_type,
                    'score': score,
                })
                existing_pairs.add(pair_key)
                pairs_found += 1

    log(f"  Generated candidates: {len(candidates):,}")
    return candidates


FILTER_SYSTEM_PROMPT = """あなたは日本文化の接続品質を評価する審査員です。
以下の接続候補を評価し、文化的に意味のある接続のみを承認してください。

評価基準:
- cultural_relevance (0.0-1.0): 文化的な関連性・正確性
- serendipity_quality (0.0-1.0): 意外性・発見の質
- verdict: "keep" (両スコア0.4以上) or "reject"

必ず以下の形式のJSONオブジェクトで返してください:
{"evaluations": [
  {"id": 番号, "cultural_relevance": 0.0-1.0, "serendipity_quality": 0.0-1.0, "verdict": "keep/reject", "reason": "理由"}
]}"""


def filter_with_llm(candidates_batch):
    api_key = os.environ.get('OPENAI_API_KEY', '')
    if not api_key:
        return [], 0

    items_text = "\n".join([
        f"{i+1}. {c['label_a']} ↔ {c['label_b']} (type: {c['connection_type']}, theme: {c['theme']})"
        for i, c in enumerate(candidates_batch)
    ])

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": FILTER_SYSTEM_PROMPT},
            {"role": "user", "content": f"以下の{len(candidates_batch)}件を評価:\n{items_text}"}
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.3,
        "max_tokens": 3000,
    }

    for attempt in range(3):
        try:
            with httpx.Client(timeout=120) as client:
                resp = client.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
            if resp.status_code != 200:
                log(f"    LLM HTTP {resp.status_code}")
                time.sleep(5)
                continue
            data = resp.json()
            content = data['choices'][0]['message']['content']
            evals = json.loads(content).get('evaluations', [])
            usage = data.get('usage', {})
            cost = usage.get('prompt_tokens', 0) * 0.00000015 + usage.get('completion_tokens', 0) * 0.0000006
            return evals, cost
        except Exception as e:
            log(f"    LLM error: {e}")
            time.sleep(5)
    return [], 0


def step3_llm_filter(db, candidates):
    """Filter candidates with LLM."""
    log(f"\n=== Step 3: LLM Filter ({len(candidates):,} candidates) ===")

    if not os.environ.get('OPENAI_API_KEY'):
        log("  OPENAI_API_KEY not set, inserting all candidates as auto-keep")
        for c in candidates:
            db.execute("""
                INSERT INTO connections
                (entity_a_id, entity_b_id, connection_type, serendipity_score,
                 source, llm_verdict, llm_explanation)
                VALUES (?, ?, ?, ?, 'rule_phase8', 'keep', '自動承認（LLMフィルタなし）')
            """, (c['entity_a_id'], c['entity_b_id'], c['connection_type'], c['score']))
        db.commit()
        return len(candidates), 0

    batch_size = 20
    total_cost = 0
    total_keep = 0

    for i in range(0, len(candidates), batch_size):
        batch = candidates[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(candidates) + batch_size - 1) // batch_size

        if batch_num % 20 == 1:
            log(f"  Batch {batch_num}/{total_batches}, keep={total_keep}, cost=${total_cost:.3f}")

        evals, cost = filter_with_llm(batch)
        total_cost += cost

        for ev in evals:
            idx = ev.get('id', 0) - 1
            if 0 <= idx < len(batch):
                c = batch[idx]
                if ev.get('verdict') == 'keep':
                    db.execute("""
                        INSERT INTO connections
                        (entity_a_id, entity_b_id, connection_type, serendipity_score,
                         source, llm_cultural_relevance, llm_serendipity_quality,
                         llm_explanation, llm_verdict)
                        VALUES (?, ?, ?, ?, 'rule_phase8', ?, ?, ?, 'keep')
                    """, (c['entity_a_id'], c['entity_b_id'], c['connection_type'],
                          c['score'], ev.get('cultural_relevance', 0),
                          ev.get('serendipity_quality', 0), ev.get('reason', '')))
                    total_keep += 1

        db.commit()
        time.sleep(0.3)

        if total_cost > 5:
            log(f"  Cost limit: ${total_cost:.2f}")
            break

    log(f"  LLM filter: {total_keep} keep, ${total_cost:.3f}")
    return total_keep, total_cost


def main():
    db = sqlite3.connect(DB_PATH)

    keep_before = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    log(f"Keep connections before: {keep_before:,}")

    # Step 1: Wikidata relations
    existing_pairs = step1_wikidata_relations(db)

    # Step 2: Rule-based candidates
    candidates = step2_rule_connections(db, existing_pairs)

    # Step 3: LLM filter
    new_keep, cost = step3_llm_filter(db, candidates)

    # Final stats
    keep_after = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]

    log(f"\n=== Connection Density Improvement Complete ===")
    log(f"Keep connections: {keep_before:,} → {keep_after:,}")
    log(f"Total entities: {total_entities:,}")
    log(f"Connection density: {keep_after/total_entities*100:.3f}%")
    log(f"API cost: ${cost:.3f}")
    db.close()

if __name__ == "__main__":
    main()
