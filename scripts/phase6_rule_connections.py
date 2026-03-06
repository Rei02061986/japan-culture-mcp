"""
Phase 6E: Rule-based connection generation + GPT-4o-mini filter.
Generate connections between entities sharing theme tags,
then filter with LLM for quality.
"""

import sqlite3
import json
import time
import random
import os
import sys
import httpx

DB_PATH = "ontology/culture_ontology.db"

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[RuleConn] {msg}\n")

def log_error(msg):
    print(f"ERROR: {msg}", flush=True)
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[RuleConn] {msg}\n")


def generate_rule_connections(db):
    """Generate connections between entities sharing theme tags but differing in medium/era."""
    log("Generating rule-based connection candidates...")

    # Get entities grouped by theme
    theme_groups = {}
    rows = db.execute("""
        SELECT et.entity_id, et.value_code, e.label_ja, e.entity_type
        FROM entity_tags et
        JOIN entities e ON et.entity_id = e.id
        WHERE et.axis = 'theme' AND e.label_ja IS NOT NULL
    """).fetchall()

    for eid, theme, label, etype in rows:
        if theme not in theme_groups:
            theme_groups[theme] = []
        theme_groups[theme].append((eid, label, etype))

    log(f"  Theme groups: {len(theme_groups)}")
    for theme, entities in sorted(theme_groups.items(), key=lambda x: -len(x[1]))[:10]:
        log(f"    {theme}: {len(entities)} entities")

    # Get medium/era tags for cross-checking
    entity_tags = {}
    rows = db.execute("""
        SELECT entity_id, axis, value_code FROM entity_tags
        WHERE axis IN ('medium', 'era')
    """).fetchall()
    for eid, axis, code in rows:
        if eid not in entity_tags:
            entity_tags[eid] = {}
        entity_tags[eid][axis] = code

    # Get existing connections to avoid duplicates
    existing_pairs = set()
    rows = db.execute("SELECT entity_a_id, entity_b_id FROM connections").fetchall()
    for a, b in rows:
        existing_pairs.add((min(a, b), max(a, b)))

    log(f"  Existing connection pairs: {len(existing_pairs):,}")

    # Generate candidates
    candidates = []
    for theme, entities in theme_groups.items():
        if len(entities) < 2:
            continue

        # Sample pairs from this theme group
        # Prioritize cross-medium and cross-era pairs
        random.seed(42)
        shuffled = list(entities)
        random.shuffle(shuffled)

        pairs_for_theme = 0
        max_pairs_per_theme = min(100, len(entities) * 2)

        for i in range(len(shuffled)):
            if pairs_for_theme >= max_pairs_per_theme:
                break
            for j in range(i + 1, min(i + 20, len(shuffled))):
                eid_a, label_a, type_a = shuffled[i]
                eid_b, label_b, type_b = shuffled[j]

                pair_key = (min(eid_a, eid_b), max(eid_a, eid_b))
                if pair_key in existing_pairs:
                    continue

                # Check for cross-medium or cross-era
                tags_a = entity_tags.get(eid_a, {})
                tags_b = entity_tags.get(eid_b, {})

                medium_diff = tags_a.get('medium') != tags_b.get('medium')
                era_diff = tags_a.get('era') != tags_b.get('era')
                type_diff = type_a != type_b

                if not (medium_diff or era_diff or type_diff):
                    continue

                # Determine connection type
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
                    conn_type = 'geo_theme'
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
                pairs_for_theme += 1

    log(f"  Generated candidates: {len(candidates):,}")
    return candidates


FILTER_SYSTEM_PROMPT = """あなたは日本文化の接続品質を評価する審査員です。
以下の接続候補を評価し、文化的に意味のある接続のみを承認してください。

評価基準:
- cultural_relevance (0.0-1.0): 文化的な関連性・正確性
- serendipity_quality (0.0-1.0): 意外性・発見の質
- verdict: "keep" (両スコア0.5以上) or "reject"

必ず以下の形式のJSONオブジェクトで返してください:
{"evaluations": [
  {"id": 番号, "cultural_relevance": 0.0-1.0, "serendipity_quality": 0.0-1.0, "verdict": "keep/reject", "reason": "理由"}
]}"""


def filter_with_llm(candidates_batch):
    """Filter connection candidates with GPT-4o-mini via httpx."""
    api_key = os.environ.get('OPENAI_API_KEY', '')
    if not api_key:
        log_error("OPENAI_API_KEY not set")
        return [], 0

    items_text = "\n".join([
        f"{i+1}. {c['label_a']} ↔ {c['label_b']} (type: {c['connection_type']}, theme: {c['theme']})"
        for i, c in enumerate(candidates_batch)
    ])

    count = len(candidates_batch)
    prompt = f"""以下の{count}件の接続候補を評価してください。
必ず{count}件全ての評価を含めてください。

{items_text}"""

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": FILTER_SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
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
                log_error(f"Filter API HTTP {resp.status_code}: {resp.text[:200]}")
                time.sleep(5)
                continue

            data = resp.json()
            content = data['choices'][0]['message']['content']
            evaluations = json.loads(content).get('evaluations', [])

            usage = data.get('usage', {})
            input_t = usage.get('prompt_tokens', 0)
            output_t = usage.get('completion_tokens', 0)
            cost = input_t * 0.00000015 + output_t * 0.0000006

            return evaluations, cost

        except Exception as e:
            log_error(f"Filter API attempt {attempt+1}: {e}")
            time.sleep(5)

    return [], 0


def main():
    db = sqlite3.connect(DB_PATH)

    log("=== Phase 6E: Rule Connections + LLM Filter ===")

    # Generate candidates
    candidates = generate_rule_connections(db)

    if not candidates:
        log("No candidates generated, exiting")
        db.close()
        return

    # Limit to manageable number for LLM filtering
    max_candidates = 10000
    if len(candidates) > max_candidates:
        # Sort by score and take top
        candidates.sort(key=lambda x: -x['score'])
        candidates = candidates[:max_candidates]
        log(f"  Trimmed to top {max_candidates:,} candidates")

    # LLM filter in batches
    batch_size = 20
    total_cost = 0
    total_keep = 0
    total_reject = 0

    for i in range(0, len(candidates), batch_size):
        batch = candidates[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(candidates) + batch_size - 1) // batch_size

        if batch_num % 20 == 1:
            log(f"  Filter batch {batch_num}/{total_batches}, keep={total_keep}, cost=${total_cost:.3f}")

        evaluations, cost = filter_with_llm(batch)
        total_cost += cost

        # Map evaluations back to candidates
        for eval_item in evaluations:
            idx = eval_item.get('id', 0) - 1
            if 0 <= idx < len(batch):
                c = batch[idx]
                verdict = eval_item.get('verdict', 'reject')
                cr = eval_item.get('cultural_relevance', 0)
                sq = eval_item.get('serendipity_quality', 0)
                reason = eval_item.get('reason', '')

                if verdict == 'keep':
                    db.execute("""
                        INSERT INTO connections
                        (entity_a_id, entity_b_id, connection_type, serendipity_score,
                         source, llm_cultural_relevance, llm_serendipity_quality,
                         llm_explanation, llm_verdict)
                        VALUES (?, ?, ?, ?, 'rule_phase6', ?, ?, ?, 'keep')
                    """, (c['entity_a_id'], c['entity_b_id'], c['connection_type'],
                          c['score'], cr, sq, reason))
                    total_keep += 1
                else:
                    total_reject += 1

        db.commit()
        time.sleep(0.3)

        if total_cost > 10:
            log(f"  Cost limit reached: ${total_cost:.2f}")
            break

    log(f"\n=== Rule Connections Complete ===")
    log(f"Candidates evaluated: {len(candidates):,}")
    log(f"Keep: {total_keep:,}")
    log(f"Reject: {total_reject:,}")
    log(f"Keep rate: {total_keep/(total_keep+total_reject)*100:.1f}%" if (total_keep+total_reject) > 0 else "N/A")
    log(f"API cost: ${total_cost:.3f}")

    # Final stats
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_connections = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    keep_connections = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]

    log(f"Total entities: {total_entities:,}")
    log(f"Total connections: {total_connections:,}")
    log(f"Keep connections: {keep_connections:,}")

    db.close()

if __name__ == "__main__":
    main()
