"""
Phase 6D: Simplified LLM connection generation.
Uses subprocess-friendly approach.
"""
import sqlite3
import json
import os
import sys
import time

DB_PATH = "ontology/culture_ontology.db"

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[LLM-Simple] {msg}\n")

def log_error(msg):
    sys.stderr.write(f"ERROR: {msg}\n")
    sys.stderr.flush()
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[LLM-Simple] {msg}\n")

SYSTEM = """あなたは日本文化の学芸員です。文化的に興味深い繋がりを提案してください。
接続タイプ: influence, thematic_resonance, era_bridge, medium_cross, shared_motif, geographic_cultural, adaptation
必ずJSONで返してください: {"suggestions": [{"source_name": "元エンティティ名", "target_name": "接続先名", "target_type": "person/work/place", "connection_type": "タイプ", "explanation": "理由", "cultural_relevance": 0.7, "serendipity_quality": 0.7}]}
意外性のある接続を重視。自明な接続は避ける。"""

def call_api(entities_text, count):
    """Call GPT-4o-mini via raw httpx to avoid SDK issues."""
    import httpx

    api_key = os.environ.get('OPENAI_API_KEY', '')
    if not api_key:
        log_error("OPENAI_API_KEY not set")
        return [], 0

    prompt = f"""以下の{count}つのエンティティについて、それぞれ3-5件の接続を提案してください。

{entities_text}

各提案に source_name フィールドを必ず含めてください。"""

    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": prompt}
        ],
        "response_format": {"type": "json_object"},
        "temperature": 0.8,
        "max_tokens": 4000,
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
                log_error(f"API HTTP {resp.status_code}: {resp.text[:200]}")
                time.sleep(5)
                continue

            data = resp.json()
            content = data['choices'][0]['message']['content']
            suggestions = json.loads(content).get('suggestions', [])

            usage = data.get('usage', {})
            input_t = usage.get('prompt_tokens', 0)
            output_t = usage.get('completion_tokens', 0)
            cost = input_t * 0.00000015 + output_t * 0.0000006

            return suggestions, cost

        except Exception as e:
            log_error(f"API attempt {attempt+1}: {e}")
            time.sleep(5)

    return [], 0


def main():
    db = sqlite3.connect(DB_PATH)

    # Select priority entities
    log("Selecting priority entities...")
    entities = []

    for query, label in [
        ("SELECT id, label_ja, entity_type FROM entities WHERE wikidata_id IS NOT NULL AND lat IS NOT NULL LIMIT 500", "Wikidata places"),
        ("SELECT id, label_ja, entity_type FROM entities WHERE wikidata_id IS NOT NULL AND entity_type = 'person' LIMIT 300", "Wikidata people"),
        ("""SELECT e.id, e.label_ja, e.entity_type FROM entities e
            JOIN (SELECT entity_a_id as eid, COUNT(*) as cnt FROM connections WHERE llm_verdict='keep' GROUP BY entity_a_id
                  UNION ALL SELECT entity_b_id as eid, COUNT(*) as cnt FROM connections WHERE llm_verdict='keep' GROUP BY entity_b_id
            ) c ON e.id = c.eid GROUP BY e.id ORDER BY SUM(c.cnt) DESC LIMIT 200""", "High-connectivity"),
        ("SELECT id, label_ja, entity_type FROM entities WHERE ndl_id IS NOT NULL LIMIT 500", "NDL works"),
        ("SELECT id, label_ja, entity_type FROM entities WHERE source = 'wikidata_phase6' LIMIT 500", "New Wikidata"),
    ]:
        rows = db.execute(query).fetchall()
        entities.extend(rows)
        log(f"  {label}: {len(rows)}")

    # Deduplicate
    seen = set()
    unique = []
    for eid, label, etype in entities:
        if eid not in seen and label:
            seen.add(eid)
            unique.append((eid, label, etype))

    log(f"  Total unique: {len(unique)}")

    # Build entity name -> id index for matching
    all_entities = {}
    for eid, label, etype in db.execute("SELECT id, label_ja, entity_type FROM entities WHERE label_ja IS NOT NULL").fetchall():
        all_entities[label] = eid

    # Existing pairs
    existing_pairs = set()
    for a, b in db.execute("SELECT entity_a_id, entity_b_id FROM connections").fetchall():
        existing_pairs.add((min(a, b), max(a, b)))

    # Process in batches
    batch_size = 5
    total_cost = 0.0
    total_saved = 0
    total_suggestions = 0

    for i in range(0, len(unique), batch_size):
        batch = unique[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(unique) + batch_size - 1) // batch_size

        if batch_num % 10 == 1 or batch_num <= 3:
            log(f"Batch {batch_num}/{total_batches}, saved={total_saved}, cost=${total_cost:.3f}")

        entities_text = "\n".join([f"- {label} ({etype})" for _, label, etype in batch])
        suggestions, cost = call_api(entities_text, len(batch))
        total_cost += cost
        total_suggestions += len(suggestions)

        # Save suggestions
        source_map = {label: eid for eid, label, _ in batch}

        for sug in suggestions:
            target_name = sug.get('target_name', '')
            source_name = sug.get('source_name', '')
            if not target_name:
                continue

            # Find source entity
            source_eid = source_map.get(source_name)
            if not source_eid:
                for label, eid in source_map.items():
                    if source_name in label or label in source_name:
                        source_eid = eid
                        break
            if not source_eid:
                source_eid = batch[0][0]

            # Find or create target
            target_eid = all_entities.get(target_name)
            if not target_eid:
                # Partial match
                for label, eid in all_entities.items():
                    if target_name in label or label in target_name:
                        target_eid = eid
                        break
            if not target_eid:
                # Create
                target_type = sug.get('target_type', 'work')
                db.execute("INSERT INTO entities (label_ja, entity_type, source) VALUES (?, ?, 'llm_phase6')",
                          (target_name, target_type))
                target_eid = db.execute("SELECT last_insert_rowid()").fetchone()[0]
                all_entities[target_name] = target_eid

            # Check duplicate
            pair = (min(source_eid, target_eid), max(source_eid, target_eid))
            if pair in existing_pairs or source_eid == target_eid:
                continue

            conn_type = sug.get('connection_type', 'thematic_resonance')
            cr = sug.get('cultural_relevance', 0.7)
            sq = sug.get('serendipity_quality', 0.7)
            explanation = sug.get('explanation', '')

            db.execute("""
                INSERT INTO connections
                (entity_a_id, entity_b_id, connection_type, serendipity_score,
                 source, llm_cultural_relevance, llm_serendipity_quality,
                 llm_explanation, llm_verdict)
                VALUES (?, ?, ?, ?, 'llm_phase6', ?, ?, ?, 'keep')
            """, (source_eid, target_eid, conn_type, sq * 0.8 + cr * 0.2,
                  cr, sq, explanation))
            existing_pairs.add(pair)
            total_saved += 1

        db.commit()
        time.sleep(0.3)

        if total_cost > 10:
            log(f"Cost limit: ${total_cost:.2f}")
            break

    # Stats
    total_entities = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    total_connections = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    keep_connections = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]

    log(f"\n=== LLM Connection Generation Complete ===")
    log(f"Entities processed: {len(unique):,}")
    log(f"Suggestions: {total_suggestions:,}")
    log(f"Saved: {total_saved:,}")
    log(f"Cost: ${total_cost:.3f}")
    log(f"Total entities: {total_entities:,}")
    log(f"Total connections: {total_connections:,}")
    log(f"Keep connections: {keep_connections:,}")

    db.close()

if __name__ == "__main__":
    main()
