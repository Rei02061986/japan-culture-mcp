"""
Phase 15 Step 6: LLM batch quality connections.

Strategy: Use GPT-4o-mini to evaluate and create high-quality connections
for a sample of still-isolated entities that have rich metadata (wikidata_id
or multiple tags). Pairs are evaluated in batches of 10 for cost efficiency.

Budget: $5 (GPT-4o-mini ~$0.15/1M input + $0.60/1M output)
Source: p15_llm
"""
import sqlite3
import json
import time
import shutil
import os
import random
from datetime import datetime

try:
    import openai
    HAS_OPENAI = True
except ImportError:
    HAS_OPENAI = False

SRC_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
TMP_DB = "/tmp/culture_ontology_p15.db"
BATCH_SIZE = 10  # pairs per LLM call
MAX_BUDGET_USD = 5.0
# GPT-4o-mini pricing (as of 2024)
COST_PER_1K_INPUT = 0.00015
COST_PER_1K_OUTPUT = 0.0006
MAX_PAIRS = 5000  # safety limit


def open_db():
    db = sqlite3.connect(TMP_DB, timeout=30)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=30000")
    return db


def db_commit_retry(db, retries=5):
    for i in range(retries):
        try:
            db.commit()
            return True
        except sqlite3.OperationalError as e:
            print(f"  Commit retry {i+1}: {e}", flush=True)
            time.sleep(3)
    return False


def get_rich_isolated_entities(db, limit=2000):
    """Get isolated entities with the richest metadata."""
    # Priority: entities with wikidata_id and tags
    rows = db.execute("""
        SELECT e.id, e.label_ja, e.label_en, e.entity_type, e.wikidata_id,
               GROUP_CONCAT(DISTINCT et.axis || ':' || et.value_code)
        FROM entities e
        LEFT JOIN entity_tags et ON et.entity_id = e.id
        WHERE NOT EXISTS (
            SELECT 1 FROM connections c
            WHERE c.entity_a_id = e.id OR c.entity_b_id = e.id
        )
        AND (e.wikidata_id IS NOT NULL OR EXISTS (
            SELECT 1 FROM entity_tags et2 WHERE et2.entity_id = e.id
        ))
        GROUP BY e.id
        HAVING COUNT(DISTINCT et.axis) >= 2
        ORDER BY RANDOM()
        LIMIT ?
    """, (limit,)).fetchall()
    return rows


def build_prompt(pairs):
    """Build a prompt for evaluating entity pairs."""
    lines = ["For each pair of Japanese cultural entities below, rate the cultural connection strength (0-10). Return JSON array of objects with 'pair_idx', 'score' (0-10), 'type' (connection type), 'explanation' (brief)."]
    lines.append("")
    for i, (e1, e2) in enumerate(pairs):
        label1 = e1[1] or e1[2] or "?"
        label2 = e2[1] or e2[2] or "?"
        type1 = e1[3] or "unknown"
        type2 = e2[3] or "unknown"
        tags1 = e1[5] or "none"
        tags2 = e2[5] or "none"
        lines.append(f"Pair {i}: [{type1}] {label1} (tags: {tags1}) <-> [{type2}] {label2} (tags: {tags2})")
    return "\n".join(lines)


def call_llm(prompt, client):
    """Call GPT-4o-mini and return parsed response."""
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=1000,
            response_format={"type": "json_object"},
        )
        content = response.choices[0].message.content
        input_tokens = response.usage.prompt_tokens
        output_tokens = response.usage.completion_tokens
        cost = (input_tokens * COST_PER_1K_INPUT + output_tokens * COST_PER_1K_OUTPUT) / 1000
        data = json.loads(content)
        results = data if isinstance(data, list) else data.get("pairs", data.get("results", []))
        return results, cost, input_tokens, output_tokens
    except Exception as e:
        print(f"    LLM error: {e}", flush=True)
        return [], 0, 0, 0


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 15 Step 6: LLM Batch Quality Connections", flush=True)
    print("=" * 70, flush=True)

    if not HAS_OPENAI:
        print("\n  [SKIP] openai package not installed. pip install openai", flush=True)
        print("  Step 6 skipped.", flush=True)
        return

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("\n  [SKIP] OPENAI_API_KEY not set.", flush=True)
        print("  Step 6 skipped.", flush=True)
        return

    client = openai.OpenAI(api_key=api_key)

    # --- Copy DB to /tmp ---
    print(f"\nCopying DB to {TMP_DB} ...", flush=True)
    shutil.copy2(SRC_DB, TMP_DB)
    print("  Done.", flush=True)

    db = open_db()

    # --- Counts before ---
    conn_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    print(f"Connections before:  {conn_before:,}", flush=True)

    # --- Get rich isolated entities ---
    print("\nQuerying rich isolated entities...", flush=True)
    entities = get_rich_isolated_entities(db, limit=MAX_PAIRS * 2)
    print(f"Rich isolated entities found: {len(entities):,}", flush=True)

    if len(entities) < 2:
        print("  Not enough entities to pair. Skipping.", flush=True)
        db.close()
        return

    # --- Load existing pairs ---
    print("Loading existing connection pairs...", flush=True)
    existing_pairs = set()
    cursor = db.execute("SELECT entity_a_id, entity_b_id FROM connections")
    while True:
        rows = cursor.fetchmany(100000)
        if not rows:
            break
        for a, b in rows:
            existing_pairs.add((min(a, b), max(a, b)))

    # --- Create pairs to evaluate ---
    random.shuffle(entities)
    pairs = []
    for i in range(0, min(len(entities) - 1, MAX_PAIRS * 2), 2):
        e1, e2 = entities[i], entities[i + 1]
        pair = (min(e1[0], e2[0]), max(e1[0], e2[0]))
        if pair not in existing_pairs:
            pairs.append((e1, e2))
        if len(pairs) >= MAX_PAIRS:
            break

    print(f"Pairs to evaluate: {len(pairs):,}", flush=True)

    # --- Evaluate in batches ---
    total_cost = 0.0
    total_connections = 0
    total_evaluated = 0
    total_accepted = 0
    batch_num = 0

    for i in range(0, len(pairs), BATCH_SIZE):
        if total_cost >= MAX_BUDGET_USD:
            print(f"\n  Budget limit reached (${total_cost:.2f})", flush=True)
            break

        batch_pairs = pairs[i:i + BATCH_SIZE]
        prompt = build_prompt(batch_pairs)

        results, cost, in_tok, out_tok = call_llm(prompt, client)
        total_cost += cost
        total_evaluated += len(batch_pairs)
        batch_num += 1

        for r in results:
            if not isinstance(r, dict):
                continue
            idx = r.get("pair_idx", -1)
            score = r.get("score", 0)
            conn_type = r.get("type", "llm_cultural")
            explanation = r.get("explanation", "")

            if idx < 0 or idx >= len(batch_pairs) or score < 5:
                continue

            e1, e2 = batch_pairs[idx]
            pair = (min(e1[0], e2[0]), max(e1[0], e2[0]))
            if pair in existing_pairs:
                continue

            confidence = min(score / 10.0, 1.0)
            serendipity = max(1.0 - score / 10.0, 0.1)

            try:
                db.execute("""
                    INSERT OR IGNORE INTO connections
                        (entity_a_id, entity_b_id, connection_type,
                         theme_distance, serendipity_score,
                         explanation, source, confidence, created_at,
                         llm_cultural_relevance, llm_explanation, llm_verdict)
                    VALUES (?, ?, ?, 0.3, ?, ?, 'p15_llm', ?, ?, ?, ?, 'accept')
                """, (pair[0], pair[1], conn_type[:50],
                      serendipity, explanation[:200], confidence, now,
                      score / 10.0, explanation[:200]))
                existing_pairs.add(pair)
                total_connections += 1
                total_accepted += 1
            except sqlite3.IntegrityError:
                pass

        if batch_num % 10 == 0:
            db_commit_retry(db)
            print(f"  Batch {batch_num}: evaluated={total_evaluated}, "
                  f"accepted={total_accepted}, cost=${total_cost:.3f}", flush=True)

    # Final commit
    db_commit_retry(db)

    # --- Counts after ---
    conn_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    elapsed = time.time() - t0

    print(f"\n{'=' * 70}", flush=True)
    print("PHASE 15 STEP 6 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Pairs evaluated:     {total_evaluated:,}", flush=True)
    print(f"  Pairs accepted:      {total_accepted:,}", flush=True)
    print(f"  New connections:     +{total_connections:,}", flush=True)
    print(f"  Connections before:   {conn_before:,}", flush=True)
    print(f"  Connections after:    {conn_after:,}", flush=True)
    print(f"  Total cost:          ${total_cost:.3f}", flush=True)
    print(f"  Duration:             {elapsed:.1f}s", flush=True)

    db.close()

    # --- Copy DB back ---
    print(f"\nCopying DB back to {SRC_DB} ...", flush=True)
    shutil.copy2(TMP_DB, SRC_DB)
    print("  Done.", flush=True)
    print("Phase 15 Step 6 complete.", flush=True)


if __name__ == "__main__":
    main()
