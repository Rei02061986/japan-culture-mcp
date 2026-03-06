"""
Phase 16 B1: Quality audit of Phase 15 prefix connections.

Strategy: Audit the 2,987,055 connections from source='p15_label_prefix'
(connection_type='label_similarity'). Sample 500 random connections,
optionally evaluate 100 via GPT-4o-mini, then filter low-quality
connections based on prefix patterns.

Filters applied:
  - Purely numeric prefixes (e.g., "123456", full-width digits too)
  - Prefixes with only 1-2 unique characters (e.g., "ああああああ")
  - Overly generic prefixes with >400 connections (unless LLM scored >= 3)
  - Safety limit: max 500,000 deletions total

Source: p16_quality_audit
"""
import sqlite3
import time
import shutil
import os
import re
import json
import random
from datetime import datetime
from collections import Counter

SRC_DB = os.path.join(os.path.dirname(__file__), "..", "ontology", "culture_ontology.db")
TMP_DB = "/tmp/culture_ontology_p16.db"
BATCH_SIZE = 5000
MAX_DELETE = 500_000


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


def extract_prefix(explanation):
    """Extract the 6-char prefix from explanation field format 'prefix:XXXXXX'."""
    if explanation and explanation.startswith("prefix:"):
        return explanation[7:]
    return None


def is_purely_numeric(prefix):
    """Check if prefix is purely numeric (half-width or full-width digits)."""
    return bool(re.fullmatch(r"[0-9\uff10-\uff19]+", prefix))


def has_low_unique_chars(prefix, max_unique=2):
    """Check if prefix has only 1-2 unique characters."""
    return len(set(prefix)) <= max_unique


def run_llm_evaluation(pairs, api_key):
    """Evaluate 100 pairs via GPT-4o-mini in batches of 10.

    Args:
        pairs: list of (pair_idx, label_a_ja, label_a_en, label_b_ja, label_b_en)
        api_key: OpenAI API key

    Returns:
        dict mapping pair_idx -> {score, reason}
    """
    try:
        from openai import OpenAI
    except ImportError:
        print("  openai package not installed. Skipping LLM evaluation.", flush=True)
        return {}

    client = OpenAI(api_key=api_key)
    results = {}
    batch_size = 10

    for batch_start in range(0, len(pairs), batch_size):
        batch = pairs[batch_start:batch_start + batch_size]
        batch_num = batch_start // batch_size + 1
        total_batches = (len(pairs) + batch_size - 1) // batch_size

        # Build prompt
        pair_lines = []
        for idx, la_ja, la_en, lb_ja, lb_en in batch:
            a_label = la_ja or la_en or "(no label)"
            b_label = lb_ja or lb_en or "(no label)"
            a_extra = f" ({la_en})" if la_en and la_ja else ""
            b_extra = f" ({lb_en})" if lb_en and lb_ja else ""
            pair_lines.append(f"  {idx}: \"{a_label}{a_extra}\" <-> \"{b_label}{b_extra}\"")

        pairs_text = "\n".join(pair_lines)

        prompt = (
            "Rate the cultural relevance of each pair of Japanese cultural items below.\n"
            "Score 1-5:\n"
            "  1 = unrelated coincidence (just share a common prefix)\n"
            "  2 = very weak connection\n"
            "  3 = moderate thematic relation\n"
            "  4 = strong relation (same creator, same franchise)\n"
            "  5 = same series or clear sequel/prequel\n\n"
            f"Pairs:\n{pairs_text}\n\n"
            "Return ONLY a JSON array of objects: [{\"pair_idx\": N, \"score\": N, \"reason\": \"...\"}]\n"
            "No markdown formatting, just the raw JSON array."
        )

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=2000,
            )
            content = response.choices[0].message.content.strip()

            # Try to parse JSON - handle possible markdown wrapping
            if content.startswith("```"):
                content = content.split("\n", 1)[-1].rsplit("```", 1)[0].strip()

            scored = json.loads(content)
            for item in scored:
                pair_idx = item.get("pair_idx")
                score = item.get("score")
                reason = item.get("reason", "")
                if pair_idx is not None and score is not None:
                    results[pair_idx] = {"score": int(score), "reason": reason}

            print(f"  Batch {batch_num}/{total_batches}: {len(scored)} scores received", flush=True)

        except Exception as e:
            print(f"  Batch {batch_num}/{total_batches}: LLM error: {e}", flush=True)

        # Rate limiting
        if batch_start + batch_size < len(pairs):
            time.sleep(1)

    return results


def main():
    t0 = time.time()
    now = datetime.now().isoformat()

    print("=" * 70, flush=True)
    print("Phase 16 B1: Quality Audit of Phase 15 Prefix Connections", flush=True)
    print(f"Started: {now}", flush=True)
    print("=" * 70, flush=True)

    # --- Copy DB to /tmp ---
    print(f"\nCopying DB to {TMP_DB} ...", flush=True)
    shutil.copy2(SRC_DB, TMP_DB)
    print("  Done.", flush=True)

    db = open_db()

    # --- Counts before ---
    conn_total_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    prefix_conn_count = db.execute(
        "SELECT COUNT(*) FROM connections WHERE source = 'p15_label_prefix'"
    ).fetchone()[0]
    print(f"\nTotal connections:         {conn_total_before:,}", flush=True)
    print(f"Prefix connections (p15):  {prefix_conn_count:,}", flush=True)

    # =========================================================================
    # STEP 1: SAMPLE ANALYSIS (no LLM)
    # =========================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("STEP 1: Sample Analysis (500 random prefix connections)", flush=True)
    print(f"{'=' * 70}", flush=True)

    # Get 500 random prefix connections with entity labels
    sample_rows = db.execute("""
        SELECT c.id, c.explanation,
               ea.label_ja AS a_ja, ea.label_en AS a_en, ea.entity_type AS a_type,
               eb.label_ja AS b_ja, eb.label_en AS b_en, eb.entity_type AS b_type
        FROM connections c
        JOIN entities ea ON c.entity_a_id = ea.id
        JOIN entities eb ON c.entity_b_id = eb.id
        WHERE c.source = 'p15_label_prefix'
        ORDER BY RANDOM()
        LIMIT 500
    """).fetchall()
    print(f"Sampled: {len(sample_rows)} connections", flush=True)

    # Parse prefixes and categorize
    prefix_counter = Counter()
    for row in sample_rows:
        prefix = extract_prefix(row[1])
        if prefix:
            prefix_counter[prefix] += 1

    # Get full prefix distribution from ALL prefix connections
    print("\nLoading full prefix distribution from all prefix connections...", flush=True)
    all_explanations = db.execute(
        "SELECT explanation FROM connections WHERE source = 'p15_label_prefix'"
    ).fetchall()
    full_prefix_counter = Counter()
    for (expl,) in all_explanations:
        prefix = extract_prefix(expl)
        if prefix:
            full_prefix_counter[prefix] += 1

    print(f"Unique prefixes in DB: {len(full_prefix_counter):,}", flush=True)

    # Top 50 most common prefixes
    print(f"\nTop 50 most common prefixes:", flush=True)
    print(f"  {'Rank':>4}  {'Prefix':12}  {'Count':>8}  {'Type':12}", flush=True)
    print(f"  {'----':>4}  {'------':12}  {'-----':>8}  {'----':12}", flush=True)
    for rank, (prefix, count) in enumerate(full_prefix_counter.most_common(50), 1):
        # Categorize
        if is_purely_numeric(prefix):
            ptype = "NUMERIC"
        elif has_low_unique_chars(prefix):
            ptype = "LOW-UNIQ"
        elif count > 400:
            ptype = "GENERIC"
        else:
            ptype = "ok"
        print(f"  {rank:>4}  {prefix:12}  {count:>8,}  {ptype:12}", flush=True)

    # Count generic prefixes (>200 connections)
    generic_prefixes = {p for p, c in full_prefix_counter.items() if c > 200}
    numeric_prefixes = {p for p in full_prefix_counter if is_purely_numeric(p)}
    low_unique_prefixes = {p for p in full_prefix_counter if has_low_unique_chars(p)}

    print(f"\nPrefix categories (across all {len(full_prefix_counter):,} unique prefixes):", flush=True)
    print(f"  Generic (>200 connections):   {len(generic_prefixes):,} prefixes", flush=True)
    print(f"  Purely numeric:               {len(numeric_prefixes):,} prefixes", flush=True)
    print(f"  Low unique chars (<=2):       {len(low_unique_prefixes):,} prefixes", flush=True)

    # Sample 20 connections with both labels
    print(f"\nSample of 20 connections:", flush=True)
    print(f"  {'Prefix':12}  {'Entity A':35}  {'Entity B':35}", flush=True)
    print(f"  {'------':12}  {'--------':35}  {'--------':35}", flush=True)
    for row in sample_rows[:20]:
        prefix = extract_prefix(row[1]) or "???"
        a_label = (row[2] or row[3] or "(no label)")[:33]
        b_label = (row[5] or row[6] or "(no label)")[:33]
        print(f"  {prefix:12}  {a_label:35}  {b_label:35}", flush=True)

    # =========================================================================
    # STEP 2: LLM EVALUATION (optional, 100 pairs)
    # =========================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("STEP 2: LLM Evaluation (100 random pairs via GPT-4o-mini)", flush=True)
    print(f"{'=' * 70}", flush=True)

    api_key = os.environ.get("OPENAI_API_KEY", "")
    llm_scores = {}
    llm_avg_score = 0.0
    llm_high_scoring_prefixes = set()  # prefixes that scored >= 3 on average

    if not api_key:
        print("  OPENAI_API_KEY not set. Skipping LLM evaluation.", flush=True)
        print("  (Filter will proceed without LLM-based exemptions.)", flush=True)
    else:
        # Select 100 random prefix connections with labels
        llm_sample = db.execute("""
            SELECT c.id, c.explanation,
                   ea.label_ja, ea.label_en,
                   eb.label_ja, eb.label_en
            FROM connections c
            JOIN entities ea ON c.entity_a_id = ea.id
            JOIN entities eb ON c.entity_b_id = eb.id
            WHERE c.source = 'p15_label_prefix'
            ORDER BY RANDOM()
            LIMIT 100
        """).fetchall()

        # Prepare pairs for LLM
        llm_pairs = []
        pair_prefix_map = {}  # pair_idx -> prefix
        for idx, row in enumerate(llm_sample):
            prefix = extract_prefix(row[1])
            llm_pairs.append((idx, row[2], row[3], row[4], row[5]))
            pair_prefix_map[idx] = prefix

        print(f"  Sending {len(llm_pairs)} pairs to GPT-4o-mini ...", flush=True)
        llm_scores = run_llm_evaluation(llm_pairs, api_key)
        print(f"  Received {len(llm_scores)} scores.", flush=True)

        if llm_scores:
            # Compute average
            scores_list = [v["score"] for v in llm_scores.values()]
            llm_avg_score = sum(scores_list) / len(scores_list)

            # Score distribution
            score_dist = Counter(scores_list)
            print(f"\n  LLM Score Distribution:", flush=True)
            for s in range(1, 6):
                count = score_dist.get(s, 0)
                bar = "#" * count
                print(f"    Score {s}: {count:>3} {bar}", flush=True)
            print(f"    Average: {llm_avg_score:.2f}", flush=True)

            # Identify high-scoring prefixes (score >= 3)
            prefix_scores = {}  # prefix -> list of scores
            for pair_idx, result in llm_scores.items():
                prefix = pair_prefix_map.get(pair_idx)
                if prefix:
                    if prefix not in prefix_scores:
                        prefix_scores[prefix] = []
                    prefix_scores[prefix].append(result["score"])

            for prefix, scores in prefix_scores.items():
                avg = sum(scores) / len(scores)
                if avg >= 3.0:
                    llm_high_scoring_prefixes.add(prefix)

            print(f"\n  Prefixes scoring >= 3.0 avg (LLM exempted): "
                  f"{len(llm_high_scoring_prefixes)}", flush=True)
            if llm_high_scoring_prefixes:
                for p in sorted(llm_high_scoring_prefixes):
                    avg = sum(prefix_scores[p]) / len(prefix_scores[p])
                    print(f"    {p}  avg={avg:.1f}", flush=True)

            # Show some low-scoring examples
            low_scored = [(idx, v) for idx, v in llm_scores.items() if v["score"] <= 2]
            if low_scored:
                print(f"\n  Sample low-scoring pairs (score <= 2):", flush=True)
                for idx, v in low_scored[:10]:
                    row = llm_sample[idx]
                    a_label = (row[2] or row[3] or "?")[:30]
                    b_label = (row[4] or row[5] or "?")[:30]
                    print(f"    [{v['score']}] {a_label} <-> {b_label}: {v['reason'][:60]}", flush=True)

    # =========================================================================
    # STEP 3: FILTER LOW-QUALITY CONNECTIONS
    # =========================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("STEP 3: Filter Low-Quality Connections", flush=True)
    print(f"{'=' * 70}", flush=True)

    # Determine which prefixes to delete
    prefixes_to_delete_numeric = set()
    prefixes_to_delete_low_unique = set()
    prefixes_to_delete_generic = set()

    for prefix, count in full_prefix_counter.items():
        # Rule 1: Purely numeric
        if is_purely_numeric(prefix):
            prefixes_to_delete_numeric.add(prefix)

        # Rule 2: Low unique characters (<=2)
        if has_low_unique_chars(prefix):
            prefixes_to_delete_low_unique.add(prefix)

        # Rule 3: Overly generic (>400 connections) BUT keep LLM-exempted
        if count > 400 and prefix not in llm_high_scoring_prefixes:
            prefixes_to_delete_generic.add(prefix)

    # Combine all prefixes to delete (union)
    all_bad_prefixes = prefixes_to_delete_numeric | prefixes_to_delete_low_unique | prefixes_to_delete_generic

    # Count how many connections each rule would affect
    count_numeric = sum(full_prefix_counter[p] for p in prefixes_to_delete_numeric)
    count_low_unique = sum(full_prefix_counter[p] for p in prefixes_to_delete_low_unique)
    count_generic = sum(full_prefix_counter[p] for p in prefixes_to_delete_generic)
    count_combined = sum(full_prefix_counter[p] for p in all_bad_prefixes)

    print(f"\nFilter targets:", flush=True)
    print(f"  Numeric prefixes:         {len(prefixes_to_delete_numeric):,} prefixes -> "
          f"{count_numeric:,} connections", flush=True)
    print(f"  Low-unique prefixes:      {len(prefixes_to_delete_low_unique):,} prefixes -> "
          f"{count_low_unique:,} connections", flush=True)
    print(f"  Generic (>400) prefixes:  {len(prefixes_to_delete_generic):,} prefixes -> "
          f"{count_generic:,} connections", flush=True)
    print(f"  Combined (union):         {len(all_bad_prefixes):,} prefixes -> "
          f"{count_combined:,} connections", flush=True)

    # Safety limit check
    if count_combined > MAX_DELETE:
        print(f"\n  WARNING: Combined count {count_combined:,} exceeds safety limit {MAX_DELETE:,}.", flush=True)
        print(f"  Capping deletions to {MAX_DELETE:,} connections.", flush=True)

    if not all_bad_prefixes:
        print("\n  No prefixes matched filter rules. Nothing to delete.", flush=True)
    else:
        # Build the explanation patterns to delete
        # We delete by matching "prefix:XXXXXX" in explanation
        print(f"\n  Deleting connections...", flush=True)

        total_deleted = 0
        deleted_by_numeric = 0
        deleted_by_low_unique = 0
        deleted_by_generic = 0

        # Process each bad prefix category separately for tracking
        # To avoid exceeding safety limit, we process in order of priority:
        # 1. Numeric (most clearly bad)
        # 2. Low unique chars
        # 3. Generic

        categories = [
            ("numeric", prefixes_to_delete_numeric),
            ("low_unique", prefixes_to_delete_low_unique),
            ("generic", prefixes_to_delete_generic),
        ]

        # Track which prefixes have already been deleted (to avoid double-counting)
        already_deleted_prefixes = set()

        for cat_name, cat_prefixes in categories:
            if total_deleted >= MAX_DELETE:
                print(f"  Safety limit reached. Stopping deletions.", flush=True)
                break

            # Only process prefixes not yet deleted
            new_prefixes = cat_prefixes - already_deleted_prefixes
            if not new_prefixes:
                continue

            cat_deleted = 0
            batch_explanations = []

            for prefix in sorted(new_prefixes):
                if total_deleted >= MAX_DELETE:
                    break

                explanation_match = f"prefix:{prefix}"
                prefix_count = full_prefix_counter[prefix]

                # Check if this deletion would exceed safety limit
                if total_deleted + prefix_count > MAX_DELETE:
                    print(f"  Skipping prefix '{prefix}' ({prefix_count:,} conns) - "
                          f"would exceed safety limit", flush=True)
                    continue

                batch_explanations.append(explanation_match)

                # Execute in batches of explanations
                if len(batch_explanations) >= 50:
                    placeholders = ",".join(["?"] * len(batch_explanations))
                    cursor = db.execute(
                        f"DELETE FROM connections "
                        f"WHERE source = 'p15_label_prefix' "
                        f"AND explanation IN ({placeholders})",
                        batch_explanations
                    )
                    batch_deleted = cursor.rowcount
                    db_commit_retry(db)
                    cat_deleted += batch_deleted
                    total_deleted += batch_deleted
                    batch_explanations = []

            # Flush remaining batch
            if batch_explanations:
                placeholders = ",".join(["?"] * len(batch_explanations))
                cursor = db.execute(
                    f"DELETE FROM connections "
                    f"WHERE source = 'p15_label_prefix' "
                    f"AND explanation IN ({placeholders})",
                    batch_explanations
                )
                batch_deleted = cursor.rowcount
                db_commit_retry(db)
                cat_deleted += batch_deleted
                total_deleted += batch_deleted

            already_deleted_prefixes.update(new_prefixes)

            if cat_name == "numeric":
                deleted_by_numeric = cat_deleted
            elif cat_name == "low_unique":
                deleted_by_low_unique = cat_deleted
            elif cat_name == "generic":
                deleted_by_generic = cat_deleted

            print(f"  {cat_name:12s}: deleted {cat_deleted:,} connections "
                  f"({len(new_prefixes):,} prefixes)", flush=True)

        print(f"\n  Total deleted: {total_deleted:,}", flush=True)

    # --- Counts after ---
    conn_total_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    prefix_conn_after = db.execute(
        "SELECT COUNT(*) FROM connections WHERE source = 'p15_label_prefix'"
    ).fetchone()[0]
    elapsed = time.time() - t0

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print(f"\n{'=' * 70}", flush=True)
    print("PHASE 16 B1 SUMMARY", flush=True)
    print(f"{'=' * 70}", flush=True)
    print(f"  Total connections before:      {conn_total_before:,}", flush=True)
    print(f"  Total connections after:       {conn_total_after:,}", flush=True)
    print(f"  Net change:                   {conn_total_after - conn_total_before:+,}", flush=True)
    print(f"  Prefix connections before:     {prefix_conn_count:,}", flush=True)
    print(f"  Prefix connections after:      {prefix_conn_after:,}", flush=True)
    print(f"  Prefix connections removed:   -{prefix_conn_count - prefix_conn_after:,}", flush=True)
    if all_bad_prefixes:
        print(f"  ---", flush=True)
        print(f"  Deleted by numeric prefix:    {deleted_by_numeric:,}", flush=True)
        print(f"  Deleted by low-unique chars:  {deleted_by_low_unique:,}", flush=True)
        print(f"  Deleted by generic (>400):    {deleted_by_generic:,}", flush=True)
    if api_key and llm_scores:
        print(f"  ---", flush=True)
        print(f"  LLM pairs evaluated:           {len(llm_scores)}", flush=True)
        print(f"  LLM average score:             {llm_avg_score:.2f}", flush=True)
        print(f"  LLM-exempted prefixes:         {len(llm_high_scoring_prefixes)}", flush=True)
    elif not api_key:
        print(f"  ---", flush=True)
        print(f"  LLM evaluation:                skipped (no OPENAI_API_KEY)", flush=True)
    print(f"  ---", flush=True)
    print(f"  Unique prefixes total:         {len(full_prefix_counter):,}", flush=True)
    print(f"  Bad prefixes identified:       {len(all_bad_prefixes):,}", flush=True)
    print(f"  Safety limit:                  {MAX_DELETE:,}", flush=True)
    print(f"  Duration:                      {elapsed:.1f}s", flush=True)

    db.close()

    # --- Copy DB back ---
    print(f"\nCopying DB back to {SRC_DB} ...", flush=True)
    shutil.copy2(TMP_DB, SRC_DB)
    print("  Done.", flush=True)
    print("Phase 16 B1 complete.", flush=True)


if __name__ == "__main__":
    main()
