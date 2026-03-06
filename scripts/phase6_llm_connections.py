"""
Phase 6D: LLM connection generation using GPT-4o-mini.
Select top ~2000 culturally important entities, generate connections.
"""

import sqlite3
import json
import os
import time
from typing import Dict, List, Optional, Tuple
from openai import OpenAI

DB_PATH = "ontology/culture_ontology.db"

def log(msg):
    print(msg, flush=True)
    with open('data/progress_log.txt', 'a') as f:
        f.write(f"[LLM-Gen] {msg}\n")

def log_error(msg):
    print(f"ERROR: {msg}", flush=True)
    with open('data/error_log.txt', 'a') as f:
        f.write(f"[LLM-Gen] {msg}\n")

SYSTEM_PROMPT = """あなたは日本文化の深い知識を持つ学芸員です。
与えられたエンティティ（人物・作品・場所）について、
文化的に興味深い繋がりを持つ他のエンティティを提案してください。

接続タイプ:
- influence: 師弟関係、影響関係
- thematic_resonance: テーマ的共鳴（時代・媒体を超えた類似性）
- era_bridge: 時代を橋渡しする接続
- medium_cross: 異なる媒体間の接続（小説→映画、浮世絵→アニメなど）
- shared_motif: 共通モチーフ
- geographic_cultural: 地理的・文化的結びつき
- adaptation: 原作→翻案

必ず以下の形式のJSONオブジェクトで返してください:
{"suggestions": [
  {
    "target_name": "接続先エンティティ名",
    "target_type": "person/work/place",
    "connection_type": "上記のいずれか",
    "explanation": "接続の文化的意義（1-2文）",
    "cultural_relevance": 0.0-1.0,
    "serendipity_quality": 0.0-1.0
  }
]}

意外性のある接続（時代を超えた共鳴、異なるジャンル間の接続）を重視してください。
自明な接続（同じシリーズ、同じ作者の別作品）は避けてください。"""

USER_PROMPT = """以下の{count}つのエンティティについて、それぞれ3-5件の接続を提案してください。

{entities_text}

各提案に "source_name" フィールド（提案元のエンティティ名）を必ず含めてください。
合計で{count}エンティティ × 3-5件 = {min_total}-{max_total}件の提案を含めてください。
形式: {{"suggestions": [{{"source_name": "元エンティティ名", "target_name": "...", ...}}]}}"""


class EntityMatcher:
    """Match LLM-generated entity names to existing DB entities."""

    def __init__(self, db):
        self.db = db
        self._cache = {}
        self._load_index()

    def _load_index(self):
        rows = self.db.execute("SELECT id, label_ja, entity_type FROM entities").fetchall()
        self.index = {}
        for eid, label, etype in rows:
            if label:
                self.index[label] = (eid, etype)

    def match_or_create(self, name, entity_type):
        if name in self._cache:
            return self._cache[name]

        # Exact match
        if name in self.index:
            eid = self.index[name][0]
            self._cache[name] = eid
            return eid

        # Partial match (contains)
        for label, (eid, _) in self.index.items():
            if name in label or label in name:
                self._cache[name] = eid
                return eid

        # Create new entity
        self.db.execute(
            "INSERT INTO entities (label_ja, entity_type, source) VALUES (?, ?, 'llm_phase6')",
            (name, entity_type)
        )
        eid = self.db.execute("SELECT last_insert_rowid()").fetchone()[0]
        self.index[name] = (eid, entity_type)
        self._cache[name] = eid
        return eid


def select_priority_entities(db):
    """Select culturally important entities for LLM connection generation."""
    entities = []

    # 1. AniList top popularity (anime/manga with highest popularity)
    rows = db.execute("""
        SELECT id, label_ja, entity_type FROM entities
        WHERE anilist_id IS NOT NULL
        ORDER BY anilist_id ASC
        LIMIT 500
    """).fetchall()
    entities.extend(rows)
    log(f"  AniList top: {len(rows)}")

    # 2. Wikidata entities (places with coordinates - culturally significant)
    rows = db.execute("""
        SELECT id, label_ja, entity_type FROM entities
        WHERE wikidata_id IS NOT NULL AND lat IS NOT NULL
        LIMIT 500
    """).fetchall()
    entities.extend(rows)
    log(f"  Wikidata places: {len(rows)}")

    # 3. Wikidata entities (people - culturally significant)
    rows = db.execute("""
        SELECT id, label_ja, entity_type FROM entities
        WHERE wikidata_id IS NOT NULL AND entity_type = 'person'
        LIMIT 300
    """).fetchall()
    entities.extend(rows)
    log(f"  Wikidata people: {len(rows)}")

    # 4. Existing entities with most keep connections (high-quality nodes)
    rows = db.execute("""
        SELECT e.id, e.label_ja, e.entity_type FROM entities e
        JOIN (
            SELECT entity_a_id as eid, COUNT(*) as cnt FROM connections WHERE llm_verdict='keep' GROUP BY entity_a_id
            UNION ALL
            SELECT entity_b_id as eid, COUNT(*) as cnt FROM connections WHERE llm_verdict='keep' GROUP BY entity_b_id
        ) c ON e.id = c.eid
        GROUP BY e.id
        ORDER BY SUM(c.cnt) DESC
        LIMIT 200
    """).fetchall()
    entities.extend(rows)
    log(f"  High-connectivity: {len(rows)}")

    # 5. NDL classical works (unique cultural artifacts)
    rows = db.execute("""
        SELECT id, label_ja, entity_type FROM entities
        WHERE ndl_id IS NOT NULL
        LIMIT 500
    """).fetchall()
    entities.extend(rows)
    log(f"  NDL works: {len(rows)}")

    # Deduplicate
    seen = set()
    unique = []
    for eid, label, etype in entities:
        if eid not in seen:
            seen.add(eid)
            unique.append((eid, label, etype))

    log(f"  Total unique priority entities: {len(unique)}")
    return unique


def generate_connections_batch(client, entities_batch, matcher, db):
    """Generate connections for a batch of entities."""
    entities_text = "\n".join([
        f"- {label} ({etype})" for _, label, etype in entities_batch
    ])
    count = len(entities_batch)

    prompt = USER_PROMPT.format(
        count=count,
        entities_text=entities_text,
        min_total=count * 3,
        max_total=count * 5
    )

    log(f"    Calling GPT-4o-mini for {count} entities...")

    for attempt in range(3):
        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                response_format={"type": "json_object"},
                temperature=0.8,
                max_tokens=4000,
            )

            content = response.choices[0].message.content
            data = json.loads(content)
            suggestions = data.get('suggestions', [])

            usage = response.usage
            input_tokens = usage.prompt_tokens if usage else 0
            output_tokens = usage.completion_tokens if usage else 0
            cost = input_tokens * 0.00000015 + output_tokens * 0.0000006

            log(f"    Got {len(suggestions)} suggestions, cost=${cost:.4f}")
            return suggestions, cost

        except Exception as e:
            log_error(f"API error (attempt {attempt+1}): {e}")
            time.sleep(5)

    return [], 0


def save_connections(db, matcher, suggestions, source_entities):
    """Save generated connections to DB."""
    saved = 0
    source_map = {label: eid for eid, label, _ in source_entities}

    for sug in suggestions:
        target_name = sug.get('target_name', '')
        target_type = sug.get('target_type', 'work')
        conn_type = sug.get('connection_type', 'thematic_resonance')
        explanation = sug.get('explanation', '')
        cr = sug.get('cultural_relevance', 0.7)
        sq = sug.get('serendipity_quality', 0.7)
        source_name = sug.get('source_name', '')

        if not target_name:
            continue

        # Find source entity
        source_eid = None
        # First try source_name field
        if source_name and source_name in source_map:
            source_eid = source_map[source_name]
        # Then try partial match on source_name
        if source_eid is None and source_name:
            for label, eid in source_map.items():
                if label and (source_name in label or label in source_name):
                    source_eid = eid
                    break
        # Then try explanation
        if source_eid is None:
            for label, eid in source_map.items():
                if label and label in explanation:
                    source_eid = eid
                    break
        if source_eid is None:
            source_eid = source_entities[0][0]

        target_eid = matcher.match_or_create(target_name, target_type)

        # Check for duplicate
        existing = db.execute("""
            SELECT id FROM connections
            WHERE (entity_a_id = ? AND entity_b_id = ?) OR (entity_a_id = ? AND entity_b_id = ?)
        """, (source_eid, target_eid, target_eid, source_eid)).fetchone()

        if existing:
            continue

        db.execute("""
            INSERT INTO connections
            (entity_a_id, entity_b_id, connection_type, serendipity_score,
             source, llm_cultural_relevance, llm_serendipity_quality,
             llm_explanation, llm_verdict)
            VALUES (?, ?, ?, ?, 'llm_phase6', ?, ?, ?, 'keep')
        """, (source_eid, target_eid, conn_type, sq * 0.8 + cr * 0.2,
              cr, sq, explanation))
        saved += 1

    return saved


def main():
    client = OpenAI()
    db = sqlite3.connect(DB_PATH)

    # Ensure columns exist
    try:
        db.execute("SELECT source FROM entities LIMIT 1")
    except:
        db.execute("ALTER TABLE entities ADD COLUMN source TEXT DEFAULT 'phase3'")
        db.commit()

    matcher = EntityMatcher(db)

    log("=== Phase 6D: LLM Connection Generation ===")
    log("Selecting priority entities...")
    priority = select_priority_entities(db)

    # Batch size: 10 entities per API call
    batch_size = 10
    total_cost = 0
    total_saved = 0
    total_suggestions = 0

    for i in range(0, len(priority), batch_size):
        batch = priority[i:i+batch_size]
        batch_num = i // batch_size + 1
        total_batches = (len(priority) + batch_size - 1) // batch_size

        if batch_num % 10 == 1:
            log(f"  Batch {batch_num}/{total_batches}, cost so far: ${total_cost:.3f}")

        suggestions, cost = generate_connections_batch(client, batch, matcher, db)
        total_cost += cost
        total_suggestions += len(suggestions)

        saved = save_connections(db, matcher, suggestions, batch)
        total_saved += saved

        db.commit()
        time.sleep(0.5)  # Rate limit buffer

        # Cost check
        if total_cost > 10:
            log(f"  Cost limit reached: ${total_cost:.2f}")
            break

    log(f"\n=== LLM Connection Generation Complete ===")
    log(f"Entities processed: {len(priority):,}")
    log(f"Suggestions generated: {total_suggestions:,}")
    log(f"Connections saved: {total_saved:,}")
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
