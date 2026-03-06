"""Phase 5: LLM-driven connection generation pipeline.

Uses GPT-4o to generate culturally meaningful connections,
then matches them against existing DB entities.
"""

import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path

from typing import Dict, List, Optional

from openai import OpenAI

DB_PATH = Path(__file__).parent / "culture_ontology.db"

# ── Prompts ─────────────────────────────────────────────

GENERATION_SYSTEM_PROMPT = """あなたは日本文化の深い知識を持つ学芸員です。

与えられたエンティティ（人物・作品・場所）について、
文化的に興味深い繋がりを持つ他のエンティティを提案してください。

**良い接続の例:**
- 葛飾北斎 → 蟲師（浮世絵的自然観がアニメで再解釈された）
- 鳥山石燕 → ゲゲゲの鬼太郎（妖怪画の系譜が現代漫画に継承された）
- 浅草 → 落語（江戸庶民文化の舞台として共有）
- 金閣寺 → 三島由紀夫「金閣寺」→ 京アニの京都作品（場所が文学・アニメに変奏された）

**求める接続の質:**
1. 時代を超えた接続（江戸の浮世絵 → 現代のアニメ）
2. 媒体を超えた接続（文学 → 映画 → 漫画）
3. 意外だが納得できる接続（「言われてみれば！」）
4. 地域を超えた接続（京都の寺 → 東北の祭り、共通テーマで）

**避けるべき接続:**
- 同じ作者の別作品（当たり前すぎる）
- 同じジャンルの類似作品（ガンダム→マクロス のような）
- 地理的に近いだけ（同じ県にある、程度）

各エンティティについて5-10個の接続候補を提案してください。

必ず以下の形式のJSONオブジェクトで返してください:
{"suggestions": [
  {
    "source_entity": "<入力エンティティ名>",
    "target": {
      "name": "<接続先の名前>",
      "name_variants": ["<表記揺れ候補1>", "<候補2>"],
      "type": "person" | "work" | "place",
      "era": "<時代>",
      "medium": "<媒体>"
    },
    "connection": {
      "type": "era_bridge" | "medium_cross" | "thematic_resonance" | "influence" | "adaptation" | "shared_motif" | "geographic_cultural",
      "explanation": "<2-3文の説明>",
      "cultural_relevance": <0.0-1.0>,
      "serendipity_quality": <0.0-1.0>
    }
  }
]}"""

THEME_GENERATION_PROMPT = """あなたは日本文化の深い知識を持つ学芸員です。

以下のテーマについて、時代・媒体を横断する文化的ネットワークを提案してください。

テーマ: {theme}

以下の形式で、このテーマに関連する重要なエンティティとその接続を提案してください:

1. 古典（江戸以前）の代表的作品・人物・場所を2-3個
2. 近代（明治〜昭和）の代表的作品・人物を2-3個
3. 現代（平成〜令和）の代表的作品・人物を2-3個
4. これらの間の最も面白い接続を5-10個

必ず以下の形式のJSONオブジェクトで返してください:
{
  "theme": "<テーマ名>",
  "entities": [
    {"name": "...", "name_variants": ["..."], "type": "person" | "work" | "place", "era": "...", "medium": "..."}
  ],
  "connections": [
    {
      "from": "<エンティティ名>",
      "to": "<エンティティ名>",
      "type": "era_bridge" | "medium_cross" | "thematic_resonance" | "influence" | "adaptation" | "shared_motif",
      "explanation": "...",
      "cultural_relevance": 0.0,
      "serendipity_quality": 0.0
    }
  ]
}"""


# ── ERA / MEDIUM mapping ────────────────────────────────

ERA_MAP = {
    "古代": "ancient", "中世": "medieval", "室町": "medieval",
    "江戸": "edo_late", "江戸前期": "edo_early", "江戸後期": "edo_late",
    "明治": "meiji_taisho", "大正": "meiji_taisho",
    "昭和": "showa_postwar", "昭和戦前": "showa_prewar", "昭和戦後": "showa_postwar",
    "平成": "heisei", "令和": "reiwa", "現代": "heisei", "近代": "meiji_taisho",
    "戦国": "edo_early", "鎌倉": "medieval", "奈良": "ancient", "飛鳥": "ancient",
    "安土桃山": "edo_early",
}

MEDIUM_MAP = {
    "浮世絵": "ukiyoe", "日本画": "painting", "絵画": "painting", "版画": "ukiyoe",
    "漫画": "manga", "マンガ": "manga", "アニメ": "anime", "TVアニメ": "anime_tv",
    "映画": "anime_movie", "アニメ映画": "anime_movie", "ゲーム": "game",
    "小説": "literature", "文学": "literature", "ライトノベル": "light_novel",
    "音楽": "music", "歌舞伎": "kabuki", "能": "noh", "落語": "theater",
    "狂言": "noh", "神社": "architecture", "寺": "architecture", "城": "architecture",
    "庭園": "architecture", "建築": "architecture", "特撮": "anime",
    "舞踊": "performing_art", "茶道": "performing_art", "書道": "calligraphy",
    "陶芸": "crafts", "工芸": "crafts",
}


def resolve_era(text: str) -> Optional[str]:
    if not text:
        return None
    for key, code in ERA_MAP.items():
        if key in text:
            return code
    return None


def resolve_medium(text: str) -> Optional[str]:
    if not text:
        return None
    for key, code in MEDIUM_MAP.items():
        if key in text:
            return code
    return None


# ── EntityMatcher ────────────────────────────────────────

class EntityMatcher:
    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self._cache: Dict[int, dict] = {}
        self._load_cache()

    def _load_cache(self):
        rows = self.db.execute(
            "SELECT id, label_ja, label_en, entity_type, wikidata_id FROM entities"
        ).fetchall()
        for row in rows:
            self._cache[row["id"]] = dict(row)

    def match(self, name: str, name_variants: Optional[list] = None, entity_type: Optional[str] = None) -> Optional[dict]:
        all_names = [name] + (name_variants or [])
        # Filter by type helper
        def type_ok(e):
            return entity_type is None or entity_type == "concept" or e["entity_type"] == entity_type

        # 1. Exact match
        for n in all_names:
            for e in self._cache.values():
                if not type_ok(e):
                    continue
                if e["label_ja"] == n:
                    return e
                if e["label_en"] and e["label_en"].lower() == n.lower():
                    return e

        # 2. Partial match (contains)
        for n in all_names:
            if len(n) < 2:
                continue
            for e in self._cache.values():
                if not type_ok(e):
                    continue
                if e["label_ja"] and n in e["label_ja"]:
                    return e
                if e["label_en"] and n.lower() in e["label_en"].lower():
                    return e

        # 3. Fuzzy match (SequenceMatcher >= 0.8)
        best_match = None
        best_score = 0.0
        for n in all_names:
            for e in self._cache.values():
                if not type_ok(e):
                    continue
                if e["label_ja"]:
                    score = SequenceMatcher(None, n, e["label_ja"]).ratio()
                    if score > best_score and score >= 0.8:
                        best_score = score
                        best_match = e
                if e["label_en"]:
                    score = SequenceMatcher(None, n.lower(), e["label_en"].lower()).ratio()
                    if score > best_score and score >= 0.8:
                        best_score = score
                        best_match = e

        return best_match

    def create_entity(self, name: str, entity_type: str, era: Optional[str] = None, medium: Optional[str] = None) -> int:
        if entity_type == "concept":
            entity_type = "work"  # Store concepts as works
        cursor = self.db.execute(
            "INSERT INTO entities (label_ja, entity_type) VALUES (?, ?)",
            (name, entity_type),
        )
        new_id = cursor.lastrowid

        era_code = resolve_era(era) if era else None
        if era_code:
            self.db.execute(
                "INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'era', ?, 'llm_generated', 0.7)",
                (new_id, era_code),
            )
        medium_code = resolve_medium(medium) if medium else None
        if medium_code:
            self.db.execute(
                "INSERT OR IGNORE INTO entity_tags (entity_id, axis, value_code, source, confidence) VALUES (?, 'medium', ?, 'llm_generated', 0.7)",
                (new_id, medium_code),
            )

        self.db.commit()
        self._cache[new_id] = {
            "id": new_id, "label_ja": name, "label_en": None,
            "entity_type": entity_type, "wikidata_id": None,
        }
        return new_id


# ── LLMConnectionGenerator ──────────────────────────────

class LLMConnectionGenerator:
    def __init__(self, model: str = "gpt-4o"):
        self.client = OpenAI()
        self.model = model
        self.total_input_tokens = 0
        self.total_output_tokens = 0

    def estimated_cost(self) -> float:
        return self.total_input_tokens * 2.50 / 1_000_000 + self.total_output_tokens * 10.00 / 1_000_000

    def generate_entity_connections(self, entities: list[dict]) -> list[dict]:
        """Generate connections for a batch of entities (max 5)."""
        descs = []
        for e in entities:
            ctx = {"name": e["label_ja"], "type": e.get("entity_type", "unknown")}
            if e.get("label_en"):
                ctx["name_en"] = e["label_en"]
            tags = e.get("tags", {})
            if tags:
                ctx["tags"] = tags
            descs.append(json.dumps(ctx, ensure_ascii=False))

        user_prompt = (
            f"以下の{len(entities)}個のエンティティそれぞれについて、文化的に関連する接続候補を5-10個提案してください。\n\n"
            + "\n\n".join(descs)
        )

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": GENERATION_SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.7,
            response_format={"type": "json_object"},
            max_tokens=8000,
        )

        self.total_input_tokens += response.usage.prompt_tokens
        self.total_output_tokens += response.usage.completion_tokens

        content = response.choices[0].message.content
        parsed = json.loads(content)

        if isinstance(parsed, dict):
            return parsed.get("suggestions", parsed.get("results", parsed.get("connections", [])))
        return parsed if isinstance(parsed, list) else []

    def generate_theme_connections(self, theme: str) -> dict:
        """Generate a cultural network for a theme."""
        user_prompt = THEME_GENERATION_PROMPT.replace("{theme}", theme)

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "あなたは日本文化の深い知識を持つ学芸員です。"},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.7,
            response_format={"type": "json_object"},
            max_tokens=8000,
        )

        self.total_input_tokens += response.usage.prompt_tokens
        self.total_output_tokens += response.usage.completion_tokens

        content = response.choices[0].message.content
        return json.loads(content)


# ── Pipeline ─────────────────────────────────────────────

PRIORITY_ENTITIES = [
    "葛飾北斎", "浅草神社",
    "歌川広重", "手塚治虫", "宮崎駿", "水木しげる",
    "鹿苑寺", "伏見稲荷大社", "厳島神社", "東大寺",
    "ゲゲゲの鬼太郎", "千と千尋の神隠し", "もののけ姫", "蟲師", "鬼滅の刃",
]

PRIORITY_THEMES = [
    "妖怪", "忍者", "侍", "浮世絵", "茶道",
    "祭り", "温泉", "桜", "紅葉", "禅",
]


def get_all_tags(db: sqlite3.Connection, entity_id: int) -> Dict[str, List[str]]:
    tags: Dict[str, List[str]] = {}
    for r in db.execute("SELECT axis, value_code FROM entity_tags WHERE entity_id=?", (entity_id,)):
        tags.setdefault(r["axis"], []).append(r["value_code"])
    return tags


def run_entity_generation(db: sqlite3.Connection, generator: LLMConnectionGenerator, matcher: EntityMatcher, cost_limit: float = 5.0):
    """Task B: Generate connections for priority entities."""
    print("=== Task B: Entity Connection Generation ===\n")

    entities = []
    for name in PRIORITY_ENTITIES:
        row = db.execute("SELECT * FROM entities WHERE label_ja LIKE ?", (f"%{name}%",)).fetchone()
        if row:
            tags = get_all_tags(db, row["id"])
            entities.append({**dict(row), "tags": tags})
        else:
            print(f"  NOT FOUND: {name} → will create")
            entities.append({
                "id": None, "label_ja": name, "label_en": None,
                "entity_type": "unknown", "wikidata_id": None, "tags": {},
            })

    all_connections = []
    for i in range(0, len(entities), 5):
        if generator.estimated_cost() > cost_limit:
            print(f"\nCost limit reached: ${generator.estimated_cost():.2f}")
            break

        batch = entities[i : i + 5]
        print(f"Batch {i // 5 + 1}: {[e['label_ja'] for e in batch]}")

        try:
            suggestions = generator.generate_entity_connections(batch)
            print(f"  → {len(suggestions)} suggestions, cost=${generator.estimated_cost():.3f}")

            for s in suggestions:
                target = s.get("target", {})
                target_name = target.get("name", "")
                target_variants = target.get("name_variants", [])
                target_type = target.get("type", "work")
                conn_info = s.get("connection", {})

                matched = matcher.match(target_name, target_variants, target_type)
                if matched:
                    match_type = "existing"
                    target_id = matched["id"]
                else:
                    match_type = "new"
                    target_id = matcher.create_entity(
                        target_name, target_type,
                        era=target.get("era"), medium=target.get("medium"),
                    )

                source_name = s.get("source_entity", "")
                source_entity = next(
                    (e for e in entities if e["label_ja"] and source_name in e["label_ja"]),
                    None,
                )
                source_id = source_entity["id"] if source_entity and source_entity.get("id") else None
                if source_id is None and source_entity:
                    source_id = matcher.create_entity(
                        source_name, source_entity.get("entity_type", "unknown"),
                    )

                if source_id and target_id and source_id != target_id:
                    all_connections.append({
                        "entity_a_id": source_id,
                        "entity_b_id": target_id,
                        "connection_type": conn_info.get("type", "thematic_resonance"),
                        "explanation": conn_info.get("explanation", ""),
                        "llm_cultural_relevance": conn_info.get("cultural_relevance", 0.5),
                        "llm_serendipity_quality": conn_info.get("serendipity_quality", 0.5),
                        "match_type": match_type,
                    })
                    print(f"    {source_name} → {target_name} [{match_type}] ({conn_info.get('type', '?')})")

        except Exception as e:
            print(f"  → ERROR: {e}")

        time.sleep(2)

    return all_connections


def run_theme_generation(db: sqlite3.Connection, generator: LLMConnectionGenerator, matcher: EntityMatcher, cost_limit: float = 5.0):
    """Task C: Generate connections for priority themes."""
    print("\n=== Task C: Theme Connection Generation ===\n")

    all_connections = []
    all_new_entities = 0

    for theme in PRIORITY_THEMES:
        if generator.estimated_cost() > cost_limit:
            print(f"\nCost limit reached: ${generator.estimated_cost():.2f}")
            break

        print(f"Theme: {theme}")
        try:
            result = generator.generate_theme_connections(theme)
            print(f"  → {len(result.get('entities', []))} entities, {len(result.get('connections', []))} connections, cost=${generator.estimated_cost():.3f}")

            # Match/create entities
            entity_id_map: Dict[str, int] = {}
            for ent in result.get("entities", []):
                ename = ent.get("name", "")
                evariants = ent.get("name_variants", [])
                etype = ent.get("type", "work")

                matched = matcher.match(ename, evariants, etype)
                if matched:
                    entity_id_map[ename] = matched["id"]
                else:
                    new_id = matcher.create_entity(
                        ename, etype,
                        era=ent.get("era"), medium=ent.get("medium"),
                    )
                    entity_id_map[ename] = new_id
                    all_new_entities += 1
                    print(f"    NEW: {ename} ({etype})")

            # Process connections
            for conn in result.get("connections", []):
                from_name = conn.get("from", "")
                to_name = conn.get("to", "")

                from_id = entity_id_map.get(from_name)
                to_id = entity_id_map.get(to_name)

                # Try matching if not in map
                if not from_id:
                    m = matcher.match(from_name)
                    if m:
                        from_id = m["id"]
                if not to_id:
                    m = matcher.match(to_name)
                    if m:
                        to_id = m["id"]

                if from_id and to_id and from_id != to_id:
                    all_connections.append({
                        "entity_a_id": from_id,
                        "entity_b_id": to_id,
                        "connection_type": conn.get("type", "thematic_resonance"),
                        "explanation": conn.get("explanation", ""),
                        "llm_cultural_relevance": conn.get("cultural_relevance", 0.5),
                        "llm_serendipity_quality": conn.get("serendipity_quality", 0.5),
                        "match_type": "theme_generated",
                    })
                    print(f"    {from_name} → {to_name} ({conn.get('type', '?')})")

        except Exception as e:
            print(f"  → ERROR: {e}")

        time.sleep(2)

    print(f"\n  New entities from themes: {all_new_entities}")
    return all_connections


def save_connections(db: sqlite3.Connection, connections: list[dict]) -> int:
    """Save connections to DB, skipping duplicates."""
    saved = 0
    for conn in connections:
        # Check duplicate (either direction)
        existing = db.execute(
            "SELECT id FROM connections WHERE (entity_a_id=? AND entity_b_id=?) OR (entity_a_id=? AND entity_b_id=?)",
            (conn["entity_a_id"], conn["entity_b_id"], conn["entity_b_id"], conn["entity_a_id"]),
        ).fetchone()
        if existing:
            continue

        score = (conn["llm_cultural_relevance"] + conn["llm_serendipity_quality"]) / 2
        db.execute(
            """INSERT INTO connections
               (entity_a_id, entity_b_id, connection_type, explanation,
                llm_cultural_relevance, llm_serendipity_quality, llm_verdict, llm_explanation,
                serendipity_score, source, confidence)
               VALUES (?, ?, ?, ?, ?, ?, 'keep', ?, ?, 'llm_generated', 0.9)""",
            (
                conn["entity_a_id"], conn["entity_b_id"],
                conn["connection_type"], conn["explanation"],
                conn["llm_cultural_relevance"], conn["llm_serendipity_quality"],
                conn["explanation"], score,
            ),
        )
        saved += 1

    db.commit()
    return saved


def main():
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row

    generator = LLMConnectionGenerator(model="gpt-4o")
    matcher = EntityMatcher(db)

    # Baseline
    entity_count_before = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conn_count_before = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    keep_before = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]

    print(f"Baseline: {entity_count_before} entities, {conn_count_before} connections ({keep_before} keep)\n")

    # Task B: Entity connections
    entity_connections = run_entity_generation(db, generator, matcher, cost_limit=3.0)

    # Task C: Theme connections
    theme_connections = run_theme_generation(db, generator, matcher, cost_limit=4.5)

    # Save all
    all_conns = entity_connections + theme_connections
    print(f"\n=== Saving ===")
    print(f"Total candidates: {len(all_conns)}")
    saved = save_connections(db, all_conns)
    print(f"Saved (new, non-duplicate): {saved}")

    # Final stats
    entity_count_after = db.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
    conn_count_after = db.execute("SELECT COUNT(*) FROM connections").fetchone()[0]
    keep_after = db.execute("SELECT COUNT(*) FROM connections WHERE llm_verdict='keep'").fetchone()[0]

    print(f"\n=== Final Stats ===")
    print(f"Entities: {entity_count_before} → {entity_count_after} (+{entity_count_after - entity_count_before})")
    print(f"Connections: {conn_count_before} → {conn_count_after} (+{conn_count_after - conn_count_before})")
    print(f"Keep: {keep_before} → {keep_after} (+{keep_after - keep_before})")
    print(f"API cost: ${generator.estimated_cost():.3f}")
    print(f"Tokens: input={generator.total_input_tokens:,}, output={generator.total_output_tokens:,}")

    db.close()


if __name__ == "__main__":
    main()
