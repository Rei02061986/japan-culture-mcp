"""Phase 4.5: LLM-based connection quality evaluator using GPT-4o."""

import json
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path

from openai import OpenAI

DB_PATH = Path(__file__).parent / "culture_ontology.db"

SYSTEM_PROMPT = """あなたは日本文化の専門家です。
日本文化オントロジーにおける2つのエンティティ間の「セレンディピティ接続」を評価してください。

セレンディピティ接続とは、一見無関係に見えるが、深い文化的つながりを持つエンティティ同士の関係です。

各接続について以下を0.0〜1.0で評価してください:
- cultural_relevance: 文化的に意味のある関連性があるか（0=無関係、1=深い文化的つながり）
- serendipity_quality: 意外性と発見の質（0=自明すぎるか無理がある、1=知的な驚きがある）

verdict: "keep"（両スコアの平均が0.4以上）または "reject"（平均0.4未満）
reason: 判定理由を1文で

評価基準:
- 単に同じ地域・時代というだけの接続はreject
- テーマが近くても文化的文脈が全く異なる場合はreject（例: アンパンマンと鬼滅の刃は「超自然」で括れるが文化的文脈が違いすぎる）
- 異なるジャンル・時代を超えた本質的な文化的共鳴があればkeep
- 人物と場所の接続は、その人物とその場所に具体的なゆかりがある場合のみkeep"""

USER_PROMPT_TEMPLATE = """以下の{count}件の接続を全て評価してください。
必ず{count}件全ての評価を含めてください。

{connections_text}

必ず以下の形式のJSONオブジェクトで返してください:
{{"evaluations": [{{"id": 接続ID, "cultural_relevance": 0.0-1.0, "serendipity_quality": 0.0-1.0, "verdict": "keep"か"reject", "reason": "理由1文"}}, ... 全{count}件]}}"""


@dataclass
class ConnectionCandidate:
    id: int
    a_label: str
    a_type: str
    b_label: str
    b_type: str
    connection_type: str
    serendipity_score: float
    explanation: str
    theme_distance: float
    era_distance: float
    medium_distance: float
    geography_distance: float
    a_tags: dict = field(default_factory=dict)
    b_tags: dict = field(default_factory=dict)

    def to_prompt_text(self) -> str:
        a_tags_str = ", ".join(f"{k}={','.join(v)}" for k, v in self.a_tags.items())
        b_tags_str = ", ".join(f"{k}={','.join(v)}" for k, v in self.b_tags.items())
        return (
            f"[ID:{self.id}] {self.a_label}({self.a_type}) ↔ {self.b_label}({self.b_type})\n"
            f"  type={self.connection_type}, score={self.serendipity_score:.2f}\n"
            f"  説明: {self.explanation}\n"
            f"  A tags: {a_tags_str}\n"
            f"  B tags: {b_tags_str}\n"
            f"  距離: theme={self.theme_distance:.2f}, era={self.era_distance:.2f}, "
            f"medium={self.medium_distance:.2f}, geo={self.geography_distance:.2f}"
        )


@dataclass
class LLMEvaluation:
    connection_id: int
    cultural_relevance: float
    serendipity_quality: float
    verdict: str
    reason: str


class GPTConnectionEvaluator:
    def __init__(self, db_path: str = str(DB_PATH), model: str = "gpt-4o"):
        self.db_path = db_path
        self.model = model
        self.client = OpenAI()
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.cost_limit = 5.0

    def _get_db(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path)
        db.row_factory = sqlite3.Row
        return db

    def _ensure_schema(self):
        db = self._get_db()
        # Add columns to connections if not exist
        cols = [r["name"] for r in db.execute("PRAGMA table_info(connections)")]
        for col, typ in [
            ("llm_cultural_relevance", "REAL"),
            ("llm_serendipity_quality", "REAL"),
            ("llm_explanation", "TEXT"),
            ("llm_verdict", "TEXT"),
        ]:
            if col not in cols:
                db.execute(f"ALTER TABLE connections ADD COLUMN {col} {typ}")

        # Create llm_evaluations table
        db.execute("""CREATE TABLE IF NOT EXISTS llm_evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            connection_id INTEGER NOT NULL,
            cultural_relevance REAL,
            serendipity_quality REAL,
            verdict TEXT,
            reason TEXT,
            model TEXT,
            evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (connection_id) REFERENCES connections(id)
        )""")
        db.commit()
        db.close()

    def _load_candidates(self) -> list[ConnectionCandidate]:
        db = self._get_db()
        rows = db.execute("""
            SELECT c.id, c.connection_type, c.serendipity_score, c.explanation,
                   c.theme_distance, c.era_distance, c.medium_distance, c.geography_distance,
                   c.llm_verdict,
                   ea.id as a_id, ea.label_ja as a_label, ea.entity_type as a_type,
                   eb.id as b_id, eb.label_ja as b_label, eb.entity_type as b_type
            FROM connections c
            JOIN entities ea ON c.entity_a_id = ea.id
            JOIN entities eb ON c.entity_b_id = eb.id
            WHERE c.llm_verdict IS NULL
            ORDER BY c.id
        """).fetchall()

        candidates = []
        for r in rows:
            c = ConnectionCandidate(
                id=r["id"],
                a_label=r["a_label"],
                a_type=r["a_type"],
                b_label=r["b_label"],
                b_type=r["b_type"],
                connection_type=r["connection_type"],
                serendipity_score=r["serendipity_score"],
                explanation=r["explanation"],
                theme_distance=r["theme_distance"] or 0,
                era_distance=r["era_distance"] or 0,
                medium_distance=r["medium_distance"] or 0,
                geography_distance=r["geography_distance"] or 0,
            )
            # Load tags
            for prefix, eid in [("a", r["a_id"]), ("b", r["b_id"])]:
                tags: dict[str, list[str]] = {}
                for t in db.execute(
                    "SELECT axis, value_code FROM entity_tags WHERE entity_id=?",
                    (eid,),
                ):
                    tags.setdefault(t["axis"], []).append(t["value_code"])
                if prefix == "a":
                    c.a_tags = tags
                else:
                    c.b_tags = tags
            candidates.append(c)

        db.close()
        return candidates

    def _estimate_cost(self) -> float:
        # GPT-4o: $2.50/1M input, $10.00/1M output
        input_cost = self.total_input_tokens * 2.50 / 1_000_000
        output_cost = self.total_output_tokens * 10.00 / 1_000_000
        return input_cost + output_cost

    def _evaluate_batch(self, batch: list[ConnectionCandidate]) -> list[LLMEvaluation]:
        connections_text = "\n\n".join(c.to_prompt_text() for c in batch)
        user_prompt = USER_PROMPT_TEMPLATE.format(
            count=len(batch), connections_text=connections_text
        )

        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            response_format={"type": "json_object"},
        )

        self.total_input_tokens += response.usage.prompt_tokens
        self.total_output_tokens += response.usage.completion_tokens

        content = response.choices[0].message.content
        parsed = json.loads(content)

        # Handle various response formats
        evals_data = []
        if isinstance(parsed, list):
            evals_data = parsed
        elif isinstance(parsed, dict):
            # Single evaluation object (has "id" key but no list values)
            if "id" in parsed and "verdict" in parsed:
                evals_data = [parsed]
            else:
                # Try common keys
                for key in ("evaluations", "results", "data", "items"):
                    if key in parsed and isinstance(parsed[key], list):
                        evals_data = parsed[key]
                        break
                if not evals_data:
                    # Try first list value in the dict
                    for v in parsed.values():
                        if isinstance(v, list):
                            evals_data = v
                            break

        evaluations = []
        batch_ids = {c.id for c in batch}
        for item in evals_data:
            cid = item.get("id", 0)
            if cid not in batch_ids:
                continue
            evaluations.append(
                LLMEvaluation(
                    connection_id=cid,
                    cultural_relevance=float(item.get("cultural_relevance", 0)),
                    serendipity_quality=float(item.get("serendipity_quality", 0)),
                    verdict=item.get("verdict", "reject"),
                    reason=item.get("reason", ""),
                )
            )

        if not evaluations:
            print(f"    WARNING: parsed 0 evaluations. Raw keys: {list(parsed.keys()) if isinstance(parsed, dict) else type(parsed)}")
            print(f"    Raw content (first 300): {content[:300]}")

        return evaluations

    def _save_evaluations(self, evaluations: list[LLMEvaluation]):
        db = self._get_db()
        for ev in evaluations:
            db.execute(
                """INSERT INTO llm_evaluations
                   (connection_id, cultural_relevance, serendipity_quality, verdict, reason, model)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    ev.connection_id,
                    ev.cultural_relevance,
                    ev.serendipity_quality,
                    ev.verdict,
                    ev.reason,
                    self.model,
                ),
            )
            db.execute(
                """UPDATE connections SET
                   llm_cultural_relevance=?, llm_serendipity_quality=?,
                   llm_explanation=?, llm_verdict=?
                   WHERE id=?""",
                (
                    ev.cultural_relevance,
                    ev.serendipity_quality,
                    ev.reason,
                    ev.verdict,
                    ev.connection_id,
                ),
            )
        db.commit()
        db.close()

    def run(self, batch_size: int = 10, delay: float = 1.0):
        self._ensure_schema()
        candidates = self._load_candidates()
        total = len(candidates)
        print(f"Connections to evaluate: {total}")

        if total == 0:
            print("All connections already evaluated.")
            return

        batches = [
            candidates[i : i + batch_size]
            for i in range(0, total, batch_size)
        ]
        print(f"Batches: {len(batches)} (batch_size={batch_size})")

        evaluated = 0
        for i, batch in enumerate(batches):
            cost = self._estimate_cost()
            if cost > self.cost_limit:
                print(f"\nCost limit reached: ${cost:.2f} > ${self.cost_limit:.2f}")
                print(f"Evaluated {evaluated}/{total} connections")
                break

            try:
                evals = self._evaluate_batch(batch)
                self._save_evaluations(evals)
                evaluated += len(evals)

                keep = sum(1 for e in evals if e.verdict == "keep")
                reject = len(evals) - keep
                cost = self._estimate_cost()
                print(
                    f"  Batch {i+1}/{len(batches)}: "
                    f"{evaluated}/{total} done, "
                    f"keep={keep} reject={reject}, "
                    f"cost=${cost:.3f}"
                )
            except Exception as e:
                print(f"  Batch {i+1} ERROR: {e}")
                # Save what we can and continue
                continue

            if i < len(batches) - 1:
                time.sleep(delay)

        cost = self._estimate_cost()
        print(f"\nDone. Evaluated: {evaluated}/{total}")
        print(f"Total tokens: input={self.total_input_tokens}, output={self.total_output_tokens}")
        print(f"Estimated cost: ${cost:.3f}")


if __name__ == "__main__":
    evaluator = GPTConnectionEvaluator()
    evaluator.run(batch_size=10, delay=1.0)
