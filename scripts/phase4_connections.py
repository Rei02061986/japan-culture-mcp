"""Phase 4: Improved Connection Graph Generator
Hierarchical distance calculation, rule-driven generation, cross-type connections.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple

BASE_DIR = __import__("pathlib").Path(__file__).parent.parent
DB_PATH = BASE_DIR / "ontology" / "culture_ontology.db"

# ================================================================
# Hierarchy cache (built once from DB)
# ================================================================

def build_hierarchy(db: sqlite3.Connection, table: str) -> Dict[str, Optional[str]]:
    """Build code -> parent_code map from a *_values table."""
    m: Dict[str, Optional[str]] = {}
    try:
        for row in db.execute(f"SELECT code, parent_code FROM {table}"):
            m[row[0]] = row[1]
    except Exception:
        pass
    return m


def get_ancestors(code: str, hierarchy: Dict[str, Optional[str]]) -> List[str]:
    """Return list of ancestor codes from immediate parent to root."""
    ancestors = []
    current = hierarchy.get(code)
    visited = set()
    while current and current not in visited:
        ancestors.append(current)
        visited.add(current)
        current = hierarchy.get(current)
    return ancestors


# ================================================================
# Distance calculations
# ================================================================

def hierarchical_theme_distance(
    a_codes: Set[str], b_codes: Set[str],
    hierarchy: Dict[str, Optional[str]],
) -> float:
    """Theme distance using hierarchy. Closer in tree = lower distance."""
    if not a_codes or not b_codes:
        return 0.5

    # Direct overlap
    if a_codes & b_codes:
        return 0.0

    # Check shared parents/ancestors
    min_depth = 99
    for a in a_codes:
        a_ancs = get_ancestors(a, hierarchy)
        for b in b_codes:
            # b is ancestor of a?
            if b in a_ancs:
                min_depth = min(min_depth, a_ancs.index(b) + 1)
                continue
            b_ancs = get_ancestors(b, hierarchy)
            # a is ancestor of b?
            if a in b_ancs:
                min_depth = min(min_depth, b_ancs.index(a) + 1)
                continue
            # Shared ancestor?
            for i, anc in enumerate(a_ancs):
                if anc in b_ancs:
                    j = b_ancs.index(anc)
                    min_depth = min(min_depth, i + j + 2)
                    break

    if min_depth < 99:
        # depth 1 (siblings) = 0.2, depth 2 (cousins) = 0.4, depth 3 = 0.5, 4+ = 0.6
        return min(0.2 * min_depth, 0.6)

    # No shared hierarchy at all
    return 0.8


ERA_ORDER = [
    "ancient", "medieval", "edo_early", "edo_late",
    "meiji_taisho", "showa_prewar", "showa_postwar", "heisei", "reiwa",
]


def era_distance(a_eras: Set[str], b_eras: Set[str]) -> float:
    """Era distance based on ordinal gap. 0-8 steps normalized to 0-1."""
    if not a_eras or not b_eras:
        return 0.5
    era_idx = {e: i for i, e in enumerate(ERA_ORDER)}
    a_idx = min(era_idx.get(e, 4) for e in a_eras)
    b_idx = min(era_idx.get(e, 4) for e in b_eras)
    return abs(a_idx - b_idx) / 8.0


def jaccard_distance(a: Set[str], b: Set[str]) -> float:
    """Standard Jaccard distance."""
    if not a and not b:
        return 0.5
    if not a or not b:
        return 0.5
    inter = len(a & b)
    union = len(a | b)
    return 1.0 - (inter / union) if union else 0.5


def hierarchical_medium_distance(
    a_codes: Set[str], b_codes: Set[str],
    hierarchy: Dict[str, Optional[str]],
) -> float:
    """Medium distance with hierarchy awareness."""
    if not a_codes or not b_codes:
        return 0.5
    if a_codes & b_codes:
        return 0.0

    # Check for parent sharing
    for a in a_codes:
        a_parent = hierarchy.get(a)
        for b in b_codes:
            b_parent = hierarchy.get(b)
            if a_parent and b_parent and a_parent == b_parent:
                return 0.3  # Siblings
            if a_parent == b or b_parent == a:
                return 0.15  # Parent-child

    return 1.0  # Completely different


# ================================================================
# Connection evaluation
# ================================================================

def evaluate_connection(
    distances: Dict[str, float],
    a_type: str, b_type: str,
) -> Tuple[float, str, str]:
    """Evaluate connection quality. Returns (score, quality, rule_name)."""
    td = distances["theme"]
    ed = distances["era"]
    md = distances["medium"]
    gd = distances["geography"]
    xd = distances["experience"]

    cross_type = a_type != b_type

    # era_bridge: theme close, era far
    if td < 0.5 and ed > 0.3:
        score = 1.2 * (1.0 - td) * ed
        if cross_type:
            score *= 1.1
        return score, "good_surprise", "era_bridge"

    # medium_cross: theme close, medium far
    if td < 0.5 and md > 0.5:
        score = 1.1 * (1.0 - td) * md
        if cross_type:
            score *= 1.1
        return score, "good_surprise", "medium_cross"

    # geo_theme: theme close, geography different
    if td < 0.5 and gd > 0.3 and gd < 1.0:
        score = 1.0 * (1.0 - td) * gd
        if cross_type:
            score *= 1.1
        return score, "good_surprise", "geo_theme"

    # experience_shift: theme close, experience different
    if td < 0.5 and xd > 0.5:
        score = 0.9 * (1.0 - td) * xd
        return score, "good_surprise", "experience_shift"

    # cross_type bonus (person↔place, person↔work, work↔place)
    if cross_type and td < 0.5:
        score = 0.8 * (1.0 - td)
        rule = f"cross_type_{min(a_type, b_type)}_{max(a_type, b_type)}"
        return score, "moderate", rule

    # obvious: all close
    if td < 0.2 and ed < 0.2 and md < 0.2:
        return 0.15, "obvious", "obvious"

    # random: all far
    if td > 0.7 and ed > 0.7 and md > 0.7:
        return 0.05, "bad_surprise", "random"

    # Default
    if td < 0.5:
        return 0.3 * (1.0 - td), "moderate", "default"

    return 0.1, "weak", "default"


# ================================================================
# Explanation templates
# ================================================================

def resolve_name(db: sqlite3.Connection, axis: str, code: str) -> str:
    """Resolve axis value code to Japanese name."""
    table = f"{axis}_values"
    try:
        row = db.execute(f"SELECT name_ja FROM {table} WHERE code=?", (code,)).fetchone()
        return row[0] if row else code
    except Exception:
        return code


def generate_explanation(
    db: sqlite3.Connection,
    a_label: str, b_label: str,
    a_tags: Dict[str, Set[str]], b_tags: Dict[str, Set[str]],
    a_type: str, b_type: str,
    rule_name: str, shared_themes: Set[str],
) -> str:
    """Generate human-readable explanation."""
    # Theme names
    theme_names = []
    for code in list(shared_themes)[:2]:
        theme_names.append(resolve_name(db, "theme", code))
    theme_str = "・".join(theme_names) if theme_names else "共通のテーマ"

    def get_first(tags: Dict[str, Set[str]], axis: str) -> str:
        vals = tags.get(axis, set())
        if vals:
            return resolve_name(db, axis, next(iter(vals)))
        return ""

    a_era = get_first(a_tags, "era")
    b_era = get_first(b_tags, "era")
    a_med = get_first(a_tags, "medium")
    b_med = get_first(b_tags, "medium")
    a_geo = get_first(a_tags, "geography")
    b_geo = get_first(b_tags, "geography")
    a_exp = get_first(a_tags, "experience")
    b_exp = get_first(b_tags, "experience")

    if rule_name == "era_bridge":
        return (f"{a_label}（{a_era}）と{b_label}（{b_era}）——"
                f"「{theme_str}」というテーマが時代を超えて響き合う。")
    elif rule_name == "medium_cross":
        return (f"{a_label}（{a_med}）と{b_label}（{b_med}）——"
                f"「{theme_str}」が異なる表現形式で花開いた。")
    elif rule_name == "geo_theme":
        return (f"{a_label}（{a_geo}）と{b_label}（{b_geo}）——"
                f"「{theme_str}」が離れた土地で育まれた。")
    elif rule_name == "experience_shift":
        return (f"{a_label}（{a_exp}）と{b_label}（{b_exp}）——"
                f"「{theme_str}」を異なる体験モードで味わう。")
    elif rule_name.startswith("cross_type"):
        if "person" in rule_name and "place" in rule_name:
            return (f"{a_label}と{b_label}——"
                    f"創り手とその舞台が「{theme_str}」で結ばれる。")
        elif "person" in rule_name and "work" in rule_name:
            return (f"{a_label}の精神が{b_label}に宿る——"
                    f"「{theme_str}」という糸で繋がる創造の系譜。")
        else:
            return (f"{a_label}の世界と{b_label}——"
                    f"作品と場所が「{theme_str}」で交差する。")
    else:
        return f"{a_label}と{b_label}は「{theme_str}」で繋がる。"


# ================================================================
# Main connection builder
# ================================================================

def build_connections(db: sqlite3.Connection):
    print("=" * 60)
    print("Phase 4: Connection Graph Rebuild")
    print("=" * 60)

    # Build hierarchies
    theme_hier = build_hierarchy(db, "theme_values")
    medium_hier = build_hierarchy(db, "medium_values")
    print(f"  Theme hierarchy: {len(theme_hier)} entries")
    print(f"  Medium hierarchy: {len(medium_hier)} entries")

    # Load all entity info
    entities = {}
    for row in db.execute("SELECT id, label_ja, entity_type FROM entities"):
        entities[row[0]] = {"label": row[1], "type": row[2]}

    # Load all tags
    all_tags: Dict[int, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for row in db.execute("SELECT entity_id, axis, value_code FROM entity_tags"):
        all_tags[row[0]][row[1]].add(row[2])

    # Group entities by leaf-level and parent-level theme tags
    theme_groups: Dict[str, Set[int]] = defaultdict(set)
    for eid, tags in all_tags.items():
        for code in tags.get("theme", set()):
            theme_groups[code].add(eid)
            # Also add to parent groups
            for anc in get_ancestors(code, theme_hier):
                theme_groups[anc].add(eid)

    print(f"\n  Theme groups (including hierarchy): {len(theme_groups)}")
    for theme, eids in sorted(theme_groups.items(), key=lambda x: -len(x[1]))[:10]:
        print(f"    {theme}: {len(eids)} entities")

    # Clear existing connections
    db.execute("DELETE FROM connections")
    db.commit()

    connections_inserted = 0
    by_rule: Dict[str, int] = defaultdict(int)
    seen_pairs: Set[Tuple[int, int]] = set()
    per_entity_count: Dict[int, int] = defaultdict(int)
    MAX_PER_ENTITY = 15
    MAX_TOTAL = 800

    # Strategy: iterate theme groups, generate candidate pairs, score them
    # Process smaller/more specific groups first (better diversity)
    sorted_groups = sorted(theme_groups.items(), key=lambda x: len(x[1]))

    for theme_code, entity_ids in sorted_groups:
        if connections_inserted >= MAX_TOTAL:
            break

        eids = list(entity_ids)
        if len(eids) < 2:
            continue

        # Skip very large groups at leaf level to avoid O(n^2) explosion
        if len(eids) > 200:
            # Sample: take first 50 + random-ish selection
            import hashlib
            sampled = eids[:50]
            for e in eids[50:]:
                h = int(hashlib.md5(str(e).encode()).hexdigest()[:8], 16)
                if h % 4 == 0:  # 25% sample
                    sampled.append(e)
            eids = sampled[:100]

        for i, eid_a in enumerate(eids):
            if connections_inserted >= MAX_TOTAL:
                break
            if per_entity_count[eid_a] >= MAX_PER_ENTITY:
                continue

            for eid_b in eids[i + 1:]:
                if connections_inserted >= MAX_TOTAL:
                    break
                if per_entity_count[eid_b] >= MAX_PER_ENTITY:
                    continue

                pair = (min(eid_a, eid_b), max(eid_a, eid_b))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                a_tags = all_tags[eid_a]
                b_tags = all_tags[eid_b]
                a_info = entities[eid_a]
                b_info = entities[eid_b]

                # Calculate distances
                distances = {
                    "theme": hierarchical_theme_distance(
                        a_tags.get("theme", set()),
                        b_tags.get("theme", set()),
                        theme_hier,
                    ),
                    "era": era_distance(
                        a_tags.get("era", set()),
                        b_tags.get("era", set()),
                    ),
                    "medium": hierarchical_medium_distance(
                        a_tags.get("medium", set()),
                        b_tags.get("medium", set()),
                        medium_hier,
                    ),
                    "geography": jaccard_distance(
                        a_tags.get("geography", set()),
                        b_tags.get("geography", set()),
                    ),
                    "experience": jaccard_distance(
                        a_tags.get("experience", set()),
                        b_tags.get("experience", set()),
                    ),
                }

                score, quality, rule_name = evaluate_connection(
                    distances, a_info["type"], b_info["type"],
                )

                if score < 0.25:
                    continue

                # Skip obvious same-type same-everything
                if quality == "obvious":
                    continue

                # Find shared themes for explanation
                shared = a_tags.get("theme", set()) & b_tags.get("theme", set())
                if not shared:
                    # Use the theme group that brought them together
                    shared = {theme_code}

                explanation = generate_explanation(
                    db,
                    a_info["label"], b_info["label"],
                    a_tags, b_tags,
                    a_info["type"], b_info["type"],
                    rule_name, shared,
                )

                db.execute(
                    """INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type,
                     theme_distance, era_distance, medium_distance,
                     geography_distance, experience_distance,
                     serendipity_score, explanation, source, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        eid_a, eid_b, rule_name,
                        distances["theme"], distances["era"], distances["medium"],
                        distances["geography"], distances["experience"],
                        score, explanation, "phase4_auto", min(score, 1.0),
                    ),
                )
                connections_inserted += 1
                by_rule[rule_name] += 1
                per_entity_count[eid_a] += 1
                per_entity_count[eid_b] += 1

        if connections_inserted % 100 == 0 and connections_inserted > 0:
            db.commit()
            print(f"    ... {connections_inserted} connections")

    db.commit()

    # ================================================================
    # Phase 2: Targeted cross-type connections
    # ================================================================
    print("\n  Phase 2: Cross-type targeted connections")

    # person ↔ place with shared geography
    person_ids = [eid for eid, info in entities.items() if info["type"] == "person"]
    place_ids = [eid for eid, info in entities.items() if info["type"] == "place"]
    work_ids = [eid for eid, info in entities.items() if info["type"] == "work"]

    # person ↔ place: same geography
    geo_groups: Dict[str, List[int]] = defaultdict(list)
    for eid in person_ids + place_ids:
        for geo in all_tags[eid].get("geography", set()):
            geo_groups[geo].append(eid)

    cross_pp = 0
    for geo, eids_in_geo in geo_groups.items():
        persons_here = [e for e in eids_in_geo if entities[e]["type"] == "person"]
        places_here = [e for e in eids_in_geo if entities[e]["type"] == "place"]

        for p in persons_here[:20]:
            if per_entity_count[p] >= MAX_PER_ENTITY:
                continue
            for pl in places_here[:10]:
                if per_entity_count[pl] >= MAX_PER_ENTITY:
                    continue
                pair = (min(p, pl), max(p, pl))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                a_tags_p = all_tags[p]
                b_tags_pl = all_tags[pl]

                distances = {
                    "theme": hierarchical_theme_distance(
                        a_tags_p.get("theme", set()),
                        b_tags_pl.get("theme", set()),
                        theme_hier,
                    ),
                    "era": era_distance(
                        a_tags_p.get("era", set()),
                        b_tags_pl.get("era", set()),
                    ),
                    "medium": hierarchical_medium_distance(
                        a_tags_p.get("medium", set()),
                        b_tags_pl.get("medium", set()),
                        medium_hier,
                    ),
                    "geography": 0.0,  # Same geography (that's why they're paired)
                    "experience": jaccard_distance(
                        a_tags_p.get("experience", set()),
                        b_tags_pl.get("experience", set()),
                    ),
                }

                # Cross-type person↔place with same geo
                td = distances["theme"]
                score = 0.6 * (1.0 - td) if td < 0.7 else 0.2
                rule_name = "cross_type_person_place"

                shared = a_tags_p.get("theme", set()) & b_tags_pl.get("theme", set())
                if not shared:
                    shared = a_tags_p.get("theme", set()) or b_tags_pl.get("theme", set())
                    if not shared:
                        shared = {"craft_mastery"}

                explanation = generate_explanation(
                    db,
                    entities[p]["label"], entities[pl]["label"],
                    a_tags_p, b_tags_pl,
                    "person", "place",
                    rule_name, shared,
                )

                db.execute(
                    """INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type,
                     theme_distance, era_distance, medium_distance,
                     geography_distance, experience_distance,
                     serendipity_score, explanation, source, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        p, pl, rule_name,
                        distances["theme"], distances["era"], distances["medium"],
                        distances["geography"], distances["experience"],
                        score, explanation, "phase4_cross", min(score, 1.0),
                    ),
                )
                connections_inserted += 1
                cross_pp += 1
                by_rule[rule_name] += 1
                per_entity_count[p] += 1
                per_entity_count[pl] += 1

                if cross_pp >= 80:
                    break
            if cross_pp >= 80:
                break
        if cross_pp >= 80:
            break

    print(f"    person↔place: {cross_pp}")

    # person ↔ work: shared theme
    cross_pw = 0
    for theme_code, eids_in_theme in theme_groups.items():
        persons_t = [e for e in eids_in_theme if entities.get(e, {}).get("type") == "person"]
        works_t = [e for e in eids_in_theme if entities.get(e, {}).get("type") == "work"]

        if not persons_t or not works_t:
            continue

        for p in persons_t[:10]:
            if per_entity_count[p] >= MAX_PER_ENTITY:
                continue
            for w in works_t[:5]:
                if per_entity_count[w] >= MAX_PER_ENTITY:
                    continue
                pair = (min(p, w), max(p, w))
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                a_tags_p = all_tags[p]
                b_tags_w = all_tags[w]

                distances = {
                    "theme": hierarchical_theme_distance(
                        a_tags_p.get("theme", set()),
                        b_tags_w.get("theme", set()),
                        theme_hier,
                    ),
                    "era": era_distance(
                        a_tags_p.get("era", set()),
                        b_tags_w.get("era", set()),
                    ),
                    "medium": hierarchical_medium_distance(
                        a_tags_p.get("medium", set()),
                        b_tags_w.get("medium", set()),
                        medium_hier,
                    ),
                    "geography": jaccard_distance(
                        a_tags_p.get("geography", set()),
                        b_tags_w.get("geography", set()),
                    ),
                    "experience": jaccard_distance(
                        a_tags_p.get("experience", set()),
                        b_tags_w.get("experience", set()),
                    ),
                }

                score, quality, rule = evaluate_connection(
                    distances, "person", "work",
                )

                if score < 0.2:
                    continue

                shared = a_tags_p.get("theme", set()) & b_tags_w.get("theme", set())
                if not shared:
                    shared = {theme_code}

                explanation = generate_explanation(
                    db,
                    entities[p]["label"], entities[w]["label"],
                    a_tags_p, b_tags_w,
                    "person", "work",
                    rule, shared,
                )

                db.execute(
                    """INSERT INTO connections
                    (entity_a_id, entity_b_id, connection_type,
                     theme_distance, era_distance, medium_distance,
                     geography_distance, experience_distance,
                     serendipity_score, explanation, source, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        p, w, rule,
                        distances["theme"], distances["era"], distances["medium"],
                        distances["geography"], distances["experience"],
                        score, explanation, "phase4_cross", min(score, 1.0),
                    ),
                )
                connections_inserted += 1
                cross_pw += 1
                by_rule[rule] += 1
                per_entity_count[p] += 1
                per_entity_count[w] += 1

                if cross_pw >= 80:
                    break
            if cross_pw >= 80:
                break
        if cross_pw >= 80:
            break

    print(f"    person↔work: {cross_pw}")

    db.commit()

    # Summary
    print(f"\n  === Results ===")
    print(f"  Total connections: {connections_inserted}")
    print(f"  By rule:")
    for rule, count in sorted(by_rule.items(), key=lambda x: -x[1]):
        print(f"    {rule}: {count}")

    # Quality distribution
    for row in db.execute("""
        SELECT
          CASE
            WHEN serendipity_score >= 0.6 THEN 'high (>=0.6)'
            WHEN serendipity_score >= 0.3 THEN 'medium (0.3-0.6)'
            ELSE 'low (<0.3)'
          END as tier, COUNT(*)
        FROM connections
        GROUP BY tier
        ORDER BY tier
    """):
        print(f"    {row[0]}: {row[1]}")

    # Top connections
    print(f"\n  Top 10 connections:")
    for row in db.execute("""
        SELECT c.serendipity_score, c.connection_type, c.explanation,
               a.label_ja, a.entity_type, b.label_ja, b.entity_type
        FROM connections c
        JOIN entities a ON c.entity_a_id = a.id
        JOIN entities b ON c.entity_b_id = b.id
        ORDER BY c.serendipity_score DESC
        LIMIT 10
    """):
        print(f"    [{row[0]:.2f}] {row[1]}: {row[3]}({row[4]}) <-> {row[5]}({row[6]})")
        print(f"           {row[2][:80]}")


def main():
    db = sqlite3.connect(str(DB_PATH))
    build_connections(db)
    db.close()


if __name__ == "__main__":
    main()
