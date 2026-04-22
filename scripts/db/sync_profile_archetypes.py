#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from policy_config import display_label, extract_profile_archetypes, normalize_key

PROFILE_PATH = ROOT / "config" / "profile.yml"
SIGNAL_RULES_PATH = ROOT / "config" / "resume-signal-rules.yml"
DB_PATH = ROOT / "data" / "job-log.db"
TEMPLATE_DIR = ROOT / "config" / "resume-templates"


def parse_scalar(raw: str):
    value = raw.strip()
    if not value:
        return ""
    if value.startswith(("'", '"')) and value.endswith(("'", '"')) and len(value) >= 2:
        return value[1:-1]
    if value in {"true", "True"}:
        return True
    if value in {"false", "False"}:
        return False
    if re.fullmatch(r"-?\d+", value):
        return int(value)
    if re.fullmatch(r"-?\d+\.\d+", value):
        return float(value)
    return value


def choose_template_key(archetype_key: str) -> str:
    direct = TEMPLATE_DIR / f"{archetype_key}-template.docx"
    if direct.exists():
        return archetype_key
    fallback = TEMPLATE_DIR / "general-template.docx"
    return "general" if fallback.exists() else archetype_key


def generic_summary(archetype: dict[str, object]) -> str:
    label = display_label(str(archetype.get("name") or "Software"))
    notes = str(archetype.get("notes") or "").strip()
    if notes:
        return f"Software Engineer targeting {label} roles. {notes}"
    return f"Software Engineer targeting {label} roles."


def generic_cover_letter(archetype: dict[str, object]) -> dict[str, str | None]:
    label = display_label(str(archetype.get("name") or "software"))
    notes = str(archetype.get("notes") or "").strip()
    body = "I can speak concretely to my relevant experience and how it maps to the role."
    if notes:
        body = notes
    return {
        "opening": f"I'm applying for the {{ROLE}} role{{TEAM_SUFFIX}}. My background is strongest in {label.lower()} work and adjacent production systems.",
        "body_p1": body,
        "body_p2": None,
        "closing": "I'd welcome the chance to discuss the role further. Thank you for your time.",
    }


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS resume_profiles (
          id          INTEGER PRIMARY KEY,
          profile_key TEXT UNIQUE,
          subtitle    TEXT,
          summary     TEXT,
          approved    INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS archetypes (
          id    INTEGER PRIMARY KEY,
          name  TEXT UNIQUE,
          evals INTEGER DEFAULT 0,
          notes TEXT
        );

        CREATE TABLE IF NOT EXISTS resume_profile_signal_rules (
          id                 INTEGER PRIMARY KEY,
          profile_key        TEXT NOT NULL REFERENCES resume_profiles(profile_key) ON DELETE CASCADE,
          signal_key         TEXT NOT NULL,
          operator           TEXT NOT NULL CHECK(operator IN ('gte','lte','eq','present','absent')),
          threshold_numeric  REAL,
          threshold_text     TEXT,
          weight             INTEGER DEFAULT 0,
          action             TEXT NOT NULL DEFAULT 'score' CHECK(action IN ('score','gate','penalty')),
          approved           INTEGER DEFAULT 1,
          notes              TEXT
        );

        CREATE TABLE IF NOT EXISTS archetype_resume_configs (
          id             TEXT PRIMARY KEY,
          archetype_key  TEXT NOT NULL UNIQUE,
          label          TEXT NOT NULL,
          template_key   TEXT,
          subtitle       TEXT,
          summary        TEXT,
          approved       INTEGER NOT NULL DEFAULT 1,
          caution_rules  TEXT,
          experience     TEXT,
          skills         TEXT,
          education      TEXT,
          certifications TEXT
        );

        CREATE TABLE IF NOT EXISTS resume_generation_settings (
          key   TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cover_letter_profiles (
          archetype      TEXT PRIMARY KEY,
          base_archetype TEXT,
          opening        TEXT,
          body_p1        TEXT,
          body_p2        TEXT,
          closing        TEXT
        );

        CREATE TABLE IF NOT EXISTS resume_layouts (
          id                           INTEGER PRIMARY KEY,
          layout_key                   TEXT UNIQUE NOT NULL,
          layout_family                TEXT NOT NULL DEFAULT 'grouped_standard',
          label                        TEXT NOT NULL,
          page_count                   INTEGER NOT NULL DEFAULT 1,
          max_roles                    INTEGER NOT NULL DEFAULT 4,
          max_projects                 INTEGER NOT NULL DEFAULT 7,
          max_bullets_total            INTEGER NOT NULL DEFAULT 8,
          max_bullets_per_role         INTEGER,
          max_bullets_per_project      INTEGER,
          max_direct_bullets_per_role  INTEGER DEFAULT 0,
          skill_group_char_limit       INTEGER,
          max_education_items          INTEGER DEFAULT 2,
          max_certification_items      INTEGER DEFAULT 4,
          certification_policy         TEXT NOT NULL DEFAULT 'contextual',
          section_order                TEXT,
          min_role_score               INTEGER DEFAULT 0,
          min_project_score            INTEGER DEFAULT 0,
          min_bullet_score             INTEGER DEFAULT 0,
          allow_low_match_context      INTEGER DEFAULT 0,
          approved                     INTEGER DEFAULT 1,
          notes                        TEXT
        );

        CREATE TABLE IF NOT EXISTS resume_layout_profile_rules (
          profile_key  TEXT NOT NULL REFERENCES resume_profiles(profile_key) ON DELETE CASCADE,
          layout_key   TEXT NOT NULL REFERENCES resume_layouts(layout_key) ON DELETE CASCADE,
          priority     INTEGER DEFAULT 0,
          is_default   INTEGER DEFAULT 0,
          approved     INTEGER DEFAULT 1,
          notes        TEXT,
          PRIMARY KEY (profile_key, layout_key)
        );

        CREATE TABLE IF NOT EXISTS resume_layout_signal_rules (
          id                 INTEGER PRIMARY KEY,
          layout_key         TEXT NOT NULL REFERENCES resume_layouts(layout_key) ON DELETE CASCADE,
          signal_key         TEXT NOT NULL,
          operator           TEXT NOT NULL CHECK(operator IN ('gte','lte','eq','present','absent')),
          threshold_numeric  REAL,
          threshold_text     TEXT,
          weight             INTEGER DEFAULT 0,
          action             TEXT NOT NULL DEFAULT 'score' CHECK(action IN ('score','gate','penalty')),
          approved           INTEGER DEFAULT 1,
          notes              TEXT
        );
        """
    )


def row_exists(connection: sqlite3.Connection, table: str, where: str, params: tuple[object, ...]) -> bool:
    query = f"SELECT 1 FROM {table} WHERE {where} LIMIT 1"
    return connection.execute(query, params).fetchone() is not None


def ensure_generic_layout_rows(connection: sqlite3.Connection) -> int:
    layouts = [
        {
            "layout_key": "standard-1p",
            "layout_family": "grouped_standard",
            "label": "Standard One Page",
            "page_count": 1,
            "max_roles": 3,
            "max_projects": 5,
            "max_bullets_total": 8,
            "max_bullets_per_role": 4,
            "max_bullets_per_project": 3,
            "max_direct_bullets_per_role": 1,
            "skill_group_char_limit": 78,
            "max_education_items": 2,
            "max_certification_items": 2,
            "certification_policy": "contextual",
            "section_order": "summary,skills,experience,education,certifications",
            "min_role_score": 0,
            "min_project_score": 0,
            "min_bullet_score": 0,
            "allow_low_match_context": 0,
            "approved": 1,
            "notes": "Default compact one-page layout budget.",
        },
        {
            "layout_key": "standard-2p",
            "layout_family": "grouped_standard",
            "label": "Standard Two Page",
            "page_count": 2,
            "max_roles": 4,
            "max_projects": 8,
            "max_bullets_total": 13,
            "max_bullets_per_role": 5,
            "max_bullets_per_project": 3,
            "max_direct_bullets_per_role": 2,
            "skill_group_char_limit": 90,
            "max_education_items": 2,
            "max_certification_items": 3,
            "certification_policy": "contextual",
            "section_order": "summary,skills,experience,education,certifications",
            "min_role_score": 0,
            "min_project_score": 0,
            "min_bullet_score": 0,
            "allow_low_match_context": 0,
            "approved": 1,
            "notes": "Expanded layout for stronger multi-project fits.",
        },
        {
            "layout_key": "dense-1p",
            "layout_family": "grouped_standard",
            "label": "Dense One Page",
            "page_count": 1,
            "max_roles": 2,
            "max_projects": 4,
            "max_bullets_total": 7,
            "max_bullets_per_role": 4,
            "max_bullets_per_project": 2,
            "max_direct_bullets_per_role": 1,
            "skill_group_char_limit": 66,
            "max_education_items": 1,
            "max_certification_items": 2,
            "certification_policy": "contextual",
            "section_order": "summary,skills,experience,education,certifications",
            "min_role_score": 8,
            "min_project_score": 6,
            "min_bullet_score": 4,
            "allow_low_match_context": 0,
            "approved": 1,
            "notes": "Aggressive one-page compression for crowded profiles.",
        },
        {
            "layout_key": "adjacent-1p",
            "layout_family": "grouped_standard",
            "label": "Adjacent One Page",
            "page_count": 1,
            "max_roles": 2,
            "max_projects": 4,
            "max_bullets_total": 6,
            "max_bullets_per_role": 3,
            "max_bullets_per_project": 2,
            "max_direct_bullets_per_role": 1,
            "skill_group_char_limit": 72,
            "max_education_items": 2,
            "max_certification_items": 2,
            "certification_policy": "contextual",
            "section_order": "summary,skills,experience,education,certifications",
            "min_role_score": 10,
            "min_project_score": 8,
            "min_bullet_score": 6,
            "allow_low_match_context": 0,
            "approved": 1,
            "notes": "For adjacent and lower-match roles.",
        },
        {
            "layout_key": "top-certs-1p",
            "layout_family": "grouped_top_certs",
            "label": "Top Certs One Page",
            "page_count": 1,
            "max_roles": 2,
            "max_projects": 4,
            "max_bullets_total": 7,
            "max_bullets_per_role": 3,
            "max_bullets_per_project": 2,
            "max_direct_bullets_per_role": 1,
            "skill_group_char_limit": 72,
            "max_education_items": 1,
            "max_certification_items": 2,
            "certification_policy": "top_relevant",
            "section_order": "summary,skills,certifications,experience,education",
            "min_role_score": 8,
            "min_project_score": 6,
            "min_bullet_score": 4,
            "allow_low_match_context": 0,
            "approved": 1,
            "notes": "Grouped layout that surfaces top relevant certifications earlier.",
        },
        {
            "layout_key": "top-certs-2p",
            "layout_family": "grouped_top_certs",
            "label": "Top Certs Two Page",
            "page_count": 2,
            "max_roles": 3,
            "max_projects": 6,
            "max_bullets_total": 10,
            "max_bullets_per_role": 4,
            "max_bullets_per_project": 2,
            "max_direct_bullets_per_role": 1,
            "skill_group_char_limit": 84,
            "max_education_items": 2,
            "max_certification_items": 3,
            "certification_policy": "top_relevant",
            "section_order": "summary,skills,certifications,experience,education",
            "min_role_score": 6,
            "min_project_score": 4,
            "min_bullet_score": 3,
            "allow_low_match_context": 0,
            "approved": 1,
            "notes": "Expanded grouped layout that surfaces top relevant certifications earlier.",
        },
    ]
    inserted = 0
    for layout in layouts:
        if row_exists(connection, "resume_layouts", "layout_key = ?", (layout["layout_key"],)):
            continue
        connection.execute(
            """
            INSERT INTO resume_layouts (
              layout_key, layout_family, label, page_count, max_roles, max_projects, max_bullets_total,
              max_bullets_per_role, max_bullets_per_project, max_direct_bullets_per_role, skill_group_char_limit,
              max_education_items, max_certification_items, certification_policy, section_order,
              min_role_score, min_project_score, min_bullet_score, allow_low_match_context, approved, notes
            ) VALUES (
              @layout_key, @layout_family, @label, @page_count, @max_roles, @max_projects, @max_bullets_total,
              @max_bullets_per_role, @max_bullets_per_project, @max_direct_bullets_per_role, @skill_group_char_limit,
              @max_education_items, @max_certification_items, @certification_policy, @section_order,
              @min_role_score, @min_project_score, @min_bullet_score, @allow_low_match_context, @approved, @notes
            )
            """,
            layout,
        )
        inserted += 1
    return inserted


def ensure_layout_profile_rules(connection: sqlite3.Connection, profile_key: str) -> int:
    rules = [
        ("standard-1p", 100, 1, "Primary default layout."),
        ("standard-2p", 80, 0, "Allowed expanded layout."),
        ("dense-1p", 70, 0, "Compact layout option."),
        ("adjacent-1p", 60, 0, "Use when fit is adjacent rather than direct."),
        ("top-certs-1p", 65, 0, "Cert-forward layout when requirements justify it."),
        ("top-certs-2p", 55, 0, "Expanded cert-forward layout when requirements justify it."),
    ]
    inserted = 0
    for layout_key, priority, is_default, notes in rules:
        if not row_exists(connection, "resume_layouts", "layout_key = ?", (layout_key,)):
            continue
        if row_exists(connection, "resume_layout_profile_rules", "profile_key = ? AND layout_key = ?", (profile_key, layout_key)):
            continue
        connection.execute(
            """
            INSERT INTO resume_layout_profile_rules (profile_key, layout_key, priority, is_default, approved, notes)
            VALUES (?, ?, ?, ?, 1, ?)
            """,
            (profile_key, layout_key, priority, is_default, notes),
        )
        inserted += 1
    return inserted


def sync_archetypes(profile_path: Path, db_path: Path) -> int:
    archetypes = extract_profile_archetypes(profile_path)
    if not archetypes:
        raise SystemExit(f"No archetypes found in {profile_path}")

    defaults = {
        "require_approved": True,
        "max_role_count": 4,
        "max_bullets_per_project": 5,
        "max_skill_groups": 5,
        "include_education": True,
        "include_certifications": True,
        "caution_rules": [],
    }

    inserted = 0
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as connection:
        ensure_schema(connection)
        inserted += ensure_generic_layout_rows(connection)
        connection.execute(
            "INSERT OR IGNORE INTO resume_generation_settings (key, value) VALUES (?, ?)",
            ("defaults", json.dumps(defaults)),
        )

        for archetype in archetypes:
            name = str(archetype.get("name") or "").strip()
            if not name:
                continue
            key = normalize_key(name)
            label = display_label(name)
            summary = generic_summary(archetype)
            template_key = choose_template_key(key)
            config_id = key
            connection.execute(
                "INSERT OR IGNORE INTO archetypes (name, evals, notes) VALUES (?, 0, ?)",
                (label, str(archetype.get("notes") or "").strip() or None),
            )
            generic_config = {
                "experience": {"include_roles": [], "include_projects": [], "prefer_tags": [], "pinned_bullets": []},
                "skills": {
                    "include_groups": [
                        "programming",
                        "engines",
                        "backend-cloud",
                        "networking",
                        "security",
                        "frontend",
                        "tools",
                        "xr-platforms",
                    ],
                    "pinned_items": [],
                },
                "education": {"include_ids": []},
                "certifications": {"include_ids": []},
            }

            if not row_exists(connection, "resume_profiles", "profile_key = ?", (key,)):
                connection.execute(
                    "INSERT INTO resume_profiles (profile_key, subtitle, summary, approved) VALUES (?, ?, ?, 1)",
                    (key, "Software Engineer", summary),
                )
                inserted += 1
            inserted += ensure_layout_profile_rules(connection, key)

            if not row_exists(connection, "resume_profile_signal_rules", "profile_key = ? AND signal_key = ?", (key, f"archetype_{key}")):
                connection.execute(
                    """
                    INSERT INTO resume_profile_signal_rules
                      (profile_key, signal_key, operator, threshold_numeric, threshold_text, weight, action, approved, notes)
                    VALUES (?, ?, 'gte', 1, NULL, 70, 'score', 1, ?)
                    """,
                    (key, f"archetype_{key}", f"Primary DB-generated profile for {label}."),
                )
                inserted += 1

            if not row_exists(connection, "archetype_resume_configs", "archetype_key = ?", (key,)):
                connection.execute(
                    """
                    INSERT INTO archetype_resume_configs
                      (id, archetype_key, label, template_key, subtitle, summary, approved, caution_rules, experience, skills, education, certifications)
                    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
                    """,
                    (
                        config_id,
                        key,
                        label,
                        template_key,
                        "Software Engineer",
                        summary,
                        json.dumps([]),
                        json.dumps(generic_config["experience"]),
                        json.dumps(generic_config["skills"]),
                        json.dumps(generic_config["education"]),
                        json.dumps(generic_config["certifications"]),
                    ),
                )
                inserted += 1

            if not row_exists(connection, "cover_letter_profiles", "archetype = ?", (key,)):
                letter = generic_cover_letter(archetype)
                base_archetype = "general" if key != "general" and row_exists(connection, "cover_letter_profiles", "archetype = ?", ("general",)) else None
                connection.execute(
                    """
                    INSERT INTO cover_letter_profiles (archetype, base_archetype, opening, body_p1, body_p2, closing)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (key, base_archetype, letter["opening"], letter["body_p1"], letter["body_p2"], letter["closing"]),
                )
                inserted += 1

        connection.commit()
    return inserted


def parse_signal_rules_yaml(path: Path) -> list[dict[str, object]]:
    """Parse a simple YAML list of signal rule objects (no PyYAML required)."""
    lines = path.read_text(encoding="utf-8").splitlines()
    in_rules = False
    current: dict[str, object] | None = None
    rows: list[dict[str, object]] = []

    for raw_line in lines:
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))

        if not in_rules:
            if stripped == "rules:":
                in_rules = True
            continue

        if indent == 0 and not stripped.startswith("-"):
            break  # left the rules block

        if stripped.startswith("- "):
            if current:
                rows.append(current)
            current = {}
            rest = stripped[2:].strip()
            if rest and ":" in rest:
                key, _, value = rest.partition(":")
                current[key.strip()] = parse_scalar(value)
            continue

        if current and ":" in stripped:
            key, _, value = stripped.partition(":")
            current[key.strip()] = parse_scalar(value)

    if current:
        rows.append(current)

    return [r for r in rows if r.get("profile_key") and r.get("signal_key")]


def sync_signal_rules(rules_path: Path, db_path: Path) -> int:
    if not rules_path.exists():
        return 0

    rules = parse_signal_rules_yaml(rules_path)
    if not rules:
        return 0

    inserted = 0
    with sqlite3.connect(db_path) as connection:
        ensure_schema(connection)
        for rule in rules:
            profile_key = str(rule.get("profile_key") or "").strip()
            signal_key = str(rule.get("signal_key") or "").strip()
            if not profile_key or not signal_key:
                continue
            if row_exists(connection, "resume_profile_signal_rules", "profile_key = ? AND signal_key = ?", (profile_key, signal_key)):
                continue
            operator = str(rule.get("operator") or "gte").strip()
            threshold_numeric = rule.get("threshold_numeric")
            threshold_text = rule.get("threshold_text") or None
            weight = int(rule.get("weight") or 0)
            action = str(rule.get("action") or "score").strip()
            notes = str(rule.get("notes") or "").strip() or None
            connection.execute(
                """
                INSERT INTO resume_profile_signal_rules
                  (profile_key, signal_key, operator, threshold_numeric, threshold_text, weight, action, approved, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (profile_key, signal_key, operator, threshold_numeric, threshold_text, weight, action, notes),
            )
            inserted += 1
        connection.commit()
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync archetypes from config/profile.yml into the local DB without overwriting existing rows.")
    parser.add_argument("--profile", default=str(PROFILE_PATH))
    parser.add_argument("--signal-rules", default=str(SIGNAL_RULES_PATH))
    parser.add_argument("--db", default=str(DB_PATH))
    args = parser.parse_args()

    inserted = sync_archetypes(Path(args.profile), Path(args.db))
    rules_inserted = sync_signal_rules(Path(args.signal_rules), Path(args.db))
    total = inserted + rules_inserted
    print(f"Synced profile into DB. Inserted {inserted} archetype rows, {rules_inserted} signal rules ({total} total).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
