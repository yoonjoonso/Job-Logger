#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "job-log.db"


GROUP_DEFINITIONS = {
    "programming": {"label": "Programming", "group_rank": 10},
    "engines": {"label": "Engines", "group_rank": 20},
    "backend-cloud": {"label": "Backend & Cloud", "group_rank": 30},
    "networking": {"label": "Networking", "group_rank": 40},
    "security": {"label": "Security", "group_rank": 50},
    "frontend": {"label": "Frontend", "group_rank": 60},
    "data": {"label": "Data", "group_rank": 65},
    "xr-platforms": {"label": "Platforms", "group_rank": 70},
    "tools": {"label": "Tools", "group_rank": 80},
    "spoken_languages": {"label": "Languages", "group_rank": 90},
}


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS skill_resume_rules (
          skill_normalized  TEXT REFERENCES skills_mine(skill_normalized) ON DELETE CASCADE,
          profile_key       TEXT,
          render_group      TEXT,
          group_rank        INTEGER DEFAULT 0,
          item_rank         INTEGER DEFAULT 0,
          visibility        TEXT DEFAULT 'show',
          emphasis          TEXT DEFAULT 'plain',
          direct_match_boost INTEGER DEFAULT 0,
          singleton_penalty INTEGER DEFAULT 0,
          trigger_condition TEXT,
          notes             TEXT,
          approved          INTEGER DEFAULT 1,
          PRIMARY KEY (skill_normalized, profile_key)
        );

        CREATE TABLE IF NOT EXISTS resume_group_rules (
          profile_key             TEXT,
          group_id                TEXT,
          label                   TEXT,
          group_rank              INTEGER DEFAULT 0,
          min_items_standalone    INTEGER DEFAULT 1,
          singleton_merge_target  TEXT,
          max_items               INTEGER DEFAULT 8,
          approved                INTEGER DEFAULT 1,
          PRIMARY KEY (profile_key, group_id)
        );
        """
    )


def default_render_group(category: str) -> str:
    normalized = (category or "").strip().lower()
    if normalized == "language":
        return "programming"
    if normalized == "engine":
        return "engines"
    if normalized in {"backend", "cloud"}:
        return "backend-cloud"
    if normalized == "networking":
        return "networking"
    if normalized == "platform":
        return "xr-platforms"
    if normalized == "security":
        return "security"
    if normalized == "frontend":
        return "frontend"
    if normalized == "data":
        return "data"
    if normalized == "spoken-language":
        return "spoken_languages"
    return "tools"


def sync_resume_policy(db_path: Path) -> int:
    inserted = 0
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as connection:
        connection.row_factory = sqlite3.Row
        ensure_schema(connection)
        tables = {
            row["name"]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
        }
        if "resume_profiles" not in tables or "skills_mine" not in tables:
            return 0

        profiles = connection.execute(
            "SELECT profile_key FROM resume_profiles WHERE approved = 1 ORDER BY profile_key"
        ).fetchall()
        skills = connection.execute(
            """
            SELECT skill_name, skill_normalized, category, include_default, require_direct_match,
                   resume_visibility, resume_emphasis, resume_priority, resume_group_rank
            FROM skills_mine
            ORDER BY skill_name
            """
        ).fetchall()

        for profile in profiles:
            profile_key = profile["profile_key"]
            for group_id, group in GROUP_DEFINITIONS.items():
                existing = connection.execute(
                    "SELECT 1 FROM resume_group_rules WHERE profile_key = ? AND group_id = ? LIMIT 1",
                    (profile_key, group_id),
                ).fetchone()
                if existing:
                    continue
                connection.execute(
                    """
                    INSERT INTO resume_group_rules (
                      profile_key, group_id, label, group_rank, min_items_standalone, singleton_merge_target, max_items, approved
                    ) VALUES (?, ?, ?, ?, 1, NULL, 8, 1)
                    """,
                    (profile_key, group_id, group["label"], group["group_rank"]),
                )
                inserted += 1

        for profile in profiles:
            profile_key = profile["profile_key"]
            for skill in skills:
                existing = connection.execute(
                    "SELECT 1 FROM skill_resume_rules WHERE skill_normalized = ? AND profile_key = ? LIMIT 1",
                    (skill["skill_normalized"], profile_key),
                ).fetchone()
                if existing:
                    continue
                render_group = default_render_group(skill["category"] or "")
                visibility = skill["resume_visibility"] or ("context" if int(skill["require_direct_match"] or 0) else "show")
                emphasis = skill["resume_emphasis"] or "plain"
                direct_match_boost = 6 if int(skill["require_direct_match"] or 0) else 0
                singleton_penalty = 0 if visibility == "show" else 3 if visibility == "context" else 8
                group_rank = int(skill["resume_group_rank"] or GROUP_DEFINITIONS.get(render_group, {}).get("group_rank", 80))
                item_rank = int(skill["resume_priority"] or 0)
                notes = "DB-generated generic resume display rule."
                connection.execute(
                    """
                    INSERT INTO skill_resume_rules (
                      skill_normalized, profile_key, render_group, group_rank, item_rank, visibility, emphasis,
                      direct_match_boost, singleton_penalty, trigger_condition, notes, approved
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 1)
                    """,
                    (
                        skill["skill_normalized"],
                        profile_key,
                        render_group,
                        group_rank,
                        item_rank,
                        visibility,
                        emphasis,
                        direct_match_boost,
                        singleton_penalty,
                        notes,
                    ),
                )
                inserted += 1
        connection.commit()
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate generic resume group/skill rules from DB profiles and skills.")
    parser.add_argument("--db", default=str(DB_PATH))
    args = parser.parse_args()

    inserted = sync_resume_policy(Path(args.db))
    print(f"Synced resume policy into DB. Inserted {inserted} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
