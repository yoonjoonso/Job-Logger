import importlib.util
import sqlite3
from pathlib import Path


def load_module(name: str, relative: str):
    script_path = Path(__file__).resolve().parent.parent / relative
    spec = importlib.util.spec_from_file_location(name, script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_sync_job_intake_inserts_configured_rules(tmp_path: Path) -> None:
    sync_module = load_module("sync_job_intake", "scripts/db/sync_job_intake.py")
    db_path = tmp_path / "job-log.db"
    rules_path = tmp_path / "job-intake-rules.yml"
    rules_path.write_text(
        """
archetypes:
  - key: cpp
    keywords:
      - keyword: c++
        weight: 8
      - keyword: embedded
        weight: 7

candidate_keywords:
  - c++
  - embedded

stopwords:
  - term: the
    kind: stopword
""".strip(),
        encoding="utf-8",
    )

    inserted = sync_module.sync_job_intake(rules_path, db_path)

    assert inserted == 5
    with sqlite3.connect(db_path) as connection:
        archetype_rows = connection.execute(
            "SELECT archetype_key, keyword_text, keyword_normalized, weight FROM job_archetype_rules ORDER BY keyword_text"
        ).fetchall()
        candidate_rows = connection.execute(
            "SELECT keyword_text, keyword_normalized FROM job_keyword_candidates ORDER BY keyword_text"
        ).fetchall()
        stopword_rows = connection.execute(
            "SELECT term_text, term_normalized, kind FROM job_keyword_stopwords"
        ).fetchall()

    assert archetype_rows == [("cpp", "c++", "c++", 8), ("cpp", "embedded", "embedded", 7)]
    assert candidate_rows == [("c++", "c++"), ("embedded", "embedded")]
    assert stopword_rows == [("the", "the", "stopword")]


def test_sync_resume_policy_generates_generic_rules(tmp_path: Path) -> None:
    sync_module = load_module("sync_resume_policy", "scripts/db/sync_resume_policy.py")
    db_path = tmp_path / "job-log.db"

    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE resume_profiles (
              profile_key TEXT PRIMARY KEY,
              subtitle TEXT,
              summary TEXT,
              approved INTEGER DEFAULT 1
            );

            CREATE TABLE skills_mine (
              skill_name TEXT NOT NULL,
              skill_normalized TEXT PRIMARY KEY,
              category TEXT,
              include_default INTEGER DEFAULT 1,
              require_direct_match INTEGER DEFAULT 0,
              resume_visibility TEXT DEFAULT 'show',
              resume_emphasis TEXT DEFAULT 'plain',
              resume_priority INTEGER DEFAULT 0,
              resume_group_rank INTEGER DEFAULT 0
            );

            INSERT INTO resume_profiles (profile_key, subtitle, summary, approved) VALUES
              ('cpp', 'Software Engineer', 'Native systems profile', 1),
              ('backend', 'Software Engineer', 'Backend profile', 1);

            INSERT INTO skills_mine (skill_name, skill_normalized, category, include_default, require_direct_match, resume_visibility, resume_emphasis, resume_priority, resume_group_rank) VALUES
              ('C++', 'c++', 'language', 1, 0, 'show', 'experience', 9, 20),
              ('Git', 'git', 'tool', 1, 0, 'show', 'plain', 5, 70),
              ('FastAPI', 'fastapi', 'backend', 0, 1, 'context', 'plain', 6, 30);
            """
        )
        connection.commit()

    inserted = sync_module.sync_resume_policy(db_path)

    assert inserted > 0
    with sqlite3.connect(db_path) as connection:
        group_rows = connection.execute(
            "SELECT profile_key, group_id FROM resume_group_rules WHERE profile_key = 'cpp' ORDER BY group_id"
        ).fetchall()
        skill_rows = connection.execute(
            """
            SELECT profile_key, skill_normalized, render_group, visibility, direct_match_boost
            FROM skill_resume_rules
            WHERE profile_key = 'cpp'
            ORDER BY skill_normalized
            """
        ).fetchall()

    assert ("cpp", "programming") in group_rows
    assert ("cpp", "tools") in group_rows
    assert skill_rows == [
        ("cpp", "c++", "programming", "show", 0),
        ("cpp", "fastapi", "backend-cloud", "context", 6),
        ("cpp", "git", "tools", "show", 0),
    ]
