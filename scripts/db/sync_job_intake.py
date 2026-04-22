#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.config_utils import load_yaml_like, normalize_key, normalize_search_text

RULES_PATH = ROOT / "config" / "job-intake-rules.yml"
DB_PATH = ROOT / "data" / "job-log.db"


def row_exists(connection: sqlite3.Connection, table: str, where: str, params: tuple[object, ...]) -> bool:
    return connection.execute(f"SELECT 1 FROM {table} WHERE {where} LIMIT 1", params).fetchone() is not None


def ensure_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS job_archetype_rules (
          archetype_key       TEXT,
          keyword_text        TEXT,
          keyword_normalized  TEXT,
          weight              INTEGER DEFAULT 1,
          sort_priority       INTEGER DEFAULT 0,
          approved            INTEGER DEFAULT 1,
          notes               TEXT,
          PRIMARY KEY (archetype_key, keyword_normalized)
        );

        CREATE TABLE IF NOT EXISTS job_keyword_candidates (
          keyword_text        TEXT,
          keyword_normalized  TEXT PRIMARY KEY,
          sort_priority       INTEGER DEFAULT 0,
          approved            INTEGER DEFAULT 1,
          notes               TEXT
        );

        CREATE TABLE IF NOT EXISTS job_keyword_stopwords (
          term_text          TEXT,
          term_normalized    TEXT PRIMARY KEY,
          kind               TEXT CHECK(kind IN ('stopword','banned')) DEFAULT 'stopword',
          approved           INTEGER DEFAULT 1,
          notes              TEXT
        );
        """
    )


def parse_rules(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"archetypes": [], "candidate_keywords": [], "stopwords": []}
    payload = load_yaml_like(path) or {}
    return {
        "archetypes": payload.get("archetypes") or [],
        "candidate_keywords": payload.get("candidate_keywords") or [],
        "stopwords": payload.get("stopwords") or [],
    }


def sync_job_intake(rules_path: Path, db_path: Path) -> int:
    payload = parse_rules(rules_path)
    inserted = 0
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as connection:
        ensure_schema(connection)

        for archetype in payload["archetypes"]:
            if not isinstance(archetype, dict):
                continue
            archetype_key = normalize_key(archetype.get("key") or archetype.get("name") or "")
            if not archetype_key:
                continue
            keywords = archetype.get("keywords") or []
            for index, entry in enumerate(keywords, start=1):
                if isinstance(entry, dict):
                    keyword_text = str(entry.get("keyword") or "").strip()
                    weight = int(entry.get("weight") or 0)
                    notes = str(entry.get("notes") or "").strip() or None
                else:
                    keyword_text = str(entry or "").strip()
                    weight = 5
                    notes = None
                if not keyword_text:
                    continue
                keyword_normalized = normalize_search_text(keyword_text)
                if row_exists(connection, "job_archetype_rules", "archetype_key = ? AND keyword_normalized = ?", (archetype_key, keyword_normalized)):
                    continue
                connection.execute(
                    """
                    INSERT INTO job_archetype_rules (archetype_key, keyword_text, keyword_normalized, weight, sort_priority, approved, notes)
                    VALUES (?, ?, ?, ?, ?, 1, ?)
                    """,
                    (archetype_key, keyword_text, keyword_normalized, weight, index, notes),
                )
                inserted += 1

        for index, entry in enumerate(payload["candidate_keywords"], start=1):
            if isinstance(entry, dict):
                keyword_text = str(entry.get("keyword") or "").strip()
                notes = str(entry.get("notes") or "").strip() or None
            else:
                keyword_text = str(entry or "").strip()
                notes = None
            if not keyword_text:
                continue
            keyword_normalized = normalize_search_text(keyword_text)
            if row_exists(connection, "job_keyword_candidates", "keyword_normalized = ?", (keyword_normalized,)):
                continue
            connection.execute(
                """
                INSERT INTO job_keyword_candidates (keyword_text, keyword_normalized, sort_priority, approved, notes)
                VALUES (?, ?, ?, 1, ?)
                """,
                (keyword_text, keyword_normalized, index, notes),
            )
            inserted += 1

        for entry in payload["stopwords"]:
            if isinstance(entry, dict):
                term_text = str(entry.get("term") or "").strip()
                kind = str(entry.get("kind") or "stopword").strip() or "stopword"
                notes = str(entry.get("notes") or "").strip() or None
            else:
                term_text = str(entry or "").strip()
                kind = "stopword"
                notes = None
            if not term_text:
                continue
            term_normalized = normalize_search_text(term_text)
            if row_exists(connection, "job_keyword_stopwords", "term_normalized = ?", (term_normalized,)):
                continue
            connection.execute(
                """
                INSERT INTO job_keyword_stopwords (term_text, term_normalized, kind, approved, notes)
                VALUES (?, ?, ?, 1, ?)
                """,
                (term_text, term_normalized, kind, notes),
            )
            inserted += 1

        connection.commit()
    return inserted


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync local job intake archetype rules into the local DB without overwriting existing rows.")
    parser.add_argument("--rules", default=str(RULES_PATH))
    parser.add_argument("--db", default=str(DB_PATH))
    args = parser.parse_args()

    inserted = sync_job_intake(Path(args.rules), Path(args.db))
    print(f"Synced job intake rules into DB. Inserted {inserted} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
