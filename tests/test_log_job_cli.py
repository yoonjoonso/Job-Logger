from __future__ import annotations

import json
import sqlite3
import sys
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import log_job  # noqa: E402


SCHEMA_SQL = """
CREATE TABLE roles (
  num INTEGER PRIMARY KEY,
  date TEXT,
  first_seen_date TEXT,
  last_updated_date TEXT,
  company TEXT,
  role TEXT,
  location_text TEXT,
  work_model TEXT,
  compensation_text TEXT,
  salary_min REAL,
  salary_max REAL,
  salary_currency TEXT,
  salary_period TEXT,
  score REAL,
  cv_match REAL,
  role_fit REAL,
  comp REAL,
  work_pref REAL,
  score_pinned INTEGER DEFAULT 0,
  red_flag_penalty REAL DEFAULT 0,
  status TEXT,
  pdf INTEGER DEFAULT 0,
  report TEXT,
  source TEXT,
  url TEXT,
  jd_id TEXT,
  archetype TEXT,
  jd_text TEXT,
  found_via TEXT,
  apply_method TEXT,
  notes TEXT,
  UNIQUE(company, role)
);

CREATE TABLE found_via_sources (
  slug TEXT PRIMARY KEY,
  label TEXT NOT NULL,
  sort_priority INTEGER DEFAULT 0
);

CREATE TABLE skills_mine (
  skill_name TEXT NOT NULL,
  skill_normalized TEXT PRIMARY KEY,
  category TEXT,
  display_category TEXT,
  secondary_categories TEXT,
  resume_priority INTEGER DEFAULT 0,
  include_default INTEGER DEFAULT 1,
  require_direct_match INTEGER DEFAULT 0,
  profile_bias TEXT,
  resume_visibility TEXT DEFAULT 'show',
  resume_display TEXT,
  resume_emphasis TEXT,
  resume_group_rank INTEGER DEFAULT 0,
  level TEXT DEFAULT 'none',
  evidence TEXT
);

CREATE TABLE skill_aliases (
  alias_name TEXT NOT NULL,
  alias_normalized TEXT PRIMARY KEY,
  skill_normalized TEXT NOT NULL,
  notes TEXT
);

CREATE TABLE capabilities_mine (
  capability_name TEXT NOT NULL,
  capability_normalized TEXT PRIMARY KEY,
  category TEXT,
  level TEXT DEFAULT 'none',
  resume_priority INTEGER DEFAULT 0,
  evidence TEXT,
  notes TEXT
);

CREATE TABLE capability_aliases (
  alias_name TEXT NOT NULL,
  alias_normalized TEXT PRIMARY KEY,
  capability_normalized TEXT NOT NULL,
  notes TEXT
);

CREATE TABLE qualifications_mine (
  qualification_name TEXT NOT NULL,
  qualification_normalized TEXT PRIMARY KEY,
  category TEXT,
  met TEXT DEFAULT 'no',
  notes TEXT
);

CREATE TABLE qualification_aliases (
  alias_name TEXT NOT NULL,
  alias_normalized TEXT PRIMARY KEY,
  qualification_normalized TEXT NOT NULL,
  notes TEXT
);

CREATE TABLE archetypes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  evals INTEGER DEFAULT 0
);

CREATE TABLE skill_signals (
  archetype_id INTEGER NOT NULL,
  type TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  skill_normalized TEXT NOT NULL,
  count INTEGER DEFAULT 0,
  UNIQUE(archetype_id, type, skill_normalized)
);

CREATE TABLE role_requirements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  role_id INTEGER NOT NULL,
  raw_text TEXT,
  requirement_name TEXT,
  requirement_normalized TEXT,
  kind TEXT,
  priority TEXT DEFAULT 'unknown',
  matched_entity_type TEXT DEFAULT 'none',
  matched_normalized TEXT,
  match_method TEXT DEFAULT 'unmatched',
  confidence REAL,
  source TEXT,
  notes TEXT,
  UNIQUE(role_id, requirement_normalized, kind, priority)
);

CREATE TABLE job_archetype_rules (
  archetype_key TEXT,
  keyword_text TEXT,
  keyword_normalized TEXT,
  weight INTEGER DEFAULT 0,
  sort_priority INTEGER DEFAULT 0,
  approved INTEGER DEFAULT 1,
  notes TEXT,
  PRIMARY KEY (archetype_key, keyword_normalized)
);

CREATE TABLE job_keyword_candidates (
  keyword_text TEXT,
  keyword_normalized TEXT PRIMARY KEY,
  sort_priority INTEGER DEFAULT 0,
  approved INTEGER DEFAULT 1,
  notes TEXT
);

CREATE TABLE job_keyword_stopwords (
  term_text TEXT,
  term_normalized TEXT PRIMARY KEY,
  kind TEXT DEFAULT 'stopword',
  approved INTEGER DEFAULT 1,
  notes TEXT
);
"""


def init_test_db(db_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.executemany(
            "INSERT INTO found_via_sources (slug, label, sort_priority) VALUES (?, ?, ?)",
            [
                ("linkedin", "LinkedIn", 10),
                ("company", "Company Site", 20),
                ("referral", "Referral", 30),
            ],
        )
        conn.executemany(
            """
            INSERT INTO skills_mine (
              skill_name, skill_normalized, category, display_category, level, resume_visibility
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("Unity", "unity", "engine", "Game Tech", "advanced", "show"),
                ("Python", "python", "language", "Languages", "intermediate", "show"),
                ("Docker", "docker", "tool", "Tools", "basic", "show"),
            ],
        )
        conn.execute(
            """
            INSERT INTO capabilities_mine (
              capability_name, capability_normalized, category, level, resume_priority, evidence, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("Distributed Systems", "distributed systems", "architecture", "basic", 1, "", ""),
        )
        conn.execute(
            """
            INSERT INTO qualifications_mine (
              qualification_name, qualification_normalized, category, met, notes
            ) VALUES (?, ?, ?, ?, ?)
            """,
            ("Work Authorization", "work authorization", "eligibility", "partial", ""),
        )
        conn.executemany(
            """
            INSERT INTO job_archetype_rules (
              archetype_key, keyword_text, keyword_normalized, weight, sort_priority, approved, notes
            ) VALUES (?, ?, ?, ?, ?, 1, ?)
            """,
            [
                ("gameplay", "Unity", "unity", 10, 0, "gameplay signal"),
                ("backend", "Python", "python", 6, 1, "backend signal"),
                ("backend", "Docker", "docker", 5, 2, "backend signal"),
            ],
        )
        conn.executemany(
            """
            INSERT INTO job_keyword_candidates (
              keyword_text, keyword_normalized, sort_priority, approved, notes
            ) VALUES (?, ?, ?, 1, ?)
            """,
            [
                ("Unity", "unity", 0, "candidate"),
                ("Python", "python", 1, "candidate"),
                ("Docker", "docker", 2, "candidate"),
            ],
        )
        conn.commit()


def insert_role(
    db_path: Path,
    *,
    num: int,
    company: str,
    role: str,
    status: str = "Evaluated",
    score: float | None = 4.0,
    notes: str = "",
    work_model: str | None = None,
    archetype: str = "general",
    jd_text: str | None = None,
    found_via: str | None = None,
    apply_method: str | None = None,
) -> None:
    today = "2026-04-16"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO roles (
              num, date, first_seen_date, last_updated_date, company, role,
              location_text, work_model, compensation_text, salary_min, salary_max, salary_currency, salary_period,
              score, status, source, url, jd_id, archetype, jd_text, found_via, apply_method, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                num,
                today,
                today,
                today,
                company,
                role,
                "Remote",
                work_model,
                "$120,000 - $150,000 USD",
                120000,
                150000,
                "USD",
                "yearly",
                score,
                status,
                "seed",
                "https://example.test/job",
                "REQ-1",
                archetype,
                jd_text,
                found_via,
                apply_method,
                notes,
            ),
        )
        conn.commit()


def insert_requirement(
    db_path: Path,
    *,
    role_id: int,
    name: str,
    normalized: str,
    kind: str = "skill",
    matched_normalized: str | None = None,
    source: str = "jd_text",
) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO role_requirements (
              role_id, raw_text, requirement_name, requirement_normalized, kind, priority,
              matched_entity_type, matched_normalized, match_method, confidence, source, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                role_id,
                name,
                name,
                normalized,
                kind,
                "required",
                kind if matched_normalized else "none",
                matched_normalized,
                "exact" if matched_normalized else "new_candidate",
                1.0,
                source,
                "",
            ),
        )
        conn.commit()


class CliHarness:
    def __init__(self, repo_root: Path, db_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.repo_root = repo_root
        self.db_path = db_path
        self.monkeypatch = monkeypatch

    def run(
        self,
        *args: str,
        forbid_input: bool = False,
        interactive_tty: bool = False,
        stub_email: bool = False,
        stub_coverletter: bool = False,
    ) -> int:
        self.monkeypatch.setenv("JOB_LOG_REPO_ROOT", str(self.repo_root))
        self.monkeypatch.delenv("JOB_LOG_POLICY_DB_PATH", raising=False)
        self.monkeypatch.setattr(sys, "argv", ["log_job.py", *args])
        self.monkeypatch.setattr(log_job, "open_file", lambda path: None)
        self.monkeypatch.setattr(log_job.sys.stdin, "isatty", lambda: interactive_tty, raising=False)
        if forbid_input:
            self.monkeypatch.setattr("builtins.input", lambda prompt="": pytest.fail(f"unexpected prompt: {prompt}"))
        if stub_email:
            self.monkeypatch.setattr(log_job, "run_email_generation", self._fake_email_generation)
        if stub_coverletter:
            self.monkeypatch.setattr(log_job, "run_coverletter_generation", self._fake_coverletter_generation)
        log_job.load_job_policy.cache_clear()
        return log_job.main()

    def outputs(self) -> list[Path]:
        output_root = self.repo_root / "output"
        return sorted(
            path
            for path in output_root.glob("*/*")
            if path.is_dir()
        )

    def latest_output(self) -> Path:
        outputs = self.outputs()
        assert outputs, "expected at least one output directory"
        return outputs[-1]

    def role_rows(self) -> list[sqlite3.Row]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute("SELECT * FROM roles ORDER BY num").fetchall()

    def role_count(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            return conn.execute("SELECT COUNT(*) FROM roles").fetchone()[0]

    def get_role(self, role_id: int) -> sqlite3.Row:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM roles WHERE num = ?", (role_id,)).fetchone()
            assert row is not None
            return row

    @staticmethod
    def _fake_email_generation(repo_root: Path, record: dict[str, object], output_dir: Path) -> Path:
        output_dir.mkdir(parents=True, exist_ok=True)
        target = output_dir / "Candidate-email.txt"
        target.write_text(f"Email for {record['company']} | {record['role']}\n", encoding="utf-8")
        return target

    @staticmethod
    def _fake_coverletter_generation(repo_root: Path, record: dict[str, object], output_dir: Path) -> dict[str, Path | None]:
        output_dir.mkdir(parents=True, exist_ok=True)
        target = output_dir / "Candidate-coverletter.docx"
        target.write_bytes(b"fake-docx")
        return {"docx": target, "pdf": None}


@pytest.fixture
def cli(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CliHarness:
    repo_root = tmp_path
    (repo_root / "data").mkdir()
    (repo_root / "output").mkdir()
    db_path = repo_root / "data" / "job-log.db"
    init_test_db(db_path)
    log_job.load_job_policy.cache_clear()
    return CliHarness(repo_root, db_path, monkeypatch)


def test_jdfile_intake_creates_db_row_metadata_and_jd(cli: CliHarness) -> None:
    jd_path = cli.repo_root / "unity-jd.md"
    jd_text = textwrap.dedent(
        """
        Example Games
        Senior Unity Engineer
        Location: Remote
        Compensation: $120,000 - $150,000 USD
        Req ID: EG-2048
        We build live games in Unity with Python tooling and Docker pipelines.
        """
    ).strip()
    jd_path.write_text(jd_text, encoding="utf-8")

    exit_code = cli.run(
        "--jdfile",
        str(jd_path),
        "--ai",
        "none",
        "--via",
        "linkedin",
        "--status",
        "Evaluated",
        "--no-open",
    )

    assert exit_code == 0
    assert cli.role_count() == 1
    row = cli.get_role(1)
    assert row["company"] == "Example Games"
    assert row["role"] == "Senior Unity Engineer"
    assert row["source"] == "jdfile"
    assert row["found_via"] == "linkedin"
    assert row["status"] == "Evaluated"
    assert "Unity" in row["jd_text"]
    assert row["jd_id"] == "EG-2048"

    output_dir = cli.latest_output()
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert (output_dir / "jd.md").read_text(encoding="utf-8").strip() == jd_text
    assert metadata["db"]["status"] == "logged"
    assert metadata["db"]["role_id"] == 1
    assert metadata["company"] == "Example Games"
    assert metadata["role"] == "Senior Unity Engineer"


def test_minimal_intake_logs_without_jd_parsing(cli: CliHarness) -> None:
    exit_code = cli.run(
        "--minimal",
        "--company",
        "Minimal Co",
        "--role",
        "Open Application",
        "--archetype",
        "backend",
        "--via",
        "referral",
        "--status",
        "Applied",
        "--how",
        "email",
        "--no-open",
    )

    assert exit_code == 0
    row = cli.get_role(1)
    assert row["company"] == "Minimal Co"
    assert row["role"] == "Open Application"
    assert row["source"] == "minimal"
    assert row["status"] == "Applied"
    assert row["apply_method"] == "email"
    assert row["found_via"] == "referral"
    assert row["jd_text"] in (None, "")

    output_dir = cli.latest_output()
    assert not (output_dir / "jd.md").exists()
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["db"]["status"] == "logged"
    assert metadata["source"]["type"] == "minimal"


def test_duplicate_update_reuses_existing_row(cli: CliHarness) -> None:
    insert_role(
        cli.db_path,
        num=1,
        company="Duplicate Co",
        role="Gameplay Engineer",
        status="Evaluated",
        score=3.0,
        notes="old note",
        archetype="gameplay",
        jd_text="old jd",
    )
    jd_path = cli.repo_root / "duplicate-update.md"
    jd_path.write_text(
        "Duplicate Co\nGameplay Engineer\nRemote\nUnity and Python role.\n",
        encoding="utf-8",
    )

    exit_code = cli.run(
        "--jdfile",
        str(jd_path),
        "--ai",
        "none",
        "--status",
        "Applied",
        "--via",
        "company",
        "--how",
        "company",
        "--on-duplicate",
        "update",
        "--no-open",
    )

    assert exit_code == 0
    assert cli.role_count() == 1
    row = cli.get_role(1)
    assert row["status"] == "Applied"
    output_dir = cli.latest_output()
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert metadata["db"]["role_id"] == 1
    assert metadata["db"]["status"] == "logged"


def test_duplicate_skip_leaves_db_unchanged(cli: CliHarness) -> None:
    insert_role(cli.db_path, num=1, company="Skip Co", role="Backend Engineer", status="Evaluated", score=4.0)
    jd_path = cli.repo_root / "duplicate-skip.md"
    jd_path.write_text("Skip Co\nBackend Engineer\nRemote\nPython and Docker.\n", encoding="utf-8")

    exit_code = cli.run(
        "--jdfile",
        str(jd_path),
        "--ai",
        "none",
        "--status",
        "Applied",
        "--via",
        "linkedin",
        "--how",
        "company",
        "--on-duplicate",
        "skip",
        "--no-open",
    )

    assert exit_code == 0
    assert cli.role_count() == 1
    row = cli.get_role(1)
    assert row["status"] == "Evaluated"
    output_dir = cli.latest_output()
    assert (output_dir / "jd.md").exists()
    assert not (output_dir / "metadata.json").exists()


@pytest.mark.xfail(reason="current schema enforces UNIQUE(company, role), so duplicate create cannot insert a second matching row")
def test_duplicate_create_inserts_second_matching_row(cli: CliHarness) -> None:
    insert_role(cli.db_path, num=1, company="Create Co", role="Backend Engineer")
    jd_path = cli.repo_root / "duplicate-create.md"
    jd_path.write_text("Create Co\nBackend Engineer\nRemote\nPython and Docker.\n", encoding="utf-8")

    exit_code = cli.run(
        "--jdfile",
        str(jd_path),
        "--ai",
        "none",
        "--via",
        "linkedin",
        "--status",
        "Evaluated",
        "--on-duplicate",
        "create",
        "--no-open",
    )

    assert exit_code == 0
    assert cli.role_count() == 2


def test_lookup_by_id_generates_email_and_updates_metadata(cli: CliHarness) -> None:
    insert_role(cli.db_path, num=1, company="Lookup Co", role="Platform Engineer", archetype="backend")
    insert_requirement(cli.db_path, role_id=1, name="Python", normalized="python", matched_normalized="python")
    insert_requirement(cli.db_path, role_id=1, name="Docker", normalized="docker", matched_normalized="docker")

    exit_code = cli.run("--id", "1", "--email", "--no-open", stub_email=True)

    assert exit_code == 0
    output_dir = cli.latest_output()
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert any(output_dir.glob("*-email.txt"))
    assert metadata["db"]["role_id"] == 1
    assert metadata["email"]["status"] == "generated"
    assert metadata["email"]["path"].endswith("-email.txt")


def test_lookup_by_company_generates_coverletter_docx(cli: CliHarness) -> None:
    insert_role(cli.db_path, num=1, company="Cover Co", role="Gameplay Engineer", archetype="gameplay")
    insert_requirement(cli.db_path, role_id=1, name="Unity", normalized="unity", matched_normalized="unity")

    exit_code = cli.run(
        "--company",
        "Cover Co",
        "--role",
        "Gameplay",
        "--coverletter",
        "--no-open",
        stub_coverletter=True,
    )

    assert exit_code == 0
    output_dir = cli.latest_output()
    metadata = json.loads((output_dir / "metadata.json").read_text(encoding="utf-8"))
    assert any(output_dir.glob("*-coverletter.docx"))
    assert metadata["db"]["role_id"] == 1
    assert metadata["coverletter"]["status"] == "generated"
    assert metadata["coverletter"]["docx"].endswith("-coverletter.docx")
    assert metadata["coverletter"]["pdf"] is None


def test_inventory_updates_modify_seed_rows(cli: CliHarness) -> None:
    assert cli.run("--set-skill", "Unity", "--level", "expert") == 0
    assert cli.run("--set-capability", "Distributed Systems", "--level", "advanced") == 0
    assert cli.run("--set-qualification", "Work Authorization", "--met", "yes") == 0

    with sqlite3.connect(cli.db_path) as conn:
        skill = conn.execute("SELECT level FROM skills_mine WHERE skill_normalized = 'unity'").fetchone()[0]
        capability = conn.execute(
            "SELECT level FROM capabilities_mine WHERE capability_normalized = 'distributed systems'"
        ).fetchone()[0]
        qualification = conn.execute(
            "SELECT met FROM qualifications_mine WHERE qualification_normalized = 'work authorization'"
        ).fetchone()[0]

    assert skill == "expert"
    assert capability == "advanced"
    assert qualification == "yes"


def test_rescore_all_updates_unpinned_scores_and_preserves_pinned(cli: CliHarness) -> None:
    insert_role(
        cli.db_path,
        num=1,
        company="Scored Co",
        role="Unity Engineer",
        score=1.0,
        notes="",
        work_model="remote",
        archetype="gameplay",
    )
    insert_role(
        cli.db_path,
        num=2,
        company="Pinned Co",
        role="Backend Engineer",
        score=8.0,
        notes="score: 8.0",
        work_model="remote",
        archetype="backend",
    )
    insert_requirement(cli.db_path, role_id=1, name="Unity", normalized="unity", matched_normalized="unity")
    insert_requirement(cli.db_path, role_id=1, name="Python", normalized="python", matched_normalized="python")
    insert_requirement(cli.db_path, role_id=1, name="Docker", normalized="docker", matched_normalized="docker")
    insert_requirement(cli.db_path, role_id=2, name="Python", normalized="python", matched_normalized="python")

    exit_code = cli.run("--rescore-all")

    assert exit_code == 0
    rescored = cli.get_role(1)
    pinned = cli.get_role(2)
    assert rescored["score"] != 1.0
    assert pinned["score"] == 8.0


def test_non_interactive_flags_avoid_prompts_in_scripted_run(cli: CliHarness) -> None:
    insert_role(
        cli.db_path,
        num=1,
        company="No Prompt Co",
        role="Unity Engineer",
        status="Evaluated",
        archetype="gameplay",
    )
    jd_path = cli.repo_root / "no-prompt.md"
    jd_path.write_text(
        "No Prompt Co\nUnity Engineer\nLocation: Remote\nUnity, Python, Docker.\n",
        encoding="utf-8",
    )

    exit_code = cli.run(
        "--jdfile",
        str(jd_path),
        "--ai",
        "none",
        "--status",
        "Applied",
        "--via",
        "linkedin",
        "--how",
        "company",
        "--on-duplicate",
        "update",
        "--no-open",
        forbid_input=True,
        interactive_tty=True,
    )

    assert exit_code == 0
    assert cli.get_role(1)["status"] == "Applied"
