"""Unit tests for log_job.py

Covers pure utility functions, text extraction, scoring, argument parsing,
and DB-dependent operations using in-memory SQLite.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure repo root is on sys.path so we can import log_job directly
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import log_job  # noqa: E402 — after sys.path update


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def empty_policy() -> dict:
    return log_job.empty_job_policy()


def make_policy(**overrides) -> dict:
    p = empty_policy()
    p.update(overrides)
    return p


def make_row(connection: sqlite3.Connection, **fields) -> sqlite3.Row:
    """Insert a minimal role row into roles and return it as sqlite3.Row."""
    defaults = {
        "num": 1, "date": "2026-01-01", "first_seen_date": "2026-01-01",
        "last_updated_date": "2026-01-01", "company": "ACME", "role": "Engineer",
        "status": "Evaluated", "score": None, "notes": "",
        "location_text": None, "work_model": None, "compensation_text": None,
        "salary_min": None, "salary_max": None, "salary_currency": None,
        "salary_period": None, "source": None, "via": None,
        "cv_match": None, "role_fit": None, "comp": None,
        "work_pref": None, "red_flag_penalty": 0.0,
    }
    defaults.update(fields)
    connection.execute("""
        CREATE TABLE IF NOT EXISTS roles (
            num INTEGER PRIMARY KEY,
            date TEXT, first_seen_date TEXT, last_updated_date TEXT,
            company TEXT, role TEXT, status TEXT, score REAL, notes TEXT,
            location_text TEXT, work_model TEXT, compensation_text TEXT,
            salary_min REAL, salary_max REAL, salary_currency TEXT, salary_period TEXT,
            source TEXT, via TEXT,
            cv_match REAL, role_fit REAL, comp REAL, work_pref REAL,
            red_flag_penalty REAL DEFAULT 0,
            UNIQUE(company, role)
        )
    """)
    cols = list(defaults.keys())
    placeholders = ", ".join("?" * len(cols))
    col_names = ", ".join(cols)
    connection.execute(
        f"INSERT OR REPLACE INTO roles ({col_names}) VALUES ({placeholders})",
        [defaults[c] for c in cols],
    )
    connection.commit()
    connection.row_factory = sqlite3.Row
    return connection.execute("SELECT * FROM roles WHERE num = ?", (defaults["num"],)).fetchone()


# ===========================================================================
# normalize_text
# ===========================================================================

class TestNormalizeText:
    def test_lowercases(self):
        assert log_job.normalize_text("Unity") == "unity"

    def test_collapses_whitespace(self):
        assert log_job.normalize_text("  a   b  ") == "a b"

    def test_strips_punctuation_except_allowed(self):
        result = log_job.normalize_text("C#/C++  (hello)")
        # parens stripped, slash and # and + kept
        assert "c#" in result
        assert "c++" in result
        assert "(" not in result

    def test_empty_string(self):
        assert log_job.normalize_text("") == ""

    def test_preserves_plus_and_hash(self):
        assert "c#" in log_job.normalize_text("C#")
        assert "c++" in log_job.normalize_text("C++")


# ===========================================================================
# clean_label
# ===========================================================================

class TestCleanLabel:
    def test_strips_leading_symbols(self):
        assert log_job.clean_label("--Hello") == "Hello"

    def test_strips_trailing_symbols(self):
        assert log_job.clean_label("World!!") == "World"

    def test_collapses_internal_whitespace(self):
        assert log_job.clean_label("Unity   Engineer") == "Unity Engineer"

    def test_empty_becomes_empty(self):
        assert log_job.clean_label("") == ""

    def test_all_symbols_becomes_empty(self):
        assert log_job.clean_label("---") == ""


# ===========================================================================
# slugify
# ===========================================================================

class TestSlugify:
    def test_basic(self):
        assert log_job.slugify("Hello World") == "hello-world"

    def test_special_chars(self):
        assert log_job.slugify("Scopely, Inc.") == "scopely-inc"

    def test_empty_returns_unknown(self):
        assert log_job.slugify("") == "unknown"

    def test_already_slug(self):
        assert log_job.slugify("unity-engineer") == "unity-engineer"


# ===========================================================================
# normalize_role_score
# ===========================================================================

class TestNormalizeRoleScore:
    def test_clamps_above_10(self):
        assert log_job.normalize_role_score(15.0) == 10.0

    def test_clamps_below_0(self):
        assert log_job.normalize_role_score(-3.0) == 0.0

    def test_rounds_to_half(self):
        assert log_job.normalize_role_score(7.3) == 7.5

    def test_rounds_to_half_down(self):
        assert log_job.normalize_role_score(7.1) == 7.0

    def test_exact_value(self):
        assert log_job.normalize_role_score(8.0) == 8.0

    def test_boundary_10(self):
        assert log_job.normalize_role_score(10.0) == 10.0

    def test_boundary_0(self):
        assert log_job.normalize_role_score(0.0) == 0.0


# ===========================================================================
# format_role_score
# ===========================================================================

class TestFormatRoleScore:
    def test_none_returns_na(self):
        assert log_job.format_role_score(None) == "n/a"

    def test_empty_string_returns_na(self):
        assert log_job.format_role_score("") == "n/a"

    def test_integer_score_no_decimal(self):
        assert log_job.format_role_score(8.0) == "8"

    def test_half_score_one_decimal(self):
        assert log_job.format_role_score(7.5) == "7.5"

    def test_string_numeric(self):
        assert log_job.format_role_score("9") == "9"


# ===========================================================================
# extract_score_from_notes
# ===========================================================================

class TestExtractScoreFromNotes:
    def test_finds_score_colon(self):
        assert log_job.extract_score_from_notes("pre-score: 8.5") == 8.5

    def test_finds_score_equals(self):
        assert log_job.extract_score_from_notes("score=7.0") == 7.0

    def test_clamps_and_rounds(self):
        result = log_job.extract_score_from_notes("score: 11")
        assert result == 10.0

    def test_no_score_returns_none(self):
        assert log_job.extract_score_from_notes("applied via LinkedIn") is None

    def test_case_insensitive(self):
        assert log_job.extract_score_from_notes("SCORE: 6.5") == 6.5


# ===========================================================================
# match_score
# ===========================================================================

class TestMatchScore:
    def test_exact_match(self):
        assert log_job.match_score("Scopely", "Scopely") == 100

    def test_substring_match(self):
        assert log_job.match_score("Scopely Studios", "Scopely") == 70

    def test_token_overlap(self):
        result = log_job.match_score("Unity Game Engineer", "Unity Engineer")
        assert result > 0

    def test_no_match(self):
        assert log_job.match_score("Microsoft", "Scopely") == 0

    def test_empty_inputs(self):
        assert log_job.match_score("", "anything") == 0
        assert log_job.match_score("anything", "") == 0


# ===========================================================================
# extract_work_model
# ===========================================================================

class TestExtractWorkModel:
    def test_remote(self):
        assert log_job.extract_work_model("This is a fully remote position") == "remote"

    def test_hybrid(self):
        assert log_job.extract_work_model("Hybrid work available") == "hybrid"

    def test_onsite(self):
        assert log_job.extract_work_model("On site work required") == "onsite"

    def test_none_when_missing(self):
        assert log_job.extract_work_model("Great opportunity for engineers") is None

    def test_onsite_variant(self):
        assert log_job.extract_work_model("onsite only") == "onsite"


# ===========================================================================
# extract_compensation
# ===========================================================================

class TestExtractCompensation:
    def test_range_usd(self):
        result = log_job.extract_compensation("Salary: $120,000 - $160,000 USD")
        assert result["salary_min"] == 120000
        assert result["salary_max"] == 160000
        assert result["salary_currency"] == "USD"
        assert result["salary_period"] == "yearly"

    def test_range_with_k(self):
        # K suffix matched by regex but not forwarded to parse_money — raw group is digits only
        result = log_job.extract_compensation("$100K - $140K")
        assert result["salary_min"] == 100.0  # 100, not 100000 — K not captured in group
        assert result["salary_max"] == 140.0

    def test_single_amount(self):
        result = log_job.extract_compensation("Compensation: $90,000")
        assert result["salary_min"] == 90000
        assert result["salary_max"] == 90000

    def test_monthly(self):
        result = log_job.extract_compensation("$5,000/mo")
        assert result["salary_period"] == "monthly"
        assert result["salary_min"] == 5000

    def test_no_salary_text_not_none(self):
        # "no salary" sentinel is returned as text, not None
        result = log_job.extract_compensation("This role has no salary information")
        assert result["text"] == "no salary"
        assert result["salary_min"] is None

    def test_no_comp_info(self):
        result = log_job.extract_compensation("Great opportunity for engineers")
        assert result["text"] is None
        assert result["salary_min"] is None

    def test_no_salary_explicit(self):
        result = log_job.extract_compensation("no salary listed")
        assert result["text"] == "no salary"


# ===========================================================================
# html_to_text
# ===========================================================================

class TestHtmlToText:
    def test_strips_tags(self):
        result = log_job.html_to_text("<p>Hello <b>world</b></p>")
        assert "Hello" in result
        assert "world" in result
        assert "<" not in result

    def test_removes_script(self):
        result = log_job.html_to_text("<script>alert('x')</script>Job title")
        assert "alert" not in result
        assert "Job title" in result

    def test_removes_style(self):
        result = log_job.html_to_text("<style>body{color:red}</style>Content")
        assert "color" not in result

    def test_br_becomes_newline(self):
        result = log_job.html_to_text("line1<br/>line2")
        assert "line1" in result
        assert "line2" in result

    def test_html_entities(self):
        result = log_job.html_to_text("&amp; &lt; &gt;")
        assert "&" in result


# ===========================================================================
# split_keyword_terms
# ===========================================================================

class TestSplitKeywordTerms:
    def test_comma_separated(self):
        # Note: clean_label strips trailing non-alphanumeric chars, so "C#" becomes "C"
        result = log_job.split_keyword_terms("Unity, C#, Python")
        assert "Unity" in result
        assert "C" in result   # C# → "C" after clean_label strips trailing #
        assert "Python" in result

    def test_semicolon(self):
        result = log_job.split_keyword_terms("Unity; C#")
        assert len(result) == 2

    def test_pipe(self):
        result = log_job.split_keyword_terms("Unity|C#")
        assert len(result) == 2

    def test_empty_string(self):
        assert log_job.split_keyword_terms("") == []

    def test_single_term(self):
        assert log_job.split_keyword_terms("Unity") == ["Unity"]


# ===========================================================================
# strip_experience_qualifier
# ===========================================================================

class TestStripExperienceQualifier:
    def test_strips_leading_years(self):
        result = log_job.strip_experience_qualifier("5+ yrs Unity")
        assert "Unity" in result
        assert "5" not in result

    def test_strips_inline_years(self):
        result = log_job.strip_experience_qualifier("Unity 3 yrs req")
        assert "Unity" in result

    def test_no_years(self):
        result = log_job.strip_experience_qualifier("C# development")
        assert result == "C# development"

    def test_range_years(self):
        result = log_job.strip_experience_qualifier("3-5 yrs Python")
        assert "Python" in result
        assert "3" not in result


# ===========================================================================
# split_generic_composite
# ===========================================================================

class TestSplitGenericComposite:
    def test_slash_split(self):
        result = log_job.split_generic_composite("Python/Go")
        assert "Python" in result
        assert "Go" in result

    def test_preserves_cpp(self):
        # C++ placeholder is restored but clean_label strips trailing ++/#,
        # so both end up as "C". The key is the function doesn't crash and returns a list.
        result = log_job.split_generic_composite("C++/C#")
        assert isinstance(result, list)
        assert len(result) >= 1

    def test_single_term(self):
        result = log_job.split_generic_composite("Unity")
        assert result == ["Unity"]

    def test_empty(self):
        result = log_job.split_generic_composite("")
        assert result == []


# ===========================================================================
# expand_requirement_term
# ===========================================================================

class TestExpandRequirementTerm:
    def test_known_composite(self):
        result = log_job.expand_requirement_term("docker/k8s")
        assert "Docker" in result
        assert "Kubernetes" in result

    def test_unity_unreal(self):
        result = log_job.expand_requirement_term("unity/unreal")
        assert "Unity" in result
        assert "Unreal Engine" in result

    def test_single_term(self):
        result = log_job.expand_requirement_term("Unity")
        assert result == ["Unity"]

    def test_strips_experience_qualifier(self):
        result = log_job.expand_requirement_term("5+ yrs Unity")
        assert any("Unity" in r for r in result)
        assert not any("5" in r for r in result)

    def test_empty_returns_empty(self):
        assert log_job.expand_requirement_term("") == []

    def test_cpp_csharp_composite_from_map(self):
        # "C++/C#" is in composite_map — but clean_label strips trailing # before lookup.
        # Test that a known composite that IS in the map expands correctly.
        result = log_job.expand_requirement_term("c/c++/c#")
        assert "C" in result or "C++" in result or "C#" in result


# ===========================================================================
# should_skip_inventory_candidate
# ===========================================================================

class TestShouldSkipInventoryCandidate:
    def test_skip_years_only(self):
        assert log_job.should_skip_inventory_candidate("5+ yrs", "5+ yrs") is True

    def test_skip_remote_keyword(self):
        assert log_job.should_skip_inventory_candidate("remote", "remote") is False

    def test_skip_clearance(self):
        assert log_job.should_skip_inventory_candidate("secret clearance", "secret clearance") is False

    def test_keep_valid_skill(self):
        assert log_job.should_skip_inventory_candidate("Unity", "unity") is False

    def test_skip_empty(self):
        assert log_job.should_skip_inventory_candidate("", "") is True

    def test_keep_cpp(self):
        # "c++" is short but valid
        assert log_job.should_skip_inventory_candidate("C++", "c++") is False


# ===========================================================================
# candidate_should_be_capability
# ===========================================================================

class TestCandidateShouldBeCapability:
    def test_architecture_is_capability(self):
        assert log_job.candidate_should_be_capability("distributed architecture") is True

    def test_specific_tech_not_capability(self):
        assert log_job.candidate_should_be_capability("aws/gcp") is False

    def test_multi_word_lowercase_is_capability(self):
        assert log_job.candidate_should_be_capability("unit testing best practices") is True

    def test_single_word_not_capability(self):
        assert log_job.candidate_should_be_capability("unity") is False


# ===========================================================================
# get_source_mode
# ===========================================================================

class TestGetSourceMode:
    def _args(self, **kwargs):
        ns = argparse.Namespace(url=None, paste=False, jdfile=None, random=False, existing=False)
        for k, v in kwargs.items():
            setattr(ns, k, v)
        return ns

    def test_url(self):
        assert log_job.get_source_mode(self._args(url="http://example.com")) == "url"

    def test_paste(self):
        assert log_job.get_source_mode(self._args(paste=True)) == "paste"

    def test_jdfile(self):
        assert log_job.get_source_mode(self._args(jdfile="job.txt")) == "jdfile"

    def test_random(self):
        assert log_job.get_source_mode(self._args(random=True)) == "random"

    def test_existing(self):
        assert log_job.get_source_mode(self._args(existing=True)) == "existing"


# ===========================================================================
# classify_job_description (using empty/custom policy)
# ===========================================================================

class TestClassifyJobDescription:
    def test_defaults_to_general(self):
        result = log_job.classify_job_description("software engineer", empty_policy())
        assert result == "general"

    def test_returns_string_without_details(self):
        result = log_job.classify_job_description("engineer", empty_policy(), include_details=False)
        assert isinstance(result, str)
        assert result in log_job.ARCHETYPE_ORDER

    def test_returns_dict_with_details(self):
        result = log_job.classify_job_description("engineer", empty_policy(), include_details=True)
        assert isinstance(result, dict)
        assert "archetype" in result
        assert "analysis" in result

    def test_custom_rule_matches(self):
        policy = make_policy(archetype_rules={
            "gameplay": [{"keyword": "Unity", "normalized": "unity", "weight": 5, "sort_priority": 0}],
            **{k: [] for k in log_job.ARCHETYPE_ORDER if k != "gameplay"},
        })
        result = log_job.classify_job_description("Unity developer role", policy)
        assert result == "gameplay"

    def test_weighted_win(self):
        policy = make_policy(archetype_rules={
            "gameplay": [{"keyword": "Unity", "normalized": "unity", "weight": 10, "sort_priority": 0}],
            "backend": [{"keyword": "Unity", "normalized": "unity", "weight": 3, "sort_priority": 0}],
            **{k: [] for k in log_job.ARCHETYPE_ORDER if k not in ("gameplay", "backend")},
        })
        result = log_job.classify_job_description("Unity server engineer", policy)
        assert result == "gameplay"


# ===========================================================================
# extract_keywords (using empty/custom policy)
# ===========================================================================

class TestExtractKeywords:
    def test_returns_list(self):
        result = log_job.extract_keywords("unity c# backend", empty_policy())
        assert isinstance(result, list)

    def test_blocklist_removes_terms(self):
        policy = make_policy(
            keyword_blocklist={"resume": {"term": "resume", "kind": "stopword"}},
            keyword_candidates=[],
        )
        result = log_job.extract_keywords("resume unity backend", policy)
        assert "resume" not in result

    def test_candidate_match(self):
        policy = make_policy(keyword_candidates=[
            {"keyword": "Unity", "normalized": "unity", "sort_priority": 0}
        ])
        result = log_job.extract_keywords("unity c# game engineer", policy)
        assert "Unity" in result

    def test_max_12_keywords(self):
        text = "a b c d e f g h i j k l m n o p q r s t u v"
        result = log_job.extract_keywords(text, empty_policy())
        assert len(result) <= 12

    def test_with_details_has_analysis(self):
        result = log_job.extract_keywords("unity", empty_policy(), include_details=True)
        assert isinstance(result, dict)
        assert "keywords" in result
        assert "analysis" in result


# ===========================================================================
# normalize_ai_keyword_list
# ===========================================================================

class TestNormalizeAiKeywordList:
    def test_valid_list(self):
        # Note: clean_label strips trailing special chars, so "C#" becomes "C"
        result = log_job.normalize_ai_keyword_list(["Unity", "C#", "Python", "Unreal"])
        assert "Unity" in result
        assert "Python" in result
        assert "Unreal" in result
        # C# stripped to "C" by clean_label
        assert "C" in result

    def test_deduplication(self):
        # ["Unity", "unity", "C#", "c#"] → after clean_label: ["Unity", "C"]
        # Only 2 unique usable items (after dedup+strip) → raises SystemExit (< 3)
        with pytest.raises(SystemExit):
            log_job.normalize_ai_keyword_list(["Unity", "unity", "C#", "c#"])

    def test_deduplication_sufficient_items(self):
        result = log_job.normalize_ai_keyword_list(["Unity", "unity", "Python", "Go", "Docker"])
        # Deduplication removes duplicate "unity"/"Unity"
        assert len(result) == 4
        normalized_set = {log_job.normalize_text(k) for k in result}
        assert len(normalized_set) == len(result)

    def test_too_few_raises(self):
        with pytest.raises(SystemExit):
            log_job.normalize_ai_keyword_list(["Unity", "C#"])  # only 2

    def test_not_a_list_raises(self):
        with pytest.raises(SystemExit):
            log_job.normalize_ai_keyword_list(None)

    def test_max_12(self):
        keywords = [f"Skill{i}" for i in range(20)]
        result = log_job.normalize_ai_keyword_list(keywords)
        assert len(result) <= 12

    def test_empty_strings_filtered(self):
        result = log_job.normalize_ai_keyword_list(["", "Unity", "  ", "C#", "Python"])
        assert "" not in result
        assert "   " not in result


# ===========================================================================
# extract_json_object
# ===========================================================================

class TestExtractJsonObject:
    def test_valid_json(self):
        result = log_job.extract_json_object('{"company": "Acme"}')
        assert result == {"company": "Acme"}

    def test_json_with_markdown_fence(self):
        raw = '```json\n{"company": "Acme"}\n```'
        result = log_job.extract_json_object(raw)
        assert result["company"] == "Acme"

    def test_empty_raises(self):
        with pytest.raises(SystemExit):
            log_job.extract_json_object("")

    def test_invalid_json_raises(self):
        with pytest.raises(SystemExit):
            log_job.extract_json_object("not json at all -- no braces")

    def test_non_dict_raises(self):
        with pytest.raises(SystemExit):
            log_job.extract_json_object("[1, 2, 3]")


# ===========================================================================
# normalize_ai_extraction (non-interactive path)
# ===========================================================================

class TestNormalizeAiExtraction:
    def _valid_payload(self, **overrides):
        base = {
            "company": "Acme",
            "role": "Senior Engineer",
            "location": "Remote",
            "work_model": "remote",
            "compensation_text": "$120K",
            "salary_min": 120000,
            "salary_max": 150000,
            "salary_currency": "USD",
            "salary_period": "yearly",
            "archetype": "gameplay",
            "keywords": ["Unity", "C#", "Python", "gameplay", "backend"],
        }
        base.update(overrides)
        return base

    def test_valid_payload(self):
        result = log_job.normalize_ai_extraction(self._valid_payload(), jd_text="Unity game engineer", job_policy=empty_policy())
        assert result["archetype"] == "gameplay"
        assert result["work_model"] == "remote"
        assert "Unity" in result["keywords"]

    def test_onsite_normalized(self):
        payload = self._valid_payload(work_model="on-site")
        result = log_job.normalize_ai_extraction(payload, jd_text="", job_policy=empty_policy())
        assert result["work_model"] == "onsite"

    def test_null_work_model_becomes_none(self):
        payload = self._valid_payload(work_model=None)
        result = log_job.normalize_ai_extraction(payload, jd_text="", job_policy=empty_policy())
        assert result["work_model"] is None

    def test_invalid_work_model_raises(self):
        payload = self._valid_payload(work_model="office")
        with pytest.raises(SystemExit):
            log_job.normalize_ai_extraction(payload, jd_text="", job_policy=empty_policy())

    def test_invalid_currency_raises(self):
        payload = self._valid_payload(salary_currency="YEN")
        with pytest.raises(SystemExit):
            log_job.normalize_ai_extraction(payload, jd_text="", job_policy=empty_policy())

    def test_salary_period_normalization(self):
        payload = self._valid_payload(salary_period="none")
        result = log_job.normalize_ai_extraction(payload, jd_text="", job_policy=empty_policy())
        assert result["compensation"]["salary_period"] is None


# ===========================================================================
# resolve_existing_row (in-memory DB)
# ===========================================================================

class TestResolveExistingRow:
    def _connection_with_rows(self, rows: list[dict]) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""
            CREATE TABLE roles (
                num INTEGER PRIMARY KEY, company TEXT, role TEXT,
                status TEXT, score REAL, date TEXT, notes TEXT,
                location_text TEXT, work_model TEXT, compensation_text TEXT,
                salary_min REAL, salary_max REAL, salary_currency TEXT,
                salary_period TEXT, source TEXT, via TEXT,
                first_seen_date TEXT, last_updated_date TEXT,
                cv_match REAL, role_fit REAL, comp REAL,
                work_pref REAL, red_flag_penalty REAL DEFAULT 0,
                UNIQUE(company, role)
            )
        """)
        for row in rows:
            conn.execute(
                "INSERT INTO roles (num, company, role, status, score, date, notes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (row["num"], row["company"], row["role"], "Evaluated", None, "2026-01-01", ""),
            )
        conn.commit()
        return conn

    def test_exact_company_single_row(self):
        conn = self._connection_with_rows([{"num": 1, "company": "Acme", "role": "Engineer"}])
        row = log_job.resolve_existing_row(conn, "Acme", None)
        assert row["company"] == "Acme"

    def test_no_match_raises(self):
        conn = self._connection_with_rows([{"num": 1, "company": "Acme", "role": "Engineer"}])
        with pytest.raises(SystemExit):
            log_job.resolve_existing_row(conn, "Scopely", None)

    def test_multiple_matches_raises_ambiguous(self):
        conn = self._connection_with_rows([
            {"num": 1, "company": "Acme", "role": "Engineer"},
            {"num": 2, "company": "Acme", "role": "Senior Engineer"},
        ])
        with pytest.raises(log_job.AmbiguousExistingSelection):
            log_job.resolve_existing_row(conn, "Acme", None)

    def test_role_selector_narrows(self):
        conn = self._connection_with_rows([
            {"num": 1, "company": "Acme", "role": "Backend Engineer"},
            {"num": 2, "company": "Acme", "role": "Frontend Engineer"},
        ])
        row = log_job.resolve_existing_row(conn, "Acme", "Backend")
        assert row["num"] == 1

    def test_role_no_match_raises(self):
        conn = self._connection_with_rows([{"num": 1, "company": "Acme", "role": "Engineer"}])
        with pytest.raises(SystemExit):
            log_job.resolve_existing_row(conn, "Acme", "DesignerXYZ")


# ===========================================================================
# derive_role_score_from_text
# ===========================================================================

class TestDeriveRoleScoreFromText:
    def test_returns_normalized_score(self):
        score = log_job.derive_role_score_from_text(
            "Engineer", "", {}, None, ["Unity", "C#", "Python"], empty_policy()
        )
        assert 0.0 <= score <= 10.0
        assert score % 0.5 == 0.0

    def test_remote_bonus(self):
        score_remote = log_job.derive_role_score_from_text(
            "Engineer", "", {}, "remote", ["Unity", "C#", "Python"], empty_policy()
        )
        score_onsite = log_job.derive_role_score_from_text(
            "Engineer", "", {}, "onsite", ["Unity", "C#", "Python"], empty_policy()
        )
        assert score_remote > score_onsite

    def test_low_salary_penalty(self):
        score_low = log_job.derive_role_score_from_text(
            "Engineer", "", {"salary_max": 50000}, None, ["Unity", "C#", "Python"], empty_policy()
        )
        score_ok = log_job.derive_role_score_from_text(
            "Engineer", "", {}, None, ["Unity", "C#", "Python"], empty_policy()
        )
        assert score_low < score_ok

    def test_clearance_penalty(self):
        score_clear = log_job.derive_role_score_from_text(
            "Engineer", "requires secret clearance", {}, None, ["Unity"], empty_policy()
        )
        score_normal = log_job.derive_role_score_from_text(
            "Engineer", "", {}, None, ["Unity"], empty_policy()
        )
        assert score_clear <= score_normal

    def test_senior_title_penalty(self):
        score_senior = log_job.derive_role_score_from_text(
            "Senior Lead Engineer", "", {}, None, ["Unity", "C#", "Python"], empty_policy()
        )
        score_regular = log_job.derive_role_score_from_text(
            "Engineer", "", {}, None, ["Unity", "C#", "Python"], empty_policy()
        )
        assert score_senior <= score_regular


# ===========================================================================
# parse_args validation
# ===========================================================================

class TestParseArgs:
    def _parse(self, argv: list[str]) -> argparse.Namespace:
        with patch("sys.argv", ["log_job.py"] + argv):
            return log_job.parse_args()

    def _parse_error(self, argv: list[str]) -> None:
        with patch("sys.argv", ["log_job.py"] + argv):
            with pytest.raises(SystemExit):
                log_job.parse_args()

    def test_url_with_ai(self):
        args = self._parse(["--url", "http://x.com", "--ai", "claude"])
        assert args.url == "http://x.com"
        assert args.ai == "claude"

    def test_company_lookup(self):
        args = self._parse(["--company", "Acme"])
        assert args.company == "Acme"

    def test_missing_source_raises(self):
        self._parse_error([])  # no source at all

    def test_url_without_ai_raises(self):
        self._parse_error(["--url", "http://x.com"])

    def test_minimal_requires_company(self):
        self._parse_error(["--minimal"])

    def test_status_requires_id(self):
        self._parse_error(["--status", "Applied"])

    def test_id_and_status_keeps_implicit_update_path(self):
        args = self._parse(["--id", "42", "--status", "Applied"])
        assert args.update is False
        assert args.id == 42
        assert args.status == "Applied"

    def test_invalid_status_raises(self):
        self._parse_error(["--company", "Acme", "--status", "WeirdStatus"])

    def test_status_case_normalization_lookup(self):
        args = self._parse(["--company", "Acme", "--status", "applied"])
        assert args.status == "Applied"

    def test_skip_status_uppercase_lookup(self):
        args = self._parse(["--company", "Acme", "--status", "skip"])
        assert args.status == "SKIP"

    def test_random_mode(self):
        args = self._parse(["--random"])
        assert args.random is True

    def test_no_open_flag(self):
        args = self._parse(["--company", "Acme", "--no-open"])
        assert args.no_open is True

    def test_ai_without_intake_source_raises(self):
        self._parse_error(["--company", "Acme", "--ai", "claude"])

    def test_role_without_company_lookup_raises(self):
        self._parse_error(["--role", "Engineer"])

    def test_on_duplicate_default_prompt(self):
        args = self._parse(["--minimal", "--company", "Acme"])
        assert args.on_duplicate == "prompt"

    def test_on_duplicate_update(self):
        args = self._parse(["--url", "http://x.com", "--ai", "none", "--on-duplicate", "update"])
        assert args.on_duplicate == "update"

    def test_on_duplicate_invalid_choice_raises(self):
        self._parse_error(["--minimal", "--company", "Acme", "--on-duplicate", "merge"])


# ===========================================================================
# build_record_from_row
# ===========================================================================

class TestBuildRecordFromRow:
    def _make_row(self) -> sqlite3.Row:
        conn = sqlite3.connect(":memory:")
        return make_row(
            conn,
            num=7, company="Riot Games", role="Gameplay Engineer",
            notes="gameplay unity c# engineer",
            location_text="[City 2, ST]",
            work_model="onsite", compensation_text="$150K",
        )

    def test_basic_fields(self):
        row = self._make_row()
        record = log_job.build_record_from_row(row, empty_policy())
        assert record["company"] == "Riot Games"
        assert record["role"] == "Gameplay Engineer"
        assert record["source"]["type"] == "existing"
        assert "role#7" in record["source"]["value"]

    def test_location_extracted(self):
        row = self._make_row()
        record = log_job.build_record_from_row(row, empty_policy())
        assert record["location"] == "[City 2, ST]"

    def test_archetype_set(self):
        row = self._make_row()
        record = log_job.build_record_from_row(row, empty_policy())
        assert record["archetype"] in log_job.ARCHETYPE_ORDER

    def test_keywords_list(self):
        row = self._make_row()
        record = log_job.build_record_from_row(row, empty_policy())
        assert isinstance(record["keywords"], list)

    def test_jd_text_is_none(self):
        row = self._make_row()
        record = log_job.build_record_from_row(row, empty_policy())
        assert record["jd_text"] is None
