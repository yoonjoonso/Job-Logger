from __future__ import annotations

import argparse
import sqlite3
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import answers  # noqa: E402


def make_row(values: dict[str, object]) -> sqlite3.Row:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    columns = ", ".join(f"{key} TEXT" for key in values)
    conn.execute(f"CREATE TABLE sample ({columns})")
    placeholders = ", ".join("?" for _ in values)
    conn.execute(
        f"INSERT INTO sample ({', '.join(values)}) VALUES ({placeholders})",
        tuple(values.values()),
    )
    row = conn.execute("SELECT * FROM sample").fetchone()
    assert row is not None
    return row


class TestParseArgs:
    def _parse(self, argv: list[str]) -> argparse.Namespace:
        with patch("sys.argv", ["answers.py"] + argv):
            return answers.parse_args()

    def test_ai_accepts_codex(self):
        args = self._parse(["--question", "why-this-role", "--id", "7", "--ai", "codex"])
        assert args.ai == "codex"


class TestRunAiAdaptation:
    def test_claude_command(self):
        with patch("answers.subprocess.run") as run_mock:
            run_mock.return_value = subprocess.CompletedProcess(
                args=["claude"],
                returncode=0,
                stdout="adapted answer\n",
                stderr="",
            )

            result = answers.run_ai_adaptation("prompt body", "claude")

        assert result == "adapted answer"
        run_mock.assert_called_once_with(
            ["claude", "-p", "--permission-mode", "default", "prompt body"],
            text=True,
            capture_output=True,
            check=True,
        )

    def test_codex_command_uses_exec_read_only(self):
        with patch("answers.build_codex_command", return_value=["codex", "exec"]) as build_mock, patch(
            "answers.subprocess.run"
        ) as run_mock:
            run_mock.return_value = subprocess.CompletedProcess(
                args=["codex", "exec"],
                returncode=0,
                stdout="tailored\n",
                stderr="",
            )

            result = answers.run_ai_adaptation("prompt body", "codex")

        assert result == "tailored"
        build_mock.assert_called_once_with(
            "exec",
            "--skip-git-repo-check",
            "--sandbox",
            "read-only",
            "prompt body",
        )
        run_mock.assert_called_once_with(
            ["codex", "exec"],
            text=True,
            capture_output=True,
            check=True,
        )

    def test_missing_codex_cli_raises(self):
        with patch("answers.build_codex_command", side_effect=FileNotFoundError("codex")):
            with pytest.raises(SystemExit, match="Codex CLI not found in PATH."):
                answers.run_ai_adaptation("prompt body", "codex")


class TestAdaptWithAi:
    def test_prompt_includes_role_and_jd(self):
        question = make_row(
            {
                "slug": "why-this-role",
                "prompt": "Why this role?",
                "answer": "Base answer",
            }
        )
        role = make_row(
            {
                "company": "Acme",
                "role": "Gameplay Engineer",
                "location_text": "Remote",
            }
        )

        with patch("answers.run_ai_adaptation", return_value="Adapted answer") as adapt_mock:
            result = answers.adapt_with_ai(question, role, "Need C# and Unity", "codex")

        assert result == "Adapted answer"
        prompt_text = adapt_mock.call_args.args[0]
        provider = adapt_mock.call_args.args[1]
        assert "Company: Acme" in prompt_text
        assert "Role: Gameplay Engineer" in prompt_text
        assert "Job description:\nNeed C# and Unity" in prompt_text
        assert "Base answer" in prompt_text
        assert provider == "codex"


class TestOutputPath:
    def test_all_without_role_uses_month_folder(self):
        fake_month_dir = answers.REPO_ROOT / "output" / "2026-04"
        with patch("answers.current_output_month_dir", return_value=fake_month_dir):
            result = answers.output_path(None, "all", True)

        assert result == fake_month_dir / "answers.txt"

    def test_single_without_role_uses_month_folder(self):
        fake_month_dir = answers.REPO_ROOT / "output" / "2026-04"
        with patch("answers.current_output_month_dir", return_value=fake_month_dir):
            result = answers.output_path(None, "why-this-role", False)

        assert result == fake_month_dir / "answer-why-this-role.txt"
