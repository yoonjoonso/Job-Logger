#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_PATH = REPO_ROOT / "tests" / "resume" / "fixtures.json"
GOLDEN_DIR = REPO_ROOT / "tests" / "resume" / "golden"
OUTPUT_ROOT = REPO_ROOT / "output" / "resume-regressions"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def run_case(python_exe: str, case: dict, keep_output: bool) -> Path:
    args = case["args"]
    case_out = OUTPUT_ROOT / case["id"]
    if case_out.exists():
        shutil.rmtree(case_out)
    case_out.mkdir(parents=True, exist_ok=True)

    command = [
        python_exe,
        str(REPO_ROOT / "scripts" / "generate-resume.py"),
        f"--archetype={args['archetype']}",
        f"--out={case_out}",
    ]
    keywords = args.get("keywords") or []
    if keywords:
        command.append(f"--keywords={','.join(keywords)}")
    mapping = {
        "job_company": "--job-company",
        "job_role": "--job-role",
        "job_location": "--job-location",
        "job_work_model": "--job-work-model",
        "job_compensation": "--job-compensation",
        "job_notes": "--job-notes",
    }
    for key, flag in mapping.items():
        value = args.get(key)
        if value:
            command.append(f"{flag}={value}")

    completed = subprocess.run(command, cwd=str(REPO_ROOT), text=True, capture_output=True)
    if completed.returncode != 0:
        raise SystemExit(f"[{case['id']}] generator failed\nSTDOUT:\n{completed.stdout}\nSTDERR:\n{completed.stderr}")

    metadata_path = case_out / "resume-metadata.json"
    if not metadata_path.is_file():
        raise SystemExit(f"[{case['id']}] missing resume-metadata.json at {metadata_path}")
    if not keep_output:
        # Keep outputs by default if debugging is needed; metadata is still consumed before cleanup.
        pass
    return metadata_path


def extract_snapshot(metadata: dict) -> dict:
    explainability = metadata.get("explainability") or {}
    profile_selection = explainability.get("profile_selection") or {}
    layout_selection = explainability.get("layout_selection") or {}
    education_selection = explainability.get("education_selection") or {}
    certification_selection = explainability.get("certification_selection") or {}
    experience_selection = explainability.get("experience_selection") or {}

    role_decisions = experience_selection.get("role_decisions") or []
    dropped_items = experience_selection.get("dropped_items") or []
    return {
        "archetype": metadata.get("archetype"),
        "selected_profile_key": profile_selection.get("selected_profile_key"),
        "fit_tier": profile_selection.get("fit_tier"),
        "final_layout": layout_selection.get("selected_layout"),
        "cert_count": len(certification_selection.get("selected") or []),
        "cerritos_included": bool(((education_selection.get("cerritos") or {}).get("included"))),
        "experience_explainability_present": bool(role_decisions),
        "drop_explainability_present": bool(dropped_items or role_decisions),
    }


def compare_snapshot(case_id: str, actual: dict, expected: dict) -> list[str]:
    failures: list[str] = []
    for key, expected_value in expected.items():
        actual_value = actual.get(key)
        if actual_value != expected_value:
            failures.append(f"{case_id}: {key} expected {expected_value!r} but got {actual_value!r}")
    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description="Run lightweight resume metadata regressions.")
    parser.add_argument("--python", default=sys.executable, help="Python executable to use for generator subprocesses.")
    parser.add_argument("--case", action="append", help="Specific fixture id(s) to run.")
    parser.add_argument("--update", action="store_true", help="Rewrite golden snapshots from current outputs.")
    parser.add_argument("--keep-output", action="store_true", help="Keep output folders under output/resume-regressions.")
    args = parser.parse_args()

    fixtures = load_json(FIXTURES_PATH)
    cases = [case for case in fixtures["cases"] if case.get("mode") == "automated"]
    if args.case:
        wanted = set(args.case)
        cases = [case for case in cases if case["id"] in wanted]

    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)

    failures: list[str] = []
    for case in cases:
        metadata_path = run_case(args.python, case, keep_output=args.keep_output)
        actual = extract_snapshot(load_json(metadata_path))
        golden_path = GOLDEN_DIR / f"{case['id']}.json"
        if args.update:
            golden_path.write_text(json.dumps(actual, indent=2) + "\n", encoding="utf-8")
            print(f"updated {golden_path}")
            continue
        if not golden_path.is_file():
            failures.append(f"{case['id']}: missing golden snapshot {golden_path}")
            continue
        expected = load_json(golden_path)
        failures.extend(compare_snapshot(case["id"], actual, expected))
        print(f"checked {case['id']}")

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
