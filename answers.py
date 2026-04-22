from __future__ import annotations

import argparse
import json
import re
import shutil
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
DB_PATH = REPO_ROOT / "data" / "job-log.db"

# Seed questions with placeholder answers.
# Replace the answer text with your own after first run, or update directly in the DB
# (data/job-log.db → question_bank table). The DB is gitignored so your real answers
# stay local. These seeds only apply when the DB row doesn't already exist.
SEED_QUESTIONS = [
    {
        "slug": "tell-me-about-yourself",
        "prompt": "Tell me about yourself / Walk me through your background",
        "tags": "intro|universal",
        "adapt": 1,
        "answer": (
            "[Replace with your background summary.]\n\n"
            "I'm a software engineer with [X]+ years of experience in [your domain]. "
            "Most of my career has been at [Company], where I worked on [Project] — "
            "[brief description of the product and your role].\n\n"
            "I'm looking for a role where I can bring that production experience to [COMPANY/ROLE]."
        ),
    },
    {
        "slug": "challenge",
        "prompt": "Tell me about a challenge you faced at work and how you handled it",
        "tags": "behavioral|universal",
        "adapt": 0,
        "answer": (
            "[Replace with a specific challenge from your work history.]\n\n"
            "Situation: [what was the problem and why it was hard]\n"
            "Action: [what you specifically did to address it]\n"
            "Result: [measurable outcome and what you learned]"
        ),
    },
    {
        "slug": "leadership",
        "prompt": "Tell me about a time you led a project or team",
        "tags": "behavioral|universal",
        "adapt": 0,
        "answer": (
            "[Replace with a specific leadership example.]\n\n"
            "At [Company] I [led/owned] [project or feature]. "
            "[Describe the team size, your role, the key decisions you made, and the outcome.]"
        ),
    },
    {
        "slug": "strength",
        "prompt": "What is your greatest strength?",
        "tags": "behavioral|universal",
        "adapt": 0,
        "answer": (
            "[Replace with your honest answer — pick one strength you can back with evidence.]\n\n"
            "[Strength]: [what it means in practice] — [specific example from your work]."
        ),
    },
    {
        "slug": "weakness",
        "prompt": "What is your greatest weakness?",
        "tags": "behavioral|universal",
        "adapt": 0,
        "answer": (
            "[Replace with a real weakness and what you've done about it.]\n\n"
            "[Weakness]: [honest description]. "
            "I've improved by [concrete habit or change you made]."
        ),
    },
    {
        "slug": "why-games",
        "prompt": "Why do you want to work in games?",
        "tags": "motivation|universal",
        "adapt": 1,
        "answer": (
            "[Replace with your genuine motivation.]\n\n"
            "Games are [your honest reason — feedback loop, technical challenge, personal connection]. "
            "[Specific example from your work that illustrates why it matters to you.]"
        ),
    },
    {
        "slug": "why-this-role",
        "prompt": "Why are you interested in this role / this company?",
        "tags": "motivation|adapt-required",
        "adapt": 1,
        "answer": (
            "[This question requires per-company adaptation.]\n"
            "Run: python answers.py --question why-this-role --id <num> --ai claude|codex\n\n"
            "Base: My background aligns closely with what this role needs. I've shipped [relevant "
            "experience] and I'm looking for a team where I can bring that production depth to "
            "work that matters."
        ),
    },
    {
        "slug": "process",
        "prompt": "Describe your development / engineering process",
        "tags": "technical|universal",
        "adapt": 0,
        "answer": (
            "[Replace with your actual engineering process.]\n\n"
            "I work iteratively — [your preferred starting point]. "
            "For [your common type of work]: [how you approach it step by step]. "
            "I lean on [testing/observability/etc.] because [your reason from experience]."
        ),
    },
    {
        "slug": "feedback",
        "prompt": "How do you handle feedback or disagreement on a technical decision?",
        "tags": "behavioral|universal",
        "adapt": 0,
        "answer": (
            "[Replace with your honest approach.]\n\n"
            "I try to [your first instinct when receiving pushback]. "
            "If I still disagree, [what you do]. "
            "The times I push harder are [when the stakes justify friction — give a specific type]."
        ),
    },
    {
        "slug": "five-years",
        "prompt": "Where do you see yourself in 5 years?",
        "tags": "motivation|universal",
        "adapt": 1,
        "answer": (
            "[Replace with your genuine career direction.]\n\n"
            "I want to [specific goal — depth in a domain, technical authority, leadership, etc.]. "
            "[Why that direction makes sense given where you've been and where you're going.]"
        ),
    },
]

VALID_AI = ("claude", "codex")


def open_db() -> sqlite3.Connection:
    if not DB_PATH.is_file():
        print(f"DB not found: {DB_PATH}", file=sys.stderr)
        raise SystemExit(1)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS question_bank (
            id       INTEGER PRIMARY KEY,
            slug     TEXT UNIQUE NOT NULL,
            prompt   TEXT NOT NULL,
            answer   TEXT NOT NULL,
            tags     TEXT,
            adapt    INTEGER DEFAULT 0,
            approved INTEGER DEFAULT 1,
            notes    TEXT
        )
    """)
    conn.commit()


def seed_if_empty(conn: sqlite3.Connection) -> None:
    n = conn.execute("SELECT COUNT(*) FROM question_bank").fetchone()[0]
    if n > 0:
        return
    for q in SEED_QUESTIONS:
        conn.execute(
            "INSERT OR IGNORE INTO question_bank (slug, prompt, answer, tags, adapt, approved) VALUES (?,?,?,?,?,1)",
            (q["slug"], q["prompt"], q["answer"], q["tags"], 1 if q["adapt"] else 0),
        )
    conn.commit()


def list_questions(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT id, slug, prompt, tags, adapt FROM question_bank WHERE approved=1 ORDER BY id"
    ).fetchall()


def get_question(conn: sqlite3.Connection, selector: str) -> sqlite3.Row | None:
    if selector.isdigit():
        return conn.execute(
            "SELECT * FROM question_bank WHERE id=? AND approved=1", (int(selector),)
        ).fetchone()
    return conn.execute(
        "SELECT * FROM question_bank WHERE slug=? AND approved=1", (selector,)
    ).fetchone()


def get_role(conn: sqlite3.Connection, role_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM roles WHERE num=?", (role_id,)).fetchone()


def get_jd(role_id: int) -> str | None:
    """Try to read the jd.md from any output folder matching this role id."""
    for folder in sorted((REPO_ROOT / "output").glob("*"), reverse=True):
        meta = folder / "metadata.json"
        if meta.is_file():
            try:
                data = json.loads(meta.read_text(encoding="utf-8"))
                if data.get("db", {}).get("role_id") == role_id:
                    jd = folder / "jd.md"
                    if jd.is_file():
                        return jd.read_text(encoding="utf-8")
            except Exception:
                pass
    return None


def build_codex_command(*args: str) -> list[str]:
    codex_path = shutil.which("codex") or shutil.which("codex.cmd") or shutil.which("codex.exe") or shutil.which("codex.ps1")
    if not codex_path:
        raise FileNotFoundError("codex")

    suffix = Path(codex_path).suffix.lower()
    if suffix == ".ps1":
        pwsh_path = shutil.which("pwsh") or shutil.which("powershell")
        if not pwsh_path:
            raise FileNotFoundError("pwsh")
        return [pwsh_path, "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", codex_path, *args]
    return [codex_path, *args]


def run_ai_adaptation(prompt: str, provider: str) -> str:
    if provider == "claude":
        command = ["claude", "-p", "--permission-mode", "default", prompt]
        missing_message = "Claude CLI not found in PATH."
        failure_prefix = "Claude call failed"
    elif provider == "codex":
        missing_message = "Codex CLI not found in PATH."
        failure_prefix = "Codex call failed"
        try:
            command = build_codex_command(
                "exec",
                "--skip-git-repo-check",
                "--sandbox",
                "read-only",
                prompt,
            )
        except FileNotFoundError as exc:
            raise SystemExit(missing_message) from exc
    else:
        raise SystemExit(f"Unsupported AI provider: {provider}")

    try:
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError as exc:
        msg = exc.stderr.strip() or exc.stdout.strip() or failure_prefix
        raise SystemExit(f"AI adaptation failed: {msg}") from exc
    except FileNotFoundError as exc:
        raise SystemExit(missing_message) from exc


def adapt_with_ai(question: sqlite3.Row, role: sqlite3.Row, jd_text: str | None, provider: str) -> str:
    role_info = f"Company: {role['company']}\nRole: {role['role']}\nLocation: {role['location_text'] or ''}"
    jd_section = f"\n\nJob description:\n{jd_text}" if jd_text else ""
    prompt = (
        f"You are helping tailor a job application answer.\n\n"
        f"Question: {role['prompt'] if 'prompt' in role.keys() else question['prompt']}\n\n"
        f"Target role:\n{role_info}{jd_section}\n\n"
        f"Base answer to adapt (keep the same voice and substance, only weave in specific company/role context where natural):\n\n"
        f"{question['answer']}\n\n"
        f"Return only the adapted answer text. No preamble, no commentary."
    )
    return run_ai_adaptation(prompt, provider)


def format_answer(question: sqlite3.Row, answer_text: str, role: sqlite3.Row | None, adapted: bool) -> str:
    lines = []
    lines.append("=" * 70)
    lines.append(f"Q: {question['prompt']}")
    if role:
        suffix = " (adapted)" if adapted else ""
        lines.append(f"   Role: {role['company']} — {role['role']}{suffix}")
    lines.append("=" * 70)
    lines.append("")
    lines.append(answer_text.strip())
    lines.append("")
    return "\n".join(lines)


def current_output_month_dir() -> Path:
    return REPO_ROOT / "output" / datetime.now().strftime("%Y-%m")


def output_path(role: sqlite3.Row | None, slug: str, all_mode: bool) -> Path:
    if role:
        # find matching output folder
        for folder in sorted((REPO_ROOT / "output").glob("*"), reverse=True):
            meta = folder / "metadata.json"
            if meta.is_file():
                try:
                    data = json.loads(meta.read_text(encoding="utf-8"))
                    if data.get("db", {}).get("role_id") == role["num"]:
                        return folder / "answers.txt"
                except Exception:
                    pass
        # fallback: slug from company+role
        company = re.sub(r"[^a-z0-9]+", "-", str(role["company"] or "").lower()).strip("-")
        role_slug = re.sub(r"[^a-z0-9]+", "-", str(role["role"] or "").lower()).strip("-")
        folder = REPO_ROOT / "output" / f"{company}-{role_slug}"
        folder.mkdir(parents=True, exist_ok=True)
        return folder / "answers.txt"
    month_dir = current_output_month_dir()
    return month_dir / ("answers.txt" if all_mode else f"answer-{slug}.txt")


def run(args: argparse.Namespace) -> int:
    conn = open_db()
    ensure_table(conn)
    seed_if_empty(conn)

    if args.list:
        rows = list_questions(conn)
        print(f"\n{'#':<4} {'slug':<25} {'tags':<30} adapt  prompt")
        print("-" * 90)
        for r in rows:
            adapt_flag = "yes" if r["adapt"] else "-"
            print(f"{r['id']:<4} {r['slug']:<25} {(r['tags'] or ''):<30} {adapt_flag:<6} {r['prompt']}")
        print(f"\n{len(rows)} questions. Use --question <slug or #> to generate.")
        return 0

    role: sqlite3.Row | None = None
    jd_text: str | None = None
    if args.id is not None:
        role = get_role(conn, args.id)
        if not role:
            print(f"Role #{args.id} not found in DB.", file=sys.stderr)
            return 1
        jd_text = get_jd(args.id)

    questions_to_run: list[sqlite3.Row] = []

    if args.all:
        questions_to_run = list_questions(conn)
    elif args.question:
        q = get_question(conn, args.question)
        if not q:
            print(f"Question '{args.question}' not found. Run --list to see options.", file=sys.stderr)
            return 1
        questions_to_run = [q]
    else:
        print("Provide --list, --question <slug/#>, or --all.", file=sys.stderr)
        return 1

    blocks: list[str] = []
    for q in questions_to_run:
        if args.ai and q["adapt"] and role:
            print(f"  Adapting '{q['slug']}' with AI...", file=sys.stderr)
            answer_text = adapt_with_ai(q, role, jd_text, args.ai)
            adapted = True
        else:
            answer_text = q["answer"]
            adapted = False
        blocks.append(format_answer(q, answer_text, role, adapted))

    output = "\n".join(blocks)

    out_file = output_path(role, questions_to_run[0]["slug"] if len(questions_to_run) == 1 else "all", args.all)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    if args.all or args.id:
        out_file.write_text(output, encoding="utf-8")
        print(f"Written to: {out_file}")
    else:
        print(output)

    return 0


def cmd_fit_check(role_id: int) -> int:
    """Show a fit analysis for a role: matched vs missing requirements, score breakdown."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        role = conn.execute("SELECT * FROM roles WHERE num = ?", (role_id,)).fetchone()
        if role is None:
            print(f"No role found with id {role_id}.")
            return 1
        reqs = conn.execute(
            """
            SELECT requirement_name, requirement_normalized, kind, priority,
                   matched_entity_type, matched_normalized, match_method, confidence
            FROM role_requirements WHERE role_id = ? ORDER BY priority, requirement_name
            """,
            (role_id,),
        ).fetchall()
        my_skills = {r[0]: r[1] for r in conn.execute("SELECT skill_normalized, level FROM skills_mine").fetchall()}
        my_caps = {r[0]: r[1] for r in conn.execute("SELECT capability_normalized, level FROM capabilities_mine").fetchall()}

    LEVEL_NUM = {"exposure": 1, "basic": 2, "intermediate": 3, "advanced": 4, "expert": 5}

    matched, missing, unknown = [], [], []
    for req in reqs:
        mn = req["matched_normalized"]
        if mn:
            level = my_skills.get(mn) or my_caps.get(mn) or "none"
            matched.append((req["requirement_name"], mn, level))
        elif req["match_method"] == "new_candidate":
            unknown.append(req["requirement_name"])
        else:
            missing.append(req["requirement_name"])

    sep = "-" * 60
    print(sep)
    print(f"  Fit Check  #{role['num']}  {role['company']}  |  {role['role']}")
    print(f"  Status: {role['status'] or 'n/a'}  |  Score: {role['score'] or 'n/a'}/10  |  Archetype: {role.get('notes', '')[:30]}")
    print(sep)

    if matched:
        print(f"\n  MATCHED ({len(matched)})")
        for name, norm, level in sorted(matched, key=lambda x: -LEVEL_NUM.get(x[2], 0)):
            num = LEVEL_NUM.get(level, 0)
            bar = "█" * num + "░" * (5 - num)
            print(f"    {bar}  {level:<12} {name}")

    if missing:
        print(f"\n  MISSING / NOT IN INVENTORY ({len(missing)})")
        for name in sorted(missing):
            print(f"    --  {name}")

    if unknown:
        print(f"\n  UNMATCHED KEYWORDS ({len(unknown)})")
        for name in sorted(unknown):
            print(f"    ?   {name}")

    total = len(matched) + len(missing)
    pct = int(len(matched) / total * 100) if total else 0
    print(f"\n  Match rate: {len(matched)}/{total} ({pct}%)")
    print(sep)
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate application question answers from the question bank.")
    parser.add_argument("--list", action="store_true", help="List all available questions.")
    parser.add_argument("--question", metavar="SLUG_OR_NUM", help="Generate answer for a specific question.")
    parser.add_argument("--all", action="store_true", help="Generate all answers.")
    parser.add_argument("--id", type=int, metavar="ROLE_ID", help="Role DB id (num) for context and output folder.")
    parser.add_argument("--ai", choices=list(VALID_AI), help="Use AI to adapt answers to the role.")
    parser.add_argument("--fit-check", action="store_true", help="Show fit analysis for --id role (no AI needed).")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.fit_check:
        if not args.id:
            print("--fit-check requires --id <role_id>")
            sys.exit(1)
        sys.exit(cmd_fit_check(args.id))
    sys.exit(run(args))
