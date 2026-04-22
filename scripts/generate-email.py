#!/usr/bin/env python3
"""generate-email.py -- Generate a short outreach email body for a job application.

Usage:
    python scripts/generate-email.py \
        --archetype gameplay \
        --job-role "Senior Unity Engineer" \
        --job-company "Acme Games" \
        --out output/acme-games-2026-04-12/

    Outputs:
        <candidate>-email.txt  -- plain-text email body; copy/paste into your mail client

For minimal/cold outreach (no JD), pass --minimal to omit company/role reference.
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "job-log.db"


def _load_candidate_name() -> tuple[str, str]:
    """Return (full_name, display_name) from profile table, or generic fallbacks."""
    try:
        with sqlite3.connect(_DB_PATH) as conn:
            row = conn.execute("SELECT full_name, display_name FROM profile LIMIT 1").fetchone()
            if row and row[0]:
                full = row[0].strip()
                display = (row[1] or "").strip() or full.split()[0]
                return full, display
    except Exception:
        pass
    return "Candidate", ""

# ── Per-archetype templates ───────────────────────────────────────────────────
# Keys:
#   body          -- used when company + role are known; may contain {company}/{role}
#   body_minimal  -- used for cold/general outreach with no specific role
#   closing_note  -- optional line before the sign-off (e.g. "Happy to share more details.")
#   signoff       -- optional sign-off override (default: loaded from DB display_name)
#
# "I've attached my resume." is always inserted before closing_note + signoff.

TEMPLATES: dict[str, dict[str, str]] = {
    "gameplay": {
        "body": (
            "I'm a software engineer with 7+ years in game development, primarily in Unity. "
            "I've worked on a live multiplayer game with millions of players, building gameplay systems, "
            "networking features, and backend services. "
            "I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with 7+ years in game development, primarily in Unity. "
            "I've worked on live multiplayer games with millions of players, building gameplay systems and backend services."
        ),
    },
    "mobile": {
        "body": (
            "I'm a software engineer with 7+ years in game development with a strong mobile focus. "
            "I've shipped games on iOS and Android in Unity, including a live service title with millions of players. "
            "I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with 7+ years in game development, with a strong mobile focus. "
            "I've shipped cross-platform titles in Unity, including a live service game with millions of players."
        ),
    },
    "vr": {
        "body": (
            "I have 7+ years of experience in the games industry, primarily in VR. "
            "I've shipped titles on Meta, Steam, and PlayStation, including a live multiplayer VR game with millions of players. "
            "Most of my work has been in Unity, but I've also shipped in Unreal Engine."
        ),
        "body_minimal": (
            "I have 7+ years of experience in the games industry, primarily in VR. "
            "I've shipped titles on Meta, Steam, and PlayStation, including a live multiplayer VR game with millions of players."
        ),
        "closing_note": "Happy to share more details.",
    },
    "unreal": {
        "body": (
            "I'm a software engineer with game industry experience including Unreal Engine work in C++ and Blueprints. "
            "My primary background is Unity and C#, and I've shipped cross-platform titles including a live multiplayer VR game with millions of players. "
            "I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with game industry experience including Unreal Engine work in C++ and Blueprints. "
            "My primary background is Unity and C#, with cross-platform shipping across Meta, Steam, and PlayStation."
        ),
    },
    "gamebackend": {
        "body": (
            "I'm a software engineer with 7+ years in the games industry. "
            "I've built scalable server-side systems for a live-service game with millions of players, "
            "including player data, progression, and virtual economy features. "
            "This role aligns with the backend work I've been doing, and I'd love to connect."
        ),
        "body_minimal": (
            "I'm a software engineer with 7+ years in the games industry. "
            "I've built scalable server-side systems for a live-service game with millions of players, "
            "including player data, progression, and virtual economy features."
        ),
    },
    "liveops": {
        "body": (
            "I'm a software engineer with 7+ years building and supporting live-service game features. "
            "I've shipped progression, monetization, reward, and event systems for a game with millions of players, "
            "integrating gameplay with cloud-backed services. "
            "I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with 7+ years building and supporting live-service game features. "
            "I've shipped progression, monetization, reward, and event systems at scale, integrating gameplay with cloud-backed services."
        ),
    },
    "multiplayer": {
        "body": (
            "I'm a software engineer with game industry experience building multiplayer systems. "
            "I've worked on a live VR title with millions of players, handling real-time state sync, networking, and session features in Unity. "
            "I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with experience building multiplayer game systems, including a live VR title with millions of players. "
            "I've worked on real-time networking, session management, and server-authoritative features in Unity."
        ),
    },
    "gameserver": {
        "body": (
            "I'm a software engineer with experience building server-authoritative game systems and live multiplayer infrastructure. "
            "I've worked on session management, backend services, and cloud integrations for a live game with millions of players. "
            "I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with experience building server-authoritative game systems and live multiplayer infrastructure. "
            "I've shipped backend services and cloud integrations supporting millions of concurrent players."
        ),
    },
    "backend": {
        "body": (
            "I'm a software engineer with experience building production backend systems and services. "
            "I've worked across SQL, Redis, Azure, and REST-based integrations in live-service game environments, "
            "and I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with experience in multiplayer games and backend systems. "
            "I'm interested in what you're building."
        ),
    },
    "fullstack": {
        "body": (
            "I'm a software engineer with a background in backend services and cloud-integrated systems, with supporting frontend exposure. "
            "I've shipped end-to-end features in production game and service environments and I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with a background in backend services and end-to-end feature ownership across game and service environments."
        ),
    },
    "graphics": {
        "body": (
            "I'm a software engineer with a game industry background and exposure to rendering pipelines, shader systems, and graphics-adjacent engine work. "
            "My primary strengths are in gameplay and systems engineering, and I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with a game industry background, including exposure to rendering pipelines and graphics-adjacent engine work."
        ),
    },
    "sim": {
        "body": (
            "I'm a software engineer with 7+ years building real-time simulation and interactive systems across game engines and training platforms. "
            "I've shipped VR safety training simulations and physics-driven gameplay in Unity and Unreal Engine. "
            "I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with experience building real-time simulation and interactive systems in Unity and Unreal, "
            "including VR training applications."
        ),
    },
    "general": {
        "body": (
            "I'm a software engineer with experience building gameplay and backend systems for live multiplayer games. "
            "I've worked on systems supporting millions of players and I'm interested in the {role} role at {company}."
        ),
        "body_minimal": (
            "I'm a software engineer with experience in multiplayer games and backend systems. "
            "I'm interested in what you're building."
        ),
    },
}

# Archetypes that fall back to another template
TEMPLATE_ALIASES: dict[str, str] = {
    "cpp":        "general",
    "devops":     "general",
    "networking": "general",
    "db":         "general",
    "frontend":   "general",
    "cyber":      "general",
    "genai":      "general",
}

DEFAULT_GREETING = "Hi,"


def build_email(archetype: str, company: str, role: str, minimal: bool, signoff: str = "Thanks,") -> str:
    key  = TEMPLATE_ALIASES.get(archetype, archetype)
    tmpl = TEMPLATES.get(key, TEMPLATES["general"])

    greeting     = tmpl.get("greeting",     DEFAULT_GREETING)
    resolved_signoff = tmpl.get("signoff",  signoff)
    closing_note = tmpl.get("closing_note", "")

    if minimal or (not company and not role):
        body = tmpl.get("body_minimal", tmpl["body"])
    else:
        body = tmpl["body"].format(
            company=company or "the company",
            role=role or "the role",
        )

    parts = [greeting, "", body, "", "I've attached my resume."]
    if closing_note:
        parts += ["", closing_note]
    parts += ["", resolved_signoff]
    return "\n".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a short outreach email body.")
    parser.add_argument("--archetype",   default="general", help="Role archetype key.")
    parser.add_argument("--job-role",    default="",        help="Job title.")
    parser.add_argument("--job-company", default="",        help="Company name.")
    parser.add_argument("--minimal",     action="store_true", help="Cold/general outreach — omit company/role reference.")
    parser.add_argument("--out",         required=True,     help="Output directory path.")
    args = parser.parse_args()

    full_name, display_name = _load_candidate_name()
    signoff = f"Thanks,\n{display_name}" if display_name else "Thanks,"
    name_prefix = full_name.replace(" ", "") or "Candidate"

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    email_body = build_email(
        archetype=args.archetype,
        company=args.job_company,
        role=args.job_role,
        minimal=args.minimal,
        signoff=signoff,
    )

    out_file = out_dir / f"{name_prefix}-email.txt"
    out_file.write_text(email_body + "\n", encoding="utf-8")
    print(f"Email written to {out_file}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
