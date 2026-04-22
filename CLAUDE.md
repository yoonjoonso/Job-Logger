# job-log

Python-based job intake, lookup, update, and artifact-generation pipeline.

## Startup

1. Read `.shared/for-claude.md` before doing anything else if that local handoff file exists.
2. If it is absent or empty, continue normally.
3. If it has notes, incorporate them first, then clear resolved items.
4. After any meaningful repo change, append a short changelist to `.shared/for-codex.md` if Codex may need to know.
5. `.shared/` is local-only agent handoff state, not canonical product documentation.

## Public/Private Boundary

This repo is being prepared for public release. Before adding or modifying files, consult `docs/public-private-boundaries.md`.

- Public: generic docs, templates, deterministic scripts, tests, sanitized agent guidance
- Local/private: `config/profile.yml`, `data/job-log.db*`, `data/cv.md`, `.claude/settings.local.json`, `.shared/`, `.codex/notes/`, `.codex/pydeps/`, `.codex/resume-workflow/data/`, `output/`, `user-input/`
- Use `npm run setup` and `npm run db:setup` terminology; avoid "bootstrap"
- `config/profile.example.yml` is the public template; `config/profile.yml` is local-only and not committed

@.claude/commands.md
@.claude/files.md

<!-- On-demand: @.claude/db.md | @.claude/notes.md -->
