# Codex Init

## What This Repo Is

Career-Ops is a job-search operating system. The repo evaluates roles, tracks applications, tailors resumes, and generates PDFs from canonical source files. This `.codex/` area exists for Codex-specific workflow docs, deterministic helpers, and optional agent-facing context.

## Role Of `.codex/`

Use `.codex/` for:

- deterministic workflow docs
- optional agent-facing guidance
- Codex-specific helper material that is safe to share

Do not treat `.codex/` as a dumping ground for local machine state, personal candidate data, vendored dependencies, or generated artifacts.

## Public vs Local

Public-safe examples:

- `.codex/README.md`
- `.codex/resume-workflow/README.md`
- `.codex/resume-source/README.md`

Local/private examples:

- `.codex/notes/`
- `.codex/pydeps/`
- `.codex/backend-template/`
- `.codex/resume-workflow/data/candidate/`
- `.codex/resume-workflow/data/jobs/`
- `.codex/resume-workflow/data/builds/`

## Do Not Touch

Do not modify anything outside `.codex/` unless the user explicitly asks.

Default no-touch areas include:

- `.claude/`
- local Claude settings
- `CLAUDE.md` under any circumstance unless the user explicitly asks for that exact file to be changed
- mode behavior under `modes/` unless the task is specifically about agent pipeline behavior
- root config, source, templates, scripts, or docs not required by the task

If Codex needs local guidance, put it under `.codex`, but keep public docs separate from local notes.

## Git

Codex can inspect Git state and prepare changes, but should not create commits automatically.

- For larger or multi-file changes, ask whether the user wants a commit after the work is done.
- Do not commit, amend, reset, or rewrite history unless the user explicitly asks.

## Canonical Sources

| Source | Use |
|--------|-----|
| `data/job-log.db` | Primary tracker and policy store when present. Query via `node query.mjs`. |
| `data/cv.md` | Canonical user CV for resume generation when populated. |
| `data/cv-summary.md` | Quick reference only. Never overrides `data/cv.md`. |
| `config/profile.yml` | Canonical user profile and targeting context when populated. |
| `.codex/` docs/templates | Codex-side docs and templates only. |

### DB query interface

```bash
node query.mjs tracker    [--status <status>] [--company <name>] [--limit N]
node query.mjs gaps       [--archetype <name>] [--type hard|closeable|soft|strength]
node query.mjs training   [--limit N]   # top closeable gaps by demand
node query.mjs project    [--limit N]   # closeable gaps only
node query.mjs bullets    [--archetype <name>] [--limit N]
node query.mjs stories    [--archetype <name>] [--limit N]
node query.mjs archetypes
node query.mjs profile
```

Add `--human` to any command for readable table output instead of JSON.

### Logging new roles

```bash
node log.mjs <applied|evaluated> "<company>" "<role>" "<location>" "<salary>" "<notes>" \
  [--archetype "<archetype>"] \
  [--matched "skill1,skill2"] \
  [--missing "skill1,skill2"]
```

### Re-seeding the DB

If the DB is out of sync with the markdown files, run:
```bash
node db-init.mjs          # full re-seed from applications.md, evaluated.md, skill-gap-cache.md
node db-init.mjs roles    # roles only
node db-init.mjs signals  # skill signals only
```

For Codex work, query the DB first. Fall back to reading markdown source files only if the DB is unavailable.

Never invent metrics or claims not supported by those files.

## Safety Rules

- Do not overstate C++, Unreal, graphics, frontend, or security experience.
- Treat Unreal as limited, Blueprint-oriented exposure unless the source explicitly says more.
- Treat security as adjacent coursework/certification plus anti-cheat and validation work, not deep professional security engineering.
- Do not generate resume claims from freeform interpretation when the structured source does not support them.
- Prefer narrowing claims over stretching them.

## Working Style

When Codex works in this repo:

- preserve existing workflow boundaries
- prefer deterministic config and source files over prompt-only behavior
- keep outputs traceable to explicit source entries
- put Codex-only public docs and helpers under `.codex/`
- keep local notes and generated artifacts out of the public repo state
- avoid changing anything outside `.codex/` unless explicitly requested
- treat non-`.codex` files as read-only reference material by default

## Practical Start Point

If starting a Codex session here:

1. Read this file.
2. Check `.shared/for-codex.md` if it exists locally.
3. If `.shared/for-codex.md` is absent or empty, there is no pending handoff.
4. Check whether the task belongs to Codex or to another workflow.
5. For tracker data, skill gaps, bullets, or stories — query the DB first: `node query.mjs <command> --human`
6. For resume content — read `data/cv.md` directly. The DB does not replace it.
7. For comp, location, and archetype targeting — read `config/profile.yml`.
8. If it is Codex-side resume or deterministic workflow work, stay inside `.codex/`.
9. Treat files outside `.codex/` as read-only reference unless the user explicitly requests edits.
10. Leave `.claude` and `CLAUDE.md` alone unless the user explicitly requests a change there.

## Shared Handoff Contract

Codex may use `.shared/` for short local cross-agent changelists.

- Inbound: read `.shared/for-codex.md` at startup if it exists.
- Outbound: after any meaningful repo change that another agent may need to know about, append a short changelist to the corresponding local handoff file.
- Once Codex has fully handled an inbound note, it can clear or remove it.
- Do not use `.shared/` for long notes, audits, or Codex-only working docs.

Required changelist fields:

- `Status`
- `Files`
- `Change`
- `Why`
- `Follow-up` when applicable
