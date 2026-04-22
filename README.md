# Job-Logger

`Job-Logger` is a local-first workflow for job intake, application tracking, and tailored application materials.

It keeps the core path deterministic, stores state locally, and lets AI stay optional instead of becoming a hard dependency.

## Quick Start

### 1. Run setup

```bash
python scripts/setup.py
```

Setup will:

- create starter config files when they are missing
- capture your candidate/profile basics
- create or copy the local SQLite database
- apply DB migrations
- sync profile, intake, and resume-policy data into the DB
- optionally install Node dependencies

### 2. Verify the install

```bash
npm run sync-check
```

### 3. Explore the CLI

```bash
python log_job.py --help
```

## What It Does

`Job-Logger` combines a few layers that are usually scattered across notes, trackers, spreadsheets, and one-off prompts:

- `job intake`: parse a posting from a URL, pasted text, file, or minimal manual entry
- `classification`: map roles into your local archetypes using DB-backed intake rules
- `tracking`: store role metadata, status, notes, sourcing, and scoring in SQLite
- `artifacts`: generate resume, cover letter, and outreach email outputs
- `setup`: initialize a new install with generic starter templates and a template-backed deterministic DB

## Modes

You can use the app in three broad ways:

- `deterministic`: use `--ai none` and rely on local config plus DB-backed policy
- `AI-assisted`: use `--ai claude` or `--ai codex` where available
- `hybrid`: keep setup and storage deterministic, but use AI only for selective intake or drafting tasks

## Common Commands

### Intake a new role

```bash
python log_job.py --ai none --jdfile samples/jd.md --resume
python log_job.py --ai claude --paste
python log_job.py --ai codex --url <url> --resume --coverletter
python log_job.py --minimal --company "Example Games" --role "Software Engineer"
```

### Look up an existing role

```bash
python log_job.py --id 42
python log_job.py --company "Example Games" --role "Backend Engineer"
python log_job.py --random --resume
```

### Update existing records

```bash
python log_job.py --id 42 --status Applied
python log_job.py --id 42 --set-archetype backend
python log_job.py --id 42 --email
```

### Maintain inventory and scoring

```bash
python log_job.py --set-skill Unity --level expert
python log_job.py --set-qualification CCNA --met partial
python log_job.py --rescore-all
```

## Setup and Bootstrap Commands

```bash
python scripts/setup.py
npm run db:setup
npm run db:sync-profile
npm run db:sync-intake
npm run db:sync-resume-policy
npm run db:init
npm run db:roles
npm run db:signals
npm run query
```

What they do:

- `python scripts/setup.py`: guided first-run setup
- `npm run db:setup`: copy the tracked template DB if missing, then apply migrations
- `npm run db:sync-profile`: sync local profile archetypes and local resume signal rules into the local DB without overwriting existing rows
- `npm run db:sync-intake`: sync local job intake rules into the local DB without overwriting existing rows
- `npm run db:sync-resume-policy`: generate generic resume display/group rules from the current local DB state
- `npm run db:init`: full seed/update path used by the current repo workflow
- `npm run db:roles`: reseed role records from tracker sources
- `npm run db:signals`: reseed skill signal data
- `npm run query`: inspect the SQLite DB through the lightweight query helper

## Key Files

- [`log_job.py`](log_job.py): main CLI entry point
- [`scripts/setup.py`](scripts/setup.py): guided setup flow
- [`scripts/db/db-init.mjs`](scripts/db/db-init.mjs): DB template bootstrap and migration entry point
- [`scripts/generate-resume.py`](scripts/generate-resume.py): DOCX/PDF resume generation
- [`scripts/generate-coverletter.py`](scripts/generate-coverletter.py): cover letter generation
- [`scripts/generate-email.py`](scripts/generate-email.py): outreach email generation
- [`docs/log-job-spec.md`](docs/log-job-spec.md): CLI contract details
- [`docs/public-private-boundaries.md`](docs/public-private-boundaries.md): what belongs in the public repo vs local/private state

## Outputs

Generated files are written under `output/`.

Typical artifacts include:

- `jd.md`
- `metadata.json`
- `<candidate>-resume.docx`
- `<candidate>-resume.pdf`
- `resume-metadata.json`
- `<candidate>-coverletter.docx`
- `<candidate>-coverletter.pdf`
- `<candidate>-email.txt`

Some filenames still reflect older internal naming and will be cleaned up further as the repo is generalized.

## Privacy and Public Repo Hygiene

This project currently separates:

- `public repo material`: docs, deterministic scripts, templates, tests, and sanitized agent guidance
- `local/private state`: personal candidate data, generated artifacts, local agent settings, local notes, and machine-specific state

Read [`docs/public-private-boundaries.md`](docs/public-private-boundaries.md) before publishing changes.

## Current Status

The repo is still being cleaned up from a personal workflow into a public-safe general-use tool.

What is already in place:

- guided setup for new users
- deterministic setup mode
- public/private repo boundary documentation
- optional AI usage rather than mandatory agent dependency

What still needs more cleanup:

- further generalizing non-archetype policy defaults and import flows
- sanitizing tracked sample/generated files
- final history cleanup before publication

## Notes for Agent Users

If you use Claude/Codex-style tools, the repo includes optional agent-facing docs under:

- `.claude/`
- `.codex/`

Those are meant to help contributors and power users. They are not required for normal app usage.
