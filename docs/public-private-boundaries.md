# Public vs Private Repo Boundaries

This repo supports two audiences:

- public users who should be able to run the app without inheriting someone else's personal state
- power users who may use Claude/Codex-style agents locally

The boundary is:

- public: generic docs, templates, deterministic scripts, tests, and sanitized examples
- private/local: personal candidate data, generated artifacts, local agent state, machine paths, vendored local dependencies, and session handoff notes

## Public

These are appropriate to keep in the public repo:

- repo docs in `README.md`, `docs/`, `.claude/*.md`, and `.codex/*.md` when sanitized
- deterministic setup scripts
- tests and generic fixtures
- agent instructions that describe the repo, not a person or machine
- example config/templates such as `config/profile.example.yml`

## Private or Local

These should be local-only or replaced with sanitized examples:

- `config/profile.yml`
- `data/cv.md`, `data/cv-summary.md`
- `data/applications.md`, `data/evaluated.md`
- `data/job-log.db*`
- `.claude/settings.local.json`
- `.shared/for-claude.md`, `.shared/for-codex.md`
- `.codex/notes/`
- `.codex/pydeps/`
- `.codex/backend-template/` when it contains personal document metadata
- `.codex/resume-workflow/data/candidate/`
- `.codex/resume-workflow/data/jobs/`
- `.codex/resume-workflow/data/builds/`
- generated files under `output/`, `temp/`, or `user-input/`

## Practical Rule

Before adding or keeping a file in the public repo, ask:

1. Does this help a stranger run or understand the project?
2. Is it free of personal identity data, local machine paths, and local permissions state?
3. Is it reusable by another user without editing around someone else's workflow residue?

If the answer to any of those is no, the file belongs in local/private state instead of the public repo.
