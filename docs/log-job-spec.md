# `log_job.py` Spec

## Purpose

`log_job.py` is the root-level user entry point for:

- logging new roles
- looking up existing roles
- updating stored roles
- generating resume, cover letter, and email artifacts
- maintaining normalized skills, capabilities, qualifications, and scores

## Current CLI Surface

```text
python .\log_job.py [intake source | existing lookup | inventory command | --rescore-all]
                    [artifact flags]
                    [update flags]
                    [output flags]
```

Top-level help currently exposes:

```text
intake:
  --url URL | --paste | --jdfile FILE | --minimal
  [--company NAME] [--role TITLE]
  [--ai {claude,codex,none}]
  [--via SOURCE] [--how METHOD] [--no-db]

lookup:
  [--id N] [--company NAME] [--role TITLE] [--random]

artifacts:
  [--resume] [--coverletter] [--email]
  [--archetype ARCHETYPE] [--no-open] [--dry-run]

update:
  [--status STATUS] [--notes TEXT] [--score N]
  [--set-company NAME] [--set-role TITLE] [--set-location TEXT]
  [--set-work-model {remote,hybrid,onsite}]
  [--set-compensation-text TEXT]
  [--set-via SOURCE] [--set-how METHOD]
  [--set-archetype ARCHETYPE]

inventory:
  [--set-skill NAME --level LEVEL]
  [--set-capability NAME --level LEVEL]
  [--set-qualification NAME --met {yes,partial,no}]
  [--rename NEW_NAME]
  [--rescore-all]

output:
  [--verbose|-v] [--jd]
```

## Modes

### New intake

Valid sources:

- `--url`
- `--paste`
- `--jdfile`
- `--minimal`

Rules:

- JD-backed intake (`--url`, `--paste`, `--jdfile`) requires `--ai {claude|codex|none}`
- `--minimal` requires `--company`
- `--company` and `--role` act as overrides for JD-backed intake
- `--via` and `--how` capture sourcing / apply-method metadata
- `--no-db` skips DB writes
- artifacts may be requested during the same run

### Existing lookup

Selectors:

- `--id N`
- `--company NAME` with optional `--role TITLE`
- `--random`

Rules:

- `--company` without `--role` may yield a single best match or an ambiguity list
- `--role` without `--company` is invalid unless an intake source is also provided
- without artifact flags, lookup prints the matched row and stops
- with artifact flags, lookup becomes artifact generation from the stored row

### Update

Update mode is synthesized when `--id` is combined with:

- manual update flags such as `--status`, `--notes`, `--score`, `--set-*`
- or an AI refresh source (`--url`, `--paste`, `--jdfile`, optionally with `--ai`)

Rules:

- manual update fields require `--id`
- manual field updates and AI refresh cannot be mixed
- `--status` must normalize to one of:
  - `Applied`
  - `Discarded`
  - `Evaluated`
  - `Interview`
  - `Offer`
  - `Rejected`
  - `Responded`
  - `SKIP`
- `--id N --status X` without explicit update mode still updates the row
- `--id N --set-archetype A` persists the archetype and may continue to artifact generation if requested

### Inventory / maintenance

Supported commands:

- `--set-skill NAME --level LEVEL`
- `--set-capability NAME --level LEVEL`
- `--set-qualification NAME --met {yes|partial|no}`
- `--rename NEW_NAME` with one of the inventory selectors above
- `--rescore-all`

Rules:

- `--set-skill` and `--set-capability` require `--level` or `--rename`
- `--set-qualification` requires `--met` or `--rename`
- levels accept `0..5` or named values:
  - `none`
  - `exposure`
  - `basic`
  - `intermediate`
  - `advanced`
  - `expert`
- `--rescore-all` is a standalone maintenance command

## Artifact behavior

Artifact flags:

- `--resume`
- `--coverletter`
- `--email`

Supporting flags:

- `--archetype ARCHETYPE` overrides archetype during artifact generation
- `--no-open` suppresses auto-opening generated files
- `--dry-run` prints planned actions without creating files

Current artifact outputs:

- resume:
  - `<candidate>-resume.docx`
  - `<candidate>-resume.pdf`
  - `resume-metadata.json`
- cover letter:
  - `<candidate>-coverletter.docx`
  - `<candidate>-coverletter.pdf` when conversion succeeds
- email:
  - `<candidate>-email.txt`

## Behavior order

1. Parse CLI and validate mode constraints.
2. Run `--rescore-all` or inventory commands immediately when requested.
3. Resolve update, intake, or lookup source.
4. For lookup without artifact flags, print the matched row and stop.
5. Build the active record:
   - AI-assisted or deterministic extraction for new intake
   - DB-backed record for existing roles
   - manual record for `--minimal`
6. Build the output directory path:
   - `output/{company-slug}-{role-slug}-{YYYY-MM-DD}/`
7. If not `--dry-run`:
   - write `jd.md` when JD text exists
   - log or update DB rows when enabled
   - prompt for duplicate handling on new JD-backed intake
   - prompt to review unmatched requirement candidates
8. Generate requested artifacts.
9. Write `metadata.json`.
10. Print the final summary.

## `metadata.json`

Common top-level keys:

- `ai_provider`
- `source`
- `company`
- `role`
- `date`
- `location`
- `archetype`
- `keywords`
- `output_dir`
- `compensation`
- `work_model`
- `jd_id`
- `resume`
- `coverletter`
- `email`
- `db`

Additional keys appear when available:

- `analysis`
- `analysis_input`

Resume metadata is summarized into `metadata.json` when `resume-metadata.json` exists, including selection explainability for:

- profile
- layout
- education
- certifications
- experience
- derived features

## Filesystem output contract

When not in `--dry-run`, always create the output directory and write:

- `metadata.json`

Write conditionally:

- `jd.md` for JD-backed intake
- resume files when `--resume` succeeds
- cover letter files when `--coverletter` succeeds
- email text when `--email` succeeds

## Known interactive behavior

The script still contains interactive prompts in several paths:

- `--paste` reads until a line containing only `END`
- duplicate JD-backed intake prompts whether to create, update, or cancel
- new logged roles may prompt for found-via / apply-method metadata
- `general` archetype intake may prompt for an override
- unmatched requirement candidates may be reviewed and added to inventory interactively

Those prompts are part of the current contract and should be preserved or deliberately redesigned, not treated as incidental behavior.
