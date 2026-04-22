# Codex Resume Workflow

This folder documents the resume path managed by Codex inside this repo. It exists to keep resume tailoring notes, prompts, and intermediate Codex-only artifacts under `.codex` without changing or depending on anything in `.claude`.

## Purpose

Use this workflow to take a job description, tailor the resume against the repo's canonical CV, and produce a final PDF with the existing repo tooling.

## Source Of Truth

- `data/job-log.db` is the primary source for tracker data, skill signals, gaps, bullets, and stories. Query via `node query.mjs`. See `.codex/README.md` for the full interface.
- `data/cv.md` is the canonical CV and the main source for resume content generation. Read directly.
- `data/cv-summary.md` is only a quick-check aid for fast review and should not override `data/cv.md`.
- `config/profile.yml` is the canonical source for comp targets, archetype interest scores, and location policy.
- `generate-pdf.mjs` and `npm run pdf` produce the final PDF from the current resume content.

Local candidate facts, job snapshots, and generated build outputs under `.codex/resume-workflow/data/` are user-specific and should be treated as private unless explicitly sanitized.

## What Lives Here

Under `.codex/resume-workflow`, keep only Codex-side workflow material such as:

- intake notes for a target role
- tailoring prompts or templates
- intermediate working files used during resume adaptation
- small helper docs that explain the Codex process

Do not use this area to modify or mirror `.claude`.

## Expected Flow

1. Intake the job description or posting URL and capture the target role requirements.
2. Review `data/cv.md` first, using `data/cv-summary.md` only for quick checks.
3. Draft or refine the tailored resume content with Codex-side working files under `.codex/resume-workflow`.
4. Apply the approved changes to the resume content used for generation.
5. Run the repo PDF step to produce the final resume PDF.

Keep the workflow practical: `data/cv.md` stays canonical, `.codex/resume-workflow` holds the working process, and the repo's existing PDF generator handles output.
