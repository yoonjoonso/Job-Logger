# Codex Local Setup Prompt

Use this prompt with Codex after cloning the repo if you want Codex to help create your own local candidate workflow files.

This prompt is for local use. It should generate or update user-specific files locally, not turn personal data into public repo content.

## Prompt

```md
You are helping set up this Career-Ops repo for a new local user.

Goals:
- learn the repo structure and setup flow
- keep all personal data local-only
- generate local user-specific setup files that mirror the intended workflow shape
- avoid changing public docs or templates unless necessary for correctness

Constraints:
- do not assume the repo's tracked candidate data is safe or canonical for this user
- do not reuse any personal identity data that may still exist in old files
- treat `config/profile.example.yml` as the public template, not `config/profile.yml`
- treat `docs/public-private-boundaries.md` as the boundary contract
- create or update only local/user-specific files unless the task explicitly requires a public repo change
- prefer deterministic setup and structured files over freeform notes

First steps:
1. Read `README.md`
2. Read `docs/public-private-boundaries.md`
3. Read `.codex/README.md`
4. Read `.codex/resume-workflow/README.md`
5. Read `scripts/setup.py`
6. Inspect `.gitignore` to understand which files are local-only

Then do this work:
1. Explain which files in this repo are public templates vs local private state.
2. Check whether the following local files exist:
   - `config/profile.yml`
   - `data/cv.md`
   - `data/cv-summary.md`
   - `data/applications.md`
   - `data/evaluated.md`
   - `user-input/resume-examples/`
   - `.codex/resume-workflow/data/candidate/`
   - `.codex/resume-workflow/data/jobs/`
   - `.codex/resume-workflow/data/builds/`
3. If missing, create local-only starter files in the expected locations.
4. Ask the user for the minimum source inputs needed to personalize setup:
   - name / display name
   - email / phone / location
   - target role directions
   - resume or CV source files
   - whether they want deterministic setup only or Codex-assisted drafting
5. Use the user's source material to generate draft local workflow files such as:
   - `config/profile.yml`
   - `data/cv.md`
   - `.codex/resume-workflow/data/candidate/facts.yml`
   - `.codex/resume-workflow/data/candidate/profiles.yml`
6. If resume examples exist, use them as source material for draft structured files, but mark unclear facts for review instead of inventing details.
7. Keep generated outputs clearly labeled as draft where appropriate.

Expected output behavior:
- summarize what you found
- identify which local files were created or updated
- identify any missing information still needed from the user
- do not commit
- do not rewrite history
- do not publish personal data into public repo files

If the repo still contains tracked personal files from an older user:
- do not trust them as source material for the new user
- treat them as legacy/private residue
- avoid copying any values from them unless the current user explicitly confirms they are theirs and should be reused
```

## Intended Use

This works best when the user has one of:

- a current resume in `.docx`, `.pdf`, `.md`, or `.txt`
- a LinkedIn-export-style summary
- a manually filled-out profile plus a few work-history bullets

## Recommended Follow-Up

After using the prompt:

1. run `npm run setup`
2. run `npm run sync-check`
3. review local generated candidate files
4. only then use resume/cover letter generation
