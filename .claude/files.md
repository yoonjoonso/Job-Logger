# Key Files

- `log_job.py` - main entry point
- `scripts/generate-resume.py` - DOCX/PDF resume generation plus explainability metadata
- `scripts/generate-coverletter.py` - DOCX/PDF cover letter generation
- `scripts/generate-email.py` - outreach email text generation
- `scripts/db/db-init.mjs` - schema init and seed/update entry point
- `data/job-log.db` - SQLite canonical data store
- `config/profile.yml` - profile, scoring, and preference config (local-only, not committed)
- `config/profile.example.yml` - public template for new users

# Output

Generated files land in `output/{company-slug}-{role-slug}-{YYYY-MM-DD}/`.

- `metadata.json`
- `jd.md` for JD-backed intake
- `<candidate>-resume.docx` / `<candidate>-resume.pdf`
- `resume-metadata.json`
- `<candidate>-coverletter.docx` / `<candidate>-coverletter.pdf`
- `<candidate>-email.txt`
