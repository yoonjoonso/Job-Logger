# Notes

- The older `--existing` selector is gone; existing-role lookup is now `--id`, `--company`, or `--random`.
- Cover letter generation is active.
- Some scripts under `scripts/verify/` still reflect the legacy markdown tracker flow rather than the current SQLite-first pipeline.
- `.claude/settings.local.json` is local-only and should not be treated as a shared repo contract.
- Setup terminology: use `npm run setup` / `npm run db:setup`; "bootstrap" is deprecated.
- Git history still contains personal/generated artifacts from before the public-repo split. A history rewrite is needed before publication — do not assume history is clean.
