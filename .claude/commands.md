# CLI Usage

```bash
python log_job.py --help
python log_job.py --ai claude --paste
python log_job.py --ai codex --url <url> --resume --coverletter
python log_job.py --minimal --company "Example Games" --role "Software Engineer"
python log_job.py --id <n>
python log_job.py --company <name> --role <title>
python log_job.py --id <n> --email
python log_job.py --id <n> --status Applied
python log_job.py --set-skill Unity --level expert
python log_job.py --rescore-all
python log_job.py --report
python log_job.py --report --month 2026-04
```

Supported AI analyzers: `claude`, `codex`, `none`. `--ai none` skips AI extraction.
