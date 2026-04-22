# Job-Logger

`Job-Logger` helps you keep your job search in one place instead of scattering it across notes, tabs, drafts, and half-finished documents.

Paste in a job post, track where you applied, and generate tailored materials when you need them. It is built for real day-to-day job hunting, not for looking impressive in a demo. ✨

## Features

- `🧾 Job intake`: save a role from a URL, pasted text, a file, or a quick manual entry
- `📌 Application tracking`: keep status, company, role, notes, and follow-up details together
- `📄 Tailored resumes`: generate resumes matched to a specific role
- `✉️ Application materials`: draft cover letters and outreach emails without starting from scratch every time
- `🗣️ Interview answers`: keep reusable answers to common questions and tailor them for a specific role
- `🎯 Repeatable workflow`: keep a consistent process for reviewing, scoring, and updating opportunities
- `🤖 Optional AI help`: use AI when you want it, skip it when you do not

## Quick Start

### 1. Run setup

```bash
python setup.py
```

This walks you through the first-time setup and prepares the project for your own job search.

### 2. Check the main CLI

```bash
python log_job.py --help
```

### 3. Verify everything looks healthy

```bash
npm run sync-check
```

## Common Commands

### Add or review a role

```bash
python log_job.py --ai none --jdfile samples/jd.md --resume
python log_job.py --ai claude --paste
python log_job.py --ai codex --url <url> --resume --coverletter
python log_job.py --minimal --company "Example Games" --role "Software Engineer"
```

### Look up something you already logged

```bash
python log_job.py --id 42
python log_job.py --company "Example Games" --role "Backend Engineer"
python log_job.py --random --resume
```

### Update a job entry

```bash
python log_job.py --id 42 --status Applied
python log_job.py --id 42 --set-archetype backend
python log_job.py --id 42 --email
```

### Work on interview answers

```bash
python answers.py --list
python answers.py --question why-this-role --id 42 --ai codex
python answers.py --all
```

## What You Get

Generated files are written to `output/`.

That usually includes things like:

- role notes and metadata
- a tailored resume
- a cover letter
- an outreach email
- saved interview answers

## Notes

- `Job-Logger` is designed for local use, so your job-search material stays on your machine
- some naming is still being cleaned up from older versions of the project
