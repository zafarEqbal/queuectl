# QueueCTL (Node.js)

QueueCTL is a CLI-based background job queue built in **Node.js**.  
It supports persistent jobs, multiple worker processes, retries with exponential backoff, and a Dead Letter Queue (DLQ).

---

**Working CLI Demo** [Link](https://drive.google.com/file/d/1l7259F5B-uSzRnCnJUvHzAfOCsf8EzRt/view?usp=sharing)

---
## Tech Stack

- Node.js (CommonJS)
- better-sqlite3 (SQLite-based embedded database)

## Features

- CLI tool: `queuectl` (or `node queuectl.js`)
- Persistent job storage (SQLite, ~/.queuectl/queuectl.db)
- Multiple worker processes
- Automatic retries with exponential backoff
- Dead Letter Queue (DLQ) for permanently failed jobs
- Configurable max retries, backoff base, poll interval
- Graceful worker shutdown via a shared stop flag
- Minimal test script to validate core flows

## Project Layout

```text
.
├── src
│   ├── core.js    # Job model, DB, worker logic
│   ├── cli.js     # CLI argument parsing and subcommands
│   └── worker.js  # Worker process entrypoint
├── queuectl.js     # Top-level CLI entry
├── test_script.js  # Simple end-to-end test harness
├── package.json
└── README.md
