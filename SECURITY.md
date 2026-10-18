# Security Policy

## Supported versions

Security fixes are applied to the latest code on the `main` branch.

## Reporting a vulnerability

Please **do not open a public issue** for security problems.

Report vulnerabilities privately, using either:

- GitHub's [private vulnerability reporting](https://github.com/tewarishishir/GraphRAGGoldenData/security/advisories/new), or
- Email: **tewarishishir@gmail.com** with the subject line `[SECURITY] GraphRAGGoldenData`.

Please include a description of the issue, steps to reproduce, and the potential impact. You can expect an acknowledgement within 7 days. We'll keep you updated on the fix and credit you in the release notes unless you prefer to stay anonymous.

## Secrets and data

- Never commit `.env` files, Neo4j passwords, API keys, or other credentials. `.env` is git-ignored; use `.env.example` as a template.
- Never commit real or personally identifiable data. The bundled CSVs are synthetic (see `scripts/generate_synthetic_data.py`).
- If you accidentally committed a secret, rotate it immediately — removing it in a later commit does not remove it from git history.
- The web UI (`app.py`) runs arbitrary Cypher against the configured database and is intended for local development. Do not expose it to untrusted networks without adding authentication and read-only database credentials.
