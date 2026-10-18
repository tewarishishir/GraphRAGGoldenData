# Contributing to GraphRAGGoldenData

Thanks for your interest in contributing! Bug reports, feature ideas, documentation fixes, and code changes are all welcome.

By participating, you agree to follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## Development setup

```bash
git clone https://github.com/<your-fork>/GraphRAGGoldenData
cd GraphRAGGoldenData
python -m venv .venv
source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
cp .env.example .env        # fill in Neo4j credentials if you want to load the graph
```

## Running the pipeline on sample data

All data bundled in this repo is **synthetic**. To regenerate it and rebuild the golden records:

```bash
python scripts/generate_synthetic_data.py
python golden_records.py
```

To also load Neo4j (requires a running instance and `.env` credentials):

```bash
python run_pipeline.py
```

Use `python run_pipeline.py --golden-only` if you don't have Neo4j available.

## Data rules (important)

- **Never commit real, customer, employer, or personally identifiable data** (names, emails, phone numbers, addresses, internal IDs, revenue figures, etc.).
- If you need new sample data for a feature or test, extend `scripts/generate_synthetic_data.py` instead of adding hand-edited CSVs.
- Keep private data outside the repo, or in `data_private/` / `*.private.csv`, which are git-ignored.
- Never commit `.env` or any credentials.

## Workflow

1. Open an issue first for anything non-trivial so we can agree on the approach.
2. Fork the repo and create a branch from `main` (e.g. `feature/contact-project-links` or `fix/ask-timeout`).
3. Keep changes focused; one logical change per pull request.
4. Make sure `python scripts/generate_synthetic_data.py && python golden_records.py` still runs cleanly.
5. Update `README.md` or `docs/` when you change behavior.
6. Open a pull request and fill in the template.

## Developer Certificate of Origin (DCO)

This project uses the [Developer Certificate of Origin](https://developercertificate.org/). By signing off your commits, you certify that you wrote the code or otherwise have the right to submit it under the project's [MIT License](LICENSE).

Sign off each commit with `-s`:

```bash
git commit -s -m "Add contact-to-project assignment"
```

This adds a line like `Signed-off-by: Your Name <you@example.com>` to the commit message.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE).

## Using the project?

If your organization uses GraphRAGGoldenData, please consider adding yourself to [ADOPTERS.md](ADOPTERS.md).
