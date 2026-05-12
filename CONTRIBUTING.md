# Contributing

Thanks for your interest in Maldini Stats. The notes below cover conventions and where to put new code.

## Local setup

See [docs/SETUP.md](docs/SETUP.md) for how to get the project running end-to-end.

## Code conventions

- **Python**: PEP 8 with type hints where present. `python-dotenv` for env loading. Keep functions small -- `pipeline.py` and `render.py` should read top-to-bottom.
- **DuckDB SQL**: `snake_case` for columns. Inline SQL inside `pipeline.py` and `render.py` as CTE chains; one query per logical step (extract, score, summarise).
- **Comments**: only when the WHY is non-obvious. Don't describe what the code does; well-named identifiers cover that.
- **No emojis** in code, commit messages, or docs.

## Where to add things

| You want to add... | Put it in... |
|---|---|
| A new external data source | A new function in `pipeline.py` (or a sibling module imported by it) |
| A new derived metric | A new CTE in the DuckDB SQL in `render.py` |
| A new dashboard section | A new Jinja partial under `templates/` + a section entry in `render.py` |
| A new language translation | A new entry in `render.py`'s translations dict |

## Tests

Unit tests live in `tests/`. External APIs (YouTube, Anthropic, TheSportsDB) are mocked, so tests run offline with no credentials. `pytest` is not in `requirements.txt` -- install it (`uv pip install pytest`) and run `pytest` from the repo root before opening a PR.

## Commits and PRs

- Imperative subject, under 70 characters. Example: `Add knockout-leg handling to Brier CTE`.
- One logical change per PR. Reference the affected file in the title.
- If you change `requirements.txt`, call it out in the PR body so reviewers re-install.
