# Setup Guide

End-to-end setup for running Maldini Stats locally. For project architecture, see the [README](../README.md).

## 1. Prerequisites

| Tool | Why | Install |
|---|---|---|
| `uv` | Python env + dependencies | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| YouTube Data API v3 key | Video metadata + transcript | <https://console.cloud.google.com/apis/credentials> |
| Anthropic API key | Claude Haiku prediction extraction | <https://console.anthropic.com/settings/keys> |

## 2. Clone and install

```bash
git clone https://github.com/0trm/maldini.git
cd maldini
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"
cp .env.example .env
# edit .env and fill in YOUTUBE_API_KEY and ANTHROPIC_API_KEY
```

## 3. Run the pipeline

`maldini.pipeline` ingests one or more YouTube videos, extracts predictions via Claude, fetches match results, computes Brier scores, and appends to `data/predictions.parquet`.

```bash
# single video
python -m maldini.pipeline --video-url "https://www.youtube.com/watch?v=..."

# or a CSV of video URLs
python -m maldini.pipeline --file data/videos.csv
```

The pipeline is idempotent -- videos already represented in the parquet are skipped, and stored predictions without a score are retried.

## 4. Render the dashboard

```bash
python -m maldini.render
```

`maldini.render` reads `data/predictions.parquet`, runs DuckDB SQL to compute summary statistics, and renders the Jinja2 template to `dist/index.html` (Spanish) and `dist/index.en.html` (English), each linking to the other.

## 5. View the dashboard

```bash
open dist/index.html
```

## 6. Automate weekly

The repo ships `.github/workflows/weekly.yml`. Its Sunday 08:00 UTC cron is commented out, so it runs only when triggered from the Actions tab (`workflow_dispatch`). Uncomment the `schedule` block to run it weekly. The workflow:

1. Runs `python -m maldini.pipeline --file data/videos.csv`.
2. Runs `python -m maldini.render` to regenerate `dist/`.
3. Commits the updated `data/predictions.parquet` and `dist/` back to `main`.

GitHub Pages serves `docs/`, not `dist/`: the published page is `docs/stats.html`, the final snapshot styled like 0trm.dev. A fresh render stays in `dist/`; copying it over `docs/stats.html` would replace that page with the plain dashboard.

Required repository secrets:

- `YOUTUBE_API_KEY`
- `ANTHROPIC_API_KEY`

To add new videos: append rows to `data/videos.csv`, push, and run the workflow from the Actions tab (or re-enable the schedule).

## 7. Verify

After step 4:

- `dist/index.html` exists and opens in a browser.
- `data/predictions.parquet` contains the expected row count:

```bash
python -c "import duckdb; print(duckdb.sql(\"SELECT COUNT(*) FROM 'data/predictions.parquet'\"))"
```

## Troubleshooting

- **`RuntimeError: YOUTUBE_API_KEY not set in .env`** -- `.env` is missing or not populated. `maldini.pipeline` loads `.env` from the repo root.
- **`[WARN] transcript unavailable (captions disabled); skipping`** -- the video has captions disabled. The pipeline skips it and carries on.
- **A prediction never gets scored** -- run `python -m maldini.pipeline --list-pending` to list predictions without a score, then add their result to `data/results_overrides.csv` (see [DATA_FORMAT.md](DATA_FORMAT.md)).
