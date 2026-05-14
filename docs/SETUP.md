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
git clone https://github.com/tomas-ravalli/maldini-stats.git
cd maldini-stats
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

The pipeline is idempotent -- videos already represented in the parquet are skipped.

## 4. Render the dashboard

```bash
python -m maldini.render
```

`maldini.render` reads `data/predictions.parquet`, runs DuckDB SQL to compute summary statistics, and renders Jinja2 templates to `dist/index.html` (bilingual EN/ES).

## 5. View the dashboard

```bash
open dist/index.html
# or, if you hit relative-path issues with file://
python -m http.server -d dist 8000
```

## 6. Automate weekly

The repo ships `.github/workflows/weekly.yml`. The workflow:

1. Runs `python -m maldini.pipeline --file data/videos.csv` every Sunday at 08:00 UTC.
2. Runs `python -m maldini.render` to regenerate `dist/`.
3. Commits the updated `data/predictions.parquet` and `dist/` back to `main`.
4. GitHub Pages auto-publishes `dist/`.

Required repository secrets:

- `YOUTUBE_API_KEY`
- `ANTHROPIC_API_KEY`

To add new videos: append rows to `data/videos.csv` and push. The next scheduled run picks them up.

## 7. Verify

After step 4:

- `dist/index.html` exists and opens in a browser.
- `data/predictions.parquet` contains the expected row count:

```bash
python -c "import duckdb; print(duckdb.sql(\"SELECT COUNT(*) FROM 'data/predictions.parquet'\"))"
```

## Troubleshooting

- **`KeyError: 'YOUTUBE_API_KEY'`** -- `.env` is missing or not populated. `maldini.pipeline` calls `dotenv.load_dotenv()` from the repo root.
- **"Could not fetch transcript"** -- the video has captions disabled. The pipeline skips such videos with a warning.
- **Dashboard styling looks broken locally** -- some templates load CSS via relative paths that browsers block on `file://`. Serve via `python -m http.server -d dist` instead.
