# Maldini

[![tests](https://github.com/0trm/maldini/actions/workflows/tests.yml/badge.svg)](https://github.com/0trm/maldini/actions/workflows/tests.yml)

*Maldini* is one of Spain's most prominent football journalists. Every week on his YouTube channel [@mundomaldini](https://www.youtube.com/@mundomaldini) he makes explicit, probabilistic predictions about upcoming matches. This project captures every prediction, scores it objectively with a [Brier score](https://en.wikipedia.org/wiki/Brier_score), and surfaces the answer on a [site](https://0trm.dev/maldini/).

<img width="775" alt="maldini-youtube" src="https://github.com/user-attachments/assets/87c6d194-7d76-4261-be76-b031587856e7" />

---

## **Is Julio Maldonado ("Maldini") a superforecaster?**

A Brier score measures the accuracy of probabilistic predictions – **lower is better**, 0 is perfect.

| Benchmark | Brier Score |
|---|---|
| Naive baseline (guess 1/3 each outcome) | 0.222 |
| Betting markets | ~0.19 |
| **Superforecaster threshold** | **< 0.20** |
| Perfect forecaster | 0.00 |

Maldini earns the superforecaster badge only when his all-time average Brier score drops below 0.20 – and only once he has 100+ scored predictions for statistical reliability. 

> The project tracks **1,500 predictions** from 2022-Q4 to 2026-Q2. Final data from 11 May 2026.

---

## Analysis

Over all 1,500 predictions, Maldini's Brier score is **0.1916**. He clears the threshold: one person, performing at the level of the betting markets.

The [site](https://0trm.dev/maldini/) breaks it down by quarter, by competition and match by match, in English and Spanish.

---

## Architecture

```
                ┌──────────────────────────────────┐
                │  data/videos.csv                 │
                │  data/results_overrides.csv  (*) │
                └─────────────────┬────────────────┘
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                       maldini.pipeline                          │
│                                                                 │
│    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐     │
│    │ ingest  │───►│ extract │───►│ results │───►│ scoring │     │
│    └────┬────┘    └────┬────┘    └────┬────┘    └────┬────┘     │
│         ▼              ▼              ▼              ▼          │
│      YouTube         Claude        TheSportsDB     DuckDB       │
│      transcript      Haiku LLM     match data      Brier 2/3w   │
└─────────────────────────────────┬───────────────────────────────┘
                                  ▼
                ┌──────────────────────────────────────┐
                │  data/predictions.parquet            │
                │  one row per prediction; single      │
                │  source of truth, committed to git   │
                └─────────────────┬────────────────────┘
                                  ▼
                ┌──────────────────────────────────────┐
                │            maldini.render            │
                │  DuckDB CTEs  →  summary stats       │
                │  Jinja2       →  dist/index.html     │
                │                  dist/index.en.html  │
                └─────────────────┬────────────────────┘
                                  ▼
              dist/ (local) · docs/stats.html (published)

(*) Manual scoreline fixups for matches TheSportsDB can't auto-resolve.
Schedule: manual. .github/workflows/weekly.yml runs on workflow_dispatch;
its Sunday 08:00 UTC cron is switched off.
```

**Parquet is the single source of truth.** It lives in git, so every dashboard build is reproducible from a commit hash. The pipeline is idempotent – re-running it on the same `videos.csv` only processes new `video_id`s, and pending predictions (matches not yet played) are persisted with null results so the next run picks them up.

---

## Transformation

All SQL runs in **DuckDB** in-process, embedded inside `src/maldini/pipeline.py` (scoring) and `src/maldini/render.py` (summary stats).

Brier score variants:

- **3-outcome** (league matches): `((p_home - I_home)² + (p_draw - I_draw)² + (p_away - I_away)²) / 3`
- **2-outcome** (knockout, where `pred_draw_pct = 0`): renormalise home + away to sum to 1, then `((p_home - I_home)² + (p_away - I_away)²) / 2`

Summary statistics (all-time average, accuracy, monthly trend, competition breakdown, Brier distribution) are computed by `maldini.render` from the parquet at render time – a few short CTEs, no separate materialised tables.

---

## Stack

| Layer | Technology |
|---|---|
| Pipeline | Python package (`src/maldini/`) |
| Transformations | DuckDB (in-process SQL) |
| Storage | Parquet file in git (`data/predictions.parquet`) |
| LLM | Anthropic Claude Haiku |
| External APIs | YouTube Data API v3, youtube-transcript-api, TheSportsDB |
| Dashboard | Jinja2 → static HTML in `dist/`; the published page is the final snapshot, styled like [0trm.dev](https://0trm.dev) |
| Schedule | GitHub Actions, manual trigger (weekly cron switched off) |
| Hosting | GitHub Pages, serving `docs/` (the final May 2026 snapshot in `docs/stats.html`) |
| CI | GitHub Actions runs `pytest` on every push |

---

## How to run locally

For a full step-by-step guide, see [docs/SETUP.md](docs/SETUP.md). The summary below is enough to get going.

### Prerequisites

```bash
git clone https://github.com/0trm/maldini.git
cd maldini
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
cp .env.example .env   # fill in YOUTUBE_API_KEY and ANTHROPIC_API_KEY
```

### Run

```bash
# 1. Ingest, extract, fetch results, score
python -m maldini.pipeline --file data/videos.csv

# 2. Generate static HTML from the parquet
python -m maldini.render

# 3. View
open dist/index.html
```

Equivalently, the package exposes `maldini-pipeline` and `maldini-render` console scripts. Run `pytest` for the unit tests.

To add new videos: append rows to `data/videos.csv` and re-run.

---

## Design notes

- **Parquet lives in git** – every dashboard build is reproducible from a commit hash. If scoring logic changes, rebuild from `data/videos.csv`.
- **DuckDB for everything SQL** – no warehouse, no credentials, no quotas; the whole pipeline runs on a laptop or a free-tier GitHub Actions runner in under a minute.
- **Fuzzy team matching** – normalisation strips accents, common prefixes (`Real`, `Atlético`), and applies Spanish→English word substitutions before substring matching against TheSportsDB results.
- **No-date window** – predictions without a `match_date` use a 45-day window from `publish_date` to find the matching fixture.
- **No-draw handling** – when `pred_draw_pct == 0`, a 2-outcome Brier formula is applied automatically.
- **Level knockouts stay pending** – a two-outcome forecast is about who goes through, so a tie level on aggregate or a match decided on penalties is never scored as a draw or on the first leg. It waits for a `winner` in `data/results_overrides.csv`.
- **Pending rows are retried** – every run re-resolves predictions without a score, so matches played since the last run and new overrides get picked up.

---

## Documentation

- [docs/SETUP.md](docs/SETUP.md) – step-by-step local setup and verification
- [docs/DATA_FORMAT.md](docs/DATA_FORMAT.md) – input/output schemas for the pipeline

---

## License

MIT

The favicon is the crystal ball emoji from [Noto Emoji](https://github.com/googlefonts/noto-emoji) (Apache 2.0).
