# Maldini Stats

Maldini is one of Spain's most prominent football journalists. Every week on his YouTube channel [@mundomaldini](https://www.youtube.com/@mundomaldini) he makes explicit, probabilistic predictions about upcoming matches. This project captures every prediction, scores it objectively with a [Brier score](https://en.wikipedia.org/wiki/Brier_score), and surfaces the answer in a live dashboard.

**→ [Live dashboard](https://0trm.blog/maldini-stats/)**

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

> The project tracks **1,500 predictions** from 2022-Q4 to 2026-Q2.

---

## Architecture

```
data/videos.csv  ──┐
                   ▼
              ┌──────────────┐
              │ pipeline.py  │  fetch transcript      (youtube-transcript-api)
              │              │  extract predictions   (Claude Haiku)
              │              │  fetch match results   (TheSportsDB)
              │              │  compute Brier scores  (DuckDB SQL)
              └──────┬───────┘
                     ▼
       data/predictions.parquet   (one row per scored prediction)
                     │
                     ▼
              ┌──────────────┐
              │ render.py    │  summary stats   (DuckDB SQL)
              │              │  bilingual HTML  (Jinja2, EN/ES)
              └──────┬───────┘
                     ▼
              dist/index.html   →   served by GitHub Pages
```

**Parquet is the single source of truth.** It lives in git, so every dashboard build is reproducible from a commit hash. `pipeline.py` is idempotent – re-running it on the same `videos.csv` only processes new `video_id`s.

Schedule: **GitHub Actions** runs a weekly cron (Sundays 08:00 UTC) that executes the pipeline + render and commits the artifacts back to `main`. GitHub Pages auto-publishes `dist/`. No infrastructure to maintain.

---

## Transformation

All SQL runs in **DuckDB** in-process, embedded inside `pipeline.py` (scoring) and `render.py` (summary stats). No warehouse, no credentials, no quotas.

Brier score variants:

- **3-outcome** (league matches): `((p_home - I_home)² + (p_draw - I_draw)² + (p_away - I_away)²) / 3`
- **2-outcome** (knockout, where `pred_draw_pct = 0`): renormalise home + away to sum to 1, then `((p_home - I_home)² + (p_away - I_away)²) / 2`

Summary statistics (all-time average, accuracy, monthly trend, competition breakdown, Brier distribution) are computed by `render.py` from the parquet at render time – a few short CTEs, no separate materialised tables.

---

## Stack

| Layer | Technology |
|---|---|
| Pipeline | Python (`pipeline.py`) |
| Transformations | DuckDB (in-process SQL) |
| Storage | Parquet file in git (`data/predictions.parquet`) |
| LLM | Anthropic Claude Haiku |
| External APIs | YouTube Data API v3, youtube-transcript-api, TheSportsDB |
| Dashboard | Jinja2 → static HTML |
| Schedule | GitHub Actions (weekly cron) |
| Hosting | GitHub Pages |

---

## How to run locally

For a full step-by-step guide, see [docs/SETUP.md](docs/SETUP.md). The summary below is enough to get going.

### Prerequisites

```bash
git clone https://github.com/tomas-ravalli/maldini-stats.git
cd maldini-stats
uv venv && source .venv/bin/activate
uv pip install -r requirements.txt
cp .env.example .env   # fill in YOUTUBE_API_KEY and ANTHROPIC_API_KEY
```

### Run

```bash
# 1. Ingest, extract, fetch results, score
python pipeline.py --file data/videos.csv

# 2. Generate static HTML from the parquet
python render.py

# 3. View
open dist/index.html
```

To add new videos: append rows to `data/videos.csv` and re-run.

---

## Design notes

- **Parquet lives in git** – every dashboard build is reproducible from a commit hash. If scoring logic changes, rebuild from `data/videos.csv`.
- **DuckDB for everything SQL** – no warehouse, no credentials, no quotas; the whole pipeline runs on a laptop or a free-tier GitHub Actions runner in under a minute.
- **Fuzzy team matching** – normalisation strips accents, common prefixes (`Real`, `Atlético`), and applies Spanish→English word substitutions before substring matching against TheSportsDB results.
- **No-date window** – predictions without a `match_date` use a 45-day window from `publish_date` to find the matching fixture.
- **No-draw handling** – when `pred_draw_pct == 0`, a 2-outcome Brier formula is applied automatically.
- **Data scope** – 2022-Q4 onwards; earlier data excluded due to quality and availability.

---

## Documentation

- [docs/SETUP.md](docs/SETUP.md) – step-by-step local setup and verification
- [docs/DATA_FORMAT.md](docs/DATA_FORMAT.md) – input/output schemas for the pipeline
- [CONTRIBUTING.md](CONTRIBUTING.md) – coding conventions and where to add new logic

---

## License

MIT

<br>

*Built ~~by~~ with AI.* <br>
© 2026 trm
