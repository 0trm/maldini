# Data Format

Maldini Stats stores all state in two files inside the repo:

- `data/videos.csv` -- input: the list of YouTube videos to process
- `data/predictions.parquet` -- output: one row per prediction (scored or pending)

A third file, `data/results_overrides.csv`, is optional and lets you supply match scorelines manually for predictions TheSportsDB cannot auto-match.

---

## Input: `data/videos.csv`

| Column | Required | Description |
|---|---|---|
| `video_url` | Yes | Full YouTube URL. Both `youtube.com/watch?v=...` and `youtu.be/...` formats accepted. |

Example:

```csv
video_url
https://www.youtube.com/watch?v=EXAMPLE1234A
https://www.youtube.com/watch?v=EXAMPLE5678B
https://youtu.be/EXAMPLE9ABCD
```

To add new videos: append rows. `maldini.pipeline` skips `video_id`s already represented in `data/predictions.parquet`.

---

## Output: `data/predictions.parquet`

One row per prediction. Pending predictions (match not played yet, no TheSportsDB match, or a level knockout without a `winner` override) have a null `brier_score` and are retried on every run.

| Column | Type | Description |
|---|---|---|
| `prediction_id` | string | Stable identifier: `<video_id>_<match index>`, e.g. `EXAMPLE1234A_000`. |
| `video_id` | string | YouTube video ID the prediction was extracted from. |
| `publish_date` | date | Date the video was published. |
| `match_date` | date | Date of the actual match (nullable for predictions without an explicit date). |
| `home_team` | string | Home team name as extracted. |
| `away_team` | string | Away team name as extracted. |
| `competition` | string | League or cup name. |
| `match_type` | string | `single` or `knockout`. Knockout matches use a 2-outcome Brier formula. |
| `leg_number` | int32 | Reserved for the leg of a two-leg tie; the pipeline leaves it null. |
| `pred_home_win_pct` | int32 | Maldini's predicted probability of home win (0-100). |
| `pred_draw_pct` | int32 | Predicted probability of draw (0 for knockout matches). |
| `pred_away_win_pct` | int32 | Predicted probability of away win. |
| `actual_result` | string | `H`, `D`, or `A`. For a knockout, who went through (`H` or `A`). |
| `home_goals` | int32 | Final score. |
| `away_goals` | int32 | Final score. |
| `brier_score` | float64 | Brier score (lower is better; 0 = perfect). |
| `fetched_at` | timestamp | When the result was fetched. |

Brier formula:

- **3-outcome** (league): `((p_home - I_home)² + (p_draw - I_draw)² + (p_away - I_away)²) / 3`
- **2-outcome** (knockout, after renormalising home + away to sum to 1): `((p_home - I_home)² + (p_away - I_away)²) / 2`

---

## Optional: `data/results_overrides.csv`

When TheSportsDB cannot match a prediction (unusual competition name, ET/penalties scoreline, etc.), supply the result manually. `maldini.pipeline` reads this file before computing Brier scores.

| Column | Required | Description |
|---|---|---|
| `prediction_id` | Yes | From a `python -m maldini.pipeline --list-pending` run. |
| `home_goals` | Yes | Integer. |
| `away_goals` | Yes | Integer. |
| `match_date` | No | `YYYY-MM-DD`. Falls back to the prediction's stored date. |
| `match_type` | No | `single` or `knockout`. Provide to override an incorrect extraction. |
| `winner` | No | `H` or `A`: who went through when a knockout ended level (aggregate tie, penalties). Required for those rows to be scored. |

`actual_result` (H/D/A) is derived from goals automatically, unless `winner` is set.

Example:

```csv
prediction_id,home_goals,away_goals,match_date,winner
EXAMPLE1234A_000,2,1,2026-04-12,
EXAMPLE1234A_001,0,0,,
EXAMPLE5678B_002,1,1,2026-04-15,A
```

(An empty `match_date` falls back to the prediction's stored date. The last row is a knockout level after extra time that the away side won on penalties.)
