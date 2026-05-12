# Data Format

Maldini Stats stores all state in two files inside the repo:

- `data/videos.csv` -- input: the list of YouTube videos to process
- `data/predictions.parquet` -- output: one row per scored prediction

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

To add new videos: append rows. `pipeline.py` skips `video_id`s already represented in `data/predictions.parquet`.

---

## Output: `data/predictions.parquet`

One row per scored prediction.

| Column | Type | Description |
|---|---|---|
| `prediction_id` | string | Stable identifier (video_id + match index). |
| `video_id` | string | YouTube video ID the prediction was extracted from. |
| `publish_date` | date | Date the video was published. |
| `match_date` | date | Date of the actual match (nullable for predictions without an explicit date). |
| `home_team` | string | Home team name as extracted. |
| `away_team` | string | Away team name as extracted. |
| `competition` | string | League or cup name. |
| `match_type` | string | `single` or `knockout`. Knockout matches use a 2-outcome Brier formula. |
| `leg_number` | int32 | 1 or 2 for knockout ties; null for league matches. |
| `pred_home_win_pct` | int32 | Maldini's predicted probability of home win (0-100). |
| `pred_draw_pct` | int32 | Predicted probability of draw (0 for knockout matches). |
| `pred_away_win_pct` | int32 | Predicted probability of away win. |
| `actual_result` | string | `H`, `D`, or `A`. |
| `home_goals` | int32 | Final score. |
| `away_goals` | int32 | Final score. |
| `brier_score` | float64 | Brier score (lower is better; 0 = perfect). |
| `fetched_at` | timestamp | When the result was fetched. |

Brier formula:

- **3-outcome** (league): `((p_home - I_home)² + (p_draw - I_draw)² + (p_away - I_away)²) / 3`
- **2-outcome** (knockout, after renormalising home + away to sum to 1): `((p_home - I_home)² + (p_away - I_away)²) / 2`

---

## Optional: `data/results_overrides.csv`

When TheSportsDB cannot match a prediction (unusual competition name, ET/penalties scoreline, etc.), supply the result manually. `pipeline.py` reads this file before computing Brier scores.

| Column | Required | Description |
|---|---|---|
| `prediction_id` | Yes | From a `pipeline.py --list-pending` run. |
| `home_goals` | Yes | Integer. |
| `away_goals` | Yes | Integer. |
| `match_date` | No | `YYYY-MM-DD`. Falls back to the prediction's stored date. |
| `match_type` | No | `single` or `knockout`. Provide to override an incorrect extraction. |

`actual_result` (H/D/A) is derived from goals automatically.

Example:

```csv
prediction_id,home_goals,away_goals,match_date
pred_0001,2,1,2026-04-12
pred_0002,0,0,
pred_0003,3,2,2026-04-13
```

(An empty `match_date` falls back to the prediction's stored date.)
