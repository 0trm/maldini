# maldini_dbt

dbt project for Maldini Stats. Transforms raw BigQuery tables into scored, aggregated mart tables consumed by the dashboard.

## Models

```
raw.transcripts
raw.predictions_extracted
raw.match_results
        │
        ▼ staging/
stg_transcripts         — typed + cleaned transcripts
stg_predictions         — typed + cleaned predictions
stg_match_results       — typed + cleaned match results
        │
        ▼ intermediate/
int_predictions_with_legs     — resolves two-legged tie structure
int_predictions_matched       — joins predictions with match results
        │
        ▼ marts/
fct_predictions               — one row per scored prediction (Brier score computed here)
mart_monthly_scores           — monthly avg Brier + 3-month rolling average
mart_competition_summary      — per-competition avg Brier and prediction count
mart_scores_summary           — single headline row: all-time avg, accuracy %, superforecaster flag
```

## Usage

```bash
cd maldini_dbt
source ../venv/bin/activate

dbt run        # rebuild all mart tables
dbt test       # run schema + data tests
dbt run --select mart_scores_summary   # rebuild a single model
```

## Brier score formula

3-outcome (league matches):
```
(pH - rH)² + (pD - rD)² + (pA - rA)²   divided by 3
```

2-outcome (knockout matches, `pred_draw_pct = 0`):
```
(pH - rH)² + (pA - rA)²   divided by 2
```

Superforecaster threshold: avg Brier **< 0.20** over 100+ predictions.
Naive baseline (3-outcome): **0.2222**. Bookmaker benchmark: **~0.19**.
