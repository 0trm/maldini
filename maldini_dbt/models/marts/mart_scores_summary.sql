-- Top-level scorecard: all-time Brier average, accuracy, superforecaster flag.
-- This replaces scores_summary.json as the dashboard's primary data source.

with fct as (
    select * from {{ ref('fct_predictions') }}
)

select
    count(*)                                as total_predictions,
    round(avg(brier_score), 4)              as all_time_avg_brier,

    -- Accuracy: highest-probability outcome was the actual outcome
    round(
        countif(
            (actual_result = 'H' and pred_home_win_pct >= greatest(pred_draw_pct, pred_away_win_pct))
            or (actual_result = 'D' and pred_draw_pct >= greatest(pred_home_win_pct, pred_away_win_pct))
            or (actual_result = 'A' and pred_away_win_pct >= greatest(pred_home_win_pct, pred_draw_pct))
        ) * 100.0 / count(*),
        2
    )                                       as accuracy_pct,

    -- Superforecaster threshold: all-time avg Brier < 0.20
    avg(brier_score) < 0.20                 as is_superforecaster,

    -- Benchmarks for reference
    0.2222                                  as naive_baseline_3outcome,
    0.25                                    as naive_baseline_2outcome,
    0.19                                    as bookmaker_benchmark

from fct
