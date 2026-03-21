-- Per-competition Brier score breakdown.

with fct as (
    select * from {{ ref('fct_predictions') }}
)

select
    competition,
    count(*)                   as prediction_count,
    round(avg(brier_score), 4) as avg_brier_score,
    round(min(brier_score), 4) as min_brier_score,
    round(max(brier_score), 4) as max_brier_score,

    -- Accuracy: prediction for the actual outcome had highest probability
    round(
        countif(
            (actual_result = 'H' and pred_home_win_pct >= greatest(pred_draw_pct, pred_away_win_pct))
            or (actual_result = 'D' and pred_draw_pct >= greatest(pred_home_win_pct, pred_away_win_pct))
            or (actual_result = 'A' and pred_away_win_pct >= greatest(pred_home_win_pct, pred_draw_pct))
        ) / count(*),
        4
    ) as accuracy

from fct
group by competition
order by prediction_count desc
