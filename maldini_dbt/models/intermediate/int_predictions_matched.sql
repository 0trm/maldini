-- Joins predictions to match results.
-- Unscored predictions (no result yet) are kept with NULL result columns.

with predictions as (
    select * from {{ ref('int_predictions_with_legs') }}
),

results as (
    select * from {{ ref('stg_match_results') }}
)

select
    p.prediction_id,
    p.video_id,
    p.publish_date,
    p.match_date,
    p.home_team,
    p.away_team,
    p.competition,
    p.match_type,
    p.leg_number,
    p.pred_home_win_pct,
    p.pred_draw_pct,
    p.pred_away_win_pct,
    p.pred_sum_valid,
    p.raw_quote,
    p.extracted_at,

    r.actual_result,
    r.home_goals,
    r.away_goals,
    r.fetched_at,

    case
        when r.prediction_id is not null then 'scored'
        else 'pending'
    end as result_status

from predictions p
left join results r on p.prediction_id = r.prediction_id
