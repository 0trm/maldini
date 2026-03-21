-- Adds leg_number for multi-leg ties using a window function.
-- A "tie" is identified by (video_id, home_team, away_team, competition).
-- leg_number is ordered by prediction_id within each tie group.

with predictions as (
    select * from {{ ref('stg_predictions') }}
),

with_legs as (
    select
        *,
        coalesce(
            leg_number,
            row_number() over (
                partition by video_id, home_team, away_team, competition
                order by prediction_id
            )
        ) as leg_number_computed

    from predictions
)

select
    prediction_id,
    video_id,
    video_url,
    publish_date,
    match_date,
    home_team,
    away_team,
    competition,
    match_type,
    leg_number_computed as leg_number,
    pred_home_win_pct,
    pred_draw_pct,
    pred_away_win_pct,
    pred_sum,
    pred_sum_valid,
    raw_quote,
    extracted_at

from with_legs
