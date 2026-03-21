-- One row per prediction with Brier score computed in SQL.
--
-- Brier score formula:
--   3-outcome (standard): (p_home - I_home)^2 + (p_draw - I_draw)^2 + (p_away - I_away)^2
--   2-outcome (knockout):  (p_home - I_home)^2 + (p_away - I_away)^2
--
-- Where p_* are predicted probabilities (0–1) and I_* are indicator variables (1 if outcome, else 0).

with matched as (
    select * from {{ ref('int_predictions_matched') }}
    where result_status = 'scored'
      and pred_sum_valid = true
),

scored as (
    select
        prediction_id,
        video_id,
        publish_date,
        match_date,
        home_team,
        away_team,
        competition,
        match_type,
        leg_number,
        pred_home_win_pct,
        pred_draw_pct,
        pred_away_win_pct,
        pred_home_win_pct / 100.0 as p_home,
        pred_draw_pct    / 100.0 as p_draw,
        pred_away_win_pct / 100.0 as p_away,
        actual_result,
        home_goals,
        away_goals,
        fetched_at,

        -- Indicator variables
        case when actual_result = 'H' then 1.0 else 0.0 end as i_home,
        case when actual_result = 'D' then 1.0 else 0.0 end as i_draw,
        case when actual_result = 'A' then 1.0 else 0.0 end as i_away

    from matched
),

with_brier as (
    select
        *,
        case
            -- Knockout (2-outcome): renormalise home+away to sum to 1, then divide by 2
            when match_type = 'knockout' and (p_home + p_away) > 0 then
                round(
                    (
                        pow(p_home / (p_home + p_away) - i_home, 2)
                        + pow(p_away / (p_home + p_away) - i_away, 2)
                    ) / 2.0,
                    4
                )
            -- Standard (3-outcome): divide by 3
            else
                round(
                    (
                        pow(p_home - i_home, 2)
                        + pow(p_draw - i_draw, 2)
                        + pow(p_away - i_away, 2)
                    ) / 3.0,
                    4
                )
        end as brier_score

    from scored
)

select
    prediction_id,
    video_id,
    publish_date,
    match_date,
    home_team,
    away_team,
    competition,
    match_type,
    leg_number,
    pred_home_win_pct,
    pred_draw_pct,
    pred_away_win_pct,
    actual_result,
    home_goals,
    away_goals,
    brier_score,
    fetched_at

from with_brier
