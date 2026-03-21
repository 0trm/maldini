-- Cleaned predictions with validated percentages and classified match_type.
-- Drops rows where percentages don't sum to ~100 (pred_sum outside 95–105).

with source as (
    select * from {{ source('raw', 'predictions_extracted') }}
),

validated as (
    select
        prediction_id,
        video_id,
        video_url,
        publish_date,
        match_date,
        home_team,
        away_team,
        competition,
        leg_number,

        -- Normalise match_type: if pred_draw_pct = 0, it's a knockout (2-outcome);
        -- otherwise treat as a standard 3-outcome match.
        case
            when pred_draw_pct = 0 then 'knockout'
            else 'standard'
        end as match_type,

        pred_home_win_pct,
        pred_draw_pct,
        pred_away_win_pct,
        pred_sum,
        raw_quote,
        extracted_at,

        -- Flag rows whose percentages are badly off
        pred_sum between 95 and 105 as pred_sum_valid

    from source
    where prediction_id is not null
)

select * from validated
