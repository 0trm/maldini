-- Cleaned match results with normalised actual_result values.

with source as (
    select * from {{ source('raw', 'match_results') }}
),

cleaned as (
    select
        result_id,
        prediction_id,
        competition,
        home_team,
        away_team,
        home_goals,
        away_goals,
        match_date,
        fetched_at,

        -- Normalise result to H / D / A
        case
            when actual_result = 'H' then 'H'
            when actual_result = 'D' then 'D'
            when actual_result = 'A' then 'A'
            when home_goals > away_goals then 'H'
            when home_goals = away_goals then 'D'
            when home_goals < away_goals then 'A'
        end as actual_result

    from source
    where prediction_id is not null
)

select * from cleaned
