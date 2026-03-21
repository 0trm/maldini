-- Monthly Brier score averages with 3-month rolling average.

with fct as (
    select * from {{ ref('fct_predictions') }}
),

monthly as (
    select
        date_trunc(publish_date, month)                    as month,
        count(*)                                           as prediction_count,
        round(avg(brier_score), 4)                         as avg_brier_score

    from fct
    group by 1
),

with_rolling as (
    select
        month,
        prediction_count,
        avg_brier_score,
        round(
            avg(avg_brier_score) over (
                order by month
                rows between 2 preceding and current row
            ),
            4
        ) as rolling_3m_avg_brier

    from monthly
)

select * from with_rolling
order by month
