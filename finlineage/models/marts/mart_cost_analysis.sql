-- mart_cost_analysis: Cost breakdown by period, segment, cost centre, and type.
-- Powers the Power BI Cost Analysis view.

{{ config(materialized='table') }}

with entries as (
    select * from {{ ref('int_entries_cost_centre_mapped') }}
    where account_type in ('COGS', 'OpEx')
),

monthly as (
    select
        date_trunc('month', entry_date)::date   as period_month,
        segment_governed                         as segment,
        cost_centre,
        cost_centre_name,
        cost_centre_location,
        cost_centre_country,
        account_type,
        account_code,
        account_name,
        sum(abs(amount_inr))                     as cost_inr,
        count(*)                                 as entry_count
    from entries
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9
)

select
    m.*,
    lag(m.cost_inr) over (
        partition by m.segment, m.cost_centre, m.account_code
        order by m.period_month
    )                                            as prev_month_cost,
    case
        when lag(m.cost_inr) over (
            partition by m.segment, m.cost_centre, m.account_code
            order by m.period_month
        ) is null or lag(m.cost_inr) over (
            partition by m.segment, m.cost_centre, m.account_code
            order by m.period_month
        ) = 0 then null
        else round(
            (m.cost_inr - lag(m.cost_inr) over (
                partition by m.segment, m.cost_centre, m.account_code
                order by m.period_month
            )) / lag(m.cost_inr) over (
                partition by m.segment, m.cost_centre, m.account_code
                order by m.period_month
            ) * 100, 2
        )
    end                                          as mom_change_pct
from monthly m
order by period_month, segment, cost_centre, account_type