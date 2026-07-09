-- stg_fx_rates: cleaned, typed view of daily FX rates.
-- One row per (rate_date, from_currency). All conversions target INR.

with raw as (
    select * from {{ read_csv_source('bronze', 'fx_rates') }}
)

select
    cast(rate_date     as date)           as rate_date,
    cast(from_currency as varchar)        as from_currency,
    cast(to_currency   as varchar)        as to_currency,
    cast(rate          as decimal(12,6))  as rate
from raw