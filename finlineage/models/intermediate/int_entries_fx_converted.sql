-- int_entries_fx_converted: every GL entry converted to INR (the reporting currency).
-- Joins GL entries to FX rates on the transaction date.
-- For INR entries, no conversion needed (rate = 1).
-- For foreign-currency entries, applies the FX rate effective on the entry's date.

with gl as (
    select * from {{ ref('stg_erp_gl_entries') }}
),

fx as (
    select * from {{ ref('stg_fx_rates') }}
),

-- Pad in a synthetic "INR -> INR" rate of 1 so the left-join below works uniformly
-- and we don't need conditional logic for INR entries.
fx_with_inr as (
    select
        rate_date,
        from_currency,
        to_currency,
        rate
    from fx

    union all

    select distinct
        entry_date         as rate_date,
        'INR'              as from_currency,
        'INR'              as to_currency,
        cast(1 as decimal(12,6)) as rate
    from gl
)

select
    gl.entry_id,
    gl.entry_date,
    gl.project_code,
    gl.project_name,
    gl.segment,
    gl.cost_centre,
    gl.account_code,
    gl.account_name,
    gl.account_type,
    gl.currency                                     as source_currency,
    gl.amount                                       as source_amount,
    fx.rate                                         as fx_rate,
    cast(gl.amount * fx.rate as decimal(18,2))      as amount_inr,
    'INR'                                           as reporting_currency,
    gl.description
from gl
left join fx_with_inr fx
    on gl.entry_date = fx.rate_date
   and gl.currency   = fx.from_currency