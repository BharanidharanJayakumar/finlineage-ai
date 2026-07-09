-- int_entries_cost_centre_mapped: enrich every FX-converted entry with
-- governed cost-centre and project metadata from the dimensional reference tables.
-- This model is where referential integrity is enforced — every entry must map
-- to a known cost centre AND a known project, or it's surfaced as a violation.

with entries as (
    select * from {{ ref('int_entries_fx_converted') }}
),

cost_centres as (
    select * from {{ ref('dim_cost_centres') }}
),

projects as (
    select * from {{ ref('dim_projects') }}
)

select
    e.entry_id,
    e.entry_date,

    -- Project enrichment (from dim, governed)
    e.project_code,
    p.project_name             as project_name_governed,
    p.segment                  as segment_governed,
    p.client_name,
    p.billing_currency,

    -- Cost centre enrichment (from dim, governed)
    e.cost_centre,
    cc.cost_centre_name,
    cc.location                as cost_centre_location,
    cc.country                 as cost_centre_country,

    -- Account info passes through
    e.account_code,
    e.account_name,
    e.account_type,

    -- FX-converted financial amounts pass through
    e.source_currency,
    e.source_amount,
    e.fx_rate,
    e.amount_inr,
    e.reporting_currency,

    -- Referential-integrity flags (surfaced for downstream QA, not as join filters)
    case when p.project_code  is null then true else false end as is_orphan_project,
    case when cc.cost_centre  is null then true else false end as is_orphan_cost_centre,

    e.description
from entries e
left join projects p
    on e.project_code = p.project_code
left join cost_centres cc
    on e.cost_centre = cc.cost_centre