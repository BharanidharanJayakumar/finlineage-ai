-- stg_erp_gl_entries: cleaned, typed view of raw ERP GL entries.
-- One row per GL posting. Bronze -> Silver transformation.

with raw as (
    select * from {{ read_csv_source('bronze', 'erp_gl_entries') }}
)

select
    cast(entry_id      as varchar)   as entry_id,
    cast(entry_date    as date)      as entry_date,
    cast(project_code  as varchar)   as project_code,
    cast(project_name  as varchar)   as project_name,
    cast(segment       as varchar)   as segment,
    cast(cost_centre   as varchar)   as cost_centre,
    cast(account_code  as varchar)   as account_code,
    cast(account_name  as varchar)   as account_name,
    cast(account_type  as varchar)   as account_type,
    cast(currency      as varchar)   as currency,
    cast(amount        as decimal(18,2)) as amount,
    cast(description   as varchar)   as description
from raw