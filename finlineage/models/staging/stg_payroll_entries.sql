-- stg_payroll_entries: cleaned, typed view of payroll allocations.
-- One row per employee-project-day allocation. Bronze -> Silver.

with raw as (
    select * from {{ read_csv_source('bronze', 'payroll_entries') }}
)

select
    cast(allocation_id   as varchar)       as allocation_id,
    cast(allocation_date as date)          as allocation_date,
    cast(employee_id     as varchar)       as employee_id,
    cast(project_code    as varchar)       as project_code,
    cast(cost_centre     as varchar)       as cost_centre,
    cast(hours_logged    as decimal(8,2))  as hours_logged,
    cast(rate_per_hour   as decimal(10,2)) as rate_per_hour,
    cast(cost_inr        as decimal(18,2)) as cost_inr
from raw