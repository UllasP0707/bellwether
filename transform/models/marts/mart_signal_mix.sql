-- How much of each behaviour the company produced each day.
--
-- The series that makes a broken source visible. A connector that silently
-- stops reporting looks exactly like a population that suddenly stopped
-- misbehaving, and nothing in a per-employee view distinguishes them -- only
-- the shape of this over time does.

select
    p.tenant_id,
    p.dt,
    p.signal,
    c.category,
    c.weight,
    c.is_mitigating,
    p.events,
    p.employees,
    round((p.events::numeric / nullif(p.employees, 0)), 3) as events_per_employee
from {{ source('bellwether', 'raw_daily_population_signal') }} as p
left join {{ ref('signal_catalog') }} as c on p.signal = c.signal
