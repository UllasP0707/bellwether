-- Daily per-employee signal counts, joined to the catalog's weighting.
--
-- `weight` and `category` come from a seed generated out of the Python catalog
-- rather than being retyped here. A second copy of the scoring model in SQL is
-- exactly the duplication the whole project is organised to avoid, and it would
-- rot the first time somebody rebalanced a weight.

select
    s.tenant_id,
    s.employee_id,
    s.dt,
    s.signal,
    s.events,
    s.first_at,
    s.last_at,
    c.category,
    c.weight,
    c.half_life_days,
    c.is_mitigating
from {{ source('bellwether', 'raw_daily_employee_signal') }} as s
left join {{ ref('signal_catalog') }} as c on s.signal = c.signal
