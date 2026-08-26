-- The grain of stg_daily_signal is (tenant, employee, day, signal).
--
-- A fact table joined at the wrong grain double-counts, and the resulting
-- numbers stay plausible -- which is why this is asserted rather than assumed.

select tenant_id, employee_id, dt, signal, count(*) as rows_at_grain
from {{ ref('stg_daily_signal') }}
group by 1, 2, 3, 4
having count(*) > 1
