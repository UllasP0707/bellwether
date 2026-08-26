-- Every signal reaching the warehouse must exist in the catalog seed.
--
-- An unpriced signal contributes zero to a score while looking like ordinary
-- data, which is invisible in production and exactly what `spec_for()` refuses
-- to allow in Python. The warehouse should refuse it too.

select distinct s.signal
from {{ source('bellwether', 'raw_daily_employee_signal') }} as s
left join {{ ref('signal_catalog') }} as c on s.signal = c.signal
where c.signal is null
