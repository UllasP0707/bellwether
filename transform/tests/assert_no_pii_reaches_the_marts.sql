-- Nothing analytics touches may carry an email address.
--
-- Staging drops the PII columns, so this should be structurally impossible --
-- which is the point. It asserts the boundary rather than trusting that every
-- future model remembers it, and it fails on the day somebody adds `select *`
-- from the source instead of from staging.

select 'dim_employee' as model, count(*) as offending_rows
from {{ ref('dim_employee') }}
where cast(dim_employee as text) like '%@%'
having count(*) > 0
