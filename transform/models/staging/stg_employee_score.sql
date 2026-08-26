-- Batch scores as computed by Spark, unchanged.
--
-- No banding, no rounding, no recomputation. The band is carried through from
-- the scorer rather than derived from the score here, and a test enforces that
-- the two never disagree -- see tests/assert_marts_do_not_reband.sql.

select
    tenant_id,
    employee_id,
    dt,
    score,
    band,
    dominant_category,
    events_considered,
    as_of
from {{ source('bellwether', 'raw_employee_score') }}
