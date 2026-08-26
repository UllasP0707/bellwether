-- Interventions, with the message body left behind.
--
-- The copy is written for one person to read once. It is not analytics, it can
-- contain a first name, and nothing a mart computes needs it.

select
    intervention_id,
    tenant_id,
    employee_id,
    type as intervention_type,
    channel,
    trigger_signal,
    band,
    previous_band,
    score,
    dominant_category,
    copy_source,
    created_at,
    created_at::date as dt
from {{ source('bellwether', 'intervention') }}
