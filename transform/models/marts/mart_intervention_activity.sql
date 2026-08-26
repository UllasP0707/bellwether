-- What the system actually said to people, by day and rung.
--
-- The number a security team should watch hardest is not how many messages went
-- out but what fraction came from the model versus the templates. A silent
-- collapse to templates means generation is failing or the guardrails are
-- rejecting everything, and either way nobody would notice from the messages
-- themselves -- they would just get blander.

select
    tenant_id,
    dt,
    intervention_type,
    channel,
    count(*) as sent,
    count(distinct employee_id) as employees,
    count(*) filter (where copy_source = 'model') as model_written,
    count(*) filter (where copy_source = 'template') as template_written,
    count(*) filter (where trigger_signal is not null) as signal_triggered,
    round(avg(score)::numeric, 2) as mean_score_at_send
from {{ ref('stg_intervention') }}
group by 1, 2, 3, 4
