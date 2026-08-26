-- One row per employee per day: what they did, and what it was worth.
--
-- Grain is in the name. A fact table whose grain is ambiguous gets joined to
-- something at a different grain, and the resulting double count is the single
-- most common way an aggregate becomes quietly wrong.
--
-- `raw_contribution` is the day's undecayed weighted sum. It is deliberately
-- **not** a risk score: scores are a saturating function of a decayed 30-day
-- window, so a score for Monday is not something that can be added to a score
-- for Tuesday. Keeping the additive quantity here and leaving the score to the
-- scorer is what makes `sum()` over any slice of time correct.

with daily as (

    select
        tenant_id,
        employee_id,
        dt,
        sum(events) as events,
        count(distinct signal) as distinct_signals,
        sum(case when not is_mitigating then events else 0 end) as aggravating_events,
        sum(case when is_mitigating then events else 0 end) as mitigating_events,
        sum(events * weight) as raw_contribution,
        max(case when not is_mitigating then weight else null end) as worst_signal_weight
    from {{ ref('stg_daily_signal') }}
    group by 1, 2, 3

)

select
    d.tenant_id,
    d.employee_id,
    d.dt,
    e.department,
    e.seniority,
    e.tenure_cohort,
    e.is_high_value_target,
    d.events,
    d.distinct_signals,
    d.aggravating_events,
    d.mitigating_events,
    round(d.raw_contribution::numeric, 3) as raw_contribution,
    d.worst_signal_weight
from daily as d
inner join {{ ref('dim_employee') }} as e
    on d.employee_id = e.employee_id and d.tenant_id = e.tenant_id
