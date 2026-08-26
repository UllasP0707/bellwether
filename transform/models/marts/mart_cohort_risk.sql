-- Risk by tenure cohort and seniority.
--
-- The question this exists for is whether onboarding works. If new joiners
-- score consistently worse than veterans in the same department, that is a
-- training problem rather than five hundred individual problems, and no
-- per-employee view will ever show it.

select
    s.tenant_id,
    s.dt,
    e.tenure_cohort,
    e.seniority,
    e.is_high_value_target,
    count(*) as employees,
    round(avg(s.score)::numeric, 2) as mean_score,
    round(
        percentile_cont(0.5) within group (order by s.score)::numeric, 2
    ) as median_score,
    count(*) filter (where s.band in ('critical', 'high')) as needs_attention
from {{ ref('stg_employee_score') }} as s
inner join {{ ref('dim_employee') }} as e
    on s.employee_id = e.employee_id and s.tenant_id = e.tenant_id
group by 1, 2, 3, 4, 5
