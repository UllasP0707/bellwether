-- Risk by department, from the batch scores.
--
-- The API serves this shape too, computed live over Redis, and the difference
-- is the point: that one answers "right now" for a few hundred people, this one
-- answers "over any period" for any number. Serving trend from an online store
-- means scanning it, and an online store that gets scanned stops being fast.
--
-- Headcount comes from the dimension, not from the scores. Counting only scored
-- employees would make a department nobody has data on look like a safe one.

with scored as (

    select
        s.tenant_id,
        s.dt,
        e.department,
        s.employee_id,
        s.score,
        s.band
    from {{ ref('stg_employee_score') }} as s
    inner join {{ ref('dim_employee') }} as e
        on s.employee_id = e.employee_id and s.tenant_id = e.tenant_id

),

headcount as (

    select tenant_id, department, count(*) as headcount
    from {{ ref('dim_employee') }}
    group by 1, 2

)

select
    s.tenant_id,
    s.dt,
    s.department,
    h.headcount,
    count(*) as scored,
    round(avg(s.score)::numeric, 2) as mean_score,
    round(
        percentile_cont(0.9) within group (order by s.score)::numeric, 2
    ) as p90_score,
    max(s.score) as max_score,
    count(*) filter (where s.band = 'critical') as critical,
    count(*) filter (where s.band = 'high') as high,
    count(*) filter (where s.band in ('critical', 'high')) as needs_attention
from scored as s
inner join headcount as h
    on s.tenant_id = h.tenant_id and s.department = h.department
group by 1, 2, 3, 4
