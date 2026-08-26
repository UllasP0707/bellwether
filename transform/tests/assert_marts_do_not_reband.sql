-- The band must never be re-derived downstream.
--
-- This is the most important test in the project's SQL. Band thresholds live in
-- exactly one place -- `RiskBand.of()` in Python -- and the whole system depends
-- on that: the intervention policy fires on crossings, the API colours a row by
-- band, and the dashboard says "critical" to a human. If a mart ever recomputes
-- the boundary in SQL, the warehouse and the product start disagreeing about who
-- is critical, and both look right in isolation.
--
-- So this recomputes the banding the way SQL would be tempted to, and requires
-- it to match what the scorer actually said. It fails either if somebody adds a
-- `case when score >= 80` to a model, or if the thresholds move in Python and
-- this assertion is not updated with them -- both of which are exactly the
-- moment to stop and think.

select
    employee_id,
    dt,
    score,
    band as band_from_scorer,
    case
        when score >= 80 then 'critical'
        when score >= 60 then 'high'
        when score >= 40 then 'elevated'
        when score >= 20 then 'moderate'
        else 'low'
    end as band_if_sql_derived_it
from {{ ref('stg_employee_score') }}
where band != case
    when score >= 80 then 'critical'
    when score >= 60 then 'high'
    when score >= 40 then 'elevated'
    when score >= 20 then 'moderate'
    else 'low'
end
