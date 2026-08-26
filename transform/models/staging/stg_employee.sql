-- The dimension, with PII left behind.
--
-- Everything downstream of here is analytics, and analytics does not need to
-- know anybody's name. Dropping email, display name and manager at the staging
-- boundary means no mart can leak them by accident and no BI tool built on
-- these models can either -- the columns simply are not there to select.

select
    employee_id,
    tenant_id,
    department,
    seniority,
    location,
    tenure_days,
    has_admin_access,
    handles_financial_data,
    is_executive,
    (is_executive or handles_financial_data or has_admin_access) as is_high_value_target,
    case
        when tenure_days < 90 then 'new'
        when tenure_days < 365 then 'established'
        when tenure_days < 1095 then 'experienced'
        else 'veteran'
    end as tenure_cohort
from {{ source('bellwether', 'employee') }}
