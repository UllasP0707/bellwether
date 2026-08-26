-- The employee dimension for analytics. Pseudonymous by construction.

select * from {{ ref('stg_employee') }}
