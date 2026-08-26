-- Scores are normalised onto 0-100 by a saturating curve, so nothing can leave
-- that range. If something has, the normalisation is broken rather than the
-- warehouse, and every band boundary downstream is meaningless.

select employee_id, dt, score
from {{ ref('stg_employee_score') }}
where score < 0 or score > 100
