{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025", "intermediate_reports_niti_2025"]
) }}

-- Workorder endline silt details
 (
SELECT
we.workorder_id,
we.total_silt_excavated,
cast(we.endline_date as TIMESTAMP) as workorder_endline_date
from {{ ref('workorder_endline_linelist_niti_25') }} as we 
)