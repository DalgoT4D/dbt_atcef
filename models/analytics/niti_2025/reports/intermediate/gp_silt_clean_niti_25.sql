-- Joins gp_endline_niti_25 with approval_status_niti_25 and sums approved non-farm silt per work order.

{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti", "analytical_models", "reports_niti_2025", "intermediate_reports_niti_2025"]
) }}

    SELECT
    ge.endline_gp_sub_id as workorderid,
    sum(case 
    when a.approval_status = 'Approved' then COALESCE(ge.total_gp_silt_excavated_non_farm, 0) 
    else 0 end) as total_silt_excavated_by_gp_non_farm
    from {{ ref('gp_endline_niti_25') }} as ge
    INNER JOIN {{ ref('approval_status_niti_25') }} as a 
    ON ge.eid = a.entity_id 
    WHERE ge.voided = false 
    GROUP BY ge.endline_gp_sub_id
