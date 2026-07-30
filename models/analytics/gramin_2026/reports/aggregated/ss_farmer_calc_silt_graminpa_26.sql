-- Legacy-compatible farmer detail for cross-year unions, sourced entirely
-- from the approved GraminPA 2026 analytics lineage.
{{ config(
  materialized='table',
  tags=["analytics", "analytics_gramin_2026", "ss_2026", "ss_graminpa_2026"]
) }}

SELECT
    d.eid,
    d.farmer_id,
    d.work_order_id,
    d.machine_id AS machine_sub_id,
    d.state,
    d.village,
    d.district,
    d.taluka,
    d.dam,
    d.ngo_name,
    d.farmer_name,
    fr.mobile_number,
    fr.mobile_verified_status::boolean AS mobile_verified,
    fr.farmer_category AS category_of_farmer,
    d.updated_workorder_name AS work_order_name,
    NULLIF(TRIM(d.silt_target::text), '')::numeric AS silt_target,
    d.silt_carted::numeric AS total_silt_carted,
    d.date_time
FROM {{ ref('daily_farmer_linelist_graminpa_26') }} AS d
LEFT JOIN {{ ref('farmer_regn_graminpa_26') }} AS fr
    ON d.farmer_id = fr.subject_id
