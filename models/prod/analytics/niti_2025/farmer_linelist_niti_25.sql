{{ config(
  materialized='table',
  tags=["analytics","analytics_niti_2025", "niti_2025", "niti"]
) }}

SELECT
    s.farmer_id,
    s.farmer_name,
    w.state,
    w.district,
    w.village,
    w.taluka,
    w.dam,
    w.ngo_name,
    s.mobile_number,
    s.mobile_verified,
    s.category_of_farmer,
    MAX(s.land_holding::NUMERIC) AS land_holding, 
    SUM(e.total_silt_carted::numeric) AS total_silt_carted,
    SUM(e.number_of_trolleys_carted::numeric) AS total_trolleys_carted,
    SUM(e.number_of_hyvasdumper_carted::numeric) AS total_hyvasdumper_carted,
    MAX(e.area_covered_by_silt::numeric) AS area_covered_by_silt,
    COALESCE(MAX(a.approval_status)) AS approval_status -- Select the approval status, assumes only approved farmers

FROM {{ ref('encounters_niti_2025') }} AS e
LEFT JOIN {{ ref('farmer_niti_2025') }} AS s
    ON e.farmer_sub_id = s.farmer_id
LEFT JOIN {{ ref('work_order_niti_2025') }} AS w
    ON e.subject_id = w.work_order_id
-- New Join: To include the approval status
LEFT JOIN {{ ref('approval_status_niti_2025') }} AS a --new
    ON s.farmer_id = a.entity_id -- new

WHERE w.work_order_voided != TRUE AND s.farmer_voided != TRUE

GROUP BY
    s.farmer_id,
    s.farmer_name,
    w.state,
    w.district,
    w.village,
    w.taluka,
    w.dam,
    w.ngo_name,
    s.mobile_number,
    s.mobile_verified,
    s.category_of_farmer,
    a.approval_status