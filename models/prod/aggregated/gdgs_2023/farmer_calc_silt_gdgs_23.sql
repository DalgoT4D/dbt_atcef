{{ config(
  materialized='table',
  tags=["aggregated","aggregated_gdgs_2023", "gdgs_2023", "gdgs"]
) }}

SELECT
    e.eid,
    s.farmer_id,
    w.work_order_id,
    w.state,
    w.village,
    e.machine_sub_id,
    w.district,
    w.taluka,
    w.dam,
    w.ngo_name,
    s.farmer_name,
    s.mobile_number,
    s.mobile_verified,
    s.category_of_farmer,
    w.work_order_name,
    w.silt_target,
    e.total_silt_carted,
    e.date_time
FROM {{ ref('encounters_gdgs_2023') }} AS e
LEFT JOIN {{ ref('farmer_gdgs_23') }} AS s
    ON e.farmer_sub_id = s.farmer_id
LEFT JOIN {{ ref('work_order_gdgs_23') }} AS w
    ON e.subject_id = w.work_order_id
WHERE w.work_order_voided != TRUE AND s.farmer_voided != TRUE
