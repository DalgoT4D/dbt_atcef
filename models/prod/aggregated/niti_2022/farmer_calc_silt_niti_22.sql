{{ config(
  materialized='table',
  tags=["aggregated","aggregated_niti_2022", "niti_2022", "niti"]
) }}

SELECT
    e.eid,
    s.farmer_id,
    w.work_order_id,
    w.state,
    w.village,
    w.district,
    w.taluka,
    e.machine_sub_id,
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
FROM {{ ref('encounter_2022') }} AS e
LEFT JOIN {{ ref('farmer_niti_22') }} AS s
    ON e.farmer_sub_id = s.farmer_id
LEFT JOIN {{ ref('work_order_niti_22') }} AS w
    ON e.subject_id = w.work_order_id
WHERE
    w.work_order_voided != TRUE
    AND s.farmer_voided != TRUE
    AND total_silt_carted::text != 'NaN'
